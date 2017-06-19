// Copyright 2016 Husky Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <string.h>
#include <netdb.h>
#include <sys/param.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <thread>

#include "io/output/redis_outputformat.hpp"

#include "base/log.hpp"
#include "core/network.hpp"

// for experiments
#include <ctime>
#include <chrono>

#define CHECKREPLY(X) if ( !X || X->type == REDIS_REPLY_ERROR ) { LOG_E << "Error"; exit(-1); }

namespace husky {
namespace io {

using base::log_msg;

enum RedisOutputFormatSetUp {
    NotSetUp = 0,
    ServerSetUp = 1 << 2,
    AuthSetUp = 1 << 2,
    AllSetUp = ServerSetUp | AuthSetUp,
};

RedisOutputFormat::RedisOutputFormat(int number_clients, int flush_buffer_size): number_clients_(number_clients), flush_buffer_size_(flush_buffer_size) {
    records_map_.clear();
    cons_.clear();
    splits_.clear();
}

RedisOutputFormat::~RedisOutputFormat() { 
    records_map_.clear(); 
    for (auto& con : cons_) {
        if (con.second.first) {
            redisFree(con.second.first);
            con.second.first = NULL;
        }
    }
    cons_.clear();
    splits_.clear();
    sorted_split_group_name_.clear();
}

void RedisOutputFormat::set_server() { 
    ask_redis_masters_info();
    create_redis_con_pool();
    is_setup_ |= RedisOutputFormatSetUp::ServerSetUp;
}

void RedisOutputFormat::set_auth(const std::string& password) {
    password_ = password;
    need_auth_ = true;
    is_setup_ |= RedisOutputFormatSetUp::AuthSetUp;
}

bool RedisOutputFormat::is_setup() const { 
    return !(is_setup_ ^ RedisOutputFormatSetUp::AllSetUp);
}

void RedisOutputFormat::ask_redis_masters_info() {
    BinStream question;
    question << 0;
    BinStream answer = husky::Context::get_coordinator()->ask_master(question, husky::TYPE_REDIS_QRY_REQ);
    answer >> splits_;

    num_split_groups_ = splits_.size();

    sorted_split_group_name_.clear();
    for (auto& split_group : splits_) {
        sorted_split_group_name_.push_back(split_group.first);
    }
    std::sort(sorted_split_group_name_.begin(), sorted_split_group_name_.end(), 
            [&](std::string& a, std::string& b){
            return splits_[a].get_sstart() < splits_[b].get_sstart();
            });
    num_slots_per_group_ = 16384 / num_split_groups_;
}

std::string RedisOutputFormat::parse_host(const std::string& hostname) {
    hostent * record = gethostbyname(hostname.c_str());
    if(record == NULL){
        LOG_E << "Hostname parse failed:" << hostname;
        return "failed";
    }
    in_addr * address = (in_addr*)record->h_addr;
    std::string ip_address = inet_ntoa(*address);
    return ip_address;
}

void RedisOutputFormat::create_redis_con_pool() {
    redisReply *reply = NULL;
    for (auto& split: splits_) {
        std::string proc_ip = parse_host(get_hostname());
        redisContext * c = NULL;
        if (!split.second.get_ip().compare(proc_ip)) {
            std::string sock_file_path = "/tmp/redis_";
            sock_file_path += std::to_string(split.second.get_port()) + ".sock";
            c = redisConnectUnixWithTimeout(sock_file_path.c_str(), timeout_);
        } else {
            c = redisConnectWithTimeout(split.second.get_ip().c_str(), split.second.get_port(), timeout_);
        }
        if (NULL == c || c->err) {
            if (c){
                LOG_E << "Connection error: " + std::string(c->errstr);
                redisFree(c);
            } else {
                LOG_E << "Connection error: can't allocate redis context";
            }
            return;
        }
        if (need_auth_) {
            reply = redisCmd(c, "AUTH %s", password_.c_str());
            CHECKREPLY(reply);
        }
        std::pair<redisContext *, int> con(c, 0);
        cons_[split.second.get_id()] = con;
    }
    if (reply) {
        freeReplyObject(reply);
    }
}

int RedisOutputFormat::flush_all() {
    int buffer_size = records_bytes_;

    if (records_map_.empty())
        return -1;

    redisContext * c = NULL;
    int * c_count = 0;
    redisReply *reply = NULL;

    for (auto& record : records_map_) {
        std::string key = record.first;
        RedisOutputFormat::DataType data_type = record.second.first;
        BinStream result_stream = record.second.second;

        uint16_t target_slot = gen_slot_crc16(key.c_str(), key.length());
        int master_i = target_slot / num_slots_per_group_;
        master_i = master_i > num_split_groups_-1 ? --master_i : master_i;
        std::string master_id = sorted_split_group_name_[master_i];
        if (target_slot < splits_[master_id].get_sstart()) {
            master_i--;
        } else if ( target_slot > splits_[master_id].get_send() ) {
            master_i++;
        }
        master_id = sorted_split_group_name_[master_i];
        c = cons_[master_id].first;
        c_count = &cons_[master_id].second;

        // husky::LOG_I << key.c_str() << " " << target_slot << " " << splits_[master_id].get_ip() << ":" << splits_[master_id].get_port();
        
        switch (data_type) {
            case RedisOutputFormat::DataType::RedisString:
                {
                    std::string result_string;
                    result_stream >> result_string;
                    redisAppendCommand(c, "SET %b %b", key.c_str(), (size_t) key.length(), result_string.c_str(), (size_t) result_string.length());
                    ++(*c_count);
                }
                break;
            case RedisOutputFormat::DataType::RedisList:
                {
                    int inner_data_type = RedisOutputFormat::InnerDataType::Other;
                    result_stream >> inner_data_type;
                    switch (inner_data_type) {
                        case RedisOutputFormat::InnerDataType::Char:
                            {
                                std::vector<char> result_list;
                                result_stream >> result_list;
                                for (auto& result : result_list) {
                                    redisCmd(c, "LPUSH %s %c", key.c_str(), result);
                                }
                            }
                        case RedisOutputFormat::InnerDataType::Short: 
                        case RedisOutputFormat::InnerDataType::Int: 
                        case RedisOutputFormat::InnerDataType::Long:
                            {
                                std::vector<long int> result_list;
                                result_stream >> result_list;
                                for (auto& result : result_list) {
                                    redisCmd(c, "LPUSH %s %d", key.c_str(), result);
                                }
                            }
                            break;
                        case RedisOutputFormat::InnerDataType::Bool:
                            {
                                std::vector<bool> result_list;
                                result_stream >> result_list;
                                for (auto result : result_list) {
                                    redisCmd(c, "LPUSH %s %s", key.c_str(), result ? "true" : "false");
                                }
                            }
                            break;
                        case RedisOutputFormat::InnerDataType::Float:
                            {
                                std::vector<float> result_list;
                                result_stream >> result_list;
                                for (auto& result : result_list) {
                                    redisCmd(c, "LPUSH %s %f", key.c_str(), result);
                                }
                            }
                            break;
                        case RedisOutputFormat::InnerDataType::Double:
                            {
                                std::vector<double> result_list;
                                result_stream >> result_list;
                                for (auto& result : result_list) {
                                    redisCmd(c, "LPUSH %s %lf", key.c_str(), result);
                                }
                            }
                            break;
                        case RedisOutputFormat::InnerDataType::String:
                            {
                                std::vector<std::string> result_list;
                                result_stream >> result_list;
                                for (auto& result : result_list) {
                                    redisCmd(c, "LPUSH %s %s", key.c_str(), result.c_str());
                                }
                            }
                            break;
                        default:
                            LOG_E << "undefined inner data type of vector";
                            break;
                    }
                }
                break;
            case RedisOutputFormat::DataType::RedisHash:
                {
                    int inner_data_type = RedisOutputFormat::InnerDataType::Other;
                    result_stream >> inner_data_type;
                    switch (inner_data_type) {
                        case RedisOutputFormat::InnerDataType::Char:
                            {
                                std::map<std::string, char> result_map;
                                result_stream >> result_map;
                                for (auto& result : result_map) {
                                    redisCmd(c, "HSET %s %s %c", key.c_str(), result.first.c_str(), result.second);
                                }
                            }
                        case RedisOutputFormat::InnerDataType::Short: 
                        case RedisOutputFormat::InnerDataType::Int: 
                        case RedisOutputFormat::InnerDataType::Long:
                            {
                                std::map<std::string, long int> result_map;
                                result_stream >> result_map;
                                for (auto& result : result_map) {
                                    redisCmd(c, "HSET %s %s %d", key.c_str(), result.first.c_str(), result.second);
                                }
                            }
                            break;
                        case RedisOutputFormat::InnerDataType::Bool:
                            {
                                std::map<std::string, bool> result_map;
                                result_stream >> result_map;
                                for (auto& result : result_map) {
                                    redisCmd(c, "HSET %s %s %s", key.c_str(), result.first.c_str(), result.second ? "true" : "false");
                                }
                            }
                            break;
                        case RedisOutputFormat::InnerDataType::Float:
                            {
                                std::map<std::string, float> result_map;
                                result_stream >> result_map;
                                for (auto& result : result_map) {
                                    redisCmd(c, "HSET %s %s %f", key.c_str(), result.first.c_str(), result.second);
                                }
                            }
                            break;
                        case RedisOutputFormat::InnerDataType::Double:
                            {
                                std::map<std::string, double> result_map;
                                result_stream >> result_map;
                                for (auto& result : result_map) {
                                    redisCmd(c, "HSET %s %s %lf", key.c_str(), result.first.c_str(), result.second);
                                }
                            }
                            break;
                        case RedisOutputFormat::InnerDataType::String:
                            {
                                std::map<std::string, std::string> result_map;
                                result_stream >> result_map;
                                for (auto& result : result_map) {
                                    redisCmd(c, "HSET %s %s %s", key.c_str(), result.first.c_str(), result.second.c_str());
                                }
                            }
                            break;
                        default:
                            LOG_E << "undefined inner data type of map";
                            break;
                    }
                }
                break;
            case RedisOutputFormat::DataType::RedisSet:
                // redisCmd(c, "SADD %s %s", );
                break;
            case RedisOutputFormat::DataType::RedisZSet:
                // with score, batch available
                // redisCmd("ZADD %s %d %s", );
                break;
            default:
                LOG_E << "undefined data structure";
                break;
        }
    }


    for (auto& con : cons_) {
        while (con.second.second-- > 0) {
            int r = redisGetReply(con.second.first, (void **) &reply );
            if (r == REDIS_ERR) { 
                LOG_E << "REDIS_ERR"; 
                exit(-1); 
            }
            // CHECKREPLY(reply);
            if (!reply) {
                LOG_E << "NULL REPLY"; 
            } else if (reply->type == REDIS_REPLY_ERROR) { 
                LOG_E << "REDIS_REPLY_ERROR -> " << reply->str; 
                LOG_E << "pipeline remained -> " << con.second.second;
                ask_redis_masters_info();
            }
        }
        con.second.second = con.second.second < 0 ? 0 : con.second.second;
    }


    if (reply) {
        freeReplyObject(reply);
        reply = NULL;
    }

    records_map_.clear();
    records_bytes_ = 0;
    
    return buffer_size;
}

uint16_t RedisOutputFormat::gen_slot_crc16(const char *buf, int len) {
    int counter;
    uint16_t crc = 0;
    for (counter = 0; counter < len; counter++)
        crc = (crc<<8) ^ crc16tab_[((crc>>8) ^ *buf++)&0x00FF];
    return crc % 16384;
}

}  // namespace io
}  // namespace husky
