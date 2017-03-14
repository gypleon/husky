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

#include "io/output/redis_outputformat.hpp"

#include "base/log.hpp"

namespace husky {
namespace io {

using base::log_msg;

// TODO: perhaps could be dynamically tuned
const int kMaxNumberOfRecordBytes = 10240;

RedisOutputFormat::RedisOutputFormat() {
    records_map_.clear();
}

RedisOutputFormat::~RedisOutputFormat() { records_map_.clear(); }

void RedisOutputFormat::set_server(const std::string ip, const std::string port) { 
    ip_ = ip; 
    port_ = atoi(port.c_str()); 
    ask_masters_info();
    create_redis_con_pool();
}

void RedisOutputFormat::ask_masters_info() {
    WorkerInfo worker_info = husky::Context::get_worker_info();
    int request_code = worker_info.get_largest_tid() + REQ_REDIS_MASTERS_INFO;
    BinStream question;
    question << request_code;
    BinStream answer = husky::Context::get_coordinator()->ask_master(question, husky::TYPE_REDIS_REQ);
    answer >> splits_;
}

void RedisOutputFormat::create_redis_con_pool() {
    redisReply *reply = NULL;
    for ( auto& split: splits_ ) {
        redisContext * c = redisConnectWithTimeout( split.second.get_ip().c_str(), split.second.get_port(), timeout_);
        if (NULL == c || c->err) {
            if (c){
                LOG_I << "Connection error: " + std::string(c->errstr);
                redisFree(c);
            } else {
                LOG_I << "Connection error: can't allocate redis context";
            }
            return;
        }
        if (need_auth_) {
            reply = redisCmd(c, "AUTH %s", password_.c_str());
        }
        cons_[split.second.get_id()] = c;
    }
    freeReplyObject(reply);
}

void RedisOutputFormat::set_auth(const std::string& password) {
    password_ = password;
    need_auth_ = true;
}

bool RedisOutputFormat::commit(const std::string& key, const std::string& result_string) {
    if (result_string.empty())
        return false;

    const RedisOutputFormat::DataType data_type = RedisOutputFormat::DataType::RedisString;
    BinStream result_stream;
    result_stream << result_string;
    const std::string result_stream_buffer = result_stream.to_string();
    std::pair<DataType, std::string> result_type_buffer(data_type, result_stream_buffer);

    records_map_[key] = result_type_buffer;
    records_bytes_ += result_stream_buffer.length();

    if (records_bytes_ >= kMaxNumberOfRecordBytes)
    {
        flush_all();
        return true;
    }
}

template <class DataT>
bool RedisOutputFormat::commit(const std::string& key, const std::vector<DataT>& result_list) {
    if (result_list.empty())
        return false;

    RedisOutputFormat::DataType data_type = RedisOutputFormat::DataType::RedisList;
    BinStream result_stream;
    char inner_data_type = get_template_type(result_list[0]);
    result_stream << inner_data_type;
    result_stream << result_list;
    const std::string result_stream_buffer = result_stream.to_string();
    std::pair<DataType, std::string> result_type_buffer(data_type, result_stream_buffer);

    records_map_[key] = result_type_buffer;
    // TODO: get stream's length
    records_bytes_ += result_stream_buffer.length();

    if (records_bytes_ >= kMaxNumberOfRecordBytes)
    {
        flush_all();
        return true;
    }
}

template <class DataT>
bool RedisOutputFormat::commit(const std::string& key, const std::map<std::string, DataT>& result_hash) {
    if (result_hash.empty())
        return false;

    RedisOutputFormat::DataType data_type = RedisOutputFormat::DataType::RedisHash;
    BinStream result_stream;
    char inner_data_type = get_template_type(result_hash.begin().second);
    result_stream << inner_data_type;
    result_stream << result_hash;
    const std::string result_stream_buffer = result_stream.to_string();
    std::pair<DataType, std::string> result_type_buffer(data_type, result_stream_buffer);

    records_map_[key] = result_type_buffer;
    // TODO: get stream's length
    records_bytes_ += result_stream_buffer.length();

    if (records_bytes_ >= kMaxNumberOfRecordBytes)
    {
        flush_all();
        return true;
    }
}

void RedisOutputFormat::flush_all() {
    if (records_map_.empty())
        return;

    redisContext *c = NULL;
    redisReply *reply = NULL;

    for ( auto& record : records_map_){
        std::string key = record.first;
        RedisOutputFormat::DataType data_type = record.second.first;
        BinStream result_stream;
        result_stream << record.second.second;

        uint16_t target_slot = gen_slot_crc16(key.c_str(), key.length());
        // TODO: according to cached RedisMaster info
        for (auto& redis_master : splits_) {
            if (target_slot >= redis_master.second.get_sstart() && target_slot <= redis_master.second.get_send()) {
                c = cons_[redis_master.second.get_id()];
                break;
            }
        }
        
        switch (data_type) {
            case RedisOutputFormat::DataType::RedisString:
                {
                    std::string result_string;
                    result_stream >> result_string;
                    redisCmd(c, "SET %s %s", key, result_string);
                }
                break;
            case RedisOutputFormat::DataType::RedisList:
                {
                    char inner_data_type = RedisOutputFormat::InnerDataType::Other;
                    result_stream >> inner_data_type;
                    switch (inner_data_type) {
                        case RedisOutputFormat::InnerDataType::Char:
                            {
                                std::vector<char> result_list;
                                result_stream >> result_list;
                                for ( auto& result : result_list ) {
                                    redisCmd(c, "LPUSH %s %c", key, result);
                                }
                            }
                        case RedisOutputFormat::InnerDataType::Short: 
                        case RedisOutputFormat::InnerDataType::Int: 
                        case RedisOutputFormat::InnerDataType::Long:
                            {
                                std::vector<long int> result_list;
                                result_stream >> result_list;
                                for ( auto& result : result_list ) {
                                    redisCmd(c, "LPUSH %s %d", key, result);
                                }
                            }
                            break;
                        case RedisOutputFormat::InnerDataType::Bool:
                            {
                                std::vector<bool> result_list;
                                result_stream >> result_list;
                                for ( auto result : result_list ) {
                                    redisCmd(c, "LPUSH %s %s", key, result ? "true" : "false");
                                }
                            }
                            break;
                        case RedisOutputFormat::InnerDataType::Float:
                            {
                                std::vector<float> result_list;
                                result_stream >> result_list;
                                for ( auto& result : result_list ) {
                                    redisCmd(c, "LPUSH %s %f", key, result);
                                }
                            }
                            break;
                        case RedisOutputFormat::InnerDataType::Double:
                            {
                                std::vector<double> result_list;
                                result_stream >> result_list;
                                for ( auto& result : result_list ) {
                                    redisCmd(c, "LPUSH %s %lf", key, result);
                                }
                            }
                            break;
                        case RedisOutputFormat::InnerDataType::String:
                            {
                                std::vector<std::string> result_list;
                                result_stream >> result_list;
                                for ( auto& result : result_list ) {
                                    redisCmd(c, "LPUSH %s %s", key, result);
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
                    char inner_data_type = RedisOutputFormat::InnerDataType::Other;
                    result_stream >> inner_data_type;
                    switch (inner_data_type) {
                        case RedisOutputFormat::InnerDataType::Char:
                            {
                                std::map<RedisOutputFormat::DataType, char> result_map;
                                result_stream >> result_map;
                                for ( auto& result : result_map) {
                                    redisCmd(c, "HSET %s %s %c", key, result.first, result.second);
                                }
                            }
                        case RedisOutputFormat::InnerDataType::Short: 
                        case RedisOutputFormat::InnerDataType::Int: 
                        case RedisOutputFormat::InnerDataType::Long:
                            {
                                std::map<RedisOutputFormat::DataType, long int> result_map;
                                result_stream >> result_map;
                                for ( auto& result : result_map) {
                                    redisCmd(c, "HSET %s %s %d", key, result.first, result.second);
                                }
                            }
                            break;
                        case RedisOutputFormat::InnerDataType::Bool:
                            {
                                std::map<RedisOutputFormat::DataType, bool> result_map;
                                result_stream >> result_map;
                                for ( auto& result : result_map) {
                                    redisCmd(c, "HSET %s %s %s", key, result.first, result.second ? "true" : "false");
                                }
                            }
                            break;
                        case RedisOutputFormat::InnerDataType::Float:
                            {
                                std::map<RedisOutputFormat::DataType, float> result_map;
                                result_stream >> result_map;
                                for ( auto& result : result_map) {
                                    redisCmd(c, "HSET %s %s %f", key, result.first, result.second);
                                }
                            }
                            break;
                        case RedisOutputFormat::InnerDataType::Double:
                            {
                                std::map<RedisOutputFormat::DataType, double> result_map;
                                result_stream >> result_map;
                                for ( auto& result : result_map) {
                                    redisCmd(c, "HSET %s %s %lf", key, result.first, result.second);
                                }
                            }
                            break;
                        case RedisOutputFormat::InnerDataType::String:
                            {
                                std::map<RedisOutputFormat::DataType, std::string> result_map;
                                result_stream >> result_map;
                                for ( auto& result : result_map) {
                                    redisCmd(c, "HSET %s %s %s", key, result.first, result.second);
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
                break;
        }
    }

    freeReplyObject(reply);
    redisFree(c);

    records_map_.clear();
    records_bytes_ = 0;
}

uint16_t RedisOutputFormat::gen_slot_crc16(const char *buf, int len) {
    int counter;
    uint16_t crc = 0;
    for (counter = 0; counter < len; counter++)
        crc = (crc<<8) ^ crc16tab_[((crc>>8) ^ *buf++)&0x00FF];
    return crc % 16384;
}

template<class DataT>
char RedisOutputFormat::get_template_type(DataT sample) {
    const char * sample_type = typeid(sample).name();
    char test_char;
    short int test_short;
    int test_int;
    long int test_long;
    bool test_bool;
    float test_float;
    double test_double;
    std::string test_string;
    if (!strcmp(typeid(test_char).name(), sample_type)) {
        return RedisOutputFormat::InnerDataType::Char;
    } else if (!strcmp(typeid(test_short).name(), sample_type)) {
        return RedisOutputFormat::InnerDataType::Short;
    } else if (!strcmp(typeid(test_int).name(), sample_type)) {
        return RedisOutputFormat::InnerDataType::Int;
    } else if (!strcmp(typeid(test_long).name(), sample_type)) {
        return RedisOutputFormat::InnerDataType::Long;
    } else if (!strcmp(typeid(test_bool).name(), sample_type)) {
        return RedisOutputFormat::InnerDataType::Bool;
    } else if (!strcmp(typeid(test_float).name(), sample_type)) {
        return RedisOutputFormat::InnerDataType::Float;
    } else if (!strcmp(typeid(test_double).name(), sample_type)) {
        return RedisOutputFormat::InnerDataType::Double;
    } else if (!strcmp(typeid(test_string).name(), sample_type)) {
        return RedisOutputFormat::InnerDataType::String;
    } else {
        return RedisOutputFormat::InnerDataType::Other;
    }
}

}  // namespace io
}  // namespace husky
