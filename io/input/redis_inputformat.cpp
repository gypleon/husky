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

#include "io/input/redis_inputformat.hpp"

#include <string>
#include <vector>
#include <sstream>
#include <cstring>
#include <utility>
#include <map>

#include <string.h>
#include <netdb.h>
#include <sys/param.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "base/serialization.hpp"
#include "core/constants.hpp"
#include "core/context.hpp"
#include "core/coordinator.hpp"
#include "core/network.hpp"

#include "hiredis/hiredis.h"
#include "boost/property_tree/ptree.hpp"
#include "boost/property_tree/json_parser.hpp"

namespace husky {
namespace io {

namespace pt = boost::property_tree;

enum RedisInputFormatSetUp {
    NotSetUp = 0,
    ServerSetUp = 1 << 2,
    AuthSetUp = 1 << 2,
    AllSetUp = ServerSetUp | AuthSetUp,
};

RedisInputFormat::RedisInputFormat() {
    is_setup_ = RedisInputFormatSetUp::NotSetUp;
}

RedisInputFormat::~RedisInputFormat() { 
    records_vector_.clear(); 
    best_keys_.clear();
    for ( auto& con : cons_ ) {
        redisFree(con.second);
    }
    cons_.clear();
    split_i_id_.clear();
}

bool RedisInputFormat::is_setup() const { 
    return !(is_setup_ ^ RedisInputFormatSetUp::AllSetUp); 
}

void RedisInputFormat::set_server() {
    ask_redis_splits_info();
    create_redis_cons();
    is_setup_ |= RedisInputFormatSetUp::ServerSetUp;
}

void RedisInputFormat::set_auth(const std::string& password) {
    need_auth_ = true;
    password_ = password;
    is_setup_ |= RedisInputFormatSetUp::AuthSetUp;
}

void RedisInputFormat::reset_auth() {
    need_auth_ = false;
}

void RedisInputFormat::ask_redis_splits_info() {
    BinStream question;
    question << 1;
    BinStream answer = husky::Context::get_coordinator()->ask_master(question, husky::TYPE_REDIS_QRY_REQ);
    answer >> splits_;

    split_i_id_.clear();
    for (int split_i=0; split_i<splits_.size(); split_i++) {
        split_i_id_.push_back("");
        for (auto& split : splits_) {
            if (split.second.get_sn() == split_i) {
                split_i_id_[split_i] = split.first;
                break;
            }
        }
    }
}

void RedisInputFormat::create_redis_cons() {
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
            CHECK(reply);
        }
        cons_[split.second.get_id()] = c;
    }
    if (reply) {
        freeReplyObject(reply);
    }
}

// ask master for a set of best keys, with their location 
int RedisInputFormat::ask_best_keys() {
    BinStream question;
    question << husky::Context::get_global_tid();
    BinStream answer = husky::Context::get_coordinator()->ask_master(question, husky::TYPE_REDIS_REQ);
    int task_status;

    answer >> task_status;
    answer >> best_keys_;

    return task_status;
}

std::string RedisInputFormat::parse_host(const std::string& hostname) {
    hostent * record = gethostbyname(hostname.c_str());
    if(record == NULL){
        LOG_E << "hostname parse failed: " << hostname;
        return "failed";
    }
    in_addr * address = (in_addr * )record->h_addr;
    std::string ip_address = inet_ntoa(*address);
    return ip_address;
}

// connect Redis split to retrieve RECORDS
void RedisInputFormat::fetch_split_records(int split_i, const std::vector<RedisRangeKey>& keys) {

    std::string split_id = split_i_id_[split_i];
    RedisSplit split = splits_[split_id];
    redisContext * c = cons_[split_id];

    if (!split.is_valid()) {
        LOG_E << "Redis split invalid: " << split.get_id();
        return;
    }

    // slave read-only, master-slaves load balance
    if (split.get_master().compare("-")) {
        redisReply * reply;
        reply = redisCmd(c, "READONLY");
        if ( strcmp(reply->str, "OK") ) {
            LOG_E << "Slave failed to start up read-only";
            freeReplyObject(reply);
            return;
        }
        freeReplyObject(reply);
    }

    for (auto& key : keys) {
        if (!key.str_.compare("")) {
            LOG_E << "empty key <- " << split.get_ip() << ":" << split.get_port();
            continue;
        }
        redisReply * cur_data = redisCmd(c, "TYPE %s", key.str_.c_str());
        if (!strcmp(cur_data->str, "string")) {
            pt::ptree string_js;
            cur_data = redisCmd(c, "GET %s", key.str_.c_str());
            std::string value(cur_data->str);
            string_js.put(key.str_, value); 
            std::stringstream jsonvalue;
            pt::write_json(jsonvalue, string_js);
            std::string jsonstring = jsonvalue.str();
            records_vector_.push_back(std::make_pair<std::string, std::string>("string", jsonstring.c_str()));
        } else if (!strcmp(cur_data->str, "list")) {
			pt::ptree root;
            pt::ptree list_js;
            cur_data = redisCmd(c, "LRANGE %s %d %d", key.str_.c_str(), key.start_, key.end_);
            for (int j=0; j<cur_data->elements; j++) {
                std::string value(cur_data->element[j]->str);
                pt::ptree element;
                element.put("", value);
                list_js.push_back(std::make_pair("", element));
            }
			root.add_child(key.str_, list_js);
            std::stringstream jsonvalue;
            pt::write_json(jsonvalue, root);
            std::string jsonstring = jsonvalue.str();
            records_vector_.push_back(std::make_pair<std::string, std::string>("list", jsonstring.c_str()));
        } else if (!strcmp(cur_data->str, "hash")) {
            pt::ptree root;
            pt::ptree hash_js;
            int hcursor = 0;
            do {
                cur_data = redisCmd(c, "HSCAN %s %d COUNT %d", key.str_.c_str(), hcursor, ITER_STEP);
                hcursor = atoi(cur_data->element[0]->str);
                std::string field;
                std::string value;
                for (int j=0; j<cur_data->element[1]->elements; j+=2) {
                    field = cur_data->element[1]->element[j]->str;
                    value = cur_data->element[1]->element[j+1]->str;
                    LOG_I << "input HASH: " << key.str_ << "-" << field << "-" << value;
                    hash_js.put(field, value);
                }
            } while (hcursor);
			root.add_child(key.str_, hash_js);
            std::stringstream jsonvalue;
            pt::write_json(jsonvalue, root);
            std::string jsonstring = jsonvalue.str();
            records_vector_.push_back(std::make_pair<std::string, std::string>("hash", jsonstring.c_str()));
        } else if (!strcmp(cur_data->str, "set")) {
            pt::ptree root;
            pt::ptree set_js;
            int scursor = 0;
            do {
                cur_data = redisCmd(c, "SSCAN %s %d COUNT %d", key.str_.c_str(), scursor, ITER_STEP);
                scursor = atoi(cur_data->element[0]->str);
                std::string value;
                for (int j=0; j<cur_data->element[1]->elements; j++) {
                    value = cur_data->element[1]->element[j]->str;
				    pt::ptree element;
                    element.put("", value);
                    LOG_I << "input SET: " << key.str_ << "-" << value;
                    set_js.push_back(std::make_pair("", element));
                }
            } while (scursor);
			root.add_child(key.str_, set_js);
            std::stringstream jsonvalue;
            pt::write_json(jsonvalue, root);
            std::string jsonstring = jsonvalue.str();
            records_vector_.push_back(std::make_pair<std::string, std::string>("set", jsonstring.c_str()));
        } else if (!strcmp(cur_data->str, "zset")) {
            pt::ptree root;
            pt::ptree zset_js;
            int zcursor = 0;
            do {
                cur_data = redisCmd(c, "ZSCAN %s %d COUNT %d", key.str_.c_str(), zcursor, ITER_STEP);
                zcursor = atoi(cur_data->element[0]->str);
                std::string value;
                std::string score;
                for (int j=0; j<cur_data->element[1]->elements; j+=2) {
                    value = cur_data->element[1]->element[j]->str;
                    score = cur_data->element[1]->element[j+1]->str;
				    pt::ptree element;
                    element.put(value, score);
                    LOG_I << "input SET: " << key.str_ << "-" << value << "-" << score;
                    zset_js.push_back(std::make_pair("", element));
                }
            } while (zcursor);
			root.add_child(key.str_, zset_js);
            std::stringstream jsonvalue;
            pt::write_json(jsonvalue, root);
            std::string jsonstring = jsonvalue.str();
            records_vector_.push_back(std::make_pair<std::string, std::string>("zset", jsonstring.c_str()));
        } else if (nullptr != cur_data->str) {
            LOG_E << "Failed to read data :" << std::string(cur_data->str) << " <- " << gen_slot_crc16(key.str_.c_str(), key.str_.length()) << " " << split.get_ip() << ":" << split.get_port() << " <- " << key.str_;
        } else {
            LOG_E << "Failed to read data without data";
		}
        freeReplyObject(cur_data);
    }
}

// all data have been read
void RedisInputFormat::send_end(std::vector<std::vector<RedisRangeKey> >& best_keys) {
    BinStream question;
    int num_received_keys = 0;
    for (auto& split_keys : best_keys) {
        num_received_keys += split_keys.size();
    }
    question << num_received_keys;
    question << husky::Context::get_global_tid();
    husky::Context::get_coordinator()->ask_master(question, husky::TYPE_REDIS_END_REQ);
    return;
}

bool RedisInputFormat::next(RecordT& ref) {
    if (if_pop_record_) {
        records_vector_.pop_back();
        if_pop_record_ = false;
    }

    int task_status = 0;
    if (records_vector_.empty()) {
        task_status = ask_best_keys();
        bool if_best_keys_empty = true;
        for (auto& split_keys : best_keys_) {
            if (!split_keys.empty()) {
                if_best_keys_empty = false;
                break;
            }
        }
        if (!if_best_keys_empty) {
            for (int split_i=0; split_i<best_keys_.size(); split_i++) { 
                if (best_keys_[split_i].empty()) continue;
                fetch_split_records(split_i, best_keys_[split_i]);
            }
            send_end(best_keys_);
            best_keys_.clear();
            ref = records_vector_.back();
            if_pop_record_ = true;
        } else if (2 != task_status) {
            LOG_E << "no keys assigned.";
        }
    } else {
        ref = records_vector_.back();
        if_pop_record_ = true;
    }

    switch (task_status) {
        case 1: {
                    return false;
                }
        default: {
                     return true;
                 }
    }
}

uint16_t RedisInputFormat::gen_slot_crc16(const char *buf, int len) {
    int counter;
    uint16_t crc = 0;
    for (counter = 0; counter < len; counter++)
        crc = (crc<<8) ^ crc16tab_[((crc>>8) ^ *buf++)&0x00FF];
    return crc % 16384;
}

}  // namespace io
}  // namespace husky
