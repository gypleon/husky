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

#include "base/serialization.hpp"
#include "core/constants.hpp"
#include "core/context.hpp"
#include "core/coordinator.hpp"

#include "hiredis/hiredis.h"
#include "boost/property_tree/ptree.hpp"
#include "boost/property_tree/json_parser.hpp"

namespace husky {
namespace io {

namespace pt = boost::property_tree;

enum RedisInputFormatSetUp {
    NotSetUp = 0,
    ServerSetUp = 1 << 2,
    // TODO: how to check if needs auth.
    AuthSetUp = 1 << 2,
    AllSetUp = ServerSetUp | AuthSetUp,
};

RedisInputFormat::RedisInputFormat() {
    records_vector_.clear();
    best_keys_.clear();
    cons_.clear();
    is_setup_ = RedisInputFormatSetUp::NotSetUp;

    // for test
    // base::log_msg("[worker_" + std::to_string(husky::Context::get_global_tid()) + "] num_splits: " + std::to_string(best_keys_.get_keys().size()));
    // for ( auto& split : best_keys_.get_keys()) {
    //     base::log_msg(" |-split: " + split.first.get_ip());
    //     for ( auto& key : split.second) {
    //         base::log_msg("   |-key: " + key.str_);
    //     }
    // } 
    is_setup_ |= RedisInputFormatSetUp::ServerSetUp;
}

RedisInputFormat::~RedisInputFormat() { 
    records_vector_.clear(); 
    best_keys_.clear();
    for ( auto& con : cons_ ) {
        redisFree(con.second);
    }
    cons_.clear();
}

bool RedisInputFormat::is_setup() const { 
    return !(is_setup_ ^ RedisInputFormatSetUp::AllSetUp); 
}

void RedisInputFormat::set_auth(const std::string& password){
    need_auth_ = true;
    password_ = password;
}

void RedisInputFormat::reset_auth(){
    need_auth_ = false;
}

// ask master for a set of best keys, with their location 
bool RedisInputFormat::ask_best_keys() {
    BinStream question;
    question << husky::Context::get_global_tid();

    BinStream answer = husky::Context::get_coordinator()->ask_master(question, husky::TYPE_REDIS_REQ);

    answer >> best_keys_;

    // int tid = husky::Context::get_global_tid();
    if ( best_keys_.get_keys().empty() ) {
        // base::log_msg("[worker_" + std::to_string(tid) + "] finished, num_splits: " + std::to_string(best_keys_.get_keys().size()));
        return false;
    } else {
        // base::log_msg("[worker_" + std::to_string(tid) + "] received, num_splits: " + std::to_string(best_keys_.get_keys().size()));
        return true;
    }
}

// connect Redis split to retrieve RECORDS
void RedisInputFormat::fetch_split_records(const RedisSplit& split, const std::vector<RedisRangeKey>& keys){

    if (!split.is_valid()) {
        LOG_I << "Redis split invalid: " << split.get_id();
        return;
    }

    redisContext * c;
    if ( cons_.end() != cons_.find(split.get_id()) ) {
        c = cons_[split.get_id()];
    } else {
        c = redisConnectWithTimeout(split.get_ip().c_str(), split.get_port(), timeout_);
        if (NULL == c || c->err){
            if (c){
                redisFree(c);
            }else{
                LOG_I << "Connection error: can't allocate redis context";
            }
            return;
        }

        if (need_auth_) {
            redisReply * reply;
            reply = redisCmd(c, "AUTH %s", password_.c_str());
            freeReplyObject(reply);
        }
        cons_[split.get_id()] = c;
    }

    // slave read-only, master-slaves load balance
    if ( split.get_master().compare("-") ) {
        redisReply * reply;
        reply = redisCmd(c, "READONLY");
        if ( strcmp(reply->str, "OK") ) {
            LOG_I << "Slave fail to start up read-only";
            freeReplyObject(reply);
            return;
        }
        freeReplyObject(reply);
    }

    for ( auto& key : keys) {
        redisReply * cur_data = redisCmd(c, "TYPE %s", key.str_.c_str());
        if (!strcmp(cur_data->str, "string")){
            // TODO: range
            pt::ptree string_js;
            cur_data = redisCmd(c, "GET %s", key.str_.c_str());
            std::string value(cur_data->str);
            string_js.put(key.str_, value); 
            std::stringstream jsonvalue;
            pt::write_json(jsonvalue, string_js);
            std::string jsonstring = jsonvalue.str();
            records_vector_.push_back(std::make_pair<std::string, std::string>("string", jsonstring.c_str()));
        }else if (!strcmp(cur_data->str, "list")){
			pt::ptree root;
            pt::ptree list_js;
            // TODO: range
            cur_data = redisCmd(c, "LRANGE %s %d %d", key.str_.c_str(), key.start_, key.end_);
            for ( int j=0; j<cur_data->elements; j++ ) {
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
        }else if (!strcmp(cur_data->str, "hash")){
            // TODO: range
            pt::ptree root;
            pt::ptree hash_js;
            int hcursor = 0;
            do{
                cur_data = redisCmd(c, "HSCAN %s %d COUNT %d", key.str_.c_str(), hcursor, ITER_STEP);
                hcursor = atoi(cur_data->element[0]->str);
                std::string field;
                std::string value;
                for (int j=0; j<cur_data->element[1]->elements; j+=2){
                    field = cur_data->element[1]->element[j]->str;
                    value = cur_data->element[1]->element[j+1]->str;
                    LOG_I << "input HASH: " << key.str_ << "-" << field << "-" << value;
                    hash_js.put(field, value);
                }
            }while (hcursor);
			root.add_child(key.str_, hash_js);
            std::stringstream jsonvalue;
            pt::write_json(jsonvalue, root);
            std::string jsonstring = jsonvalue.str();
            records_vector_.push_back(std::make_pair<std::string, std::string>("hash", jsonstring.c_str()));
        }else if (!strcmp(cur_data->str, "set")){
            // TODO: range
            pt::ptree root;
            pt::ptree set_js;
            int scursor = 0;
            do{
                cur_data = redisCmd(c, "SSCAN %s %d COUNT %d", key.str_.c_str(), scursor, ITER_STEP);
                scursor = atoi(cur_data->element[0]->str);
                std::string value;
                for (int j=0; j<cur_data->element[1]->elements; j++){
                    value = cur_data->element[1]->element[j]->str;
				    pt::ptree element;
                    element.put("", value);
                    LOG_I << "input SET: " << key.str_ << "-" << value;
                    set_js.push_back(std::make_pair("", element));
                }
            }while (scursor);
			root.add_child(key.str_, set_js);
            std::stringstream jsonvalue;
            pt::write_json(jsonvalue, root);
            std::string jsonstring = jsonvalue.str();
            records_vector_.push_back(std::make_pair<std::string, std::string>("set", jsonstring.c_str()));
        }else if (!strcmp(cur_data->str, "zset")){
            // TODO: range
            pt::ptree root;
            pt::ptree zset_js;
            int zcursor = 0;
            do{
                cur_data = redisCmd(c, "ZSCAN %s %d COUNT %d", key.str_.c_str(), zcursor, ITER_STEP);
                zcursor = atoi(cur_data->element[0]->str);
                std::string value;
                std::string score;
                for (int j=0; j<cur_data->element[1]->elements; j+=2){
                    value = cur_data->element[1]->element[j]->str;
                    score = cur_data->element[1]->element[j+1]->str;
				    pt::ptree element;
                    element.put(value, score);
                    LOG_I << "input SET: " << key.str_ << "-" << value << "-" << score;
                    zset_js.push_back(std::make_pair("", element));
                }
            }while (zcursor);
			root.add_child(key.str_, zset_js);
            std::stringstream jsonvalue;
            pt::write_json(jsonvalue, root);
            std::string jsonstring = jsonvalue.str();
            records_vector_.push_back(std::make_pair<std::string, std::string>("zset", jsonstring.c_str()));
        }else if (nullptr != cur_data->str){
            LOG_I << "Failed to read data " << std::string(cur_data->str);
        }else{
            LOG_I << "Failed to read data without data";
		}
        freeReplyObject(cur_data);
    }

    return;
}

// all data have been read
void RedisInputFormat::send_end(RedisBestKeys& best_keys) {
    BinStream question;
    question << best_keys;
    question << husky::Context::get_global_tid();
    husky::Context::get_coordinator()->ask_master(question, husky::TYPE_REDIS_END_REQ);
    return;
}

bool RedisInputFormat::next(RecordT& ref) {
    if ( if_pop_record_ ) {
        records_vector_.pop_back();
        if_pop_record_ = false;
    }

    if ( records_vector_.empty() ) {
        if ( best_keys_.get_keys().empty() ) {
            if ( ask_best_keys() ) {
                for ( auto& split : best_keys_.get_keys()){ 
                    fetch_split_records(split.first, split.second);
                    // TODO: if split invalid
                }
                send_end(best_keys_);
                best_keys_.clear();
                ref = records_vector_.back();
                if_pop_record_ = true;
                return true;
            } else {
                return false;
            }
        } else {
            for ( auto& split : best_keys_.get_keys()){ 
                fetch_split_records(split.first, split.second);
                // TODO: if split invalid
            }
            send_end(best_keys_);
            best_keys_.clear();
            ref = records_vector_.back();
            if_pop_record_ = true;
            return true;
        }
    } else {
        ref = records_vector_.back();
        if_pop_record_ = true;
        return true;
    }
}

}  // namespace io
}  // namespace husky
