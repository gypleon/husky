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
    // TODO: cache RedisMaster info from HuskyMaster
    ask_masters_info();
    create_redis_con_pool();
}

void RedisOutputFormat::ask_masters_info() {

}

void RedisOutputFormat::create_redis_con_pool() {
    redisReply *reply;
    for ( auto& split: splits_) {
        redisContext * c = redisConnectWithTimeout( split.get_ip().c_str(), split.get_port(), timeout_);
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
        cons_[split.get_id()] = c;
    }
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

    // records_map_[key] = std::make_pair<data_type, result_stream_buffer>;
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
    result_stream << result_list;
    const std::string result_stream_buffer = result_stream.to_string();
    std::pair<DataType, std::string> result_type_buffer(data_type, result_stream_buffer);

    // records_map_[key] = std::make_pair<data_type, result_stream>;
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
    result_stream << result_hash;
    const std::string result_stream_buffer = result_stream.to_string();
    std::pair<DataType, std::string> result_type_buffer(data_type, result_stream_buffer);

    // records_map_[key] = std::make_pair<data_type, result_stream>;
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

    redisContext *c;
    redisReply *reply;
    c = redisConnectWithTimeout( ip_.c_str(), port_, timeout_);
    if (NULL == c || c->err) {
        if (c){
            LOG_I << "Connection error: " << c->errstr;
            redisFree(c);
        }else{
            LOG_I << "Connection error: can't allocate redis context";
        }
        return;
    }

    if (need_auth_) {
        reply = redisCmd(c, "AUTH %s", password_.c_str());
        freeReplyObject(reply);
    }
    
    for ( auto& record : records_map_){
        std::string key = record.first;
        RedisOutputFormat::DataType data_type = record.second.first;
        BinStream result_stream;
        result_stream << record.second.second;

        uint16_t target_slot = gen_slot_crc16(key.c_str(), key.length());
        // TODO: according to cached RedisMaster info
        
        switch (data_type){
            case RedisOutputFormat::DataType::RedisString:
                {
                    std::string result_string;
                    result_stream >> result_string;
                    redisCmd(c, "SET %s %s", key, result_string);
                }
                break;
            case RedisOutputFormat::DataType::RedisList:
                {
                    // TODO: how to define inner Type
                    // TODO: dataT should be brought
                    template <class DataT>
                    std::vector<DataT> result_list;
                    result_stream >> result_list;
                    for ( auto& result : result_list ) {
                        redisCmd(c, "LPUSH %s %s", key, result);
                    }
                }
                break;
            case RedisOutputFormat::DataType::RedisHash:
                {
                    // TODO: how to define inner Type
                    std::map<RedisOutputFormat::DataType, BinStream> result_map;
                    result_stream >> result_map;
                    for ( auto& result : result_map ) {
                        redisCmd(c, "HSET %s %s %s", key, result.first, result.second);
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

}  // namespace io
}  // namespace husky
