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
#include "boost/property_tree/ptree.hpp"
#include "boost/property_tree/json_parser.hpp"

#include <string>
#include <sstream>
#include <vector>

#include "base/log.hpp"

namespace husky {
namespace io {

namespace pt = boost::property_tree;

using base::log_msg;

// TODO: perhaps could be dynamically tuned
const int kMaxNumberOfRecordBytes = 10240;

RedisOutputFormat::RedisOutputFormat() {
    records_vector_.clear();
}

RedisOutputFormat::~RedisOutputFormat() { records_vector_.clear(); }

void RedisOutputFormat::set_server(const std::string ip, const std::string port) { ip_ = ip; port_ = atoi(port); }

void RedisOutputFormat::set_auth(const std::string& password) {
    password_ = password;
    need_auth_ = true;
}

// TODO: BinStream
bool RedisOutputFormat::commit(BinStream& result_stream, const std::string& jsonstring) {
    if (jsonstring.empty())
        return false;

    records_vector_.push_back(std::make_pair<datatype, jsonstring>);
    records_bytes_ += jsonstring.length();

    if (records_bytes_ >= kMaxNumberOfRecordBytes)
        flush_all();

    return true;
}

void RedisOutputFormat::flush_all() {
    if (records_vector_.empty())
        return;

    redisContext *c;
    redisReply *reply;
    c = redisConnectWithTimeout( server_, ip_, timeout_);
    if (NULL == c || c->err) {
        if (c){
            LOG_I << "Connection error: " << c->errstr;
            redisFree(c);
        }else{
            LOG_I << "Connection error: can't allocate redis context";
        }
        return;
    }

    // if (need_auth_)
    if (false){
        reply = redisCmd(c, "AUTH %s", password_.c_str());
        freeReplyObject(reply);
    }

    pt::tree root;
    for ( RecordT& record : records_vector_){
        switch (record.first){
            case RedisString:
                std::stringstream jsonvalue = record.second;
                pt::read_json(jsonvalue, root);
                redisCmd("SET %s %s", );
            break;
            case RedisList:
                redisCmd("LPUSH %s %s", );
            break;
            case RedisHash:
                redisCmd("HSET %s %s %s", );
            break;
            case RedisSet:
                redisCmd("SADD %s %s", );
            break;
            case RedisZset:
                // with score, batch available
                redisCmd("ZADD %s %d %s", );
            break;
            default:
        }
    }

    freeReplyObject(reply);
    redisFree(c);

    records_vector_.clear();
}

}  // namespace io
}  // namespace husky
