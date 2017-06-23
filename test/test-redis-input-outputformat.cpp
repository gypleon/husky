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

#include <functional>
#include <iostream>
#include <string>
#include <map>
#include <vector>

#include "core/engine.hpp"
#include "io/input/inputformat_store.hpp"
#include "io/output/redis_outputformat.hpp"

#include "hiredis/hiredis.h"
#include "boost/property_tree/ptree.hpp"
#include "boost/property_tree/json_parser.hpp"

namespace pt = boost::property_tree;
namespace hi = husky::io;

void test() {
    auto& inputformat = hi::InputFormatStore::create_redis_inputformat();
    inputformat.set_server();
    // inputformat.set_auth(pwd);

    hi::RedisOutputFormat outputformat;
    outputformat.set_server();
    // outputformat.set_auth(pwd);

    std::string sfx("_x");

    auto read_and_write = [&](hi::RedisInputFormat::RecordT& record_pair) {
        auto datatype = record_pair.first;

        pt::ptree reader;
        std::stringstream jsonstream;
        jsonstream << record_pair.second;
        pt::read_json(jsonstream, reader);

        const auto& key = reader.begin()->first;

        switch (datatype) {
            case hi::RedisInputFormat::RedisDataType::String: 
                {
                    outputformat.commit(key + sfx, reader.begin()->second.get_value<std::string>());
                } break;
            case hi::RedisInputFormat::RedisDataType::List: 
                {
                    std::map<std::string, std::string> map_data;
                    for (auto& kv : reader.begin()->second) {
                        map_data[kv.first] = kv.second.get_value<std::string>();
                    }
                    outputformat.commit(key + sfx, map_data);
                } break;
            case hi::RedisInputFormat::RedisDataType::Hash: 
                {
                    // for Redis List, commit() performs as creating and/or appending list elements
                    std::vector<std::string> vec_data;
                    for (auto& kv : reader.begin()->second) {
                        vec_data.push_back(kv.second.get_value<std::string>());
                    }
                    outputformat.commit(key + sfx, vec_data);
                } break;
            default:
                husky::LOG_E << "undefined data structure";
                break;
        }
    };

    husky::load(inputformat, read_and_write);
    outputformat.flush_all();

    husky::LOG_I << "Done";
}

int main(int argc, char** argv) {
    if (!husky::init_with_args(argc, argv, {"redis_hostname", "redis_port", "redis_keys_pattern"}))
        return 1;
    husky::run_job(test);
    return 0;
}
