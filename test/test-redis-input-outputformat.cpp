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

#include "boost/tokenizer.hpp"
#include "mongo/bson/bson.h"
#include "mongo/client/dbclient.h"

#include "core/engine.hpp"
#include "io/input/inputformat_store.hpp"
#include "io/output/redis_outputformat.hpp"

#include "hiredis/hiredis.h"
#include "boost/property_tree/ptree.hpp"
#include "boost/property_tree/json_parser.hpp"

void test() {
    auto& inputformat = husky::io::InputFormatStore::create_redis_inputformat();
    inputformat.set_server();

    husky::io::RedisOutputFormat outputformat;
    outputformat.set_server();
    // outputformat.set_auth(pwd);

    auto read_and_write = [&](husky::io::RedisInputFormat::RecordT& record_pair) {
        namespace pt = boost::property_tree;
        pt::ptree reader;
        std::stringstream jsonstream;
        std::string datatype = record_pair.first;
        jsonstream << record_pair.second;
        pt::read_json(jsonstream, reader);

        std::string key;

        if ("string" == datatype) {
            std::string str_data;
            key = reader.begin()->first;
            str_data = reader.begin()->second.get_value<std::string>();
            outputformat.commit(key, str_data);
        } else if ("hash" == datatype) {
            std::map<std::string, std::string> map_data;
            key = reader.begin()->first;
            auto& value = reader.begin()->second;
            for (auto& kv : value) {
                auto k = kv.first;
                auto v = kv.second.get_value<std::string>();
                map_data[k] = v;
            }
            outputformat.commit(key, map_data);
        } else if ("list" == datatype) {
            std::vector<std::string> vec_data;
            key = reader.begin()->first;
            auto& value = reader.begin()->second;
            for (auto& kv : value) {
                vec_data.push_back(kv.second.get_value<std::string>());
            }
            outputformat.commit(key, vec_data);
        } else {
            husky::LOG_E << "undefined data structure";
        }
    };

    husky::load(inputformat, read_and_write);
    outputformat.flush_all();

    husky::LOG_I << "Done";
}

int main(int argc, char** argv) {
    husky::ASSERT_MSG(husky::init_with_args(argc, argv, {"mongo_server", "mongo_db", "mongo_collection"}), "Wrong arguments!");
    husky::run_job(test);
    return 0;
}
