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

    husky::io::RedisOutputFormat outputformat;
    outputformat.set_server();
    // outputformat.set_auth(pwd);

    auto read_and_write = [&](husky::io::RedisInputFormat::RecordT& record_pair) {

        namespace pt = boost::property_tree;
        pt::ptree reader;
        std::stringstream jsonstream;
        std::string datatype = record_pair.first;
        jsonstream << record_pair.second;

        std::string key;
        std::string str_data;

        if ("string" == datatype) {
            pt::read_json(jsonstream, reader);
            for ( auto& kv : reader ) {
                key = kv.first;
                str_data = kv.second.get_value<std::string>();
                // husky::LOG_I << key;// << " <=== " << str_data;
            }
        }
        outputformat.commit(key, str_data);

        // commit map
        // std::string key = fields[4].toString(false, true);
        // std::map<std::string, std::string> map_data;
        // map_data["title"] = fields[0].toString(false, true);
        // map_data["url"] = fields[1].toString(false, true);
        // map_data["content"] = fields[2].toString(false, true);
        // map_data["id"] = fields[3].toString(false, true);
        // outputformat.commit(key, map_data);

        // commit vector
        // std::string key = fields[4].toString(false, true);
        // std::vector<std::string> vec_data;
        // vec_data.push_back(fields[0].toString(false, true));
        // vec_data.push_back(fields[1].toString(false, true));
        // vec_data.push_back(fields[2].toString(false, true));
        // vec_data.push_back(fields[3].toString(false, true));
        // outputformat.commit(key, vec_data);
    };

    husky::load(inputformat, read_and_write);
    outputformat.flush_all();

    husky::LOG_I << "Done";
}

int main(int argc, char** argv) {
    ASSERT_MSG(husky::init_with_args(argc, argv, {"mongo_server", "mongo_db", "mongo_collection"}), "Wrong arguments!");
    husky::run_job(test);
    return 0;
}
