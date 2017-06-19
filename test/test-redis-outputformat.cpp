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

// #include "boost/tokenizer.hpp"
// #include "mongo/bson/bson.h"
// #include "mongo/client/dbclient.h"

#include "core/engine.hpp"
#include "io/input/inputformat_store.hpp"
#include "io/output/redis_outputformat.hpp"

void test() {
    std::string server = husky::Context::get_param("mongo_server");
    std::string db = husky::Context::get_param("mongo_db");
    std::string collection = husky::Context::get_param("mongo_collection");
    // std::string user = husky::Context::get_param("mongo_user");
    // std::string pwd = husky::Context::get_param("mongo_pwd");

    auto& inputformat = husky::io::InputFormatStore::create_mongodb_inputformat();
    inputformat.set_server(server);
    inputformat.set_ns(db, collection);
    // inputformat.set_auth(user, pwd);


    husky::io::RedisOutputFormat outputformat;
    outputformat.set_server();
    // outputformat.set_auth(pwd);

    const char * field_names[] = {"md5", "title", "url", "content", "id"};
    int length_field_names = sizeof(field_names) / sizeof(field_names[0]);
    mongo::BSONElement fields[length_field_names];
    auto read_and_write = [&](std::string& chunk) {
        mongo::BSONObj o = mongo::fromjson(chunk);
        o.getFields(length_field_names, field_names, fields);

        /* commit string
        std::string key = fields[0].toString(false, true);
        std::string str_data = fields[3].toString(false, true);
        key = key.substr(1, key.size()-2);
        str_data = str_data.substr(1, str_data.size()-2);
        outputformat.commit(key, str_data);
        */

        /* commit map
        */
        std::string key = fields[0].toString(false, true);
        key = key.substr(1, key.size()-2);
        std::map<std::string, std::string> map_data;
        std::string value;
        for (int i=1; i<length_field_names; i++) {
            value = fields[i].toString(false, true);
            value = value.substr(1, value.size()-2);
            map_data[std::string(field_names[i])] = value;
        }
        outputformat.commit(key, map_data);

        /* commit vector
        std::string key = fields[0].toString(false, true);
        key = key.substr(1, key.size()-2);
        std::vector<std::string> vec_data;
        std::string value;
        for (int i=1; i<length_field_names; i++) {
            value = fields[i].toString(false, true);
            value = value.substr(1, value.size()-2);
            vec_data.push_back(value);
        }
        outputformat.commit(key, vec_data);
        */
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
