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

#include <set>
#include <string>
#include <utility>
#include <vector>
#include <iostream>
#include <fstream>

#include "hiredis/hiredis.h"
#include "boost/property_tree/ptree.hpp"
#include "boost/property_tree/json_parser.hpp"

#include "base/serialization.hpp"
#include "core/engine.hpp"
#include "io/input/inputformat_store.hpp"
#include "lib/aggregator_factory.hpp"

// for experiments
#include <chrono>
#include <ctime>

class Vertex {
   public:
    using KeyT = int;

    Vertex() : pr(0.15) {}
    explicit Vertex(const KeyT& id) : vertexId(id), pr(0.15) {}
    const KeyT& id() const { return vertexId; }

    // Serialization and deserialization
    friend husky::BinStream& operator<<(husky::BinStream& stream, const Vertex& u) {
        stream << u.vertexId << u.adj << u.pr;
        return stream;
    }
    friend husky::BinStream& operator>>(husky::BinStream& stream, Vertex& u) {
        stream >> u.vertexId >> u.adj >> u.pr;
        return stream;
    }

    int vertexId;
    std::vector<int> adj;
    float pr;
};

// print time interval
void print_time(std::chrono::time_point<std::chrono::system_clock>& last, int tid, const char* state) {
    if ( tid == husky::Context::get_global_tid() ) {
        std::chrono::duration<double> interval = std::chrono::system_clock::now() - last;
        last = std::chrono::system_clock::now();
        if ( strcmp(state, "") ) {
            std::cout << "[" << std::to_string(tid) << "]" << std::string(state) << ":" << interval.count() << std::endl;
        }
    }
}


void pagerank() {
    int my_tid = husky::Context::get_global_tid();
    int print_tid = my_tid;
    std::chrono::time_point<std::chrono::system_clock> start = std::chrono::system_clock::now();
    std::chrono::time_point<std::chrono::system_clock> last = std::chrono::system_clock::now();
    auto& infmt = husky::io::InputFormatStore::create_redis_inputformat();
    // print_time(last, print_tid, "initiate time");
    print_time(last, print_tid, "");

    // Create and globalize vertex objects
    auto& vertex_list = husky::ObjListStore::create_objlist<Vertex>();
    auto parse = [&vertex_list](husky::io::RedisInputFormat::RecordT& record_pair) {

        namespace pt = boost::property_tree;
        pt::ptree reader;
        std::stringstream jsonstream;
        std::string datatype = record_pair.first;
        jsonstream << record_pair.second;

        std::string key;
        std::vector<std::string> values;

        if ("list" == datatype) {
            pt::read_json(jsonstream, reader);
            for ( auto& kv : reader ) {
                key = kv.first;
                for ( auto& v : kv.second ) {
                    if ( v.second.data().compare("[]"))
                        values.push_back(v.second.data());
                }
            }
        }
        int id = stoi(key);
        Vertex v(id);
        for ( auto& value : values ) {
            v.adj.push_back(stoi(value));
        }
        vertex_list.add_object(std::move(v));
    };
    // print_time(last, print_tid, "Create Objlist");
    print_time(last, print_tid, "");
    husky::load(infmt, parse);
    print_time(last, print_tid, "load time");
    // TODO: what if Vertexes with identical ID
    husky::globalize(vertex_list);
    print_time(last, print_tid, "");

    // Iterative PageRank computation
    auto& prch =
        husky::ChannelStore::create_push_combined_channel<float, husky::SumCombiner<float>>(vertex_list, vertex_list);
    int numIters = stoi(husky::Context::get_param("iters"));
    print_time(last, print_tid, "");
    for (int iter = 0; iter < numIters; ++iter) {
        husky::list_execute(vertex_list, [&prch, iter](Vertex& u) {
            if (iter > 0)
                u.pr = 0.85 * prch.get(u) + 0.15;

            if (u.adj.size() == 0)
                return;
            float sendPR = u.pr / u.adj.size();
            for (auto& nb : u.adj) {
                prch.push(sendPR, nb);
            }
        });
    }
    print_time(last, print_tid, "");

    // Display the top K nodes
    const int kMaxNum = 10;

    if (my_tid < 1) {
        int count = 0;
        auto& sorted_vertex_list = vertex_list.get_data();
        std::sort(sorted_vertex_list.begin(), sorted_vertex_list.end(), [](const Vertex& a, const Vertex& b) { return a.pr > b.pr;});
        for ( auto& v : sorted_vertex_list ) {
            husky::LOG_I << std::to_string(my_tid) << ": [" << std::to_string(count+1) << "] " << std::to_string(v.id()) << " " << std::to_string(v.pr);
            if ( ++count >= kMaxNum )
                break;
        }
    }
    print_time(start, print_tid, "");
}

int main(int argc, char** argv) {
    std::vector<std::string> args;
    args.push_back("iters");
    if (husky::init_with_args(argc, argv, args)) {
        husky::run_job(pagerank);
        return 0;
    }
    return 1;
}
