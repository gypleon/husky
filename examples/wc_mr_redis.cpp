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


#include "hiredis/hiredis.h"
#include "boost/property_tree/ptree.hpp"
#include "boost/property_tree/json_parser.hpp"
#include "boost/tokenizer.hpp"

#include "base/serialization.hpp"
#include "core/engine.hpp"
#include "io/input/inputformat_store.hpp"
#include "io/output/redis_outputformat.hpp"
#include "lib/aggregator_factory.hpp"

namespace pt = boost::property_tree;

class Word {
   public:
    using KeyT = std::string;

    Word() = default;
    explicit Word(const KeyT& w) : word(w) {}
    const KeyT& id() const { return word; }

    KeyT word;
    int count = 0;
};

bool operator<(const std::pair<int, std::string>& a, const std::pair<int, std::string>& b) {
    return a.first == b.first ? a.second < b.second : a.first < b.first;
}

void wc() {
    auto& inputformat = husky::io::InputFormatStore::create_redis_inputformat();
    inputformat.set_server();

    husky::io::RedisOutputFormat outputformat;
    outputformat.set_server();

    auto& word_list = husky::ObjListStore::create_objlist<Word>();
    auto& ch = husky::ChannelStore::create_push_combined_channel<int, husky::SumCombiner<int>>(inputformat, word_list);

    auto parse_wc = [&](husky::io::RedisInputFormat::RecordT& record_pair) {
        auto datatype = record_pair.first;

        pt::ptree reader, content_reader;
        std::stringstream jsonstream, content_stream;
        jsonstream << record_pair.second;
        pt::read_json(jsonstream, reader);

        switch (datatype) {
            case husky::io::RedisInputFormat::RedisDataType::String: 
                {
                    content_stream << reader.begin()->second.get_value<std::string>();
                    pt::read_json(content_stream, content_reader);
                    try {
                        std::string content = content_reader.get<std::string>("content");
                        boost::char_separator<char> sep(" \t");
                        boost::tokenizer<boost::char_separator<char>> tok(content, sep);
                        for (auto& w : tok) {
                            ch.push(1, w);
                        }
                    }
                    catch (pt::ptree_bad_path) {
                        husky::LOG_E << "invalid content field";
                        return;
                    }
                } break;
            default:
                return;
        }
    };

    husky::load(inputformat, parse_wc);

    // Show topk words.
    const int kMaxNum = 10;
    typedef std::set<std::pair<int, std::string>> TopKPairs;
    auto add_to_topk = [](TopKPairs& pairs, const std::pair<int, std::string>& p) {
        if (pairs.size() == kMaxNum && *pairs.begin() < p)
            pairs.erase(pairs.begin());
        if (pairs.size() < kMaxNum)
            pairs.insert(p);
    };
    husky::lib::Aggregator<TopKPairs> unique_topk(
        TopKPairs(),
        [add_to_topk](TopKPairs& a, const TopKPairs& b) {
            for (auto& i : b)
                add_to_topk(a, i);
        },
        [](TopKPairs& a) { a.clear(); },
        [add_to_topk](husky::base::BinStream& in, TopKPairs& pairs) {
            pairs.clear();
            for (size_t n = husky::base::deser<size_t>(in); n--;)
                add_to_topk(pairs, husky::base::deser<std::pair<int, std::string>>(in));
        },
        [](husky::base::BinStream& out, const TopKPairs& pairs) {
            out << pairs.size();
            for (auto& p : pairs)
                out << p;
        });

    husky::list_execute(word_list, [&ch, &unique_topk, add_to_topk](Word& word) {
        unique_topk.update(add_to_topk, std::make_pair(ch.get(word), word.id()));
    });

    husky::lib::AggregatorFactory::sync();

    if (husky::Context::get_global_tid() == 0) {
        for (auto& i : unique_topk.get_value())
            husky::LOG_I << i.second << " " << i.first;
    }

    /* Output result to Redis as a Hash table
    std::string result_key("WordCountResult");
    std::map<std::string, int> result_map;
    outputformat.commit(result_key, result_map);
    */
}

int main(int argc, char** argv) {
    if (!husky::init_with_args(argc, argv, {"redis_ip", "redis_port", "redis_keys_pattern"}))
        return 1;
    husky::run_job(wc);
    return 0;
}
