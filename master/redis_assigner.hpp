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

#pragma once

#ifdef WITH_REDIS

#include <condition_variable>
#include <fstream>
#include <map>
#include <mutex>
#include <queue>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <sys/time.h>

#include "core/context.hpp"
#include "hiredis/hiredis.h"
#include "io/input/redis_split.hpp"

#define redisCmd(context, ...) static_cast<redisReply*>(redisCommand(context, __VA_ARGS__))

namespace husky {

using io::RedisSplit;
using io::RedisSplitGroup;
using io::RedisRangeKey;

class RedisSplitAssigner {
public:
    RedisSplitAssigner();
    virtual ~RedisSplitAssigner();
    void set_auth(const std::string&);
    void reset_auth();

private:
    // [proc_id][local, non_local]{split_id : [keys]}
    typedef std::vector<std::vector<std::map<std::string, std::vector<RedisRangeKey> > > > PROC_KEYS_POOLS;
    // [worker_id]{split_id : [keys]}
    typedef std::vector<std::vector<std::vector<RedisRangeKey> > > WORKER_KEYS_POOLS;

private:
    bool load_parameters();

    void master_redis_req_handler();
    void master_redis_qry_req_handler();
    void master_redis_req_end_handler();
    void master_setup_handler();

    bool create_redis_info();
    bool refresh_splits_info();
    void create_husky_info();
    void create_split_proc_map();
    void create_redis_con_pool();
    void create_schedule_thread();
    void reset_default_states();

    void answer_tid_best_keys(int global_tid, std::vector<std::vector<RedisRangeKey> >& ret);
    void answer_masters_info(std::map<std::string, RedisSplit>& redis_masters_info);
    void answer_splits_info(std::map<std::string, RedisSplit>& redis_splits_info);
    void receive_end(int global_tid, int num_received_keys);

    void import_all_pattern_keys();
    void load_batch_keys();
    void schedule_batch_keys(std::mutex * pools_lock);
    void load_schedule();

    uint64_t reduce_max_workload(PROC_KEYS_POOLS& proc_new_keys_pools, std::vector<uint64_t>& workers_load);

    std::string parse_host(const std::string& hostname);
    uint16_t gen_slot_crc16(const char *buf, int len);

private:
    // batch / streaming
    std::vector<std::vector<std::string> > all_keys_;
    std::vector<std::string> batch_keys_;
    int num_keys_amount_ = 0;
    int num_keys_batched_ = 0;
    int batch_size_;
    bool if_streaming_mode_ = false;
    std::vector<std::queue<int> > worker_num_keys_assigned_;

    // stop condition
    bool is_dynamic_imported_ = false;
    bool is_pattern_batched_ = false;
    bool is_file_imported_ = false;
    bool if_all_keys_scheduled_ = false;
    bool if_all_keys_fetched_ = false;
    bool if_scheduler_stopped_ = false;
    int num_keys_fetched_ = 0;
    std::vector<int> worker_num_fetched_keys_;
    std::vector<int> worker_task_status_;

    // husky cluster info
    WorkerInfo work_info_;
    int num_procs_;
    int num_workers_;
    std::vector<std::vector<int> > proc_worker_map_;

    // local keys assignment
    int num_slots_per_group_;
    std::map<std::string, RedisSplit> splits_;
    std::map<std::string, RedisSplitGroup> split_groups_;
    std::vector<std::string> sorted_split_group_name_;

    // non-local/heavy keys assignment
    std::map<std::string, std::vector<RedisRangeKey> > non_local_served_keys_;
    int key_split_size_ = 0;

    // keys from file
    std::string keys_path_;
    std::ifstream keys_file_;
    int cur_pos_ = 0;

    // keys from pattern
    bool is_pattern_imported_ = false;
    std::string keys_pattern_ = "";

    // keys from Redis List
    std::string keys_list_;
    RedisSplit keys_list_master_;
    bool if_found_keys_list_ = false;
    int cur_start_ = 0;

    // workload balance optimization, in microseconds
    int local_served_latency_ = 100;
    int non_local_served_latency_ = 100;
    std::map<std::string, std::vector<int> > keys_latency_map_;
    std::vector<uint64_t> procs_load_;
    std::vector<std::vector<int> > proc_keys_stat_;
    std::map<std::string, int> split_proc_map_;

    // worker-level task assignment
    int num_keys_scheduled_ = 0;
    WORKER_KEYS_POOLS worker_keys_pools_;
    std::mutex worker_pools_mutex_;
    std::thread scheduler_;
    std::vector<int> proc_worker_offset_;
    int non_local_offset_ = 0;

    // miscellaneous
    std::string ip_;
    int port_;
    struct timeval timeout_ = {1, 500000};
    bool need_auth_ = false;
    std::string password_;
    std::map<std::string, redisContext *> cons_;
    unsigned seed_;
};

}  // namespace husky

#endif
