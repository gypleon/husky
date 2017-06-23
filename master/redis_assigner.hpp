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

#include <map>
#include <set>
#include <string>
#include <vector>
#include <queue>
#include <sys/time.h>
#include <fstream>
#include <mutex>
#include <thread>
#include <condition_variable>

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

    unsigned long reduce_max_workload(PROC_KEYS_POOLS& proc_new_keys_pools, std::vector<unsigned long>& workers_load);

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
    std::vector<unsigned long> procs_load_;
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

    const uint16_t crc16tab_[256]= {
        0x0000,0x1021,0x2042,0x3063,0x4084,0x50a5,0x60c6,0x70e7,
        0x8108,0x9129,0xa14a,0xb16b,0xc18c,0xd1ad,0xe1ce,0xf1ef,
        0x1231,0x0210,0x3273,0x2252,0x52b5,0x4294,0x72f7,0x62d6,
        0x9339,0x8318,0xb37b,0xa35a,0xd3bd,0xc39c,0xf3ff,0xe3de,
        0x2462,0x3443,0x0420,0x1401,0x64e6,0x74c7,0x44a4,0x5485,
        0xa56a,0xb54b,0x8528,0x9509,0xe5ee,0xf5cf,0xc5ac,0xd58d,
        0x3653,0x2672,0x1611,0x0630,0x76d7,0x66f6,0x5695,0x46b4,
        0xb75b,0xa77a,0x9719,0x8738,0xf7df,0xe7fe,0xd79d,0xc7bc,
        0x48c4,0x58e5,0x6886,0x78a7,0x0840,0x1861,0x2802,0x3823,
        0xc9cc,0xd9ed,0xe98e,0xf9af,0x8948,0x9969,0xa90a,0xb92b,
        0x5af5,0x4ad4,0x7ab7,0x6a96,0x1a71,0x0a50,0x3a33,0x2a12,
        0xdbfd,0xcbdc,0xfbbf,0xeb9e,0x9b79,0x8b58,0xbb3b,0xab1a,
        0x6ca6,0x7c87,0x4ce4,0x5cc5,0x2c22,0x3c03,0x0c60,0x1c41,
        0xedae,0xfd8f,0xcdec,0xddcd,0xad2a,0xbd0b,0x8d68,0x9d49,
        0x7e97,0x6eb6,0x5ed5,0x4ef4,0x3e13,0x2e32,0x1e51,0x0e70,
        0xff9f,0xefbe,0xdfdd,0xcffc,0xbf1b,0xaf3a,0x9f59,0x8f78,
        0x9188,0x81a9,0xb1ca,0xa1eb,0xd10c,0xc12d,0xf14e,0xe16f,
        0x1080,0x00a1,0x30c2,0x20e3,0x5004,0x4025,0x7046,0x6067,
        0x83b9,0x9398,0xa3fb,0xb3da,0xc33d,0xd31c,0xe37f,0xf35e,
        0x02b1,0x1290,0x22f3,0x32d2,0x4235,0x5214,0x6277,0x7256,
        0xb5ea,0xa5cb,0x95a8,0x8589,0xf56e,0xe54f,0xd52c,0xc50d,
        0x34e2,0x24c3,0x14a0,0x0481,0x7466,0x6447,0x5424,0x4405,
        0xa7db,0xb7fa,0x8799,0x97b8,0xe75f,0xf77e,0xc71d,0xd73c,
        0x26d3,0x36f2,0x0691,0x16b0,0x6657,0x7676,0x4615,0x5634,
        0xd94c,0xc96d,0xf90e,0xe92f,0x99c8,0x89e9,0xb98a,0xa9ab,
        0x5844,0x4865,0x7806,0x6827,0x18c0,0x08e1,0x3882,0x28a3,
        0xcb7d,0xdb5c,0xeb3f,0xfb1e,0x8bf9,0x9bd8,0xabbb,0xbb9a,
        0x4a75,0x5a54,0x6a37,0x7a16,0x0af1,0x1ad0,0x2ab3,0x3a92,
        0xfd2e,0xed0f,0xdd6c,0xcd4d,0xbdaa,0xad8b,0x9de8,0x8dc9,
        0x7c26,0x6c07,0x5c64,0x4c45,0x3ca2,0x2c83,0x1ce0,0x0cc1,
        0xef1f,0xff3e,0xcf5d,0xdf7c,0xaf9b,0xbfba,0x8fd9,0x9ff8,
        0x6e17,0x7e36,0x4e55,0x5e74,0x2e93,0x3eb2,0x0ed1,0x1ef0
    };
};

}  // namespace husky

#endif
