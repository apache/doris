// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gtest/gtest.h>

#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/buffer_control_block.h"
#include "runtime/cache/result_cache.h"
#include "test_util/test_util.h"
#include "util/cpu_info.h"
#include "util/logging.h"

namespace doris {

class PartitionCacheTest : public testing::Test {
public:
    PartitionCacheTest() {}
    virtual ~PartitionCacheTest() {
        //        clear();
    }

protected:
    virtual void SetUp() {}

private:
    void init_default() {
        LOG(WARNING) << "init test default\n";
        init(16, 4);
    }
    void init(int max_size, int ela_size);
    void clear();
    PCacheStatus init_batch_data(int sql_num, int part_begin, int part_num, CacheType cache_type);
    ResultCache* _cache;
    PUpdateCacheRequest* _update_request;
    PCacheResponse* _update_response;
    PFetchCacheRequest* _fetch_request;
    PFetchCacheResult* _fetch_result;
    PClearCacheRequest* _clear_request;
    PCacheResponse* _clear_response;
};

void PartitionCacheTest::init(int max_size, int ela_size) {
    LOG(WARNING) << "init test\n";
    _cache = new ResultCache(max_size, ela_size);
    _update_request = new PUpdateCacheRequest();
    _update_response = new PCacheResponse();
    _fetch_request = new PFetchCacheRequest();
    _fetch_result = new PFetchCacheResult();
    _clear_request = new PClearCacheRequest();
    _clear_response = new PCacheResponse();
}

void PartitionCacheTest::clear() {
    _clear_request->set_clear_type(PClearType::CLEAR_ALL);
    _cache->clear(_clear_request, _clear_response);
    SAFE_DELETE(_cache);
    SAFE_DELETE(_update_request);
    SAFE_DELETE(_update_response);
    SAFE_DELETE(_fetch_request);
    SAFE_DELETE(_fetch_result);
    SAFE_DELETE(_clear_request);
    SAFE_DELETE(_clear_response);
}

void set_sql_key(PUniqueId* sql_key, int64 hi, int64 lo) {
    sql_key->set_hi(hi);
    sql_key->set_lo(lo);
}

PCacheStatus PartitionCacheTest::init_batch_data(int sql_num, int part_begin, int part_num,
                                                 CacheType cache_type) {
    LOG(WARNING) << "init data, sql_num:" << sql_num << ",part_num:" << part_num;
    PUpdateCacheRequest* up_req = nullptr;
    PCacheResponse* up_res = nullptr;
    PCacheStatus st = PCacheStatus::DEFAULT;
    for (int i = 1; i < sql_num + 1; i++) {
        LOG(WARNING) << "Sql:" << i;
        up_req = new PUpdateCacheRequest();
        up_res = new PCacheResponse();
        set_sql_key(up_req->mutable_sql_key(), i, i);
        //partition
        for (int j = part_begin; j < part_begin + part_num; j++) {
            PCacheValue* value = up_req->add_values();
            value->mutable_param()->set_partition_key(j);
            value->mutable_param()->set_last_version(j);
            value->mutable_param()->set_last_version_time(j);
            value->set_data_size(16);
            value->add_rows("0123456789abcdef"); //16 byte
        }
        up_req->set_cache_type(cache_type);
        _cache->update(up_req, up_res);
        LOG(WARNING) << "finish update data";
        st = up_res->status();
        SAFE_DELETE(up_req);
        SAFE_DELETE(up_res);
    }
    return st;
}

TEST_F(PartitionCacheTest, update_data) {
    init_default();
    PCacheStatus st = init_batch_data(1, 1, 1, CacheType::SQL_CACHE);
    ASSERT_TRUE(st == PCacheStatus::CACHE_OK);
    LOG(WARNING) << "clear cache";
    clear();
}

TEST_F(PartitionCacheTest, update_over_partition) {
    init_default();
    PCacheStatus st = init_batch_data(1, 1, config::query_cache_max_partition_count + 1,
                                      CacheType::PARTITION_CACHE);
    ASSERT_TRUE(st == PCacheStatus::PARAM_ERROR);
    clear();
}

TEST_F(PartitionCacheTest, cache_clear) {
    init_default();
    init_batch_data(1, 1, 1, CacheType::SQL_CACHE);
    _cache->clear(_clear_request, _clear_response);
    ASSERT_EQ(_cache->get_cache_size(), 0);
    clear();
}

TEST_F(PartitionCacheTest, fetch_simple_data) {
    init_default();
    init_batch_data(1, 1, 1, CacheType::SQL_CACHE);

    LOG(WARNING) << "finish init\n";
    set_sql_key(_fetch_request->mutable_sql_key(), 1, 1);
    PCacheParam* p1 = _fetch_request->add_params();
    p1->set_partition_key(1);
    p1->set_last_version(1);
    p1->set_last_version_time(1);
    LOG(WARNING) << "begin fetch\n";
    _cache->fetch(_fetch_request, _fetch_result);
    LOG(WARNING) << "finish fetch1\n";
    ASSERT_TRUE(_fetch_result->status() == PCacheStatus::CACHE_OK);
    ASSERT_EQ(_fetch_result->values_size(), 1);
    ASSERT_EQ(_fetch_result->values(0).rows(0), "0123456789abcdef");

    LOG(WARNING) << "finish fetch2\n";
    clear();
    LOG(WARNING) << "finish fetch3\n";
}

TEST_F(PartitionCacheTest, fetch_not_sqlid) {
    init_default();
    init_batch_data(1, 1, 1, CacheType::SQL_CACHE);

    set_sql_key(_fetch_request->mutable_sql_key(), 2, 2);
    PCacheParam* p1 = _fetch_request->add_params();
    p1->set_partition_key(1);
    p1->set_last_version(1);
    p1->set_last_version_time(1);
    _cache->fetch(_fetch_request, _fetch_result);
    ASSERT_TRUE(_fetch_result->status() == PCacheStatus::NO_SQL_KEY);

    clear();
}

TEST_F(PartitionCacheTest, fetch_range_data) {
    init_default();
    init_batch_data(1, 1, 3, CacheType::PARTITION_CACHE);

    set_sql_key(_fetch_request->mutable_sql_key(), 1, 1);
    PCacheParam* p1 = _fetch_request->add_params();
    p1->set_partition_key(2);
    p1->set_last_version(2);
    p1->set_last_version_time(2);
    PCacheParam* p2 = _fetch_request->add_params();
    p2->set_partition_key(3);
    p2->set_last_version(3);
    p2->set_last_version_time(3);
    _cache->fetch(_fetch_request, _fetch_result);

    ASSERT_TRUE(_fetch_result->status() == PCacheStatus::CACHE_OK);
    ASSERT_EQ(_fetch_result->values_size(), 2);

    clear();
}

TEST_F(PartitionCacheTest, fetch_invalid_right_range) {
    init_default();
    init_batch_data(1, 1, 3, CacheType::PARTITION_CACHE);

    set_sql_key(_fetch_request->mutable_sql_key(), 1, 1);
    PCacheParam* p1 = _fetch_request->add_params();
    p1->set_partition_key(4);
    p1->set_last_version(4);
    p1->set_last_version_time(4);
    PCacheParam* p2 = _fetch_request->add_params();
    p2->set_partition_key(5);
    p2->set_last_version(5);
    p2->set_last_version_time(5);
    _cache->fetch(_fetch_request, _fetch_result);

    ASSERT_TRUE(_fetch_result->status() == PCacheStatus::NO_PARTITION_KEY);
    ASSERT_EQ(_fetch_result->values_size(), 0);
    clear();
}

TEST_F(PartitionCacheTest, fetch_invalid_left_range) {
    init_default();
    init_batch_data(1, 1, 3, CacheType::PARTITION_CACHE);

    set_sql_key(_fetch_request->mutable_sql_key(), 1, 1);
    PCacheParam* p1 = _fetch_request->add_params();
    p1->set_partition_key(0);
    p1->set_last_version(0);
    p1->set_last_version_time(0);
    _cache->fetch(_fetch_request, _fetch_result);

    ASSERT_TRUE(_fetch_result->status() == PCacheStatus::NO_PARTITION_KEY);
    ASSERT_EQ(_fetch_result->values_size(), 0);
    clear();
}

TEST_F(PartitionCacheTest, fetch_invalid_key_range) {
    init_default();
    init_batch_data(1, 2, 1, CacheType::PARTITION_CACHE);

    set_sql_key(_fetch_request->mutable_sql_key(), 1, 1);
    PCacheParam* p1 = _fetch_request->add_params();
    p1->set_partition_key(1);
    p1->set_last_version(1);
    p1->set_last_version_time(1);

    PCacheParam* p2 = _fetch_request->add_params();
    p2->set_partition_key(2);
    p2->set_last_version(2);
    p2->set_last_version_time(2);

    PCacheParam* p3 = _fetch_request->add_params();
    p3->set_partition_key(3);
    p3->set_last_version(3);
    p3->set_last_version_time(3);
    _cache->fetch(_fetch_request, _fetch_result);
    ASSERT_TRUE(_fetch_result->status() == PCacheStatus::INVALID_KEY_RANGE);
    ASSERT_EQ(_fetch_result->values_size(), 0);
    clear();
}

TEST_F(PartitionCacheTest, fetch_data_overdue) {
    init_default();
    init_batch_data(1, 1, 1, CacheType::PARTITION_CACHE);

    set_sql_key(_fetch_request->mutable_sql_key(), 1, 1);
    PCacheParam* p1 = _fetch_request->add_params();
    p1->set_partition_key(1);
    //cache version is 1, request version is 2
    p1->set_last_version(2);
    p1->set_last_version_time(2);
    _cache->fetch(_fetch_request, _fetch_result);

    LOG(WARNING) << "fetch_data_overdue:" << _fetch_result->status();

    ASSERT_TRUE(_fetch_result->status() == PCacheStatus::DATA_OVERDUE);
    ASSERT_EQ(_fetch_result->values_size(), 0);

    clear();
}

TEST_F(PartitionCacheTest, prune_data) {
    init(1, 1);
    init_batch_data(LOOP_LESS_OR_MORE(10, 129), 1, 1024,
                    CacheType::PARTITION_CACHE);          // 16*1024*128=2M
    ASSERT_LE(_cache->get_cache_size(), 1 * 1024 * 1024); //cache_size <= 1M
    clear();
}

TEST_F(PartitionCacheTest, fetch_not_continue_partition) {
    init_default();
    init_batch_data(1, 1, 1, CacheType::PARTITION_CACHE);
    init_batch_data(1, 3, 1, CacheType::PARTITION_CACHE);
    set_sql_key(_fetch_request->mutable_sql_key(), 1, 1);
    PCacheParam* p1 = _fetch_request->add_params();
    p1->set_partition_key(1);
    p1->set_last_version(1);
    p1->set_last_version_time(1);
    PCacheParam* p2 = _fetch_request->add_params();
    p2->set_partition_key(2);
    p2->set_last_version(2);
    p2->set_last_version_time(2);
    PCacheParam* p3 = _fetch_request->add_params();
    p3->set_partition_key(3);
    p3->set_last_version(1);
    p3->set_last_version_time(1);
    _cache->fetch(_fetch_request, _fetch_result);
    ASSERT_TRUE(_fetch_result->status() == PCacheStatus::CACHE_OK);
    ASSERT_EQ(_fetch_result->values_size(), 2);
    ASSERT_EQ(_fetch_result->values(0).rows(0), "0123456789abcdef");
    ASSERT_EQ(_fetch_result->values(1).rows(0), "0123456789abcdef");
    clear();
}

TEST_F(PartitionCacheTest, update_sql_cache) {
    init_default();
    init_batch_data(1, 1, 1, CacheType::SQL_CACHE);
    set_sql_key(_fetch_request->mutable_sql_key(), 1, 1);
    PCacheParam* p1 = _fetch_request->add_params();
    p1->set_partition_key(1);
    p1->set_last_version(1);
    p1->set_last_version_time(1);
    _cache->fetch(_fetch_request, _fetch_result);
    ASSERT_TRUE(_fetch_result->status() == PCacheStatus::CACHE_OK);
    ASSERT_EQ(_fetch_result->values_size(), 1);
    ASSERT_EQ(_fetch_result->values(0).rows(0), "0123456789abcdef");
    // update sql cache and fetch cache again
    init_batch_data(1, 2, 1, CacheType::SQL_CACHE);
    set_sql_key(_fetch_request->mutable_sql_key(), 1, 1);
    p1 = _fetch_request->add_params();
    p1->set_partition_key(1);
    p1->set_last_version(1);
    p1->set_last_version_time(1);
    _cache->fetch(_fetch_request, _fetch_result);
    ASSERT_TRUE(_fetch_result->status() == PCacheStatus::NO_PARTITION_KEY);
    clear();
}

} // namespace doris

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    doris::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    doris::CpuInfo::init();
    return RUN_ALL_TESTS();
}
/* vim: set ts=4 sw=4 sts=4 tw=100 */
