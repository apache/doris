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

#include "recycler/recycler.h"

#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <random>
#include <string>
#include <thread>

#include "common/config.h"
#include "common/logging.h"
#include "common/simple_thread_pool.h"
#include "common/util.h"
#include "cpp/sync_point.h"
#include "meta-service/keys.h"
#include "meta-service/mem_txn_kv.h"
#include "meta-service/meta_service.h"
#include "meta-service/txn_kv_error.h"
#include "mock_resource_manager.h"
#include "rate-limiter/rate_limiter.h"
#include "recycler/checker.h"
#include "recycler/storage_vault_accessor.h"
#include "recycler/util.h"
#include "recycler/white_black_list.h"

using namespace doris;

static const std::string instance_id = "instance_id_recycle_test";
static int64_t current_time = 0;
static constexpr int64_t db_id = 1000;

static doris::cloud::RecyclerThreadPoolGroup thread_group;

int main(int argc, char** argv) {
    auto conf_file = "doris_cloud.conf";
    if (!cloud::config::init(conf_file, true)) {
        std::cerr << "failed to init config file, conf=" << conf_file << std::endl;
        return -1;
    }
    if (!cloud::init_glog("recycler")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }

    using namespace std::chrono;
    current_time = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();

    ::testing::InitGoogleTest(&argc, argv);
    auto s3_producer_pool = std::make_shared<SimpleThreadPool>(config::recycle_pool_parallelism);
    s3_producer_pool->start();
    auto recycle_tablet_pool = std::make_shared<SimpleThreadPool>(config::recycle_pool_parallelism);
    recycle_tablet_pool->start();
    auto group_recycle_function_pool =
            std::make_shared<SimpleThreadPool>(config::recycle_pool_parallelism);
    group_recycle_function_pool->start();
    thread_group =
            RecyclerThreadPoolGroup(std::move(s3_producer_pool), std::move(recycle_tablet_pool),
                                    std::move(group_recycle_function_pool));
    return RUN_ALL_TESTS();
}

namespace doris::cloud {

TEST(RecyclerTest, WhiteBlackList) {
    WhiteBlackList filter;
    EXPECT_FALSE(filter.filter_out("instance1"));
    EXPECT_FALSE(filter.filter_out("instance2"));
    filter.reset({}, {"instance1", "instance2"});
    EXPECT_TRUE(filter.filter_out("instance1"));
    EXPECT_TRUE(filter.filter_out("instance2"));
    EXPECT_FALSE(filter.filter_out("instance3"));
    filter.reset({"instance1", "instance2"}, {});
    EXPECT_FALSE(filter.filter_out("instance1"));
    EXPECT_FALSE(filter.filter_out("instance2"));
    EXPECT_TRUE(filter.filter_out("instance3"));
    filter.reset({"instance1"}, {"instance1"}); // whitelist overrides blacklist
    EXPECT_FALSE(filter.filter_out("instance1"));
    EXPECT_TRUE(filter.filter_out("instance2"));
}

static std::string next_rowset_id() {
    static int64_t cnt = 0;
    return std::to_string(++cnt);
}

static doris::RowsetMetaCloudPB create_rowset(const std::string& resource_id, int64_t tablet_id,
                                              int64_t index_id, int num_segments,
                                              const doris::TabletSchemaCloudPB& schema,
                                              int64_t txn_id = 0) {
    doris::RowsetMetaCloudPB rowset;
    rowset.set_rowset_id(0); // useless but required
    rowset.set_rowset_id_v2(next_rowset_id());
    rowset.set_txn_id(txn_id);
    rowset.set_num_segments(num_segments);
    rowset.set_tablet_id(tablet_id);
    rowset.set_index_id(index_id);
    rowset.set_resource_id(resource_id);
    rowset.set_schema_version(schema.schema_version());
    rowset.mutable_tablet_schema()->CopyFrom(schema);
    return rowset;
}

static int create_recycle_rowset(TxnKv* txn_kv, StorageVaultAccessor* accessor,
                                 const doris::RowsetMetaCloudPB& rowset, RecycleRowsetPB::Type type,
                                 bool write_schema_kv) {
    std::string key;
    std::string val;

    RecycleRowsetKeyInfo key_info {instance_id, rowset.tablet_id(), rowset.rowset_id_v2()};
    recycle_rowset_key(key_info, &key);

    RecycleRowsetPB rowset_pb;
    rowset_pb.set_creation_time(current_time);
    if (type != RecycleRowsetPB::UNKNOWN) {
        rowset_pb.set_type(type);
        rowset_pb.mutable_rowset_meta()->CopyFrom(rowset);
        if (write_schema_kv) { // Detach schema
            rowset_pb.mutable_rowset_meta()->set_allocated_tablet_schema(nullptr);
        }
    } else { // old version RecycleRowsetPB
        rowset_pb.set_tablet_id(rowset.tablet_id());
        rowset_pb.set_resource_id(rowset.resource_id());
    }
    rowset_pb.SerializeToString(&val);

    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != TxnErrorCode::TXN_OK) {
        return -1;
    }
    txn->put(key, val);
    std::string schema_key, schema_val;
    if (write_schema_kv) {
        meta_schema_key({instance_id, rowset.index_id(), rowset.schema_version()}, &schema_key);
        rowset.tablet_schema().SerializeToString(&schema_val);
        txn->put(schema_key, schema_val);
    }
    if (txn->commit() != TxnErrorCode::TXN_OK) {
        return -1;
    }

    for (int i = 0; i < rowset.num_segments(); ++i) {
        auto path = segment_path(rowset.tablet_id(), rowset.rowset_id_v2(), i);
        accessor->put_file(path, "");
        for (auto& index : rowset.tablet_schema().index()) {
            auto path = inverted_index_path_v1(rowset.tablet_id(), rowset.rowset_id_v2(), i,
                                               index.index_id(), index.index_suffix_name());
            accessor->put_file(path, "");
        }
    }
    return 0;
}

static int create_tmp_rowset(TxnKv* txn_kv, StorageVaultAccessor* accessor,
                             const doris::RowsetMetaCloudPB& rowset, bool write_schema_kv) {
    std::string key, val;
    meta_rowset_tmp_key({instance_id, rowset.txn_id(), rowset.tablet_id()}, &key);
    if (write_schema_kv) {
        auto rowset_copy = rowset;
        rowset_copy.clear_tablet_schema();
        rowset_copy.SerializeToString(&val);
    } else {
        rowset.SerializeToString(&val);
    }
    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != TxnErrorCode::TXN_OK) {
        return -1;
    }
    txn->put(key, val);
    std::string schema_key, schema_val;
    if (write_schema_kv) {
        meta_schema_key({instance_id, rowset.index_id(), rowset.schema_version()}, &schema_key);
        rowset.tablet_schema().SerializeToString(&schema_val);
        txn->put(schema_key, schema_val);
    }
    if (txn->commit() != TxnErrorCode::TXN_OK) {
        return -1;
    }

    for (int i = 0; i < rowset.num_segments(); ++i) {
        auto path = segment_path(rowset.tablet_id(), rowset.rowset_id_v2(), i);
        accessor->put_file(path, path);
        for (auto& index : rowset.tablet_schema().index()) {
            auto path = inverted_index_path_v1(rowset.tablet_id(), rowset.rowset_id_v2(), i,
                                               index.index_id(), index.index_suffix_name());
            accessor->put_file(path, path);
        }
    }
    return 0;
}

static int create_committed_rowset(TxnKv* txn_kv, StorageVaultAccessor* accessor,
                                   const std::string& resource_id, int64_t tablet_id,
                                   int64_t version, int num_segments = 1,
                                   int num_inverted_indexes = 1) {
    std::string key;
    std::string val;

    auto rowset_id = next_rowset_id();
    MetaRowsetKeyInfo key_info {instance_id, tablet_id, version};
    meta_rowset_key(key_info, &key);

    doris::RowsetMetaCloudPB rowset_pb;
    rowset_pb.set_rowset_id(0); // useless but required
    rowset_pb.set_rowset_id_v2(rowset_id);
    rowset_pb.set_num_segments(num_segments);
    rowset_pb.set_tablet_id(tablet_id);
    rowset_pb.set_resource_id(resource_id);
    rowset_pb.set_creation_time(current_time);
    if (num_inverted_indexes > 0) {
        auto schema = rowset_pb.mutable_tablet_schema();
        for (int i = 0; i < num_inverted_indexes; ++i) {
            schema->add_index()->set_index_id(i);
        }
    }
    rowset_pb.SerializeToString(&val);

    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != TxnErrorCode::TXN_OK) {
        return -1;
    }
    txn->put(key, val);
    if (txn->commit() != TxnErrorCode::TXN_OK) {
        return -1;
    }

    for (int i = 0; i < num_segments; ++i) {
        auto path = segment_path(tablet_id, rowset_id, i);
        accessor->put_file(path, "");
        for (int j = 0; j < num_inverted_indexes; ++j) {
            auto path = inverted_index_path_v1(tablet_id, rowset_id, i, j, "");
            accessor->put_file(path, "");
        }
    }
    return 0;
}

static int create_tablet(TxnKv* txn_kv, int64_t table_id, int64_t index_id, int64_t partition_id,
                         int64_t tablet_id) {
    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != TxnErrorCode::TXN_OK) {
        return -1;
    }
    auto key = meta_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
    doris::TabletMetaCloudPB tablet_meta;
    tablet_meta.set_tablet_id(tablet_id);
    auto val = tablet_meta.SerializeAsString();
    txn->put(key, val);
    key = meta_tablet_idx_key({instance_id, tablet_id});
    txn->put(key, val); // val is not necessary
    key = stats_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
    txn->put(key, val); // val is not necessary
    key = job_tablet_key({instance_id, table_id, index_id, partition_id, tablet_id});
    txn->put(key, val); // val is not necessary
    if (txn->commit() != TxnErrorCode::TXN_OK) {
        return -1;
    }
    return 0;
}

static int create_recycle_partiton(TxnKv* txn_kv, int64_t table_id, int64_t partition_id,
                                   const std::vector<int64_t>& index_ids) {
    std::string key;
    std::string val;

    RecyclePartKeyInfo key_info {instance_id, partition_id};
    recycle_partition_key(key_info, &key);

    RecyclePartitionPB partition_pb;
    partition_pb.set_db_id(db_id);
    partition_pb.set_table_id(table_id);
    for (auto index_id : index_ids) {
        partition_pb.add_index_id(index_id);
    }
    partition_pb.set_creation_time(current_time);
    partition_pb.set_state(RecyclePartitionPB::DROPPED);
    partition_pb.SerializeToString(&val);

    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != TxnErrorCode::TXN_OK) {
        return -1;
    }
    txn->put(key, val);
    if (txn->commit() != TxnErrorCode::TXN_OK) {
        return -1;
    }
    return 0;
}

static int create_partition_version_kv(TxnKv* txn_kv, int64_t table_id, int64_t partition_id) {
    auto key = partition_version_key({instance_id, db_id, table_id, partition_id});
    VersionPB version;
    version.set_version(1);
    auto val = version.SerializeAsString();
    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != TxnErrorCode::TXN_OK) {
        return -1;
    }
    txn->put(key, val);
    if (txn->commit() != TxnErrorCode::TXN_OK) {
        return -1;
    }
    return 0;
}

static int create_table_version_kv(TxnKv* txn_kv, int64_t table_id) {
    auto key = table_version_key({instance_id, db_id, table_id});
    std::string val(sizeof(int64_t), 0);
    *reinterpret_cast<int64_t*>(val.data()) = (int64_t)1;
    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != TxnErrorCode::TXN_OK) {
        return -1;
    }
    txn->put(key, val);
    if (txn->commit() != TxnErrorCode::TXN_OK) {
        return -1;
    }
    return 0;
}

static int create_txn_label_kv(TxnKv* txn_kv, std::string label, int64_t db_id) {
    std::string txn_label_key_;
    std::string txn_label_val;
    auto keyinfo = TxnLabelKeyInfo({instance_id, db_id, label});
    txn_label_key(keyinfo, &txn_label_key_);
    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != TxnErrorCode::TXN_OK) {
        return -1;
    }
    txn->put(txn_label_key_, label);
    if (txn->commit() != TxnErrorCode::TXN_OK) {
        return -1;
    }
    return 0;
}

static int create_recycle_index(TxnKv* txn_kv, int64_t table_id, int64_t index_id) {
    std::string key;
    std::string val;

    RecycleIndexKeyInfo key_info {instance_id, index_id};
    recycle_index_key(key_info, &key);

    RecycleIndexPB index_pb;

    index_pb.set_table_id(table_id);
    index_pb.set_creation_time(current_time);
    index_pb.set_state(RecycleIndexPB::DROPPED);
    index_pb.SerializeToString(&val);

    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != TxnErrorCode::TXN_OK) {
        return -1;
    }
    txn->put(key, val);
    if (txn->commit() != TxnErrorCode::TXN_OK) {
        return -1;
    }
    return 0;
}

static int get_txn_info(std::shared_ptr<TxnKv> txn_kv, std::string instance_id, int64_t db_id,
                        int64_t txn_id, TxnInfoPB& txn_info_pb) {
    std::string txn_inf_key;
    std::string txn_inf_val;
    TxnInfoKeyInfo txn_inf_key_info {instance_id, db_id, txn_id};

    LOG(INFO) << instance_id << "|" << db_id << "|" << txn_id;

    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != TxnErrorCode::TXN_OK) {
        return -1;
    }
    txn_info_key(txn_inf_key_info, &txn_inf_key);
    LOG(INFO) << "txn_inf_key:" << hex(txn_inf_key);
    TxnErrorCode err = txn->get(txn_inf_key, &txn_inf_val);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "txn->get failed, err=" << err;
        return -2;
    }

    if (!txn_info_pb.ParseFromString(txn_inf_val)) {
        LOG(WARNING) << "ParseFromString failed";
        return -3;
    }
    LOG(INFO) << "txn_info_pb" << txn_info_pb.DebugString();
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "txn->commit failed, err=" << err;
        return -4;
    }
    return 0;
}

static int check_recycle_txn_keys(std::shared_ptr<TxnKv> txn_kv, std::string instance_id,
                                  int64_t db_id, int64_t txn_id, const std::string& label) {
    std::string txn_inf_key;
    std::string txn_inf_val;
    TxnInfoKeyInfo txn_inf_key_info {instance_id, db_id, txn_id};

    LOG(INFO) << instance_id << "|" << db_id << "|" << txn_id;

    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != TxnErrorCode::TXN_OK) {
        return -1;
    }
    txn_info_key(txn_inf_key_info, &txn_inf_key);
    TxnErrorCode err = txn->get(txn_inf_key, &txn_inf_val);
    if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        return -2;
    }

    std::string label_key, label_val;
    txn_label_key({instance_id, db_id, label}, &label_key);
    err = txn->get(label_key, &label_val);
    if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        return -3;
    }

    std::string index_key, index_val;
    index_key = txn_index_key({instance_id, txn_id});
    err = txn->get(index_key, &index_val);
    if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        return -4;
    }

    std::string running_key;
    std::string running_value;
    TxnRunningKeyInfo running_key_info {instance_id, db_id, txn_id};
    txn_running_key(running_key_info, &running_key);
    err = txn->get(running_key, &running_value);
    if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        return -5;
    }

    std::string rec_txn_key;
    std::string rec_txn_val;
    RecycleTxnKeyInfo recycle_txn_key_info {instance_id, db_id, txn_id};
    recycle_txn_key(recycle_txn_key_info, &rec_txn_key);
    err = txn->get(rec_txn_key, &rec_txn_val);
    if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        return -6;
    }

    return 0;
}

static int create_instance(const std::string& internal_stage_id,
                           const std::string& external_stage_id, InstanceInfoPB& instance_info) {
    // create internal stage
    {
        std::string s3_prefix = "internal_prefix";
        std::string stage_prefix = fmt::format("{}/stage/root/{}/", s3_prefix, internal_stage_id);
        ObjectStoreInfoPB object_info;
        object_info.set_id(internal_stage_id); // test use accessor_map_ in recycle

        StagePB internal_stage;
        internal_stage.set_type(StagePB::INTERNAL);
        internal_stage.set_stage_id(internal_stage_id);
        ObjectStoreInfoPB internal_object_info;
        internal_object_info.set_prefix(stage_prefix);
        internal_object_info.set_id("0");
        internal_stage.mutable_obj_info()->CopyFrom(internal_object_info);

        instance_info.add_obj_info()->CopyFrom(object_info);
        instance_info.add_stages()->CopyFrom(internal_stage);
    }

    // create external stage
    {
        ObjectStoreInfoPB object_info;
        object_info.set_id(external_stage_id);

        StagePB external_stage;
        external_stage.set_type(StagePB::EXTERNAL);
        external_stage.set_stage_id(external_stage_id);
        external_stage.mutable_obj_info()->CopyFrom(object_info);

        instance_info.add_obj_info()->CopyFrom(object_info);
        instance_info.add_stages()->CopyFrom(external_stage);
    }

    instance_info.set_instance_id(instance_id);
    return 0;
}

static int create_copy_job(TxnKv* txn_kv, const std::string& stage_id, int64_t table_id,
                           StagePB::StageType stage_type, CopyJobPB::JobStatus job_status,
                           std::vector<ObjectFilePB> object_files, int64_t timeout_time,
                           int64_t start_time = 0, int64_t finish_time = 0) {
    std::string key;
    std::string val;
    CopyJobKeyInfo key_info {instance_id, stage_id, table_id, "copy_id", 0};
    copy_job_key(key_info, &key);

    CopyJobPB copy_job;
    copy_job.set_stage_type(stage_type);
    copy_job.set_job_status(job_status);
    copy_job.set_timeout_time_ms(timeout_time);
    if (start_time != 0) {
        copy_job.set_start_time_ms(start_time);
    }
    if (finish_time != 0) {
        copy_job.set_finish_time_ms(finish_time);
    }
    for (const auto& file : object_files) {
        copy_job.add_object_files()->CopyFrom(file);
    }
    copy_job.SerializeToString(&val);

    std::vector<std::string> file_keys;
    std::string file_val;
    CopyFilePB copy_file;
    copy_file.set_copy_id("copy_id");
    copy_file.set_group_id(0);
    file_val = copy_file.SerializeAsString();

    // create job files
    for (const auto& file : object_files) {
        CopyFileKeyInfo file_info {instance_id, stage_id, table_id, file.relative_path(),
                                   file.etag()};
        std::string file_key;
        copy_file_key(file_info, &file_key);
        file_keys.push_back(file_key);
    }

    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != TxnErrorCode::TXN_OK) {
        return -1;
    }
    txn->put(key, val);
    for (const auto& file_key : file_keys) {
        txn->put(file_key, file_val);
    }
    if (txn->commit() != TxnErrorCode::TXN_OK) {
        return -1;
    }
    return 0;
}

static int copy_job_exists(TxnKv* txn_kv, const std::string& stage_id, int64_t table_id,
                           bool* exist) {
    std::string key;
    std::string val;
    CopyJobKeyInfo key_info {instance_id, stage_id, table_id, "copy_id", 0};
    copy_job_key(key_info, &key);

    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != TxnErrorCode::TXN_OK) {
        return -1;
    }
    TxnErrorCode err = txn->get(key, &val);
    if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        return -1;
    }
    *exist = err == TxnErrorCode::TXN_OK;
    return 0;
}

static int create_object_files(StorageVaultAccessor* accessor,
                               std::vector<ObjectFilePB>* object_files) {
    for (auto& file : *object_files) {
        auto key = file.relative_path();
        if (accessor->put_file(key, "") != 0) {
            return -1;
        }
        file.set_etag("");
    }
    return 0;
}

static int get_copy_file_num(TxnKv* txn_kv, const std::string& stage_id, int64_t table_id,
                             int* file_num) {
    *file_num = 0;
    std::string key0;
    std::string key1;
    CopyFileKeyInfo key_info0 {instance_id, stage_id, table_id, "", ""};
    CopyFileKeyInfo key_info1 {instance_id, stage_id, table_id + 1, "", ""};
    copy_file_key(key_info0, &key0);
    copy_file_key(key_info1, &key1);

    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != TxnErrorCode::TXN_OK) {
        return -1;
    }
    std::unique_ptr<RangeGetIterator> it;
    do {
        if (txn->get(key0, key1, &it) != TxnErrorCode::TXN_OK) {
            return -1;
        }
        while (it->has_next()) {
            it->next();
            ++(*file_num);
        }
        key0.push_back('\x00');
    } while (it->more());
    return 0;
}

TEST(RecyclerTest, recycle_empty) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    auto obj_info = instance.add_obj_info();
    obj_info->set_id("recycle_empty");
    obj_info->set_ak(config::test_s3_ak);
    obj_info->set_sk(config::test_s3_sk);
    obj_info->set_endpoint(config::test_s3_endpoint);
    obj_info->set_region(config::test_s3_region);
    obj_info->set_bucket(config::test_s3_bucket);
    obj_info->set_prefix("recycle_empty");

    InstanceRecycler recycler(txn_kv, instance, thread_group);
    ASSERT_EQ(recycler.init(), 0);

    ASSERT_EQ(recycler.recycle_rowsets(), 0);
}

TEST(RecyclerTest, recycle_rowsets) {
    config::retention_seconds = 0;
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    auto obj_info = instance.add_obj_info();
    obj_info->set_id("recycle_rowsets");
    obj_info->set_ak(config::test_s3_ak);
    obj_info->set_sk(config::test_s3_sk);
    obj_info->set_endpoint(config::test_s3_endpoint);
    obj_info->set_region(config::test_s3_region);
    obj_info->set_bucket(config::test_s3_bucket);
    obj_info->set_prefix("recycle_rowsets");

    config::instance_recycler_worker_pool_size = 1;

    int insert_no_inverted_index = 0;
    int insert_inverted_index = 0;
    auto sp = SyncPoint::get_instance();
    std::unique_ptr<int, std::function<void(int*)>> defer(
            (int*)0x01, [](int*) { SyncPoint::get_instance()->clear_all_call_backs(); });
    sp->set_call_back("InvertedIndexIdCache::insert1", [&](auto&&) { ++insert_no_inverted_index; });
    sp->set_call_back("InvertedIndexIdCache::insert2", [&](auto&&) { ++insert_inverted_index; });
    sp->enable_processing();

    InstanceRecycler recycler(txn_kv, instance, thread_group);
    ASSERT_EQ(recycler.init(), 0);

    std::vector<doris::TabletSchemaCloudPB> schemas;
    for (int i = 0; i < 5; ++i) {
        auto& schema = schemas.emplace_back();
        schema.set_schema_version(i);
        schema.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V1);
        for (int j = 0; j < i; ++j) {
            auto index = schema.add_index();
            index->set_index_id(j);
            index->set_index_type(IndexType::INVERTED);
        }
    }

    auto accessor = recycler.accessor_map_.begin()->second;
    constexpr int index_id = 10001, tablet_id = 10002;
    for (int i = 0; i < 1000; ++i) {
        auto rowset = create_rowset("recycle_rowsets", tablet_id, index_id, 5, schemas[i % 5]);
        create_recycle_rowset(
                txn_kv.get(), accessor.get(), rowset,
                static_cast<RecycleRowsetPB::Type>(i % (RecycleRowsetPB::Type_MAX + 1)), i & 1);
    }
    for (int i = 0; i < 1000; ++i) {
        auto rowset = create_rowset("recycle_rowsets", tablet_id, index_id, 5, schemas[i % 5]);
        create_recycle_rowset(txn_kv.get(), accessor.get(), rowset, RecycleRowsetPB::COMPACT, true);
    }

    ASSERT_EQ(recycler.recycle_rowsets(), 0);

    // check rowset does not exist on obj store
    std::unique_ptr<ListIterator> list_iter;
    ASSERT_EQ(0, accessor->list_directory(tablet_path_prefix(tablet_id), &list_iter));
    EXPECT_FALSE(list_iter->has_next());
    // check all recycle rowset kv have been deleted
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::unique_ptr<RangeGetIterator> it;
    auto begin_key = recycle_key_prefix(instance_id);
    auto end_key = recycle_key_prefix(instance_id + '\xff');
    ASSERT_EQ(txn->get(begin_key, end_key, &it), TxnErrorCode::TXN_OK);
    EXPECT_EQ(it->size(), 0);
    // Check InvertedIndexIdCache
    EXPECT_EQ(insert_inverted_index, 4);
    EXPECT_EQ(insert_no_inverted_index, 1);
}

TEST(RecyclerTest, bench_recycle_rowsets) {
    config::retention_seconds = 0;
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    auto obj_info = instance.add_obj_info();
    obj_info->set_id("recycle_rowsets");
    obj_info->set_ak(config::test_s3_ak);
    obj_info->set_sk(config::test_s3_sk);
    obj_info->set_endpoint(config::test_s3_endpoint);
    obj_info->set_region(config::test_s3_region);
    obj_info->set_bucket(config::test_s3_bucket);
    obj_info->set_prefix("recycle_rowsets");

    config::instance_recycler_worker_pool_size = 10;
    config::recycle_task_threshold_seconds = 0;
    InstanceRecycler recycler(txn_kv, instance, thread_group);
    ASSERT_EQ(recycler.init(), 0);

    auto sp = SyncPoint::get_instance();
    std::unique_ptr<int, std::function<void(int*)>> defer(
            (int*)0x01, [](int*) { SyncPoint::get_instance()->clear_all_call_backs(); });
    sp->set_call_back("memkv::Transaction::get", [](auto&& args) {
        auto* limit = try_any_cast<int*>(args[0]);
        *limit = 100;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    });
    sp->set_call_back("MockAccessor::delete_files", [&](auto&& args) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        bool found = recycler.check_recycle_tasks();
        ASSERT_EQ(found, true);
    });
    sp->set_call_back("MockAccessor::delete_prefix",
                      [&](auto&&) { std::this_thread::sleep_for(std::chrono::milliseconds(20)); });
    sp->enable_processing();

    std::vector<doris::TabletSchemaCloudPB> schemas;
    for (int i = 0; i < 5; ++i) {
        auto& schema = schemas.emplace_back();
        schema.set_schema_version(i);
        schema.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V1);
        for (int j = 0; j < i; ++j) {
            auto index = schema.add_index();
            index->set_index_id(j);
            index->set_index_type(IndexType::INVERTED);
        }
    }

    auto accessor = recycler.accessor_map_.begin()->second;
    constexpr int index_id = 10001, tablet_id = 10002;
    for (int i = 0; i < 2000; ++i) {
        auto rowset = create_rowset("recycle_rowsets", tablet_id, index_id, 5, schemas[i % 5]);
        create_recycle_rowset(txn_kv.get(), accessor.get(), rowset,
                              i % 10 < 2 ? RecycleRowsetPB::PREPARE : RecycleRowsetPB::COMPACT,
                              i & 1);
    }

    ASSERT_EQ(recycler.recycle_rowsets(), 0);
    ASSERT_EQ(recycler.check_recycle_tasks(), false);

    // check rowset does not exist on obj store
    std::unique_ptr<ListIterator> list_iter;
    ASSERT_EQ(0, accessor->list_directory(tablet_path_prefix(tablet_id), &list_iter));
    ASSERT_FALSE(list_iter->has_next());
    // check all recycle rowset kv have been deleted
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::unique_ptr<RangeGetIterator> it;
    auto begin_key = recycle_key_prefix(instance_id);
    auto end_key = recycle_key_prefix(instance_id + '\xff');
    ASSERT_EQ(txn->get(begin_key, end_key, &it), TxnErrorCode::TXN_OK);
    ASSERT_EQ(it->size(), 0);
}

TEST(RecyclerTest, recycle_tmp_rowsets) {
    config::retention_seconds = 0;
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    auto obj_info = instance.add_obj_info();
    obj_info->set_id("recycle_tmp_rowsets");
    obj_info->set_ak(config::test_s3_ak);
    obj_info->set_sk(config::test_s3_sk);
    obj_info->set_endpoint(config::test_s3_endpoint);
    obj_info->set_region(config::test_s3_region);
    obj_info->set_bucket(config::test_s3_bucket);
    obj_info->set_prefix("recycle_tmp_rowsets");

    int insert_no_inverted_index = 0;
    int insert_inverted_index = 0;
    auto sp = SyncPoint::get_instance();
    std::unique_ptr<int, std::function<void(int*)>> defer(
            (int*)0x01, [](int*) { SyncPoint::get_instance()->clear_all_call_backs(); });
    sp->set_call_back("InvertedIndexIdCache::insert1", [&](auto&&) { ++insert_no_inverted_index; });
    sp->set_call_back("InvertedIndexIdCache::insert2", [&](auto&&) { ++insert_inverted_index; });
    sp->enable_processing();

    InstanceRecycler recycler(txn_kv, instance, thread_group);
    ASSERT_EQ(recycler.init(), 0);

    std::vector<doris::TabletSchemaCloudPB> schemas;
    for (int i = 0; i < 5; ++i) {
        auto& schema = schemas.emplace_back();
        schema.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V1);
        schema.set_schema_version(i);
        for (int j = 0; j < i; ++j) {
            auto index = schema.add_index();
            index->set_index_id(j);
            index->set_index_type(IndexType::INVERTED);
        }
    }

    auto accessor = recycler.accessor_map_.begin()->second;
    int64_t txn_id_base = 114115;
    int64_t tablet_id_base = 10015;
    int64_t index_id_base = 1000;
    for (int i = 0; i < 100; ++i) {
        int64_t txn_id = txn_id_base + i;
        for (int j = 0; j < 20; ++j) {
            auto rowset = create_rowset("recycle_tmp_rowsets", tablet_id_base + j,
                                        index_id_base + j % 4, 5, schemas[i % 5], txn_id);
            create_tmp_rowset(txn_kv.get(), accessor.get(), rowset, i & 1);
        }
    }

    ASSERT_EQ(recycler.recycle_tmp_rowsets(), 0);

    // check rowset does not exist on obj store
    std::unique_ptr<ListIterator> list_iter;
    ASSERT_EQ(0, accessor->list_directory("data/", &list_iter));
    ASSERT_FALSE(list_iter->has_next());
    // check all tmp rowset kv have been deleted
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::unique_ptr<RangeGetIterator> it;
    auto begin_key = meta_rowset_tmp_key({instance_id, 0, 0});
    auto end_key = meta_rowset_tmp_key({instance_id, INT64_MAX, 0});
    ASSERT_EQ(txn->get(begin_key, end_key, &it), TxnErrorCode::TXN_OK);
    ASSERT_EQ(it->size(), 0);
    // Check InvertedIndexIdCache
    EXPECT_EQ(insert_inverted_index, 16);
    EXPECT_EQ(insert_no_inverted_index, 4);
}

TEST(RecyclerTest, recycle_tablet) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    auto obj_info = instance.add_obj_info();
    obj_info->set_id("recycle_tablet");
    obj_info->set_ak(config::test_s3_ak);
    obj_info->set_sk(config::test_s3_sk);
    obj_info->set_endpoint(config::test_s3_endpoint);
    obj_info->set_region(config::test_s3_region);
    obj_info->set_bucket(config::test_s3_bucket);
    obj_info->set_prefix("recycle_tablet");

    InstanceRecycler recycler(txn_kv, instance, thread_group);
    ASSERT_EQ(recycler.init(), 0);

    std::vector<doris::TabletSchemaCloudPB> schemas;
    for (int i = 0; i < 5; ++i) {
        auto& schema = schemas.emplace_back();
        schema.set_schema_version(i);
        for (int j = 0; j < i; ++j) {
            auto index = schema.add_index();
            index->set_index_id(j);
            index->set_index_type(IndexType::INVERTED);
        }
    }

    constexpr int table_id = 10000, index_id = 10001, partition_id = 10002, tablet_id = 10003;
    auto accessor = recycler.accessor_map_.begin()->second;
    create_tablet(txn_kv.get(), table_id, index_id, partition_id, tablet_id);
    for (int i = 0; i < 500; ++i) {
        auto rowset = create_rowset("recycle_tablet", tablet_id, index_id, 5, schemas[i % 5]);
        create_recycle_rowset(txn_kv.get(), accessor.get(), rowset,
                              i % 10 < 2 ? RecycleRowsetPB::PREPARE : RecycleRowsetPB::COMPACT,
                              i & 1);
    }
    for (int i = 0; i < 500; ++i) {
        create_committed_rowset(txn_kv.get(), accessor.get(), "recycle_tablet", tablet_id, i);
    }

    ASSERT_EQ(0, recycler.recycle_tablets(table_id, index_id));

    // check rowset does not exist on s3
    std::unique_ptr<ListIterator> list_iter;
    ASSERT_EQ(0, accessor->list_directory(tablet_path_prefix(tablet_id), &list_iter));
    ASSERT_FALSE(list_iter->has_next());
    // check all related kv have been deleted
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::unique_ptr<RangeGetIterator> it;
    // meta_tablet_key, meta_tablet_idx_key, meta_rowset_key
    auto begin_key = meta_key_prefix(instance_id);
    auto end_key = meta_key_prefix(instance_id + '\xff');
    ASSERT_EQ(txn->get(begin_key, end_key, &it), TxnErrorCode::TXN_OK);
    ASSERT_EQ(it->size(), 0);
    // job_tablet_key
    begin_key = job_tablet_key({instance_id, table_id, 0, 0, 0});
    end_key = job_tablet_key({instance_id, table_id + 1, 0, 0, 0});
    ASSERT_EQ(txn->get(begin_key, end_key, &it), TxnErrorCode::TXN_OK);
    ASSERT_EQ(it->size(), 0);
    // stats_tablet_key
    begin_key = stats_tablet_key({instance_id, table_id, 0, 0, 0});
    end_key = stats_tablet_key({instance_id, table_id + 1, 0, 0, 0});
    ASSERT_EQ(txn->get(begin_key, end_key, &it), TxnErrorCode::TXN_OK);
    ASSERT_EQ(it->size(), 0);
    // recycle_rowset_key
    begin_key = recycle_key_prefix(instance_id);
    end_key = recycle_key_prefix(instance_id + '\xff');
    ASSERT_EQ(txn->get(begin_key, end_key, &it), TxnErrorCode::TXN_OK);
    ASSERT_EQ(it->size(), 0);
}

TEST(RecyclerTest, recycle_indexes) {
    config::retention_seconds = 0;
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    auto obj_info = instance.add_obj_info();
    obj_info->set_id("recycle_indexes");
    obj_info->set_ak(config::test_s3_ak);
    obj_info->set_sk(config::test_s3_sk);
    obj_info->set_endpoint(config::test_s3_endpoint);
    obj_info->set_region(config::test_s3_region);
    obj_info->set_bucket(config::test_s3_bucket);
    obj_info->set_prefix("recycle_indexes");

    InstanceRecycler recycler(txn_kv, instance, thread_group);
    ASSERT_EQ(recycler.init(), 0);

    std::vector<doris::TabletSchemaCloudPB> schemas;
    for (int i = 0; i < 5; ++i) {
        auto& schema = schemas.emplace_back();
        schema.set_schema_version(i);
        schema.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V1);
        for (int j = 0; j < i; ++j) {
            auto index = schema.add_index();
            index->set_index_id(j);
            index->set_index_type(IndexType::INVERTED);
        }
    }

    constexpr int table_id = 10000, index_id = 10001, partition_id = 10002;
    auto accessor = recycler.accessor_map_.begin()->second;
    int64_t tablet_id_base = 10100;
    int64_t txn_id_base = 114115;
    for (int i = 0; i < 100; ++i) {
        int64_t tablet_id = tablet_id_base + i;
        create_tablet(txn_kv.get(), table_id, index_id, partition_id, tablet_id);
        for (int j = 0; j < 10; ++j) {
            auto rowset = create_rowset("recycle_tablet", tablet_id, index_id, 5, schemas[j % 5]);
            create_recycle_rowset(txn_kv.get(), accessor.get(), rowset,
                                  j % 10 < 2 ? RecycleRowsetPB::PREPARE : RecycleRowsetPB::COMPACT,
                                  j & 1);
            auto tmp_rowset = create_rowset("recycle_tmp_rowsets", tablet_id, index_id, 5,
                                            schemas[j % 5], txn_id_base + j);
            create_tmp_rowset(txn_kv.get(), accessor.get(), tmp_rowset, j & 1);
        }
        for (int j = 0; j < 10; ++j) {
            create_committed_rowset(txn_kv.get(), accessor.get(), "recycle_indexes", tablet_id, j);
        }
    }
    create_recycle_index(txn_kv.get(), table_id, index_id);
    ASSERT_EQ(recycler.recycle_indexes(), 0);

    // check rowset does not exist on s3
    std::unique_ptr<ListIterator> list_iter;
    ASSERT_EQ(0, accessor->list_directory("data/", &list_iter));
    ASSERT_FALSE(list_iter->has_next());
    // check all related kv have been deleted
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::unique_ptr<RangeGetIterator> it;
    // meta_rowset_key
    auto begin_key = meta_rowset_key({instance_id, 0, 0});
    auto end_key = meta_rowset_key({instance_id, INT64_MAX, 0});
    ASSERT_EQ(txn->get(begin_key, end_key, &it), TxnErrorCode::TXN_OK);
    ASSERT_EQ(it->size(), 0);
    // meta_rowset_tmp_key
    begin_key = meta_rowset_tmp_key({instance_id, 0, 0});
    end_key = meta_rowset_tmp_key({instance_id, INT64_MAX, 0});
    ASSERT_EQ(txn->get(begin_key, end_key, &it), TxnErrorCode::TXN_OK);
    ASSERT_EQ(it->size(), 1000);
    // meta_tablet_idx_key
    begin_key = meta_tablet_idx_key({instance_id, 0});
    end_key = meta_tablet_idx_key({instance_id, INT64_MAX});
    ASSERT_EQ(txn->get(begin_key, end_key, &it), TxnErrorCode::TXN_OK);
    ASSERT_EQ(it->size(), 0);
    // meta_tablet_key
    begin_key = meta_tablet_key({instance_id, 0, 0, 0, 0});
    end_key = meta_tablet_key({instance_id, INT64_MAX, 0, 0, 0});
    ASSERT_EQ(txn->get(begin_key, end_key, &it), TxnErrorCode::TXN_OK);
    ASSERT_EQ(it->size(), 0);
    // meta_schema_key
    begin_key = meta_schema_key({instance_id, 0, 0});
    end_key = meta_schema_key({instance_id, INT64_MAX, 0});
    ASSERT_EQ(txn->get(begin_key, end_key, &it), TxnErrorCode::TXN_OK);
    ASSERT_EQ(it->size(), 0);
    // job_tablet_key
    begin_key = job_tablet_key({instance_id, table_id, 0, 0, 0});
    end_key = job_tablet_key({instance_id, table_id + 1, 0, 0, 0});
    ASSERT_EQ(txn->get(begin_key, end_key, &it), TxnErrorCode::TXN_OK);
    ASSERT_EQ(it->size(), 0);
    // stats_tablet_key
    begin_key = stats_tablet_key({instance_id, table_id, 0, 0, 0});
    end_key = stats_tablet_key({instance_id, table_id + 1, 0, 0, 0});
    ASSERT_EQ(txn->get(begin_key, end_key, &it), TxnErrorCode::TXN_OK);
    ASSERT_EQ(it->size(), 0);
    // recycle_rowset_key
    begin_key = recycle_key_prefix(instance_id);
    end_key = recycle_key_prefix(instance_id + '\xff');
    ASSERT_EQ(txn->get(begin_key, end_key, &it), TxnErrorCode::TXN_OK);
    ASSERT_EQ(it->size(), 0);

    // Test recycle tmp rowsets after recycle indexes
    ASSERT_EQ(recycler.recycle_tmp_rowsets(), 0);
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    begin_key = meta_rowset_tmp_key({instance_id, 0, 0});
    end_key = meta_rowset_tmp_key({instance_id, INT64_MAX, 0});
    ASSERT_EQ(txn->get(begin_key, end_key, &it), TxnErrorCode::TXN_OK);
    ASSERT_EQ(it->size(), 0);
}

TEST(RecyclerTest, recycle_partitions) {
    config::retention_seconds = 0;
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    auto obj_info = instance.add_obj_info();
    obj_info->set_id("recycle_partitions");
    obj_info->set_ak(config::test_s3_ak);
    obj_info->set_sk(config::test_s3_sk);
    obj_info->set_endpoint(config::test_s3_endpoint);
    obj_info->set_region(config::test_s3_region);
    obj_info->set_bucket(config::test_s3_bucket);
    obj_info->set_prefix("recycle_partitions");

    InstanceRecycler recycler(txn_kv, instance, thread_group);
    ASSERT_EQ(recycler.init(), 0);

    std::vector<doris::TabletSchemaCloudPB> schemas;
    for (int i = 0; i < 5; ++i) {
        auto& schema = schemas.emplace_back();
        schema.set_schema_version(i);
        schema.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V1);
        for (int j = 0; j < i; ++j) {
            auto index = schema.add_index();
            index->set_index_id(j);
            index->set_index_type(IndexType::INVERTED);
        }
    }

    constexpr int table_id = 10000, partition_id = 30020;
    auto accessor = recycler.accessor_map_.begin()->second;
    std::vector<int64_t> index_ids {20200, 20201, 20202, 20203, 20204};

    int64_t tablet_id_base = 10100;
    for (auto index_id : index_ids) {
        for (int i = 0; i < 20; ++i) {
            int64_t tablet_id = tablet_id_base + i;
            create_tablet(txn_kv.get(), table_id, index_id, partition_id, tablet_id);
            for (int j = 0; j < 10; ++j) {
                auto rowset =
                        create_rowset("recycle_tablet", tablet_id, index_id, 5, schemas[j % 5]);
                create_recycle_rowset(
                        txn_kv.get(), accessor.get(), rowset,
                        j % 10 < 2 ? RecycleRowsetPB::PREPARE : RecycleRowsetPB::COMPACT, j & 1);
            }
            for (int j = 0; j < 10; ++j) {
                create_committed_rowset(txn_kv.get(), accessor.get(), "recycle_partitions",
                                        tablet_id, j);
            }
        }
    }
    create_recycle_partiton(txn_kv.get(), table_id, partition_id, index_ids);
    ASSERT_EQ(recycler.recycle_partitions(), 0);

    // check rowset does not exist on s3
    std::unique_ptr<ListIterator> list_iter;
    ASSERT_EQ(0, accessor->list_directory("data/", &list_iter));
    ASSERT_FALSE(list_iter->has_next());
    // check all related kv have been deleted
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::unique_ptr<RangeGetIterator> it;
    // meta_rowset_key
    auto begin_key = meta_rowset_key({instance_id, 0, 0});
    auto end_key = meta_rowset_key({instance_id, INT64_MAX, 0});
    ASSERT_EQ(txn->get(begin_key, end_key, &it), TxnErrorCode::TXN_OK);
    ASSERT_EQ(it->size(), 0);
    // meta_rowset_tmp_key
    begin_key = meta_rowset_tmp_key({instance_id, 0, 0});
    end_key = meta_rowset_tmp_key({instance_id, INT64_MAX, 0});
    ASSERT_EQ(txn->get(begin_key, end_key, &it), TxnErrorCode::TXN_OK);
    ASSERT_EQ(it->size(), 0);
    // meta_tablet_idx_key
    begin_key = meta_tablet_idx_key({instance_id, 0});
    end_key = meta_tablet_idx_key({instance_id, INT64_MAX});
    ASSERT_EQ(txn->get(begin_key, end_key, &it), TxnErrorCode::TXN_OK);
    ASSERT_EQ(it->size(), 0);
    // meta_tablet_key
    begin_key = meta_tablet_key({instance_id, 0, 0, 0, 0});
    end_key = meta_tablet_key({instance_id, INT64_MAX, 0, 0, 0});
    ASSERT_EQ(txn->get(begin_key, end_key, &it), TxnErrorCode::TXN_OK);
    ASSERT_EQ(it->size(), 0);
    // job_tablet_key
    begin_key = job_tablet_key({instance_id, table_id, 0, 0, 0});
    end_key = job_tablet_key({instance_id, table_id + 1, 0, 0, 0});
    ASSERT_EQ(txn->get(begin_key, end_key, &it), TxnErrorCode::TXN_OK);
    ASSERT_EQ(it->size(), 0);
    // stats_tablet_key
    begin_key = stats_tablet_key({instance_id, table_id, 0, 0, 0});
    end_key = stats_tablet_key({instance_id, table_id + 1, 0, 0, 0});
    ASSERT_EQ(txn->get(begin_key, end_key, &it), TxnErrorCode::TXN_OK);
    ASSERT_EQ(it->size(), 0);
    // recycle_rowset_key
    begin_key = recycle_key_prefix(instance_id);
    end_key = recycle_key_prefix(instance_id + '\xff');
    ASSERT_EQ(txn->get(begin_key, end_key, &it), TxnErrorCode::TXN_OK);
    ASSERT_EQ(it->size(), 0);
}

TEST(RecyclerTest, recycle_versions) {
    config::retention_seconds = 0;
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    std::vector<int64_t> index_ids {20001, 20002, 20003, 20004, 20005};
    std::vector<int64_t> partition_ids {30001, 30002, 30003, 30004, 30005, 30006};
    constexpr int64_t table_id = 10000;

    int64_t tablet_id = 40000;
    for (auto index_id : index_ids) {
        for (auto partition_id : partition_ids) {
            create_tablet(txn_kv.get(), table_id, index_id, partition_id, ++tablet_id);
        }
    }
    for (auto partition_id : partition_ids) {
        create_partition_version_kv(txn_kv.get(), table_id, partition_id);
    }
    create_table_version_kv(txn_kv.get(), table_id);
    // Drop partitions
    for (int i = 0; i < 5; ++i) {
        create_recycle_partiton(txn_kv.get(), table_id, partition_ids[i], index_ids);
    }

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    InstanceRecycler recycler(txn_kv, instance, thread_group);
    ASSERT_EQ(recycler.init(), 0);
    // Recycle all partitions in table except 30006
    ASSERT_EQ(recycler.recycle_partitions(), 0);
    ASSERT_EQ(recycler.recycle_versions(), 0); // `recycle_versions` should do nothing
    // All partition version kvs except version of partition 30006 must have been deleted
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    auto partition_key_begin = partition_version_key({instance_id, db_id, table_id, 0});
    auto partition_key_end = partition_version_key({instance_id, db_id, table_id, INT64_MAX});
    std::unique_ptr<RangeGetIterator> iter;
    ASSERT_EQ(txn->get(partition_key_begin, partition_key_end, &iter), TxnErrorCode::TXN_OK);
    ASSERT_EQ(iter->size(), 1);
    auto [pk, pv] = iter->next();
    EXPECT_EQ(pk, partition_version_key({instance_id, db_id, table_id, 30006}));
    // Table 10000's table version must not be deleted
    auto table_key_begin = table_version_key({instance_id, db_id, 0});
    auto table_key_end = table_version_key({instance_id, db_id, INT64_MAX});
    ASSERT_EQ(txn->get(table_key_begin, table_key_end, &iter), TxnErrorCode::TXN_OK);
    ASSERT_EQ(iter->size(), 1);
    auto [tk, tv] = iter->next();
    EXPECT_EQ(tk, table_version_key({instance_id, db_id, 10000}));

    // Drop indexes
    for (auto index_id : index_ids) {
        create_recycle_index(txn_kv.get(), table_id, index_id);
    }
    // Recycle all indexes of the table, that is, the table has been dropped
    ASSERT_EQ(recycler.recycle_indexes(), 0);
    // `recycle_versions` should delete all version kvs of the dropped table
    ASSERT_EQ(recycler.recycle_versions(), 0);
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(partition_key_begin, partition_key_end, &iter), TxnErrorCode::TXN_OK);
    ASSERT_EQ(iter->size(), 0);
    ASSERT_EQ(txn->get(table_key_begin, table_key_end, &iter), TxnErrorCode::TXN_OK);
    ASSERT_EQ(iter->size(), 0);
}

TEST(RecyclerTest, abort_timeout_txn) {
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(txn_kv.get(), nullptr);
    auto rs = std::make_shared<MockResourceManager>(txn_kv);
    auto rl = std::make_shared<RateLimiter>();
    auto meta_service = std::make_unique<MetaServiceImpl>(txn_kv, rs, rl);
    ASSERT_EQ(txn_kv->init(), 0);

    int64_t db_id = 666;
    int64_t table_id = 1234;
    int64_t txn_id = -1;
    {
        brpc::Controller cntl;
        BeginTxnRequest req;

        req.set_cloud_unique_id("test_cloud_unique_id");
        TxnInfoPB txn_info_pb;
        txn_info_pb.set_db_id(db_id);
        txn_info_pb.set_label("abort_timeout_txn");
        txn_info_pb.add_table_ids(table_id);
        txn_info_pb.set_timeout_ms(1);
        req.mutable_txn_info()->CopyFrom(txn_info_pb);
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        txn_id = res.txn_id();
        ASSERT_GT(txn_id, -1);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }
    InstanceInfoPB instance;
    instance.set_instance_id(mock_instance);
    InstanceRecycler recycler(txn_kv, instance, thread_group);
    ASSERT_EQ(recycler.init(), 0);
    sleep(1);
    ASSERT_EQ(recycler.abort_timeout_txn(), 0);
    TxnInfoPB txn_info_pb;
    get_txn_info(txn_kv, mock_instance, db_id, txn_id, txn_info_pb);
    ASSERT_EQ(txn_info_pb.status(), TxnStatusPB::TXN_STATUS_ABORTED);
}

TEST(RecyclerTest, abort_timeout_txn_and_rebegin) {
    config::label_keep_max_second = 0;
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(txn_kv.get(), nullptr);
    auto rs = std::make_shared<MockResourceManager>(txn_kv);
    auto rl = std::make_shared<RateLimiter>();
    auto meta_service = std::make_unique<MetaServiceImpl>(txn_kv, rs, rl);
    ASSERT_EQ(txn_kv->init(), 0);

    int64_t db_id = 888;
    int64_t table_id = 1234;
    int64_t txn_id = -1;
    std::string cloud_unique_id = "test_cloud_unique_id22131";
    std::string label = "abort_timeout_txn_and_rebegin";
    {
        brpc::Controller cntl;
        BeginTxnRequest req;

        req.set_cloud_unique_id(cloud_unique_id);
        TxnInfoPB txn_info_pb;
        txn_info_pb.set_db_id(db_id);
        txn_info_pb.set_label(label);
        txn_info_pb.add_table_ids(table_id);
        txn_info_pb.set_timeout_ms(1);
        req.mutable_txn_info()->CopyFrom(txn_info_pb);
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        txn_id = res.txn_id();
        ASSERT_GT(txn_id, -1);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }
    InstanceInfoPB instance;
    instance.set_instance_id(mock_instance);
    InstanceRecycler recycler(txn_kv, instance, thread_group);
    ASSERT_EQ(recycler.init(), 0);
    sleep(1);
    ASSERT_EQ(recycler.abort_timeout_txn(), 0);
    TxnInfoPB txn_info_pb;
    get_txn_info(txn_kv, mock_instance, db_id, txn_id, txn_info_pb);
    ASSERT_EQ(txn_info_pb.status(), TxnStatusPB::TXN_STATUS_ABORTED);

    {
        brpc::Controller cntl;
        BeginTxnRequest req;

        req.set_cloud_unique_id(cloud_unique_id);
        TxnInfoPB txn_info_pb;
        txn_info_pb.set_db_id(db_id);
        txn_info_pb.set_label(label);
        txn_info_pb.add_table_ids(table_id);
        txn_info_pb.set_timeout_ms(1);
        req.mutable_txn_info()->CopyFrom(txn_info_pb);
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        txn_id = res.txn_id();
        ASSERT_GT(txn_id, -1);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }
}

TEST(RecyclerTest, recycle_expired_txn_label) {
    config::label_keep_max_second = 0;
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(txn_kv.get(), nullptr);
    auto rs = std::make_shared<MockResourceManager>(txn_kv);
    auto rl = std::make_shared<RateLimiter>();
    auto meta_service = std::make_unique<MetaServiceImpl>(txn_kv, rs, rl);
    ASSERT_EQ(txn_kv->init(), 0);

    int64_t db_id = 88812123;
    int64_t table_id = 12131234;
    int64_t txn_id = -1;
    std::string cloud_unique_id = "test_cloud_unique_id2";
    std::string label = "recycle_expired_txn_label";
    {
        // 1. begin_txn
        // 2. abort_txn by db_id and label
        // 3. recycle_expired_txn_label
        // 4. check
        {
            brpc::Controller cntl;
            BeginTxnRequest req;

            req.set_cloud_unique_id(cloud_unique_id);
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label(label);
            txn_info_pb.add_table_ids(table_id);
            txn_info_pb.set_timeout_ms(100000);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            txn_id = res.txn_id();
            ASSERT_GT(txn_id, -1);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }
        InstanceInfoPB instance;
        instance.set_instance_id(mock_instance);
        InstanceRecycler recycler(txn_kv, instance, thread_group);
        ASSERT_EQ(recycler.init(), 0);
        recycler.abort_timeout_txn();
        TxnInfoPB txn_info_pb;
        ASSERT_EQ(get_txn_info(txn_kv, mock_instance, db_id, txn_id, txn_info_pb), 0);
        ASSERT_EQ(txn_info_pb.status(), TxnStatusPB::TXN_STATUS_PREPARED);

        // abort txn by db_id and label
        {
            brpc::Controller cntl;
            AbortTxnRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            req.set_db_id(db_id);
            req.set_label(label);
            req.set_reason("test");
            AbortTxnResponse res;
            meta_service->abort_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            ASSERT_EQ(res.txn_info().status(), TxnStatusPB::TXN_STATUS_ABORTED);
        }
        recycler.recycle_expired_txn_label();
        ASSERT_EQ(get_txn_info(txn_kv, mock_instance, db_id, txn_id, txn_info_pb), -2);
        ASSERT_EQ(check_recycle_txn_keys(txn_kv, mock_instance, db_id, txn_id, label), 0);
    }

    {
        // 1. begin_txn
        // 2. abort_txn by db_id and txn_id
        // 3. recycle_expired_txn_label
        // 4. check
        {
            brpc::Controller cntl;
            BeginTxnRequest req;

            req.set_cloud_unique_id(cloud_unique_id);
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label(label);
            txn_info_pb.add_table_ids(table_id);
            txn_info_pb.set_timeout_ms(10000);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            txn_id = res.txn_id();
            ASSERT_GT(txn_id, -1);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }
        InstanceInfoPB instance;
        instance.set_instance_id(mock_instance);
        InstanceRecycler recycler(txn_kv, instance, thread_group);
        ASSERT_EQ(recycler.init(), 0);
        sleep(1);
        recycler.abort_timeout_txn();
        TxnInfoPB txn_info_pb;
        get_txn_info(txn_kv, mock_instance, db_id, txn_id, txn_info_pb);
        ASSERT_EQ(txn_info_pb.status(), TxnStatusPB::TXN_STATUS_PREPARED);

        // abort txn by db_id and label
        {
            brpc::Controller cntl;
            AbortTxnRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            req.set_db_id(db_id);
            req.set_txn_id(txn_id);
            req.set_reason("test");
            AbortTxnResponse res;
            meta_service->abort_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            ASSERT_EQ(res.txn_info().status(), TxnStatusPB::TXN_STATUS_ABORTED);
        }
        recycler.recycle_expired_txn_label();
        ASSERT_EQ(get_txn_info(txn_kv, mock_instance, db_id, txn_id, txn_info_pb), -2);
        ASSERT_EQ(check_recycle_txn_keys(txn_kv, mock_instance, db_id, txn_id, label), 0);
    }

    {
        // 1. begin_txn
        // 2. commit_txn
        // 3. recycle_expired_txn_label
        // 4. check
        {
            brpc::Controller cntl;
            BeginTxnRequest req;

            req.set_cloud_unique_id(cloud_unique_id);
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label(label);
            txn_info_pb.add_table_ids(table_id);
            txn_info_pb.set_timeout_ms(10000);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            txn_id = res.txn_id();
            ASSERT_GT(txn_id, -1);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }
        InstanceInfoPB instance;
        instance.set_instance_id(mock_instance);
        InstanceRecycler recycler(txn_kv, instance, thread_group);
        ASSERT_EQ(recycler.init(), 0);
        sleep(1);
        recycler.abort_timeout_txn();
        TxnInfoPB txn_info_pb;
        get_txn_info(txn_kv, mock_instance, db_id, txn_id, txn_info_pb);
        ASSERT_EQ(txn_info_pb.status(), TxnStatusPB::TXN_STATUS_PREPARED);

        // commit_txn
        {
            brpc::Controller cntl;
            CommitTxnRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            req.set_db_id(db_id);
            req.set_txn_id(txn_id);
            CommitTxnResponse res;
            meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }
        recycler.recycle_expired_txn_label();
        ASSERT_EQ(get_txn_info(txn_kv, mock_instance, db_id, txn_id, txn_info_pb), -2);
        ASSERT_EQ(check_recycle_txn_keys(txn_kv, mock_instance, db_id, txn_id, label), 0);
    }

    label = "recycle_expired_txn_label_with_sub_txn";
    int64_t table2_id = 12131278;
    {
        // 1. begin_txn
        // 2. begin_sub_txn2
        // 3. begin_sub_txn3
        // 4. abort_sub_txn3
        // 5. commit_txn
        // 6. recycle_expired_txn_label
        // 7. check
        [[maybe_unused]] int64_t sub_txn_id1 = -1;
        int64_t sub_txn_id2 = -1;
        int64_t sub_txn_id3 = -1;
        {
            brpc::Controller cntl;
            BeginTxnRequest req;

            req.set_cloud_unique_id(cloud_unique_id);
            TxnInfoPB txn_info_pb;
            txn_info_pb.set_db_id(db_id);
            txn_info_pb.set_label(label);
            txn_info_pb.add_table_ids(table_id);
            txn_info_pb.set_timeout_ms(10000);
            req.mutable_txn_info()->CopyFrom(txn_info_pb);
            BeginTxnResponse res;
            meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                    &req, &res, nullptr);
            txn_id = res.txn_id();
            sub_txn_id1 = txn_id;
            ASSERT_GT(txn_id, -1);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }
        InstanceInfoPB instance;
        instance.set_instance_id(mock_instance);
        InstanceRecycler recycler(txn_kv, instance, thread_group);
        ASSERT_EQ(recycler.init(), 0);
        sleep(1);
        recycler.abort_timeout_txn();
        TxnInfoPB txn_info_pb;
        get_txn_info(txn_kv, mock_instance, db_id, txn_id, txn_info_pb);
        ASSERT_EQ(txn_info_pb.status(), TxnStatusPB::TXN_STATUS_PREPARED);

        // 2. begin sub_txn2
        {
            brpc::Controller cntl;
            BeginSubTxnRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            req.set_txn_id(txn_id);
            req.set_sub_txn_num(0);
            req.set_db_id(db_id);
            req.set_label("test_sub_label1");
            req.mutable_table_ids()->Add(table_id);
            req.mutable_table_ids()->Add(table2_id);
            BeginSubTxnResponse res;
            meta_service->begin_sub_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            ASSERT_EQ(res.txn_info().table_ids().size(), 2);
            ASSERT_EQ(res.txn_info().sub_txn_ids().size(), 1);
            ASSERT_TRUE(res.has_sub_txn_id());
            sub_txn_id2 = res.sub_txn_id();
            ASSERT_EQ(sub_txn_id2, res.txn_info().sub_txn_ids()[0]);
        }

        // 3. begin sub_txn3
        {
            brpc::Controller cntl;
            BeginSubTxnRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            req.set_txn_id(txn_id);
            req.set_sub_txn_num(1);
            req.set_db_id(db_id);
            req.set_label("test_sub_label2");
            req.mutable_table_ids()->Add(table_id);
            req.mutable_table_ids()->Add(table2_id);
            req.mutable_table_ids()->Add(table_id);
            BeginSubTxnResponse res;
            meta_service->begin_sub_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            ASSERT_EQ(res.txn_info().table_ids().size(), 3);
            ASSERT_EQ(res.txn_info().sub_txn_ids().size(), 2);
            ASSERT_TRUE(res.has_sub_txn_id());
            sub_txn_id3 = res.sub_txn_id();
            ASSERT_EQ(sub_txn_id2, res.txn_info().sub_txn_ids()[0]);
            ASSERT_EQ(sub_txn_id3, res.txn_info().sub_txn_ids()[1]);
        }

        // 4. abort sub_txn3
        {
            brpc::Controller cntl;
            AbortSubTxnRequest req;
            req.set_cloud_unique_id("test_cloud_unique_id");
            req.set_txn_id(txn_id);
            req.set_sub_txn_num(2);
            req.set_sub_txn_id(sub_txn_id3);
            req.set_db_id(db_id);
            req.mutable_table_ids()->Add(table_id);
            req.mutable_table_ids()->Add(table2_id);
            AbortSubTxnResponse res;
            meta_service->abort_sub_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                        &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
            // check txn state
            ASSERT_EQ(res.txn_info().table_ids().size(), 2);
            ASSERT_EQ(res.txn_info().sub_txn_ids().size(), 2);
            ASSERT_EQ(sub_txn_id2, res.txn_info().sub_txn_ids()[0]);
            ASSERT_EQ(sub_txn_id3, res.txn_info().sub_txn_ids()[1]);
        }

        // 4. commit_txn
        {
            brpc::Controller cntl;
            CommitTxnRequest req;
            req.set_cloud_unique_id(cloud_unique_id);
            req.set_db_id(db_id);
            req.set_txn_id(txn_id);
            req.set_is_txn_load(true);
            CommitTxnResponse res;
            meta_service->commit_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl),
                                     &req, &res, nullptr);
            ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
        }
        // check txn_index_key for sub_txn_id exist
        for (auto i : {sub_txn_id2, sub_txn_id3}) {
            std::string key = txn_index_key({mock_instance, i});
            std::string val;
            std::unique_ptr<Transaction> txn;
            ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
            ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
        }
        // 5. recycle
        recycler.recycle_expired_txn_label();
        ASSERT_EQ(get_txn_info(txn_kv, mock_instance, db_id, txn_id, txn_info_pb), -2);
        ASSERT_EQ(check_recycle_txn_keys(txn_kv, mock_instance, db_id, txn_id, label), 0);
        // check txn_index_key for sub_txn_id are deleted
        for (auto i : {sub_txn_id2, sub_txn_id3}) {
            std::string key = txn_index_key({mock_instance, i});
            std::string val;
            std::unique_ptr<Transaction> txn;
            ASSERT_EQ(meta_service->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
            ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_KEY_NOT_FOUND);
        }
    }
}

void create_object_file_pb(std::string prefix, std::vector<ObjectFilePB>* object_files,
                           int file_num = 10) {
    for (int i = 0; i < file_num; ++i) {
        ObjectFilePB object_file;
        // create object in S3, pay attention to the relative path
        object_file.set_relative_path(prefix + "/obj_" + std::to_string(i));
        object_file.set_etag("");
        object_files->push_back(object_file);
    }
}

TEST(RecyclerTest, recycle_copy_jobs) {
    using namespace std::chrono;
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(txn_kv.get(), nullptr);
    ASSERT_EQ(txn_kv->init(), 0);

    // create internal/external stage
    std::string internal_stage_id = "internal";
    std::string external_stage_id = "external";
    std::string nonexist_internal_stage_id = "non_exist_internal";
    std::string nonexist_external_stage_id = "non_exist_external";

    InstanceInfoPB instance_info;
    create_instance(internal_stage_id, external_stage_id, instance_info);
    InstanceRecycler recycler(txn_kv, instance_info, thread_group);
    ASSERT_EQ(recycler.init(), 0);
    auto internal_accessor = recycler.accessor_map_.find(internal_stage_id)->second;

    // create internal stage copy job with finish status
    {
        std::vector<ObjectFilePB> object_files;
        create_object_file_pb("0", &object_files);
        ASSERT_EQ(create_object_files(internal_accessor.get(), &object_files), 0);
        create_copy_job(txn_kv.get(), internal_stage_id, 0, StagePB::INTERNAL, CopyJobPB::FINISH,
                        object_files, 0);
    }
    // create internal stage copy job and files with loading status which is timeout
    {
        std::vector<ObjectFilePB> object_files;
        create_object_file_pb("5", &object_files);
        ASSERT_EQ(create_object_files(internal_accessor.get(), &object_files), 0);
        create_copy_job(txn_kv.get(), internal_stage_id, 5, StagePB::INTERNAL, CopyJobPB::LOADING,
                        object_files, 0);
    }
    // create internal stage copy job and files with loading status which is not timeout
    {
        std::vector<ObjectFilePB> object_files;
        create_object_file_pb("6", &object_files);
        ASSERT_EQ(create_object_files(internal_accessor.get(), &object_files), 0);
        create_copy_job(txn_kv.get(), internal_stage_id, 6, StagePB::INTERNAL, CopyJobPB::LOADING,
                        object_files, 9963904963479L);
    }
    // create internal stage copy job with deleted stage id
    {
        std::vector<ObjectFilePB> object_files;
        create_object_file_pb("8", &object_files);
        ASSERT_EQ(create_object_files(internal_accessor.get(), &object_files), 0);
        ASSERT_EQ(0, create_copy_job(txn_kv.get(), nonexist_internal_stage_id, 8, StagePB::INTERNAL,
                                     CopyJobPB::FINISH, object_files, 0));
    }
    // ----- external stage ----
    // <table_id, timeout_time, start_time, finish_time, job_status>
    std::vector<std::tuple<int, int64_t, int64_t, int64_t, CopyJobPB::JobStatus>>
            external_copy_jobs;
    uint64_t current_time =
            duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    int64_t expire_time = current_time - config::copy_job_max_retention_second * 1000 - 1000;
    int64_t not_expire_time = current_time - config::copy_job_max_retention_second * 1000 / 2;
    // create external stage copy job with start time not expired and no finish time
    external_copy_jobs.emplace_back(1, 0, 9963904963479L, 0, CopyJobPB::FINISH);
    // create external stage copy job with start time expired and no finish time
    external_copy_jobs.emplace_back(2, 0, expire_time, 0, CopyJobPB::FINISH);
    // create external stage copy job with start time not expired and finish time not expired
    external_copy_jobs.emplace_back(9, 0, expire_time, not_expire_time, CopyJobPB::FINISH);
    // create external stage copy job with start time expired and finish time expired
    external_copy_jobs.emplace_back(10, 0, expire_time, expire_time + 1, CopyJobPB::FINISH);
    // create external stage copy job and files with loading status which is timeout
    external_copy_jobs.emplace_back(3, 0, 0, 0, CopyJobPB::LOADING);
    // create external stage copy job and files with loading status which is not timeout
    external_copy_jobs.emplace_back(4, 9963904963479L, 0, 0, CopyJobPB::LOADING);
    for (const auto& [table_id, timeout_time, start_time, finish_time, job_status] :
         external_copy_jobs) {
        std::vector<ObjectFilePB> object_files;
        create_object_file_pb(external_stage_id + "_" + std::to_string(table_id), &object_files);
        create_copy_job(txn_kv.get(), external_stage_id, table_id, StagePB::EXTERNAL, job_status,
                        object_files, timeout_time, start_time, finish_time);
    }
    // create external stage copy job with deleted stage id
    {
        std::vector<ObjectFilePB> object_files;
        create_object_file_pb(nonexist_external_stage_id + "_7", &object_files);
        ASSERT_EQ(0, create_copy_job(txn_kv.get(), nonexist_external_stage_id, 7, StagePB::EXTERNAL,
                                     CopyJobPB::FINISH, object_files, 0));
    }
    {
        // <stage_id, table_id>
        std::vector<std::tuple<std::string, int>> stage_table_files;
        stage_table_files.emplace_back(internal_stage_id, 0);
        stage_table_files.emplace_back(nonexist_internal_stage_id, 8);
        stage_table_files.emplace_back(external_stage_id, 1);
        stage_table_files.emplace_back(external_stage_id, 2);
        stage_table_files.emplace_back(external_stage_id, 9);
        stage_table_files.emplace_back(external_stage_id, 10);
        stage_table_files.emplace_back(external_stage_id, 3);
        stage_table_files.emplace_back(external_stage_id, 4);
        stage_table_files.emplace_back(external_stage_id, 9);
        stage_table_files.emplace_back(nonexist_external_stage_id, 7);
        // check copy files
        for (const auto& [stage_id, table_id] : stage_table_files) {
            int file_num = 0;
            ASSERT_EQ(0, get_copy_file_num(txn_kv.get(), stage_id, table_id, &file_num));
            ASSERT_EQ(10, file_num);
        }
    }

    ASSERT_EQ(recycler.recycle_copy_jobs(), 0);

    // check object files
    std::vector<std::tuple<std::shared_ptr<StorageVaultAccessor>, std::string, int>>
            prefix_and_files_list;
    prefix_and_files_list.emplace_back(internal_accessor, "0/", 0);
    prefix_and_files_list.emplace_back(internal_accessor, "5/", 10);
    prefix_and_files_list.emplace_back(internal_accessor, "6/", 10);
    prefix_and_files_list.emplace_back(internal_accessor, "8/", 10);
    for (const auto& [accessor, relative_path, file_num] : prefix_and_files_list) {
        std::unique_ptr<ListIterator> list_iter;
        ASSERT_EQ(0, accessor->list_directory(relative_path, &list_iter));
        int cnt = 0;
        while (list_iter->next().has_value()) {
            ++cnt;
        }
        ASSERT_EQ(file_num, cnt) << relative_path;
    }

    // check fdb kvs
    // <stage_id, table_id, expected_files, expected_job_exists>
    std::vector<std::tuple<std::string, int, int, bool>> stage_table_files;
    stage_table_files.emplace_back(internal_stage_id, 0, 0, false);
    stage_table_files.emplace_back(nonexist_internal_stage_id, 8, 0, false);
    stage_table_files.emplace_back(internal_stage_id, 5, 0, false);
    stage_table_files.emplace_back(internal_stage_id, 6, 10, true);
    stage_table_files.emplace_back(external_stage_id, 1, 10, true);
    stage_table_files.emplace_back(external_stage_id, 2, 0, false);
    stage_table_files.emplace_back(external_stage_id, 9, 10, true);
    stage_table_files.emplace_back(external_stage_id, 10, 0, false);
    stage_table_files.emplace_back(external_stage_id, 3, 0, false);
    stage_table_files.emplace_back(external_stage_id, 4, 10, true);
    stage_table_files.emplace_back(nonexist_external_stage_id, 7, 0, false);
    for (const auto& [stage_id, table_id, files, expected_job_exists] : stage_table_files) {
        // check copy files
        int file_num = 0;
        ASSERT_EQ(0, get_copy_file_num(txn_kv.get(), stage_id, table_id, &file_num)) << table_id;
        EXPECT_EQ(files, file_num) << table_id;
        // check copy jobs
        bool exist = false;
        ASSERT_EQ(0, copy_job_exists(txn_kv.get(), stage_id, table_id, &exist)) << table_id;
        EXPECT_EQ(expected_job_exists, exist) << table_id;
    }
}

TEST(RecyclerTest, recycle_batch_copy_jobs) {
    auto sp = SyncPoint::get_instance();
    std::unique_ptr<int, std::function<void(int*)>> defer(
            (int*)0x01, [](int*) { SyncPoint::get_instance()->clear_all_call_backs(); });
    sp->set_call_back("MockAccessor::delete_files", [](auto&& args) {
        auto* ret = try_any_cast_ret<int>(args);
        ret->first = -1;
        ret->second = true;
    });
    sp->enable_processing();
    using namespace std::chrono;
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(txn_kv.get(), nullptr);
    ASSERT_EQ(txn_kv->init(), 0);

    // create internal/external stage
    std::string internal_stage_id = "internal";
    std::string external_stage_id = "external";
    std::string nonexist_internal_stage_id = "non_exist_internal";
    std::string nonexist_external_stage_id = "non_exist_external";

    InstanceInfoPB instance_info;
    create_instance(internal_stage_id, external_stage_id, instance_info);
    InstanceRecycler recycler(txn_kv, instance_info, thread_group);
    ASSERT_EQ(recycler.init(), 0);
    const auto& internal_accessor = recycler.accessor_map_.find(internal_stage_id)->second;

    // create internal stage copy job with finish status
    {
        std::vector<ObjectFilePB> object_files;
        create_object_file_pb("0", &object_files, 1000);
        ASSERT_EQ(create_object_files(internal_accessor.get(), &object_files), 0);
        create_copy_job(txn_kv.get(), internal_stage_id, 0, StagePB::INTERNAL, CopyJobPB::FINISH,
                        object_files, 0);
    }
    {
        std::vector<ObjectFilePB> object_files;
        create_object_file_pb("4", &object_files);
        ASSERT_EQ(create_object_files(internal_accessor.get(), &object_files), 0);
        create_copy_job(txn_kv.get(), internal_stage_id, 4, StagePB::INTERNAL, CopyJobPB::FINISH,
                        object_files, 0);
    }
    // create internal stage copy job with deleted stage id
    {
        std::vector<ObjectFilePB> object_files;
        create_object_file_pb("8", &object_files);
        ASSERT_EQ(create_object_files(internal_accessor.get(), &object_files), 0);
        ASSERT_EQ(0, create_copy_job(txn_kv.get(), nonexist_internal_stage_id, 8, StagePB::INTERNAL,
                                     CopyJobPB::FINISH, object_files, 0));
    }

    ASSERT_EQ(recycler.recycle_copy_jobs(), 0);

    // check object files
    std::vector<std::tuple<std::shared_ptr<StorageVaultAccessor>, std::string, int>>
            prefix_and_files_list;
    prefix_and_files_list.emplace_back(internal_accessor, "0/", 1000);
    prefix_and_files_list.emplace_back(internal_accessor, "4/", 10);
    prefix_and_files_list.emplace_back(internal_accessor, "8/", 10);
    for (const auto& [accessor, relative_path, file_num] : prefix_and_files_list) {
        std::unique_ptr<ListIterator> list_iter;
        ASSERT_EQ(0, accessor->list_directory(relative_path, &list_iter));
        int cnt = 0;
        while (list_iter->next().has_value()) {
            ++cnt;
        }
        ASSERT_EQ(file_num, cnt);
    }

    // check fdb kvs
    // <stage_id, table_id, expected_files, expected_job_exists>
    std::vector<std::tuple<std::string, int, int, bool>> stage_table_files;
    stage_table_files.emplace_back(internal_stage_id, 0, 1000, true);
    stage_table_files.emplace_back(internal_stage_id, 4, 10, true);
    stage_table_files.emplace_back(nonexist_internal_stage_id, 8, 0, false);
    for (const auto& [stage_id, table_id, files, expected_job_exists] : stage_table_files) {
        // check copy files
        int file_num = 0;
        ASSERT_EQ(0, get_copy_file_num(txn_kv.get(), stage_id, table_id, &file_num)) << table_id;
        EXPECT_EQ(files, file_num) << table_id;
        // check copy jobs
        bool exist = false;
        ASSERT_EQ(0, copy_job_exists(txn_kv.get(), stage_id, table_id, &exist)) << table_id;
        EXPECT_EQ(expected_job_exists, exist) << table_id;
    }

    sp->clear_call_back("MockAccessor::delete_files");
    ASSERT_EQ(recycler.recycle_copy_jobs(), 0);

    // check object files
    prefix_and_files_list.clear();
    prefix_and_files_list.emplace_back(internal_accessor, "0/", 0);
    prefix_and_files_list.emplace_back(internal_accessor, "4/", 0);
    prefix_and_files_list.emplace_back(internal_accessor, "8/", 10);
    for (const auto& [accessor, relative_path, file_num] : prefix_and_files_list) {
        std::unique_ptr<ListIterator> list_iter;
        ASSERT_EQ(0, accessor->list_directory(relative_path, &list_iter));
        int cnt = 0;
        while (list_iter->next().has_value()) {
            ++cnt;
        }
        ASSERT_EQ(file_num, cnt);
    }

    // check fdb kvs
    // <stage_id, table_id, expected_files, expected_job_exists>
    stage_table_files.clear();
    stage_table_files.emplace_back(internal_stage_id, 0, 0, false);
    stage_table_files.emplace_back(internal_stage_id, 4, 0, false);
    stage_table_files.emplace_back(nonexist_internal_stage_id, 8, 0, false);
    for (const auto& [stage_id, table_id, files, expected_job_exists] : stage_table_files) {
        // check copy files
        int file_num = 0;
        ASSERT_EQ(0, get_copy_file_num(txn_kv.get(), stage_id, table_id, &file_num)) << table_id;
        EXPECT_EQ(files, file_num) << table_id;
        // check copy jobs
        bool exist = false;
        ASSERT_EQ(0, copy_job_exists(txn_kv.get(), stage_id, table_id, &exist)) << table_id;
        EXPECT_EQ(expected_job_exists, exist) << table_id;
    }
}

TEST(RecyclerTest, recycle_stage) {
    [[maybe_unused]] auto sp = SyncPoint::get_instance();
    std::unique_ptr<int, std::function<void(int*)>> defer(
            (int*)0x01, [](int*) { SyncPoint::get_instance()->clear_all_call_backs(); });
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(txn_kv.get(), nullptr);
    ASSERT_EQ(txn_kv->init(), 0);

    std::string stage_prefix = "prefix/stage/bob/bc9fff5e-5f91-4168-8eaa-0afd6667f7ef";
    ObjectStoreInfoPB object_info;
    object_info.set_id("obj_id");
    InstanceInfoPB instance;
    instance.set_instance_id(mock_instance);
    instance.add_obj_info()->CopyFrom(object_info);

    InstanceRecycler recycler(txn_kv, instance, thread_group);
    ASSERT_EQ(recycler.init(), 0);
    auto accessor = recycler.accessor_map_.begin()->second;
    for (int i = 0; i < 10; ++i) {
        accessor->put_file(std::to_string(i) + ".csv", "");
    }

    SyncPoint::CallbackGuard guard;
    sp->set_call_back(
            "recycle_stage:get_accessor",
            [&](auto&& args) {
                *try_any_cast<std::shared_ptr<StorageVaultAccessor>*>(args[0]) = accessor;
                auto* ret = try_any_cast_ret<int>(args);
                ret->first = 0;
                ret->second = true;
            },
            &guard);
    sp->enable_processing();

    std::string key;
    std::string val;
    RecycleStageKeyInfo key_info {mock_instance, "stage_id"};
    recycle_stage_key(key_info, &key);
    StagePB stage;
    stage.add_mysql_user_name("user_name");
    stage.add_mysql_user_id("user_id");
    stage.mutable_obj_info()->set_id("1");
    stage.mutable_obj_info()->set_prefix(stage_prefix);
    RecycleStagePB recycle_stage;
    recycle_stage.set_instance_id(mock_instance);
    recycle_stage.mutable_stage()->CopyFrom(stage);
    val = recycle_stage.SerializeAsString();
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(TxnErrorCode::TXN_OK, txn_kv->create_txn(&txn));
    txn->put(key, val);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    ASSERT_EQ(TxnErrorCode::TXN_OK, txn_kv->create_txn(&txn));
    ASSERT_EQ(TxnErrorCode::TXN_OK, txn->get(key, &val));

    // recycle stage
    ASSERT_EQ(0, recycler.recycle_stage());
    std::unique_ptr<ListIterator> list_iter;
    ASSERT_EQ(0, accessor->list_all(&list_iter));
    ASSERT_FALSE(list_iter->has_next());
    ASSERT_EQ(TxnErrorCode::TXN_OK, txn_kv->create_txn(&txn));
    ASSERT_EQ(TxnErrorCode::TXN_KEY_NOT_FOUND, txn->get(key, &val));
}

TEST(RecyclerTest, recycle_deleted_instance) {
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(txn_kv.get(), nullptr);
    ASSERT_EQ(txn_kv->init(), 0);
    // create internal/external stage
    std::string internal_stage_id = "internal";
    std::string external_stage_id = "external";
    std::string nonexist_internal_stage_id = "non_exist_internal";
    std::string nonexist_external_stage_id = "non_exist_external";

    InstanceInfoPB instance_info;
    create_instance(internal_stage_id, external_stage_id, instance_info);
    InstanceRecycler recycler(txn_kv, instance_info, thread_group);
    ASSERT_EQ(recycler.init(), 0);
    // create txn key
    for (size_t i = 0; i < 100; i++) {
        ASSERT_EQ(0, create_txn_label_kv(txn_kv.get(), fmt::format("fake_label{}", i), i));
    }
    // create partition version key
    for (size_t i = 101; i < 200; i += 2) {
        ASSERT_EQ(0, create_partition_version_kv(txn_kv.get(), i, i + 1));
    }
    // create table version key
    for (size_t i = 101; i < 200; i += 2) {
        ASSERT_EQ(0, create_table_version_kv(txn_kv.get(), i));
    }
    // create meta key
    std::vector<doris::TabletSchemaCloudPB> schemas;
    for (int i = 0; i < 5; ++i) {
        auto& schema = schemas.emplace_back();
        schema.set_schema_version(i);
        for (int j = 0; j < i; ++j) {
            auto index = schema.add_index();
            index->set_index_id(j);
            index->set_index_type(IndexType::INVERTED);
        }
    }

    constexpr int table_id = 10000, index_id = 10001, partition_id = 10002;
    auto accessor = recycler.accessor_map_.begin()->second;
    int64_t tablet_id_base = 10100;
    int64_t txn_id_base = 114115;
    for (int i = 0; i < 100; ++i) {
        int64_t tablet_id = tablet_id_base + i;
        // creare stats key
        create_tablet(txn_kv.get(), table_id, index_id, partition_id, tablet_id);
        for (int j = 0; j < 10; ++j) {
            auto rowset = create_rowset("recycle_tablet", tablet_id, index_id, 5, schemas[j % 5]);
            // create recycle key
            create_recycle_rowset(txn_kv.get(), accessor.get(), rowset,
                                  j % 10 < 2 ? RecycleRowsetPB::PREPARE : RecycleRowsetPB::COMPACT,
                                  j & 1);
            auto tmp_rowset = create_rowset("recycle_tmp_rowsets", tablet_id, index_id, 5,
                                            schemas[j % 5], txn_id_base + j);
            // create meta key
            create_tmp_rowset(txn_kv.get(), accessor.get(), tmp_rowset, j & 1);
        }
        for (int j = 0; j < 10; ++j) {
            // create meta key
            create_committed_rowset(txn_kv.get(), accessor.get(), "recycle_indexes", tablet_id, j);
        }
    }

    ASSERT_EQ(0, recycler.recycle_deleted_instance());

    // check if all the objects are deleted
    std::for_each(recycler.accessor_map_.begin(), recycler.accessor_map_.end(),
                  [&](const auto& entry) {
                      std::unique_ptr<ListIterator> list_iter;
                      auto& acc = entry.second;
                      ASSERT_EQ(0, acc->list_all(&list_iter));
                      ASSERT_FALSE(list_iter->has_next());
                  });

    // check if all the keys are deleted
    // check all related kv have been deleted
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::unique_ptr<RangeGetIterator> it;

    std::string start_txn_key = txn_key_prefix(instance_id);
    std::string end_txn_key = txn_key_prefix(instance_id + '\x00');
    ASSERT_EQ(txn->get(start_txn_key, end_txn_key, &it), TxnErrorCode::TXN_OK);
    ASSERT_EQ(it->size(), 0);

    std::string start_partition_version_key = partition_version_key({instance_id, 0, 0, 0});
    std::string end_partition_version_key = partition_version_key({instance_id, INT64_MAX, 0, 0});
    ASSERT_EQ(txn->get(start_partition_version_key, end_partition_version_key, &it),
              TxnErrorCode::TXN_OK);
    ASSERT_EQ(it->size(), 0);

    std::string start_table_version_key = table_version_key({instance_id, 0, 0});
    std::string end_table_version_key = table_version_key({instance_id, INT64_MAX, 0});
    ASSERT_EQ(txn->get(start_table_version_key, end_table_version_key, &it), TxnErrorCode::TXN_OK);
    ASSERT_EQ(it->size(), 0);

    std::string start_version_key = version_key_prefix(instance_id);
    std::string end_version_key = version_key_prefix(instance_id + '\x00');
    ASSERT_EQ(txn->get(start_version_key, end_version_key, &it), TxnErrorCode::TXN_OK);
    ASSERT_EQ(it->size(), 0);

    std::string start_meta_key = meta_key_prefix(instance_id);
    std::string end_meta_key = meta_key_prefix(instance_id + '\x00');
    ASSERT_EQ(txn->get(start_meta_key, end_meta_key, &it), TxnErrorCode::TXN_OK);
    ASSERT_EQ(it->size(), 0);

    auto begin_recycle_key = recycle_key_prefix(instance_id);
    auto end_recycle_key = recycle_key_prefix(instance_id + '\x00');
    ASSERT_EQ(txn->get(begin_recycle_key, end_recycle_key, &it), TxnErrorCode::TXN_OK);
    ASSERT_EQ(it->size(), 0);

    std::string start_stats_tablet_key = stats_tablet_key({instance_id, 0, 0, 0, 0});
    std::string end_stats_tablet_key = stats_tablet_key({instance_id, INT64_MAX, 0, 0, 0});
    ASSERT_EQ(txn->get(start_stats_tablet_key, end_stats_tablet_key, &it), TxnErrorCode::TXN_OK);
    ASSERT_EQ(it->size(), 0);

    std::string start_copy_key = copy_key_prefix(instance_id);
    std::string end_copy_key = copy_key_prefix(instance_id + '\x00');
    ASSERT_EQ(txn->get(start_copy_key, end_copy_key, &it), TxnErrorCode::TXN_OK);
    ASSERT_EQ(it->size(), 0);
}

TEST(RecyclerTest, multi_recycler) {
    config::recycle_concurrency = 2;
    config::recycle_interval_seconds = 10;
    config::recycle_job_lease_expired_ms = 1000;
    auto mem_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(mem_kv->init(), 0);

    std::atomic_int count {0};
    auto sp = SyncPoint::get_instance();

    SyncPoint::CallbackGuard guard;
    sp->set_call_back(
            "InstanceRecycler.do_recycle",
            [&count](auto&&) {
                sleep(1);
                ++count;
            },
            &guard);
    sp->enable_processing();

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(TxnErrorCode::TXN_OK, mem_kv->create_txn(&txn));
    for (int i = 0; i < 10; ++i) {
        InstanceInfoPB instance;
        instance.set_instance_id(std::to_string(i));
        auto obj_info = instance.add_obj_info();
        obj_info->set_id("multi_recycler_test");
        obj_info->set_ak(config::test_s3_ak);
        obj_info->set_sk(config::test_s3_sk);
        obj_info->set_endpoint(config::test_s3_endpoint);
        obj_info->set_region(config::test_s3_region);
        obj_info->set_bucket(config::test_s3_bucket);
        obj_info->set_prefix("multi_recycler_test");
        InstanceKeyInfo key_info {std::to_string(i)};
        std::string key;
        instance_key(key_info, &key);
        std::string val = instance.SerializeAsString();
        txn->put(key, val);
    }
    ASSERT_EQ(TxnErrorCode::TXN_OK, txn->commit());

    Recycler r1(mem_kv);
    r1.ip_port_ = "r1:p1";
    r1.start(nullptr);
    Recycler r2(mem_kv);
    r2.ip_port_ = "r2:p2";
    r2.start(nullptr);
    Recycler r3(mem_kv);
    r3.ip_port_ = "r3:p3";
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    r3.start(nullptr);

    std::this_thread::sleep_for(std::chrono::seconds(5));
    r1.stop();
    r2.stop();
    r3.stop();

    ASSERT_EQ(TxnErrorCode::TXN_OK, mem_kv->create_txn(&txn));
    for (int i = 0; i < 10; ++i) {
        JobRecycleKeyInfo key_info {std::to_string(i)};
        JobRecyclePB job_info;
        std::string key;
        std::string val;
        job_recycle_key(key_info, &key);
        ASSERT_EQ(TxnErrorCode::TXN_OK, txn->get(key, &val)) << i;
        ASSERT_TRUE(job_info.ParseFromString(val));
        EXPECT_EQ(JobRecyclePB::IDLE, job_info.status());
        EXPECT_GT(job_info.last_finish_time_ms(), 0);
        std::cout << "host: " << job_info.ip_port() << " finish recycle job of instance_id: " << i
                  << std::endl;
    }
    EXPECT_EQ(count, 10);
}

TEST(RecyclerTest, safe_exit) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);
    auto checker_ = std::make_unique<Recycler>(txn_kv);
    brpc::Server server;
    int ret = checker_->start(&server);
    ASSERT_TRUE(ret == 0);
    checker_->stop();
}

TEST(CheckerTest, safe_exit) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);
    auto checker_ = std::make_unique<Checker>(txn_kv);
    int ret = checker_->start();
    ASSERT_TRUE(ret == 0);
    checker_->stop();
}

TEST(CheckerTest, normal_inverted_check) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    auto obj_info = instance.add_obj_info();
    obj_info->set_id("1");

    auto sp = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    sp->set_call_back(
            "InstanceChecker::do_inverted_check",
            [](auto&& args) {
                auto* ret = try_any_cast_ret<int>(args);
                ret->first = 0;
                ret->second = true;
            },
            &guard);
    sp->enable_processing();
    std::unique_ptr<int, std::function<void(int*)>> defer(
            (int*)0x01, [](int*) { SyncPoint::get_instance()->disable_processing(); });

    InstanceChecker checker(txn_kv, instance_id);
    ASSERT_EQ(checker.init(instance), 0);
    // Add some visible rowsets along with some rowsets that should be recycled
    // call inverted check after do recycle which would sweep all the rowsets not visible
    auto accessor = checker.accessor_map_.begin()->second;
    for (int t = 10001; t <= 10100; ++t) {
        for (int v = 0; v < 10; ++v) {
            create_committed_rowset(txn_kv.get(), accessor.get(), "1", t, v, 1);
        }
    }
    for (int t = 10101; t <= 10200; ++t) {
        for (int v = 0; v < 10; ++v) {
            create_committed_rowset(txn_kv.get(), accessor.get(), "1", t, v, 5);
        }
    }
    ASSERT_EQ(checker.do_inverted_check(), 0);
}

// TODO(Xiaocc): We need one mocked accessor which provides one async stream like list function
// to do the following test
TEST(CheckerTest, DISABLED_abnormal_inverted_check) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    auto obj_info = instance.add_obj_info();
    obj_info->set_id("1");
    obj_info->set_ak(config::test_s3_ak);
    obj_info->set_sk(config::test_s3_sk);
    obj_info->set_endpoint(config::test_s3_endpoint);
    obj_info->set_region(config::test_s3_region);
    obj_info->set_bucket(config::test_s3_bucket);
    obj_info->set_prefix("CheckerTest");

    auto sp = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    sp->set_call_back(
            "InstanceChecker::do_inverted_check",
            [](auto&& args) {
                auto* ret = try_any_cast_ret<int>(args);
                ret->first = 0;
                ret->second = true;
            },
            &guard);
    sp->enable_processing();
    std::unique_ptr<int, std::function<void(int*)>> defer(
            (int*)0x01, [](int*) { SyncPoint::get_instance()->disable_processing(); });

    InstanceChecker checker(txn_kv, instance_id);
    ASSERT_EQ(checker.init(instance), 0);
    // Add some visible rowsets along with some rowsets that should be recycled
    // call inverted check after do recycle which would sweep all the rowsets not visible
    auto accessor = checker.accessor_map_.begin()->second;
    for (int t = 10001; t <= 10100; ++t) {
        for (int v = 0; v < 10; ++v) {
            create_committed_rowset(txn_kv.get(), accessor.get(), "1", t, v, 1);
        }
    }
    std::vector<doris::TabletSchemaCloudPB> schemas;
    for (int i = 0; i < 5; ++i) {
        auto& schema = schemas.emplace_back();
        schema.set_schema_version(i);
        for (int j = 0; j < i; ++j) {
            auto index = schema.add_index();
            index->set_index_id(j);
            index->set_index_type(IndexType::INVERTED);
        }
    }

    // Create some rowsets not visible in S3
    constexpr int table_id = 10101, index_id = 10102, partition_id = 10103, tablet_id = 10104;
    create_tablet(txn_kv.get(), table_id, index_id, partition_id, tablet_id);
    for (int i = 0; i < 500; ++i) {
        auto rowset = create_rowset("recycle_tablet", tablet_id, index_id, 5, schemas[i % 5]);
        create_recycle_rowset(txn_kv.get(), accessor.get(), rowset,
                              i % 10 < 2 ? RecycleRowsetPB::PREPARE : RecycleRowsetPB::COMPACT,
                              i & 1);
    }
    ASSERT_NE(checker.do_inverted_check(), 0);
}

TEST(CheckerTest, normal) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    auto obj_info = instance.add_obj_info();
    obj_info->set_id("1");
    InstanceChecker checker(txn_kv, instance_id);
    ASSERT_EQ(checker.init(instance), 0);

    auto accessor = checker.accessor_map_.begin()->second;
    for (int t = 10001; t <= 10100; ++t) {
        for (int v = 0; v < 10; ++v) {
            create_committed_rowset(txn_kv.get(), accessor.get(), "1", t, v, 1);
        }
    }
    for (int t = 10101; t <= 10200; ++t) {
        for (int v = 0; v < 10; ++v) {
            create_committed_rowset(txn_kv.get(), accessor.get(), "1", t, v, 5);
        }
    }
    ASSERT_EQ(checker.do_check(), 0);
}

TEST(CheckerTest, abnormal) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    auto obj_info = instance.add_obj_info();
    obj_info->set_id("1");

    InstanceChecker checker(txn_kv, instance_id);
    ASSERT_EQ(checker.init(instance), 0);

    auto accessor = checker.accessor_map_.begin()->second;
    for (int t = 10001; t <= 10100; ++t) {
        for (int v = 0; v < 10; ++v) {
            create_committed_rowset(txn_kv.get(), accessor.get(), "1", t, v, 1, 0);
        }
    }
    for (int t = 10101; t <= 10200; ++t) {
        for (int v = 0; v < 10; ++v) {
            create_committed_rowset(txn_kv.get(), accessor.get(), "1", t, v, 5, 0);
        }
    }

    // Delete some objects
    std::mt19937 gen(std::chrono::system_clock::now().time_since_epoch().count());
    std::vector<std::string> deleted_paths;
    std::unique_ptr<ListIterator> list_iter;
    ASSERT_EQ(0, accessor->list_directory(tablet_path_prefix(10001 + gen() % 100), &list_iter));
    auto file = list_iter->next();
    deleted_paths.push_back(file->path);
    for (auto file = list_iter->next(); file.has_value(); file = list_iter->next()) {
        if (gen() % 10 < 2) {
            deleted_paths.push_back(std::move(file->path));
        }
    }
    ASSERT_EQ(0, accessor->list_directory(tablet_path_prefix(10101 + gen() % 100), &list_iter));
    for (auto file = list_iter->next(); file.has_value(); file = list_iter->next()) {
        if (gen() % 10 < 2) {
            deleted_paths.push_back(std::move(file->path));
        }
    }

    ASSERT_EQ(0, accessor->delete_files(deleted_paths));

    std::vector<std::string> lost_paths;
    auto sp = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    sp->set_call_back(
            "InstanceChecker.do_check1",
            [&lost_paths](auto&& args) {
                lost_paths.push_back(*try_any_cast<std::string*>(args[0]));
            },
            &guard);
    sp->enable_processing();

    ASSERT_NE(checker.do_check(), 0);
    EXPECT_EQ(deleted_paths, lost_paths);
}

TEST(CheckerTest, multi_checker) {
    config::recycle_concurrency = 2;
    config::scan_instances_interval_seconds = 10;
    config::recycle_job_lease_expired_ms = 1000;
    auto mem_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(mem_kv->init(), 0);

    std::atomic_int count {0};
    auto sp = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard;
    sp->set_call_back(
            "InstanceChecker.do_check",
            [&count](auto&&) {
                sleep(1);
                ++count;
            },
            &guard);
    sp->enable_processing();

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(TxnErrorCode::TXN_OK, mem_kv->create_txn(&txn));
    for (int i = 0; i < 10; ++i) {
        InstanceInfoPB instance;
        instance.set_instance_id(std::to_string(i));
        auto obj_info = instance.add_obj_info();
        obj_info->set_id("1");
        InstanceKeyInfo key_info {std::to_string(i)};
        std::string key;
        instance_key(key_info, &key);
        std::string val = instance.SerializeAsString();
        txn->put(key, val);
    }
    ASSERT_EQ(TxnErrorCode::TXN_OK, txn->commit());

    Checker c1(mem_kv);
    c1.ip_port_ = "r1:p1";
    c1.start();
    Checker c2(mem_kv);
    c2.ip_port_ = "r2:p2";
    c2.start();
    Checker c3(mem_kv);
    c3.ip_port_ = "r3:p3";
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    c3.start();

    std::this_thread::sleep_for(std::chrono::seconds(5));
    c1.stop();
    c2.stop();
    c3.stop();

    ASSERT_EQ(TxnErrorCode::TXN_OK, mem_kv->create_txn(&txn));
    for (int i = 0; i < 10; ++i) {
        JobRecycleKeyInfo key_info {std::to_string(i)};
        JobRecyclePB job_info;
        std::string key;
        std::string val;
        job_check_key(key_info, &key);
        ASSERT_EQ(TxnErrorCode::TXN_OK, txn->get(key, &val)) << i;
        ASSERT_TRUE(job_info.ParseFromString(val));
        EXPECT_EQ(JobRecyclePB::IDLE, job_info.status());
        EXPECT_GT(job_info.last_finish_time_ms(), 0);
        std::cout << "host: " << job_info.ip_port() << " finish check job of instance_id: " << i
                  << std::endl;
    }
    EXPECT_EQ(count, 10);
}

TEST(CheckerTest, do_inspect) {
    using namespace std::chrono;
    {
        auto mem_kv = std::make_shared<MemTxnKv>();
        ASSERT_EQ(mem_kv->init(), 0);

        InstanceInfoPB instance;
        instance.set_instance_id(instance_id);
        instance.set_ctime(11111);
        auto obj_info = instance.add_obj_info();
        obj_info->set_id("1");

        Checker checker(mem_kv);
        checker.do_inspect(instance);

        {
            // empty job info
            auto sp = SyncPoint::get_instance();
            std::unique_ptr<int, std::function<void(int*)>> defer(
                    (int*)0x01, [](int*) { SyncPoint::get_instance()->clear_all_call_backs(); });
            sp->set_call_back("Checker:do_inspect", [](auto&& args) {
                auto last_ctime = *try_any_cast<int64_t*>(args[0]);
                ASSERT_EQ(last_ctime, 11111);
                std::cout << "last_ctime: " << last_ctime << std::endl;
            });
            sp->enable_processing();
        }

        {
            // add job_info but no last ctime
            std::unique_ptr<Transaction> txn;
            ASSERT_EQ(TxnErrorCode::TXN_OK, mem_kv->create_txn(&txn));
            JobRecyclePB job_info;
            job_info.set_instance_id(instance_id);
            std::string key = job_check_key({instance_id});
            std::string val = job_info.SerializeAsString();
            txn->put(key, val);
            ASSERT_EQ(TxnErrorCode::TXN_OK, txn->commit());
            checker.do_inspect(instance);
            auto sp = SyncPoint::get_instance();
            std::unique_ptr<int, std::function<void(int*)>> defer(
                    (int*)0x01, [](int*) { SyncPoint::get_instance()->clear_all_call_backs(); });
            sp->set_call_back("Checker:do_inspect", [](auto&& args) {
                ASSERT_EQ(*try_any_cast<int64_t*>(args[0]), 11111);
            });
            sp->enable_processing();
        }

        {
            // add job_info with last ctime
            std::unique_ptr<Transaction> txn;
            ASSERT_EQ(TxnErrorCode::TXN_OK, mem_kv->create_txn(&txn));
            JobRecyclePB job_info;
            job_info.set_instance_id(instance_id);
            job_info.set_last_ctime_ms(12345);
            auto sp = SyncPoint::get_instance();
            std::unique_ptr<int, std::function<void(int*)>> defer(
                    (int*)0x01, [](int*) { SyncPoint::get_instance()->clear_all_call_backs(); });
            sp->set_call_back("Checker:do_inspect", [](auto&& args) {
                ASSERT_EQ(*try_any_cast<int64_t*>(args[0]), 12345);
            });
            sp->enable_processing();
            std::string key = job_check_key({instance_id});
            std::string val = job_info.SerializeAsString();
            txn->put(key, val);
            ASSERT_EQ(TxnErrorCode::TXN_OK, txn->commit());
            checker.do_inspect(instance);
        }
        {
            // alarm
            int64_t expiration_ms = 7 > config::reserved_buffer_days
                                            ? (7 - config::reserved_buffer_days) * 3600000
                                            : 7 * 3600000;
            auto now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
            std::unique_ptr<Transaction> txn;
            ASSERT_EQ(TxnErrorCode::TXN_OK, mem_kv->create_txn(&txn));
            JobRecyclePB job_info;
            job_info.set_instance_id(instance_id);
            job_info.set_last_ctime_ms(now - expiration_ms - 10);
            auto sp = SyncPoint::get_instance();
            std::unique_ptr<int, std::function<void(int*)>> defer(
                    (int*)0x01, [](int*) { SyncPoint::get_instance()->clear_all_call_backs(); });

            bool alarm = false;
            sp->set_call_back("Checker:do_inspect", [&alarm](auto&&) { alarm = true; });
            sp->enable_processing();
            std::string key = job_check_key({instance_id});
            std::string val = job_info.SerializeAsString();
            txn->put(key, val);
            ASSERT_EQ(TxnErrorCode::TXN_OK, txn->commit());
            checker.do_inspect(instance);
            // FIXME(plat1ko): Unify SyncPoint in be and cloud
            //ASSERT_TRUE(alarm);
        }
    }
}

TEST(RecyclerTest, delete_rowset_data) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    auto obj_info = instance.add_obj_info();
    obj_info->set_id("recycle_tmp_rowsets");
    obj_info->set_ak(config::test_s3_ak);
    obj_info->set_sk(config::test_s3_sk);
    obj_info->set_endpoint(config::test_s3_endpoint);
    obj_info->set_region(config::test_s3_region);
    obj_info->set_bucket(config::test_s3_bucket);
    obj_info->set_prefix("recycle_tmp_rowsets");

    std::vector<doris::TabletSchemaCloudPB> schemas;
    for (int i = 0; i < 5; ++i) {
        auto& schema = schemas.emplace_back();
        schema.set_schema_version(i);
        schema.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V1);
        for (int j = 0; j < i; ++j) {
            auto index = schema.add_index();
            index->set_index_id(j);
            index->set_index_type(IndexType::INVERTED);
        }
    }

    {
        InstanceRecycler recycler(txn_kv, instance, thread_group);
        ASSERT_EQ(recycler.init(), 0);
        auto accessor = recycler.accessor_map_.begin()->second;
        int64_t txn_id_base = 114115;
        int64_t tablet_id_base = 10015;
        int64_t index_id_base = 1000;
        // Delete each rowset directly using one RowsetPB
        for (int i = 0; i < 100; ++i) {
            int64_t txn_id = txn_id_base + i;
            for (int j = 0; j < 20; ++j) {
                auto rowset = create_rowset("recycle_tmp_rowsets", tablet_id_base + j,
                                            index_id_base + j % 4, 5, schemas[i % 5], txn_id);
                create_tmp_rowset(txn_kv.get(), accessor.get(), rowset, i & 1);
                ASSERT_EQ(0, recycler.delete_rowset_data(rowset));
            }
        }

        std::unique_ptr<ListIterator> list_iter;
        ASSERT_EQ(0, accessor->list_all(&list_iter));
        ASSERT_FALSE(list_iter->has_next());
    }
    {
        InstanceInfoPB tmp_instance;
        std::string resource_id = "recycle_tmp_rowsets";
        tmp_instance.set_instance_id(instance_id);
        auto tmp_obj_info = tmp_instance.add_obj_info();
        tmp_obj_info->set_id(resource_id);
        tmp_obj_info->set_ak(config::test_s3_ak);
        tmp_obj_info->set_sk(config::test_s3_sk);
        tmp_obj_info->set_endpoint(config::test_s3_endpoint);
        tmp_obj_info->set_region(config::test_s3_region);
        tmp_obj_info->set_bucket(config::test_s3_bucket);
        tmp_obj_info->set_prefix(resource_id);

        InstanceRecycler recycler(txn_kv, tmp_instance, thread_group);
        ASSERT_EQ(recycler.init(), 0);
        auto accessor = recycler.accessor_map_.begin()->second;
        // Delete multiple rowset files using one series of RowsetPB
        constexpr int index_id = 10001, tablet_id = 10002;
        std::vector<doris::RowsetMetaCloudPB> rowset_pbs;
        for (int i = 0; i < 10; ++i) {
            auto rowset = create_rowset(resource_id, tablet_id, index_id, 5, schemas[i % 5]);
            create_recycle_rowset(
                    txn_kv.get(), accessor.get(), rowset,
                    static_cast<RecycleRowsetPB::Type>(i % (RecycleRowsetPB::Type_MAX + 1)), true);

            rowset_pbs.emplace_back(std::move(rowset));
        }
        ASSERT_EQ(0, recycler.delete_rowset_data(rowset_pbs));
        std::unique_ptr<ListIterator> list_iter;
        ASSERT_EQ(0, accessor->list_all(&list_iter));
        ASSERT_FALSE(list_iter->has_next());
    }
    {
        InstanceRecycler recycler(txn_kv, instance, thread_group);
        ASSERT_EQ(recycler.init(), 0);
        auto accessor = recycler.accessor_map_.begin()->second;
        // Delete multiple rowset files using one series of RowsetPB
        constexpr int index_id = 20001, tablet_id = 20002;
        // Delete each rowset file directly using it's id to construct one path
        for (int i = 0; i < 1000; ++i) {
            auto rowset =
                    create_rowset("recycle_tmp_rowsets", tablet_id, index_id, 5, schemas[i % 5]);
            create_recycle_rowset(txn_kv.get(), accessor.get(), rowset, RecycleRowsetPB::COMPACT,
                                  true);
            ASSERT_EQ(0, recycler.delete_rowset_data(rowset.resource_id(), rowset.tablet_id(),
                                                     rowset.rowset_id_v2()));
        }
        std::unique_ptr<ListIterator> list_iter;
        ASSERT_EQ(0, accessor->list_all(&list_iter));
        ASSERT_FALSE(list_iter->has_next());
    }
}

} // namespace doris::cloud
