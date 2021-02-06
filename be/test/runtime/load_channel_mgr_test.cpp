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

#include "runtime/load_channel_mgr.h"

#include <gtest/gtest.h>

#include "common/object_pool.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "olap/delta_writer.h"
#include "olap/memtable_flush_executor.h"
#include "olap/schema.h"
#include "olap/storage_engine.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/primitive_type.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"
#include "util/thrift_util.h"

namespace doris {

std::unordered_map<int64_t, int> _k_tablet_recorder;
OLAPStatus open_status;
OLAPStatus add_status;
OLAPStatus close_status;
int64_t wait_lock_time_ns;

// mock
DeltaWriter::DeltaWriter(WriteRequest* req, const std::shared_ptr<MemTracker>& mem_tracker,
                         StorageEngine* storage_engine)
        : _req(*req) {}

DeltaWriter::~DeltaWriter() {}

OLAPStatus DeltaWriter::init() {
    return OLAP_SUCCESS;
}

OLAPStatus DeltaWriter::open(WriteRequest* req, const std::shared_ptr<MemTracker>& mem_tracker,
                             DeltaWriter** writer) {
    if (open_status != OLAP_SUCCESS) {
        return open_status;
    }
    *writer = new DeltaWriter(req, mem_tracker, nullptr);
    return open_status;
}

OLAPStatus DeltaWriter::write(Tuple* tuple) {
    if (_k_tablet_recorder.find(_req.tablet_id) == std::end(_k_tablet_recorder)) {
        _k_tablet_recorder[_req.tablet_id] = 1;
    } else {
        _k_tablet_recorder[_req.tablet_id]++;
    }
    return add_status;
}

OLAPStatus DeltaWriter::close() {
    return OLAP_SUCCESS;
}

OLAPStatus DeltaWriter::close_wait(google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec) {
    return close_status;
}

OLAPStatus DeltaWriter::cancel() {
    return OLAP_SUCCESS;
}

OLAPStatus DeltaWriter::flush_memtable_and_wait(bool need_wait) {
    return OLAP_SUCCESS;
}

OLAPStatus DeltaWriter::wait_flush() {
    return OLAP_SUCCESS;
}

int64_t DeltaWriter::partition_id() const {
    return 1L;
}
int64_t DeltaWriter::mem_consumption() const {
    return 1024L;
}

class LoadChannelMgrTest : public testing::Test {
public:
    LoadChannelMgrTest() {}
    virtual ~LoadChannelMgrTest() {}
    void SetUp() override {
        _k_tablet_recorder.clear();
        open_status = OLAP_SUCCESS;
        add_status = OLAP_SUCCESS;
        close_status = OLAP_SUCCESS;
        config::streaming_load_rpc_max_alive_time_sec = 120;
    }

private:
};

TEST_F(LoadChannelMgrTest, check_builder) {
    TDescriptorTableBuilder table_builder;
    {
        TTupleDescriptorBuilder tuple;
        tuple.add_slot(
                TSlotDescriptorBuilder().type(TYPE_INT).column_name("c1").column_pos(0).build());
        tuple.add_slot(
                TSlotDescriptorBuilder().type(TYPE_BIGINT).column_name("c2").column_pos(1).build());
        tuple.add_slot(
                TSlotDescriptorBuilder().string_type(64).column_name("c3").column_pos(2).build());
        tuple.build(&table_builder);
    }
    DescriptorTbl* desc_tbl = nullptr;
    ObjectPool obj_pool;
    DescriptorTbl::create(&obj_pool, table_builder.desc_tbl(), &desc_tbl);
    auto tuple = desc_tbl->get_tuple_descriptor(0);
    ASSERT_EQ(32, tuple->byte_size());
    ASSERT_EQ(4, tuple->slots()[0]->tuple_offset());
    ASSERT_EQ(8, tuple->slots()[1]->tuple_offset());
    ASSERT_EQ(16, tuple->slots()[2]->tuple_offset());
}

TDescriptorTable create_descriptor_table() {
    TDescriptorTableBuilder dtb;
    TTupleDescriptorBuilder tuple_builder;

    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_INT).column_name("c1").column_pos(0).build());
    tuple_builder.add_slot(
            TSlotDescriptorBuilder().type(TYPE_BIGINT).column_name("c2").column_pos(1).build());
    tuple_builder.build(&dtb);

    return dtb.desc_tbl();
}

// dbId=1, tableId=2, indexId=4, columns={c1, c2}
void create_schema(DescriptorTbl* desc_tbl, POlapTableSchemaParam* pschema) {
    pschema->set_db_id(1);
    pschema->set_table_id(2);
    pschema->set_version(0);

    auto tuple_desc = desc_tbl->get_tuple_descriptor(0);
    tuple_desc->to_protobuf(pschema->mutable_tuple_desc());
    for (auto slot : tuple_desc->slots()) {
        slot->to_protobuf(pschema->add_slot_descs());
    }

    // index schema
    auto indexes = pschema->add_indexes();
    indexes->set_id(4);
    indexes->add_columns("c1");
    indexes->add_columns("c2");
    indexes->set_schema_hash(123);
}

TEST_F(LoadChannelMgrTest, normal) {
    ExecEnv env;
    LoadChannelMgr mgr;
    mgr.init(-1);

    auto tdesc_tbl = create_descriptor_table();
    ObjectPool obj_pool;
    DescriptorTbl* desc_tbl = nullptr;
    DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl);
    auto tuple_desc = desc_tbl->get_tuple_descriptor(0);
    RowDescriptor row_desc(*desc_tbl, {0}, {false});
    auto tracker = std::make_shared<MemTracker>();
    PUniqueId load_id;
    load_id.set_hi(2);
    load_id.set_lo(3);
    {
        PTabletWriterOpenRequest request;
        request.set_allocated_id(&load_id);
        request.set_index_id(4);
        request.set_txn_id(1);
        create_schema(desc_tbl, request.mutable_schema());
        for (int i = 0; i < 2; ++i) {
            auto tablet = request.add_tablets();
            tablet->set_partition_id(10 + i);
            tablet->set_tablet_id(20 + i);
        }
        request.set_num_senders(1);
        request.set_need_gen_rollup(false);
        auto st = mgr.open(request);
        request.release_id();
        ASSERT_TRUE(st.ok());
    }

    // add a batch
    {
        PTabletWriterAddBatchRequest request;
        request.set_allocated_id(&load_id);
        request.set_index_id(4);
        request.set_sender_id(0);
        request.set_eos(true);
        request.set_packet_seq(0);

        request.add_tablet_ids(20);
        request.add_tablet_ids(21);
        request.add_tablet_ids(20);

        RowBatch row_batch(row_desc, 1024, tracker.get());

        // row1
        {
            auto id = row_batch.add_row();
            auto tuple = (Tuple*)row_batch.tuple_data_pool()->allocate(tuple_desc->byte_size());
            row_batch.get_row(id)->set_tuple(0, tuple);
            memset(tuple, 0, tuple_desc->byte_size());
            *(int*)tuple->get_slot(tuple_desc->slots()[0]->tuple_offset()) = 987654;
            *(int64_t*)tuple->get_slot(tuple_desc->slots()[1]->tuple_offset()) = 1234567899876;
            row_batch.commit_last_row();
        }
        // row2
        {
            auto id = row_batch.add_row();
            auto tuple = (Tuple*)row_batch.tuple_data_pool()->allocate(tuple_desc->byte_size());
            row_batch.get_row(id)->set_tuple(0, tuple);
            memset(tuple, 0, tuple_desc->byte_size());
            *(int*)tuple->get_slot(tuple_desc->slots()[0]->tuple_offset()) = 12345678;
            *(int64_t*)tuple->get_slot(tuple_desc->slots()[1]->tuple_offset()) = 9876567899876;
            row_batch.commit_last_row();
        }
        // row3
        {
            auto id = row_batch.add_row();
            auto tuple = (Tuple*)row_batch.tuple_data_pool()->allocate(tuple_desc->byte_size());
            row_batch.get_row(id)->set_tuple(0, tuple);
            memset(tuple, 0, tuple_desc->byte_size());
            *(int*)tuple->get_slot(tuple_desc->slots()[0]->tuple_offset()) = 876545678;
            *(int64_t*)tuple->get_slot(tuple_desc->slots()[1]->tuple_offset()) = 76543234567;
            row_batch.commit_last_row();
        }
        row_batch.serialize(request.mutable_row_batch());
        google::protobuf::RepeatedPtrField<PTabletInfo> tablet_vec;
        auto st = mgr.add_batch(request, &tablet_vec);
        request.release_id();
        ASSERT_TRUE(st.ok());
    }
    // check content
    ASSERT_EQ(_k_tablet_recorder[20], 2);
    ASSERT_EQ(_k_tablet_recorder[21], 1);
}

TEST_F(LoadChannelMgrTest, cancel) {
    ExecEnv env;
    LoadChannelMgr mgr;
    mgr.init(-1);

    auto tdesc_tbl = create_descriptor_table();
    ObjectPool obj_pool;
    DescriptorTbl* desc_tbl = nullptr;
    DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl);
    RowDescriptor row_desc(*desc_tbl, {0}, {false});

    PUniqueId load_id;
    load_id.set_hi(2);
    load_id.set_lo(3);
    {
        PTabletWriterOpenRequest request;
        request.set_allocated_id(&load_id);
        request.set_index_id(4);
        request.set_txn_id(1);
        create_schema(desc_tbl, request.mutable_schema());
        for (int i = 0; i < 2; ++i) {
            auto tablet = request.add_tablets();
            tablet->set_partition_id(10 + i);
            tablet->set_tablet_id(20 + i);
        }
        request.set_num_senders(1);
        request.set_need_gen_rollup(false);
        auto st = mgr.open(request);
        request.release_id();
        ASSERT_TRUE(st.ok());
    }

    // add a batch
    {
        PTabletWriterCancelRequest request;
        request.set_allocated_id(&load_id);
        request.set_index_id(4);
        auto st = mgr.cancel(request);
        request.release_id();
        ASSERT_TRUE(st.ok());
    }
}

TEST_F(LoadChannelMgrTest, open_failed) {
    ExecEnv env;
    LoadChannelMgr mgr;
    mgr.init(-1);

    auto tdesc_tbl = create_descriptor_table();
    ObjectPool obj_pool;
    DescriptorTbl* desc_tbl = nullptr;
    DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl);
    RowDescriptor row_desc(*desc_tbl, {0}, {false});

    PUniqueId load_id;
    load_id.set_hi(2);
    load_id.set_lo(3);
    {
        PTabletWriterOpenRequest request;
        request.set_allocated_id(&load_id);
        request.set_index_id(4);
        request.set_txn_id(1);
        create_schema(desc_tbl, request.mutable_schema());
        for (int i = 0; i < 2; ++i) {
            auto tablet = request.add_tablets();
            tablet->set_partition_id(10 + i);
            tablet->set_tablet_id(20 + i);
        }
        request.set_num_senders(1);
        request.set_need_gen_rollup(false);
        open_status = OLAP_ERR_TABLE_NOT_FOUND;
        auto st = mgr.open(request);
        request.release_id();
        ASSERT_FALSE(st.ok());
    }
}

TEST_F(LoadChannelMgrTest, add_failed) {
    ExecEnv env;
    LoadChannelMgr mgr;
    mgr.init(-1);

    auto tdesc_tbl = create_descriptor_table();
    ObjectPool obj_pool;
    DescriptorTbl* desc_tbl = nullptr;
    DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl);
    auto tuple_desc = desc_tbl->get_tuple_descriptor(0);
    RowDescriptor row_desc(*desc_tbl, {0}, {false});
    auto tracker = std::make_shared<MemTracker>();
    PUniqueId load_id;
    load_id.set_hi(2);
    load_id.set_lo(3);
    {
        PTabletWriterOpenRequest request;
        request.set_allocated_id(&load_id);
        request.set_index_id(4);
        request.set_txn_id(1);
        create_schema(desc_tbl, request.mutable_schema());
        for (int i = 0; i < 2; ++i) {
            auto tablet = request.add_tablets();
            tablet->set_partition_id(10 + i);
            tablet->set_tablet_id(20 + i);
        }
        request.set_num_senders(1);
        request.set_need_gen_rollup(false);
        auto st = mgr.open(request);
        request.release_id();
        ASSERT_TRUE(st.ok());
    }

    // add a batch
    {
        PTabletWriterAddBatchRequest request;
        request.set_allocated_id(&load_id);
        request.set_index_id(4);
        request.set_sender_id(0);
        request.set_eos(true);
        request.set_packet_seq(0);

        request.add_tablet_ids(20);
        request.add_tablet_ids(21);
        request.add_tablet_ids(20);

        RowBatch row_batch(row_desc, 1024, tracker.get());

        // row1
        {
            auto id = row_batch.add_row();
            auto tuple = (Tuple*)row_batch.tuple_data_pool()->allocate(tuple_desc->byte_size());
            row_batch.get_row(id)->set_tuple(0, tuple);
            memset(tuple, 0, tuple_desc->byte_size());
            *(int*)tuple->get_slot(tuple_desc->slots()[0]->tuple_offset()) = 987654;
            *(int64_t*)tuple->get_slot(tuple_desc->slots()[1]->tuple_offset()) = 1234567899876;
            row_batch.commit_last_row();
        }
        // row2
        {
            auto id = row_batch.add_row();
            auto tuple = (Tuple*)row_batch.tuple_data_pool()->allocate(tuple_desc->byte_size());
            row_batch.get_row(id)->set_tuple(0, tuple);
            memset(tuple, 0, tuple_desc->byte_size());
            *(int*)tuple->get_slot(tuple_desc->slots()[0]->tuple_offset()) = 12345678;
            *(int64_t*)tuple->get_slot(tuple_desc->slots()[1]->tuple_offset()) = 9876567899876;
            row_batch.commit_last_row();
        }
        // row3
        {
            auto id = row_batch.add_row();
            auto tuple = (Tuple*)row_batch.tuple_data_pool()->allocate(tuple_desc->byte_size());
            row_batch.get_row(id)->set_tuple(0, tuple);
            memset(tuple, 0, tuple_desc->byte_size());
            *(int*)tuple->get_slot(tuple_desc->slots()[0]->tuple_offset()) = 876545678;
            *(int64_t*)tuple->get_slot(tuple_desc->slots()[1]->tuple_offset()) = 76543234567;
            row_batch.commit_last_row();
        }
        row_batch.serialize(request.mutable_row_batch());
        add_status = OLAP_ERR_TABLE_NOT_FOUND;
        google::protobuf::RepeatedPtrField<PTabletInfo> tablet_vec;
        auto st = mgr.add_batch(request, &tablet_vec);
        request.release_id();
        ASSERT_FALSE(st.ok());
    }
}

TEST_F(LoadChannelMgrTest, close_failed) {
    ExecEnv env;
    LoadChannelMgr mgr;
    mgr.init(-1);

    auto tdesc_tbl = create_descriptor_table();
    ObjectPool obj_pool;
    DescriptorTbl* desc_tbl = nullptr;
    DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl);
    auto tuple_desc = desc_tbl->get_tuple_descriptor(0);
    RowDescriptor row_desc(*desc_tbl, {0}, {false});
    auto tracker = std::make_shared<MemTracker>();
    PUniqueId load_id;
    load_id.set_hi(2);
    load_id.set_lo(3);
    {
        PTabletWriterOpenRequest request;
        request.set_allocated_id(&load_id);
        request.set_index_id(4);
        request.set_txn_id(1);
        create_schema(desc_tbl, request.mutable_schema());
        for (int i = 0; i < 2; ++i) {
            auto tablet = request.add_tablets();
            tablet->set_partition_id(10 + i);
            tablet->set_tablet_id(20 + i);
        }
        request.set_num_senders(1);
        request.set_need_gen_rollup(false);
        auto st = mgr.open(request);
        request.release_id();
        ASSERT_TRUE(st.ok());
    }

    // add a batch
    {
        PTabletWriterAddBatchRequest request;
        request.set_allocated_id(&load_id);
        request.set_index_id(4);
        request.set_sender_id(0);
        request.set_eos(true);
        request.set_packet_seq(0);

        request.add_tablet_ids(20);
        request.add_tablet_ids(21);
        request.add_tablet_ids(20);

        request.add_partition_ids(10);
        request.add_partition_ids(11);

        RowBatch row_batch(row_desc, 1024, tracker.get());

        // row1
        {
            auto id = row_batch.add_row();
            auto tuple = (Tuple*)row_batch.tuple_data_pool()->allocate(tuple_desc->byte_size());
            row_batch.get_row(id)->set_tuple(0, tuple);
            memset(tuple, 0, tuple_desc->byte_size());
            *(int*)tuple->get_slot(tuple_desc->slots()[0]->tuple_offset()) = 987654;
            *(int64_t*)tuple->get_slot(tuple_desc->slots()[1]->tuple_offset()) = 1234567899876;
            row_batch.commit_last_row();
        }
        // row2
        {
            auto id = row_batch.add_row();
            auto tuple = (Tuple*)row_batch.tuple_data_pool()->allocate(tuple_desc->byte_size());
            row_batch.get_row(id)->set_tuple(0, tuple);
            memset(tuple, 0, tuple_desc->byte_size());
            *(int*)tuple->get_slot(tuple_desc->slots()[0]->tuple_offset()) = 12345678;
            *(int64_t*)tuple->get_slot(tuple_desc->slots()[1]->tuple_offset()) = 9876567899876;
            row_batch.commit_last_row();
        }
        // row3
        {
            auto id = row_batch.add_row();
            auto tuple = (Tuple*)row_batch.tuple_data_pool()->allocate(tuple_desc->byte_size());
            row_batch.get_row(id)->set_tuple(0, tuple);
            memset(tuple, 0, tuple_desc->byte_size());
            *(int*)tuple->get_slot(tuple_desc->slots()[0]->tuple_offset()) = 876545678;
            *(int64_t*)tuple->get_slot(tuple_desc->slots()[1]->tuple_offset()) = 76543234567;
            row_batch.commit_last_row();
        }
        row_batch.serialize(request.mutable_row_batch());
        close_status = OLAP_ERR_TABLE_NOT_FOUND;
        google::protobuf::RepeatedPtrField<PTabletInfo> tablet_vec;
        auto st = mgr.add_batch(request, &tablet_vec);
        request.release_id();
        // even if delta close failed, the return status is still ok, but tablet_vec is empty
        ASSERT_TRUE(st.ok());
        ASSERT_TRUE(tablet_vec.empty());
    }
}

TEST_F(LoadChannelMgrTest, unknown_tablet) {
    ExecEnv env;
    LoadChannelMgr mgr;
    mgr.init(-1);

    auto tdesc_tbl = create_descriptor_table();
    ObjectPool obj_pool;
    DescriptorTbl* desc_tbl = nullptr;
    DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl);
    auto tuple_desc = desc_tbl->get_tuple_descriptor(0);
    RowDescriptor row_desc(*desc_tbl, {0}, {false});
    auto tracker = std::make_shared<MemTracker>();
    PUniqueId load_id;
    load_id.set_hi(2);
    load_id.set_lo(3);
    {
        PTabletWriterOpenRequest request;
        request.set_allocated_id(&load_id);
        request.set_index_id(4);
        request.set_txn_id(1);
        create_schema(desc_tbl, request.mutable_schema());
        for (int i = 0; i < 2; ++i) {
            auto tablet = request.add_tablets();
            tablet->set_partition_id(10 + i);
            tablet->set_tablet_id(20 + i);
        }
        request.set_num_senders(1);
        request.set_need_gen_rollup(false);
        auto st = mgr.open(request);
        request.release_id();
        ASSERT_TRUE(st.ok());
    }

    // add a batch
    {
        PTabletWriterAddBatchRequest request;
        request.set_allocated_id(&load_id);
        request.set_index_id(4);
        request.set_sender_id(0);
        request.set_eos(true);
        request.set_packet_seq(0);

        request.add_tablet_ids(20);
        request.add_tablet_ids(22);
        request.add_tablet_ids(20);

        RowBatch row_batch(row_desc, 1024, tracker.get());

        // row1
        {
            auto id = row_batch.add_row();
            auto tuple = (Tuple*)row_batch.tuple_data_pool()->allocate(tuple_desc->byte_size());
            row_batch.get_row(id)->set_tuple(0, tuple);
            memset(tuple, 0, tuple_desc->byte_size());
            *(int*)tuple->get_slot(tuple_desc->slots()[0]->tuple_offset()) = 987654;
            *(int64_t*)tuple->get_slot(tuple_desc->slots()[1]->tuple_offset()) = 1234567899876;
            row_batch.commit_last_row();
        }
        // row2
        {
            auto id = row_batch.add_row();
            auto tuple = (Tuple*)row_batch.tuple_data_pool()->allocate(tuple_desc->byte_size());
            row_batch.get_row(id)->set_tuple(0, tuple);
            memset(tuple, 0, tuple_desc->byte_size());
            *(int*)tuple->get_slot(tuple_desc->slots()[0]->tuple_offset()) = 12345678;
            *(int64_t*)tuple->get_slot(tuple_desc->slots()[1]->tuple_offset()) = 9876567899876;
            row_batch.commit_last_row();
        }
        // row3
        {
            auto id = row_batch.add_row();
            auto tuple = (Tuple*)row_batch.tuple_data_pool()->allocate(tuple_desc->byte_size());
            row_batch.get_row(id)->set_tuple(0, tuple);
            memset(tuple, 0, tuple_desc->byte_size());
            *(int*)tuple->get_slot(tuple_desc->slots()[0]->tuple_offset()) = 876545678;
            *(int64_t*)tuple->get_slot(tuple_desc->slots()[1]->tuple_offset()) = 76543234567;
            row_batch.commit_last_row();
        }
        row_batch.serialize(request.mutable_row_batch());
        google::protobuf::RepeatedPtrField<PTabletInfo> tablet_vec;
        auto st = mgr.add_batch(request, &tablet_vec);
        request.release_id();
        ASSERT_FALSE(st.ok());
    }
}

TEST_F(LoadChannelMgrTest, duplicate_packet) {
    ExecEnv env;
    LoadChannelMgr mgr;
    mgr.init(-1);

    auto tdesc_tbl = create_descriptor_table();
    ObjectPool obj_pool;
    DescriptorTbl* desc_tbl = nullptr;
    DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl);
    auto tuple_desc = desc_tbl->get_tuple_descriptor(0);
    RowDescriptor row_desc(*desc_tbl, {0}, {false});
    auto tracker = std::make_shared<MemTracker>();
    PUniqueId load_id;
    load_id.set_hi(2);
    load_id.set_lo(3);
    {
        PTabletWriterOpenRequest request;
        request.set_allocated_id(&load_id);
        request.set_index_id(4);
        request.set_txn_id(1);
        create_schema(desc_tbl, request.mutable_schema());
        for (int i = 0; i < 2; ++i) {
            auto tablet = request.add_tablets();
            tablet->set_partition_id(10 + i);
            tablet->set_tablet_id(20 + i);
        }
        request.set_num_senders(1);
        request.set_need_gen_rollup(false);
        auto st = mgr.open(request);
        request.release_id();
        ASSERT_TRUE(st.ok());
    }

    // add a batch
    {
        PTabletWriterAddBatchRequest request;
        request.set_allocated_id(&load_id);
        request.set_index_id(4);
        request.set_sender_id(0);
        request.set_eos(false);
        request.set_packet_seq(0);

        request.add_tablet_ids(20);
        request.add_tablet_ids(21);
        request.add_tablet_ids(20);

        RowBatch row_batch(row_desc, 1024, tracker.get());

        // row1
        {
            auto id = row_batch.add_row();
            auto tuple = (Tuple*)row_batch.tuple_data_pool()->allocate(tuple_desc->byte_size());
            row_batch.get_row(id)->set_tuple(0, tuple);
            memset(tuple, 0, tuple_desc->byte_size());
            *(int*)tuple->get_slot(tuple_desc->slots()[0]->tuple_offset()) = 987654;
            *(int64_t*)tuple->get_slot(tuple_desc->slots()[1]->tuple_offset()) = 1234567899876;
            row_batch.commit_last_row();
        }
        // row2
        {
            auto id = row_batch.add_row();
            auto tuple = (Tuple*)row_batch.tuple_data_pool()->allocate(tuple_desc->byte_size());
            row_batch.get_row(id)->set_tuple(0, tuple);
            memset(tuple, 0, tuple_desc->byte_size());
            *(int*)tuple->get_slot(tuple_desc->slots()[0]->tuple_offset()) = 12345678;
            *(int64_t*)tuple->get_slot(tuple_desc->slots()[1]->tuple_offset()) = 9876567899876;
            row_batch.commit_last_row();
        }
        // row3
        {
            auto id = row_batch.add_row();
            auto tuple = (Tuple*)row_batch.tuple_data_pool()->allocate(tuple_desc->byte_size());
            row_batch.get_row(id)->set_tuple(0, tuple);
            memset(tuple, 0, tuple_desc->byte_size());
            *(int*)tuple->get_slot(tuple_desc->slots()[0]->tuple_offset()) = 876545678;
            *(int64_t*)tuple->get_slot(tuple_desc->slots()[1]->tuple_offset()) = 76543234567;
            row_batch.commit_last_row();
        }
        row_batch.serialize(request.mutable_row_batch());
        google::protobuf::RepeatedPtrField<PTabletInfo> tablet_vec1;
        auto st = mgr.add_batch(request, &tablet_vec1);
        ASSERT_TRUE(st.ok());
        google::protobuf::RepeatedPtrField<PTabletInfo> tablet_vec2;
        st = mgr.add_batch(request, &tablet_vec2);
        request.release_id();
        ASSERT_TRUE(st.ok());
    }
    // close
    {
        PTabletWriterAddBatchRequest request;
        request.set_allocated_id(&load_id);
        request.set_index_id(4);
        request.set_sender_id(0);
        request.set_eos(true);
        request.set_packet_seq(0);
        google::protobuf::RepeatedPtrField<PTabletInfo> tablet_vec;
        auto st = mgr.add_batch(request, &tablet_vec);
        request.release_id();
        ASSERT_TRUE(st.ok());
    }
    // check content
    ASSERT_EQ(_k_tablet_recorder[20], 2);
    ASSERT_EQ(_k_tablet_recorder[21], 1);
}

} // namespace doris

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    doris::CpuInfo::init();
    return RUN_ALL_TESTS();
}
