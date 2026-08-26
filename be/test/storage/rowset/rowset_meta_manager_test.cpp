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

#include "storage/rowset/rowset_meta_manager.h"

#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>
#include <gmock/gmock-actions.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <boost/algorithm/string/replace.hpp>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <new>
#include <string>
#include <tuple>
#include <vector>

#include "common/config.h"
#include "gtest/gtest_pred_impl.h"
#include "runtime/exec_env.h"
#include "storage/binlog.h"
#include "storage/olap_define.h"
#include "storage/olap_meta.h"
#include "storage/options.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet_schema.h"
#include "util/uid_util.h"

using ::testing::_;
using ::testing::Return;
using ::testing::SetArgPointee;
using std::string;

namespace doris {

const std::string rowset_meta_path = "./be/test/storage/test_data/rowset_meta.json";

class RowsetMetaManagerTest : public testing::Test {
public:
    virtual void SetUp() {
        LOG(INFO) << "SetUp";

        std::string meta_path = "./meta";
        EXPECT_TRUE(std::filesystem::create_directory(meta_path));
        _meta = new (std::nothrow) OlapMeta(meta_path);
        EXPECT_NE(nullptr, _meta);
        Status st = _meta->init();
        EXPECT_TRUE(st == Status::OK());
        EXPECT_TRUE(std::filesystem::exists("./meta"));

        std::ifstream infile(rowset_meta_path);
        char buffer[1024];
        while (!infile.eof()) {
            infile.getline(buffer, 1024);
            _json_rowset_meta = _json_rowset_meta + buffer + "\n";
        }
        _json_rowset_meta = _json_rowset_meta.substr(0, _json_rowset_meta.size() - 1);
        _json_rowset_meta = _json_rowset_meta.substr(0, _json_rowset_meta.size() - 1);
        boost::replace_all(_json_rowset_meta, "\r", "");
        _tablet_uid = TabletUid(10, 10);
    }

    virtual void TearDown() {
        SAFE_DELETE(_meta);
        EXPECT_TRUE(std::filesystem::remove_all("./meta"));
        LOG(INFO) << "TearDown";
    }

protected:
    TabletSchemaSPtr create_tablet_schema(bool with_variant = false, int32_t schema_version = 1) {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(KeysType::DUP_KEYS);
        schema_pb.set_schema_version(schema_version);

        auto* key_column = schema_pb.add_column();
        key_column->set_unique_id(0);
        key_column->set_name("k");
        key_column->set_type("INT");
        key_column->set_is_key(true);
        key_column->set_is_nullable(false);

        auto* value_column = schema_pb.add_column();
        value_column->set_unique_id(1);
        value_column->set_name("v");
        value_column->set_type(with_variant ? "VARIANT" : "INT");
        value_column->set_is_key(false);
        value_column->set_is_nullable(true);

        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(schema_pb);
        return schema;
    }

    RowsetMetaSharedPtr create_rowset_meta(int64_t rowset_id, RowsetStatePB state, Version version,
                                           bool is_row_binlog = false) {
        auto rowset_meta = std::make_shared<RowsetMeta>();
        EXPECT_TRUE(rowset_meta->init_from_json(_json_rowset_meta));
        RowsetId rs_id;
        rs_id.init(rowset_id);
        rowset_meta->set_rowset_id(rs_id);
        rowset_meta->set_tablet_uid(_tablet_uid);
        rowset_meta->set_rowset_state(state);
        rowset_meta->set_version(version);
        rowset_meta->set_tablet_schema(create_tablet_schema());
        if (is_row_binlog) {
            rowset_meta->mark_row_binlog();
        }
        return rowset_meta;
    }

    OlapMeta* meta() { return _meta; }
    TabletUid tablet_uid() const { return _tablet_uid; }

private:
    OlapMeta* _meta;
    std::string _json_rowset_meta;
    TabletUid _tablet_uid {0, 0};
};

TEST_F(RowsetMetaManagerTest, SaveAndLoad) {
    auto base_rowset_meta = create_rowset_meta(20000, RowsetStatePB::COMMITTED, Version {7, 7});
    auto attach_rowset_meta =
            create_rowset_meta(20001, RowsetStatePB::COMMITTED, Version {7, 7}, true);

    auto st = RowsetMetaManager::save(meta(), tablet_uid(), base_rowset_meta->rowset_id(),
                                      *base_rowset_meta, BinlogFormatPB::ROW,
                                      attach_rowset_meta.get());
    ASSERT_TRUE(st.ok()) << st;

    RowsetMetaSharedPtr loaded_base_meta = std::make_shared<RowsetMeta>();
    st = RowsetMetaManager::get_rowset_meta(meta(), tablet_uid(), base_rowset_meta->rowset_id(),
                                            loaded_base_meta);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(loaded_base_meta->rowset_id(), base_rowset_meta->rowset_id());
    EXPECT_EQ(loaded_base_meta->tablet_uid().to_string(), tablet_uid().to_string());
    EXPECT_EQ(loaded_base_meta->version(), base_rowset_meta->version());

    RowsetMetaSharedPtr loaded_attach_meta = std::make_shared<RowsetMeta>();
    st = RowsetMetaManager::get_rowset_meta(meta(), tablet_uid(), attach_rowset_meta->rowset_id(),
                                            loaded_attach_meta);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(loaded_attach_meta->rowset_id(), attach_rowset_meta->rowset_id());
    EXPECT_EQ(loaded_attach_meta->tablet_uid().to_string(), tablet_uid().to_string());
    EXPECT_EQ(loaded_attach_meta->version(), attach_rowset_meta->version());
    EXPECT_TRUE(loaded_attach_meta->is_row_binlog());
}

TEST_F(RowsetMetaManagerTest, VariantSchemaRemainsInline) {
    auto rowset_meta = create_rowset_meta(20002, RowsetStatePB::VISIBLE, Version {8, 8});
    rowset_meta->set_tablet_schema(create_tablet_schema(true, 10));

    RowsetMetaPB rowset_meta_pb = rowset_meta->get_rowset_pb(true);
    EXPECT_TRUE(rowset_meta_pb.has_tablet_schema());
    EXPECT_TRUE(rowset_meta_pb.has_variant_type_in_schema());
    EXPECT_EQ(rowset_meta_pb.schema_version(), 10);
}

TEST_F(RowsetMetaManagerTest, VariantSchemaIsNotPersistedSeparately) {
    auto rowset_meta = create_rowset_meta(20003, RowsetStatePB::VISIBLE, Version {9, 9});
    rowset_meta->set_tablet_schema(create_tablet_schema(true, 11));

    auto st = RowsetMetaManager::save(meta(), tablet_uid(), rowset_meta->rowset_id(), *rowset_meta);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_FALSE(RowsetMetaManager::schema_exists(meta(), tablet_uid(),
                                                  rowset_meta->tablet_schema_hash(), 11));

    RowsetMetaSharedPtr loaded_rowset_meta = std::make_shared<RowsetMeta>();
    st = RowsetMetaManager::get_rowset_meta(meta(), tablet_uid(), rowset_meta->rowset_id(),
                                            loaded_rowset_meta);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(loaded_rowset_meta->tablet_schema(), nullptr);
    EXPECT_EQ(loaded_rowset_meta->tablet_schema()->num_variant_columns(), 1U);
}

TEST_F(RowsetMetaManagerTest, Remove) {
    auto base_rowset_meta = create_rowset_meta(20010, RowsetStatePB::VISIBLE, Version {9, 9});
    auto attach_rowset_meta =
            create_rowset_meta(20011, RowsetStatePB::VISIBLE, Version {9, 9}, true);

    auto st = RowsetMetaManager::save(meta(), tablet_uid(), base_rowset_meta->rowset_id(),
                                      *base_rowset_meta, BinlogFormatPB::ROW,
                                      attach_rowset_meta.get());
    ASSERT_TRUE(st.ok()) << st;

    st = RowsetMetaManager::exists(meta(), tablet_uid(), attach_rowset_meta->rowset_id());
    ASSERT_TRUE(st.ok()) << st;

    st = RowsetMetaManager::remove(meta(), tablet_uid(), attach_rowset_meta->rowset_id());
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_TRUE(RowsetMetaManager::exists(meta(), tablet_uid(), attach_rowset_meta->rowset_id())
                        .is<ErrorCode::META_KEY_NOT_FOUND>());

    auto base_rowset_meta_2 = create_rowset_meta(20012, RowsetStatePB::VISIBLE, Version {10, 10});
    auto attach_rowset_meta_2 =
            create_rowset_meta(20013, RowsetStatePB::VISIBLE, Version {10, 10}, true);
    st = RowsetMetaManager::save(meta(), tablet_uid(), base_rowset_meta_2->rowset_id(),
                                 *base_rowset_meta_2, BinlogFormatPB::ROW,
                                 attach_rowset_meta_2.get());
    ASSERT_TRUE(st.ok()) << st;

    st = RowsetMetaManager::remove(meta(), tablet_uid(), attach_rowset_meta_2->rowset_id());
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_TRUE(RowsetMetaManager::exists(meta(), tablet_uid(), attach_rowset_meta_2->rowset_id())
                        .is<ErrorCode::META_KEY_NOT_FOUND>());
}

TEST_F(RowsetMetaManagerTest, CcrBinlogDataAddsSchemaOnExport) {
    auto rowset_meta = create_rowset_meta(20014, RowsetStatePB::VISIBLE, Version {11, 11});
    ASSERT_TRUE(RowsetMetaManager::save(meta(), tablet_uid(), rowset_meta->rowset_id(),
                                        *rowset_meta, BinlogFormatPB::STATEMENT_AND_SNAPSHOT)
                        .ok());

    const std::string binlog_data_key =
            make_binlog_data_key(tablet_uid(), 11, rowset_meta->rowset_id());
    std::string stored_binlog_data;
    ASSERT_TRUE(meta()->get(META_COLUMN_FAMILY_INDEX, binlog_data_key, &stored_binlog_data).ok());
    RowsetMetaPB stored_binlog_rowset_meta_pb;
    ASSERT_TRUE(stored_binlog_rowset_meta_pb.ParseFromString(stored_binlog_data));
    EXPECT_FALSE(stored_binlog_rowset_meta_pb.has_tablet_schema());

    ASSERT_TRUE(RowsetMetaManager::remove(meta(), tablet_uid(), rowset_meta->rowset_id()).ok());
    std::string binlog_data = RowsetMetaManager::get_rowset_binlog_meta(
            meta(), tablet_uid(), "11", rowset_meta->rowset_id().to_string());
    RowsetMetaPB binlog_rowset_meta_pb;
    ASSERT_TRUE(binlog_rowset_meta_pb.ParseFromString(binlog_data));
    EXPECT_TRUE(binlog_rowset_meta_pb.has_tablet_schema());

    RowsetBinlogMetasPB binlog_metas_pb;
    ASSERT_TRUE(
            RowsetMetaManager::get_rowset_binlog_metas(meta(), tablet_uid(), {11}, &binlog_metas_pb)
                    .ok());
    ASSERT_EQ(binlog_metas_pb.rowset_binlog_metas_size(), 1);
    RowsetMetaPB snapshot_binlog_rowset_meta_pb;
    ASSERT_TRUE(snapshot_binlog_rowset_meta_pb.ParseFromString(
            binlog_metas_pb.rowset_binlog_metas(0).data()));
    EXPECT_TRUE(snapshot_binlog_rowset_meta_pb.has_tablet_schema());
}

TEST_F(RowsetMetaManagerTest, RemoveSchemas) {
    auto rowset_meta = create_rowset_meta(20020, RowsetStatePB::VISIBLE, Version {11, 11});
    const auto tablet_id = rowset_meta->tablet_id();
    const auto schema_hash = rowset_meta->tablet_schema_hash();
    for (int32_t schema_version : {1, 2, 3}) {
        auto tablet_schema = std::make_shared<TabletSchema>();
        tablet_schema->copy_from(*rowset_meta->tablet_schema());
        tablet_schema->set_schema_version(schema_version);
        ASSERT_TRUE(RowsetMetaManager::save_schema(meta(), tablet_id, tablet_uid(), schema_hash,
                                                   tablet_schema)
                            .ok());
    }

    ASSERT_TRUE(
            RowsetMetaManager::remove_schemas(meta(), tablet_id, tablet_uid(), schema_hash).ok());
    EXPECT_FALSE(RowsetMetaManager::schema_exists(meta(), tablet_uid(), schema_hash, 1));
    EXPECT_FALSE(RowsetMetaManager::schema_exists(meta(), tablet_uid(), schema_hash, 2));
    EXPECT_FALSE(RowsetMetaManager::schema_exists(meta(), tablet_uid(), schema_hash, 3));
}

} // namespace doris
