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

#include "storage/tablet/tablet_reader.h"

#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

class TabletReaderTest : public testing::Test {
protected:
    static TabletSchemaSPtr create_schema(
            const std::vector<std::pair<std::string, int32_t>>& name_and_uid,
            KeysType keys_type = KeysType::DUP_KEYS) {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(keys_type);
        bool first = true;
        for (const auto& [name, uid] : name_and_uid) {
            auto* col = schema_pb.add_column();
            col->set_unique_id(uid);
            col->set_name(name);
            col->set_type("INT");
            col->set_is_key(first);
            col->set_is_nullable(!first);
            first = false;
        }
        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(schema_pb);
        return schema;
    }

    // Give `reader` a tablet with the given keys type. Its only key column is k1, and the
    // reader returns both columns.
    static void init_tablet(StorageEngine& engine, TabletReader& reader, KeysType keys_type,
                            bool enable_mow) {
        auto tablet_meta = std::make_shared<TabletMeta>();
        tablet_meta->set_enable_unique_key_merge_on_write(enable_mow);
        reader._tablet = std::make_shared<Tablet>(engine, std::move(tablet_meta), nullptr);
        reader._tablet_schema = create_schema({{"k1", 10}, {"v1", 12}}, keys_type);
        reader._return_columns = {0, 1};
    }

    // Set up the order-by key columns for a storage TopN that sorts by the first
    // `num_prefix_columns` key columns.
    static Status init_storage_topn(TabletReader& reader, size_t num_prefix_columns) {
        TabletReader::ReaderParams params;
        params.read_orderby_key_num_prefix_columns = num_prefix_columns;
        params.read_orderby_key_limit = 10;
        return reader._init_orderby_keys_param(params);
    }
};

// DUP and merge-on-write tablets give storage TopN the leading key columns to sort by.
TEST_F(TabletReaderTest, storage_topn_sorts_by_leading_key_columns) {
    StorageEngine engine({});
    TabletReader dup_reader;
    init_tablet(engine, dup_reader, KeysType::DUP_KEYS, /*enable_mow=*/false);
    auto st = init_storage_topn(dup_reader, 1);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(std::vector<uint32_t>({0}), dup_reader._orderby_key_columns);

    TabletReader mow_reader;
    init_tablet(engine, mow_reader, KeysType::UNIQUE_KEYS, /*enable_mow=*/true);
    st = init_storage_topn(mow_reader, 1);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(std::vector<uint32_t>({0}), mow_reader._orderby_key_columns);
}

// Storage TopN without order-by key columns fails at init, instead of aborting the BE
// later in VCollectIterator.
TEST_F(TabletReaderTest, storage_topn_without_orderby_key_columns_fails) {
    StorageEngine engine({});
    // A merge-on-read tablet, such as one read as DUP, sets up no order-by key columns.
    TabletReader mor_reader;
    init_tablet(engine, mor_reader, KeysType::UNIQUE_KEYS, /*enable_mow=*/false);
    auto st = init_storage_topn(mor_reader, 1);
    EXPECT_TRUE(st.is<ErrorCode::INTERNAL_ERROR>()) << st;

    // An empty key prefix leaves nothing to sort by.
    TabletReader dup_reader;
    init_tablet(engine, dup_reader, KeysType::DUP_KEYS, /*enable_mow=*/false);
    st = init_storage_topn(dup_reader, 0);
    EXPECT_TRUE(st.is<ErrorCode::INTERNAL_ERROR>()) << st;

    // Without TopN, a merge-on-read tablet needs no order-by key columns.
    TabletReader plain_reader;
    init_tablet(engine, plain_reader, KeysType::UNIQUE_KEYS, /*enable_mow=*/false);
    st = plain_reader._init_orderby_keys_param(TabletReader::ReaderParams {});
    EXPECT_TRUE(st.ok()) << st;
}
} // namespace doris
