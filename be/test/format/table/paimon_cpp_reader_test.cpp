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

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#define private public
#include "format/table/paimon_cpp_reader.h"
#undef private
#pragma clang diagnostic pop

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/block/block.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "paimon/defs.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "testutil/desc_tbl_builder.h"

namespace doris {

class PaimonCppReaderTest : public testing::Test {
protected:
    void SetUp() override {
        _query_options.__set_batch_size(3);
        _runtime_state = std::make_unique<RuntimeState>(_query_options, _query_globals);
    }

    TFileRangeDesc _build_range_with_table_level_row_count(int64_t row_count) {
        TFileRangeDesc range;
        range.__isset.table_format_params = true;
        range.table_format_params.__isset.table_level_row_count = true;
        range.table_format_params.table_level_row_count = row_count;
        return range;
    }

    std::vector<SlotDescriptor*> _build_file_slot_descs(ObjectPool* object_pool) {
        DescriptorTblBuilder builder(object_pool);
        auto nullable_string = make_nullable(std::make_shared<DataTypeString>());
        builder.declare_tuple() << std::make_tuple(std::make_shared<DataTypeInt32>(), "id")
                                << std::make_tuple(nullable_string, "name");
        return builder.build()->get_tuple_descriptor(0)->slots();
    }

    TQueryOptions _query_options;
    TQueryGlobals _query_globals;
    std::unique_ptr<RuntimeState> _runtime_state;
    RuntimeProfile _profile {"paimon_cpp_reader_test"};
    std::vector<SlotDescriptor*> _file_slot_descs;
};

TEST_F(PaimonCppReaderTest, CountPushDownUsesTableLevelRowCount) {
    auto range = _build_range_with_table_level_row_count(5);
    PaimonCppReader reader(_file_slot_descs, _runtime_state.get(), &_profile, range, nullptr);
    reader.set_push_down_agg_type(TPushAggOp::type::COUNT);

    auto init_status = reader.init_reader();
    ASSERT_TRUE(init_status.ok()) << init_status;

    Block block;
    size_t read_rows = 0;
    bool eof = false;

    auto first_status = reader.get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(first_status.ok()) << first_status;
    EXPECT_EQ(3, read_rows);
    EXPECT_FALSE(eof);

    auto second_status = reader.get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(second_status.ok()) << second_status;
    EXPECT_EQ(2, read_rows);
    EXPECT_TRUE(eof);

    auto third_status = reader.get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(third_status.ok()) << third_status;
    EXPECT_EQ(0, read_rows);
    EXPECT_TRUE(eof);
}

TEST_F(PaimonCppReaderTest, CountPushDownHandlesZeroTableLevelRowCount) {
    auto range = _build_range_with_table_level_row_count(0);
    PaimonCppReader reader(_file_slot_descs, _runtime_state.get(), &_profile, range, nullptr);
    reader.set_push_down_agg_type(TPushAggOp::type::COUNT);

    auto init_status = reader.init_reader();
    ASSERT_TRUE(init_status.ok()) << init_status;

    Block block;
    size_t read_rows = 123;
    bool eof = false;

    auto status = reader.get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(0, read_rows);
    EXPECT_TRUE(eof);
}

TEST_F(PaimonCppReaderTest, InitReaderFailsWithoutPaimonSplit) {
    TFileRangeDesc range;
    range.__isset.table_format_params = true;
    range.table_format_params.__isset.paimon_params = true;
    range.table_format_params.paimon_params.__isset.paimon_table = true;
    range.table_format_params.paimon_params.paimon_table = "s3://bucket/db.tbl";

    PaimonCppReader reader(_file_slot_descs, _runtime_state.get(), &_profile, range, nullptr);
    auto status = reader.init_reader();

    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("missing paimon_split"), std::string::npos);
}

TEST_F(PaimonCppReaderTest, InitReaderFailsWhenPaimonSplitBase64IsInvalid) {
    TFileRangeDesc range;
    range.__isset.table_format_params = true;
    range.table_format_params.__isset.paimon_params = true;
    range.table_format_params.paimon_params.__set_paimon_split("not-base64");

    PaimonCppReader reader(_file_slot_descs, _runtime_state.get(), &_profile, range, nullptr);
    auto status = reader.init_reader();

    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("base64 decode paimon_split failed"), std::string::npos);
}

TEST_F(PaimonCppReaderTest, InitReaderFailsWhenPaimonSplitDeserializeFails) {
    TFileRangeDesc range;
    range.__isset.table_format_params = true;
    range.table_format_params.__isset.paimon_params = true;
    range.table_format_params.paimon_params.__set_paimon_split("aGVsbG8=");

    PaimonCppReader reader(_file_slot_descs, _runtime_state.get(), &_profile, range, nullptr);
    auto status = reader.init_reader();

    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("deserialize split failed"), std::string::npos);
}

TEST_F(PaimonCppReaderTest, ReadWithoutInitializationFails) {
    TFileRangeDesc range;
    PaimonCppReader reader(_file_slot_descs, _runtime_state.get(), &_profile, range, nullptr);

    Block block;
    size_t read_rows = 0;
    bool eof = false;
    auto status = reader.get_next_block(&block, &read_rows, &eof);

    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("reader is not initialized"), std::string::npos);
}

TEST_F(PaimonCppReaderTest, ResolveTablePathReturnsConfiguredPath) {
    TFileRangeDesc range;
    range.__isset.table_format_params = true;
    range.table_format_params.__isset.paimon_params = true;
    range.table_format_params.paimon_params.__set_paimon_table("s3://bucket/db.tbl");

    PaimonCppReader reader(_file_slot_descs, _runtime_state.get(), &_profile, range, nullptr);
    auto table_path = reader._resolve_table_path();

    ASSERT_TRUE(table_path.has_value());
    EXPECT_EQ(*table_path, "s3://bucket/db.tbl");
}

TEST_F(PaimonCppReaderTest, ResolveTablePathReturnsNulloptForEmptyPath) {
    TFileRangeDesc range;
    range.__isset.table_format_params = true;
    range.table_format_params.__isset.paimon_params = true;
    range.table_format_params.paimon_params.__set_paimon_table("");

    PaimonCppReader reader(_file_slot_descs, _runtime_state.get(), &_profile, range, nullptr);
    auto table_path = reader._resolve_table_path();

    EXPECT_FALSE(table_path.has_value());
}

TEST_F(PaimonCppReaderTest, BuildOptionsUsesRangeFallbacksAndBackfillsFormats) {
    TFileRangeDesc range;
    range.__isset.table_format_params = true;
    range.table_format_params.__isset.paimon_params = true;
    range.table_format_params.paimon_params.__set_paimon_options({{"scan.mode", "from-range"}});
    range.table_format_params.paimon_params.__set_hadoop_conf(
            {{"fs.oss.accessKeyId", "range-ak"},
             {"fs.oss.accessKeySecret", "range-sk"},
             {"fs.oss.endpoint", "oss-cn-beijing.aliyuncs.com"},
             {"fs.s3a.path.style.access", "true"}});
    range.table_format_params.paimon_params.__set_file_format("orc");

    PaimonCppReader reader(_file_slot_descs, _runtime_state.get(), &_profile, range, nullptr);
    auto options = reader._build_options();

    EXPECT_EQ(options["scan.mode"], "from-range");
    EXPECT_EQ(options["AWS_ACCESS_KEY"], "range-ak");
    EXPECT_EQ(options["AWS_SECRET_KEY"], "range-sk");
    EXPECT_EQ(options["AWS_ENDPOINT"], "oss-cn-beijing.aliyuncs.com");
    EXPECT_EQ(options["use_path_style"], "true");
    EXPECT_EQ(options[paimon::Options::FILE_FORMAT], "orc");
    EXPECT_EQ(options[paimon::Options::MANIFEST_FORMAT], "orc");
    EXPECT_EQ(options[paimon::Options::FILE_SYSTEM], "doris");
}

TEST_F(PaimonCppReaderTest, BuildOptionsPrefersScanParamsAndKeepsExplicitFormats) {
    TFileRangeDesc range;
    range.__isset.table_format_params = true;
    range.table_format_params.__isset.paimon_params = true;
    range.table_format_params.paimon_params.__set_paimon_options({{"scan.mode", "from-range"}});
    range.table_format_params.paimon_params.__set_hadoop_conf(
            {{"fs.oss.accessKeyId", "range-ak"}, {"fs.oss.endpoint", "range-endpoint"}});
    range.table_format_params.paimon_params.__set_file_format("orc");

    TFileScanRangeParams range_params;
    range_params.__set_paimon_options({{"scan.mode", "from-params"},
                                       {"file.format", "parquet"},
                                       {"manifest.format", "avro"}});
    range_params.__set_properties({{"AWS_ACCESS_KEY", "explicit-ak"},
                                   {"fs.s3a.secret.key", "param-sk"},
                                   {"fs.s3a.endpoint", "param-endpoint"}});

    PaimonCppReader reader(_file_slot_descs, _runtime_state.get(), &_profile, range, &range_params);
    auto options = reader._build_options();

    EXPECT_EQ(options["scan.mode"], "from-params");
    EXPECT_FALSE(options.contains("fs.oss.accessKeyId"));
    EXPECT_EQ(options["AWS_ACCESS_KEY"], "explicit-ak");
    EXPECT_EQ(options["AWS_SECRET_KEY"], "param-sk");
    EXPECT_EQ(options["AWS_ENDPOINT"], "param-endpoint");
    EXPECT_EQ(options[paimon::Options::FILE_FORMAT], "parquet");
    EXPECT_EQ(options[paimon::Options::MANIFEST_FORMAT], "avro");
    EXPECT_EQ(options[paimon::Options::FILE_SYSTEM], "doris");
}

TEST_F(PaimonCppReaderTest, BuildOptionsCopiesSessionTokenAndRegionWhenMissing) {
    TFileRangeDesc range;
    range.__isset.table_format_params = true;
    range.table_format_params.__isset.paimon_params = true;
    range.table_format_params.paimon_params.__set_hadoop_conf({{"fs.oss.sessionToken", "oss-token"},
                                                               {"fs.oss.region", "oss-region"},
                                                               {"fs.s3a.session.token", "s3-token"},
                                                               {"fs.s3a.region", "s3-region"}});

    PaimonCppReader reader(_file_slot_descs, _runtime_state.get(), &_profile, range, nullptr);
    auto options = reader._build_options();

    EXPECT_EQ(options["AWS_TOKEN"], "oss-token");
    EXPECT_EQ(options["AWS_REGION"], "oss-region");
    EXPECT_EQ(options[paimon::Options::FILE_SYSTEM], "doris");
}

TEST_F(PaimonCppReaderTest, BuildOptionsKeepsExplicitDestinationKeys) {
    TFileRangeDesc range;
    range.__isset.table_format_params = true;
    range.table_format_params.__isset.paimon_params = true;
    range.table_format_params.paimon_params.__set_hadoop_conf(
            {{"fs.s3a.session.token", "range-token"},
             {"fs.s3a.region", "range-region"},
             {"fs.s3a.path.style.access", "true"}});

    TFileScanRangeParams range_params;
    range_params.__set_properties({{"AWS_TOKEN", "explicit-token"},
                                   {"AWS_REGION", "explicit-region"},
                                   {"use_path_style", "false"}});

    PaimonCppReader reader(_file_slot_descs, _runtime_state.get(), &_profile, range, &range_params);
    auto options = reader._build_options();

    EXPECT_EQ(options["AWS_TOKEN"], "explicit-token");
    EXPECT_EQ(options["AWS_REGION"], "explicit-region");
    EXPECT_EQ(options["use_path_style"], "false");
    EXPECT_EQ(options[paimon::Options::FILE_SYSTEM], "doris");
}

TEST_F(PaimonCppReaderTest, BuildReadColumnsAndGetColumnsFollowFileSlots) {
    ObjectPool object_pool;
    auto file_slot_descs = _build_file_slot_descs(&object_pool);
    TFileRangeDesc range;
    PaimonCppReader reader(file_slot_descs, _runtime_state.get(), &_profile, range, nullptr);

    auto read_columns = reader._build_read_columns();
    EXPECT_EQ(read_columns, (std::vector<std::string> {"id", "name"}));

    std::unordered_map<std::string, DataTypePtr> name_to_type;
    auto status = reader._get_columns_impl(&name_to_type);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(name_to_type.size(), 2);
    EXPECT_EQ(name_to_type["id"]->get_name(), file_slot_descs[0]->type()->get_name());
    EXPECT_EQ(name_to_type["name"]->get_name(), file_slot_descs[1]->type()->get_name());
}

} // namespace doris
