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

#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "core/block/block.h"
#include "core/data_type/data_type_bitmap.h"
#include "core/data_type/data_type_hll.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_quantilestate.h"
#include "core/data_type/data_type_string.h"
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#define private public
#define protected public
#include "format/jni/jni_reader.h"
#include "format/table/hudi_jni_reader.h"
#include "format/table/iceberg_sys_table_jni_reader.h"
#include "format/table/jdbc_jni_reader.h"
#include "format/table/max_compute_jni_reader.h"
#include "format/table/paimon_jni_reader.h"
#include "format/table/trino_connector_jni_reader.h"
#undef protected
#undef private
#pragma clang diagnostic pop
#include "runtime/descriptors.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "testutil/desc_tbl_builder.h"
#include "util/debug_util.h"

namespace doris {

class JniReaderTest : public testing::Test {
protected:
    void SetUp() override {
        _query_options.__set_batch_size(3);
        _runtime_state = std::make_unique<RuntimeState>(_query_options, _query_globals);
        _runtime_state->set_timezone("UTC");
    }

    std::vector<SlotDescriptor*> build_file_slot_descs(
            const std::vector<std::pair<std::string, DataTypePtr>>& columns) {
        DescriptorTblBuilder builder(&_object_pool);
        auto& tuple_builder = builder.declare_tuple();
        for (const auto& [name, type] : columns) {
            tuple_builder << std::make_tuple(type, name);
        }
        return builder.build()->get_tuple_descriptor(0)->slots();
    }

    static Block make_block(const std::vector<SlotDescriptor*>& slots) {
        Block block;
        for (auto* slot : slots) {
            auto type = slot->get_data_type_ptr();
            block.insert({type->create_column(), type, slot->col_name()});
        }
        return block;
    }

    TQueryOptions _query_options;
    TQueryGlobals _query_globals;
    ObjectPool _object_pool;
    std::unique_ptr<RuntimeState> _runtime_state;
    RuntimeProfile _profile {"jni_reader_test"};
};

TEST_F(JniReaderTest, MockReaderBuildsScannerParams) {
    auto slots =
            build_file_slot_descs({{"id", std::make_shared<DataTypeInt32>()},
                                   {"name", make_nullable(std::make_shared<DataTypeString>())}});

    MockJniReader reader(slots, _runtime_state.get(), &_profile);

    EXPECT_EQ(reader._connector_class, "org/apache/doris/common/jni/MockJniScanner");
    EXPECT_EQ(reader._connector_name, "MockJniScanner");
    EXPECT_EQ(reader._column_names, std::vector<std::string>({"id", "name"}));
    EXPECT_EQ(reader._scanner_params["mock_rows"], "10240");
    EXPECT_EQ(reader._scanner_params["required_fields"], "id,name");
    EXPECT_EQ(reader._scanner_params["columns_types"], "int#string");
}

TEST_F(JniReaderTest, HudiReaderBuildsScannerParamsAndPrefixesConfigs) {
    auto slots = build_file_slot_descs({{"id", std::make_shared<DataTypeInt32>()},
                                        {"name", std::make_shared<DataTypeString>()}});

    TFileScanRangeParams scan_params;
    scan_params.__set_properties({{"hoodie.datasource.query.type", "snapshot"},
                                  {"fs.defaultFS", "hdfs://namenode:8020"}});

    THudiFileDesc hudi_params;
    hudi_params.__set_base_path("s3://warehouse/tbl");
    hudi_params.__set_data_file_path("s3://warehouse/tbl/000.parquet");
    hudi_params.__set_data_file_length(4096);
    hudi_params.__set_delta_logs({"log1", "log2"});
    hudi_params.__set_column_names({"id", "name"});
    hudi_params.__set_column_types({"int", "string"});
    hudi_params.__set_instant_time("20240422010101");
    hudi_params.__set_serde("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
    hudi_params.__set_input_format("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");

    HudiJniReader reader(scan_params, hudi_params, slots, _runtime_state.get(), &_profile);

    EXPECT_EQ(reader._connector_class, "org/apache/doris/hudi/HadoopHudiJniScanner");
    EXPECT_EQ(reader._connector_name, "HadoopHudiJniScanner");
    EXPECT_EQ(reader._column_names, std::vector<std::string>({"id", "name"}));
    EXPECT_EQ(reader._scanner_params["query_id"], print_id(_runtime_state->query_id()));
    EXPECT_EQ(reader._scanner_params["base_path"], "s3://warehouse/tbl");
    EXPECT_EQ(reader._scanner_params["data_file_path"], "s3://warehouse/tbl/000.parquet");
    EXPECT_EQ(reader._scanner_params["data_file_length"], "4096");
    EXPECT_EQ(reader._scanner_params["delta_file_paths"], "log1,log2");
    EXPECT_EQ(reader._scanner_params["hudi_column_names"], "id,name");
    EXPECT_EQ(reader._scanner_params["hudi_column_types"], "int#string");
    EXPECT_EQ(reader._scanner_params["required_fields"], "id,name");
    EXPECT_EQ(reader._scanner_params["instant_time"], "20240422010101");
    EXPECT_EQ(reader._scanner_params["serde"],
              "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
    EXPECT_EQ(reader._scanner_params["input_format"],
              "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
    EXPECT_EQ(reader._scanner_params["time_zone"], _runtime_state->timezone_obj().name());
    EXPECT_EQ(reader._scanner_params["hoodie.datasource.query.type"], "snapshot");
    EXPECT_EQ(reader._scanner_params["hadoop_conf.fs.defaultFS"], "hdfs://namenode:8020");
}

TEST_F(JniReaderTest, PaimonReaderPrefersRangeParamsForPredicateOptionsAndProperties) {
    auto slots = build_file_slot_descs({{"id", std::make_shared<DataTypeInt32>()},
                                        {"name", std::make_shared<DataTypeString>()}});

    TFileRangeDesc range;
    range.__isset.self_split_weight = true;
    range.self_split_weight = 17;
    range.__isset.table_format_params = true;
    range.table_format_params.__isset.paimon_params = true;
    range.table_format_params.__isset.table_level_row_count = true;
    range.table_format_params.table_level_row_count = 5;
    range.table_format_params.paimon_params.__set_paimon_split("encoded-split-from-range");
    range.table_format_params.paimon_params.__set_paimon_predicate("predicate-from-range");
    range.table_format_params.paimon_params.__set_paimon_options(
            {{"scan.mode", "range"}, {"file.format", "orc"}});
    range.table_format_params.paimon_params.__set_hadoop_conf({{"fs.defaultFS", "hdfs://range"}});

    TFileScanRangeParams range_params;
    range_params.__set_paimon_predicate("predicate-from-params");
    range_params.__set_serialized_table("serialized-table-from-params");
    range_params.__set_paimon_options({{"scan.mode", "params"}});
    range_params.__set_properties({{"fs.defaultFS", "hdfs://params"}});

    PaimonJniReader reader(slots, _runtime_state.get(), &_profile, range, &range_params);

    EXPECT_EQ(reader._connector_class, "org/apache/doris/paimon/PaimonJniScanner");
    EXPECT_EQ(reader._connector_name, "PaimonJniScanner");
    EXPECT_EQ(reader._self_split_weight, 17);
    EXPECT_EQ(reader._remaining_table_level_row_count, 5);
    EXPECT_EQ(reader._scanner_params["paimon_split"], "encoded-split-from-range");
    EXPECT_EQ(reader._scanner_params["paimon_predicate"], "predicate-from-params");
    EXPECT_EQ(reader._scanner_params["serialized_table"], "serialized-table-from-params");
    EXPECT_EQ(reader._scanner_params["required_fields"], "id,name");
    EXPECT_EQ(reader._scanner_params["columns_types"], "int#string");
    EXPECT_EQ(reader._scanner_params["time_zone"], "UTC");
    EXPECT_EQ(reader._scanner_params["paimon.scan.mode"], "params");
    EXPECT_EQ(reader._scanner_params["hadoop.fs.defaultFS"], "hdfs://params");
    EXPECT_EQ(reader._scanner_params.find("paimon.file.format"), reader._scanner_params.end());
}

TEST_F(JniReaderTest, PaimonReaderFallsBackToRangeWhenRangeParamsAreUnset) {
    auto slots = build_file_slot_descs({{"id", std::make_shared<DataTypeInt32>()}});

    TFileRangeDesc range;
    range.__isset.table_format_params = true;
    range.table_format_params.__isset.paimon_params = true;
    range.table_format_params.paimon_params.__set_paimon_split("encoded-split");
    range.table_format_params.paimon_params.__set_paimon_predicate("predicate-from-range");
    range.table_format_params.paimon_params.__set_paimon_options(
            {{"scan.mode", "snapshot"}, {"file.format", "parquet"}});
    range.table_format_params.paimon_params.__set_hadoop_conf(
            {{"fs.defaultFS", "hdfs://fallback"}});

    TFileScanRangeParams range_params;
    PaimonJniReader reader(slots, _runtime_state.get(), &_profile, range, &range_params);

    EXPECT_EQ(reader._scanner_params["paimon_predicate"], "predicate-from-range");
    EXPECT_EQ(reader._scanner_params["paimon.scan.mode"], "snapshot");
    EXPECT_EQ(reader._scanner_params["paimon.file.format"], "parquet");
    EXPECT_EQ(reader._scanner_params["hadoop.fs.defaultFS"], "hdfs://fallback");
    EXPECT_EQ(reader._scanner_params.find("serialized_table"), reader._scanner_params.end());
}

TEST_F(JniReaderTest, PaimonReaderCountPushdownUsesTableLevelRowCount) {
    auto slots = build_file_slot_descs({{"id", std::make_shared<DataTypeInt32>()},
                                        {"name", std::make_shared<DataTypeString>()}});

    TFileRangeDesc range;
    range.__isset.table_format_params = true;
    range.table_format_params.__isset.table_level_row_count = true;
    range.table_format_params.table_level_row_count = 5;
    range.table_format_params.__isset.paimon_params = true;
    range.table_format_params.paimon_params.__set_paimon_split("encoded-split");

    TFileScanRangeParams range_params;
    PaimonJniReader reader(slots, _runtime_state.get(), &_profile, range, &range_params);
    reader.set_push_down_agg_type(TPushAggOp::type::COUNT);

    Block block = make_block(slots);
    size_t read_rows = 0;
    bool eof = false;

    auto status = reader.get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(3, read_rows);
    EXPECT_FALSE(eof);
    EXPECT_EQ(3, block.rows());

    status = reader.get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(2, read_rows);
    EXPECT_TRUE(eof);
    EXPECT_EQ(2, block.rows());

    status = reader.get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(0, read_rows);
    EXPECT_TRUE(eof);
    EXPECT_EQ(0, block.rows());
}

TEST_F(JniReaderTest, JdbcReaderBuildsReplaceStringForSpecialTypes) {
    auto slots = build_file_slot_descs({{"bitmap_col", std::make_shared<DataTypeBitMap>()},
                                        {"hll_col", std::make_shared<DataTypeHLL>()},
                                        {"jsonb_col", std::make_shared<DataTypeJsonb>()},
                                        {"quantile_col", std::make_shared<DataTypeQuantileState>()},
                                        {"id", std::make_shared<DataTypeInt32>()}});

    std::map<std::string, std::string> jdbc_params {{"jdbc_url", "jdbc:mysql://127.0.0.1:3306/db"},
                                                    {"query_sql", "select 1"}};
    JdbcJniReader reader(slots, _runtime_state.get(), &_profile, jdbc_params);

    EXPECT_EQ(reader._connector_class, "org/apache/doris/jdbc/JdbcJniScanner");
    EXPECT_EQ(reader._connector_name, "JdbcJniScanner");
    EXPECT_EQ(reader._column_names, std::vector<std::string>({"bitmap_col", "hll_col", "jsonb_col",
                                                              "quantile_col", "id"}));
    EXPECT_EQ(reader._scanner_params["required_fields"],
              "bitmap_col,hll_col,jsonb_col,quantile_col,id");
    EXPECT_EQ(reader._scanner_params["columns_types"], "string#string#string#string#int");
    EXPECT_EQ(reader._scanner_params["replace_string"],
              "bitmap,hll,jsonb,quantile_state,not_replace");
    EXPECT_EQ(reader._scanner_params["jdbc_url"], "jdbc:mysql://127.0.0.1:3306/db");
    EXPECT_EQ(reader._scanner_params["query_sql"], "select 1");
}

TEST_F(JniReaderTest, JdbcReaderIdentifiesSpecialTypes) {
    EXPECT_TRUE(JdbcJniReader::_is_special_type(PrimitiveType::TYPE_BITMAP));
    EXPECT_TRUE(JdbcJniReader::_is_special_type(PrimitiveType::TYPE_HLL));
    EXPECT_TRUE(JdbcJniReader::_is_special_type(PrimitiveType::TYPE_JSONB));
    EXPECT_TRUE(JdbcJniReader::_is_special_type(PrimitiveType::TYPE_QUANTILE_STATE));
    EXPECT_FALSE(JdbcJniReader::_is_special_type(PrimitiveType::TYPE_INT));
    EXPECT_FALSE(JdbcJniReader::_is_special_type(PrimitiveType::TYPE_STRING));
}

TEST_F(JniReaderTest, TrinoReaderBuildsScannerParams) {
    auto slots = build_file_slot_descs({{"id", std::make_shared<DataTypeInt32>()},
                                        {"name", std::make_shared<DataTypeString>()}});

    TFileRangeDesc range;
    range.__isset.table_format_params = true;
    range.table_format_params.__isset.trino_connector_params = true;
    auto& params = range.table_format_params.trino_connector_params;
    params.__set_catalog_name("hive");
    params.__set_db_name("db1");
    params.__set_table_name("tbl1");
    params.__set_trino_connector_split("serialized-split");
    params.__set_trino_connector_table_handle("table-handle");
    params.__set_trino_connector_column_handles("column-handles");
    params.__set_trino_connector_column_metadata("column-metadata");
    params.__set_trino_connector_predicate("predicate-json");
    params.__set_trino_connector_trascation_handle("txn-handle");
    params.__set_trino_connector_options({{"hive.metastore.uri", "thrift://127.0.0.1:9083"}});

    TrinoConnectorJniReader reader(slots, _runtime_state.get(), &_profile, range);

    EXPECT_EQ(reader._connector_class, "org/apache/doris/trinoconnector/TrinoConnectorJniScanner");
    EXPECT_EQ(reader._connector_name, "TrinoConnectorJniScanner");
    EXPECT_EQ(reader._scanner_params["catalog_name"], "hive");
    EXPECT_EQ(reader._scanner_params["db_name"], "db1");
    EXPECT_EQ(reader._scanner_params["table_name"], "tbl1");
    EXPECT_EQ(reader._scanner_params["trino_connector_split"], "serialized-split");
    EXPECT_EQ(reader._scanner_params["trino_connector_table_handle"], "table-handle");
    EXPECT_EQ(reader._scanner_params["trino_connector_column_handles"], "column-handles");
    EXPECT_EQ(reader._scanner_params["trino_connector_column_metadata"], "column-metadata");
    EXPECT_EQ(reader._scanner_params["trino_connector_predicate"], "predicate-json");
    EXPECT_EQ(reader._scanner_params["trino_connector_trascation_handle"], "txn-handle");
    EXPECT_EQ(reader._scanner_params["required_fields"], "id,name");
    EXPECT_EQ(reader._scanner_params["columns_types"], "int#string");
    EXPECT_EQ(reader._scanner_params["trino.hive.metastore.uri"], "thrift://127.0.0.1:9083");
}

TEST_F(JniReaderTest, IcebergSysTableReaderValidatesRequiredRangeFields) {
    TFileRangeDesc range;
    auto status = IcebergSysTableJniReader::validate_scan_range(range);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("missing table_format_params"), std::string::npos);

    range.__isset.table_format_params = true;
    status = IcebergSysTableJniReader::validate_scan_range(range);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("missing iceberg_params"), std::string::npos);

    range.table_format_params.__isset.iceberg_params = true;
    status = IcebergSysTableJniReader::validate_scan_range(range);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("missing serialized_split"), std::string::npos);
}

TEST_F(JniReaderTest, IcebergSysTableReaderBuildsScannerParams) {
    auto slots = build_file_slot_descs({{"id", std::make_shared<DataTypeInt32>()},
                                        {"name", std::make_shared<DataTypeString>()}});

    TFileRangeDesc range;
    range.__isset.self_split_weight = true;
    range.self_split_weight = 9;
    range.__isset.table_format_params = true;
    range.table_format_params.__isset.iceberg_params = true;
    range.table_format_params.iceberg_params.__set_serialized_split("serialized-iceberg-split");

    TFileScanRangeParams range_params;
    range_params.__set_properties({{"fs.defaultFS", "hdfs://iceberg"}});

    IcebergSysTableJniReader reader(slots, _runtime_state.get(), &_profile, range, &range_params);

    EXPECT_TRUE(reader._init_status.ok());
    EXPECT_EQ(reader._self_split_weight, 9);
    EXPECT_EQ(reader._scanner_params["serialized_split"], "serialized-iceberg-split");
    EXPECT_EQ(reader._scanner_params["required_fields"], "id,name");
    EXPECT_EQ(reader._scanner_params["required_types"], "int#string");
    EXPECT_EQ(reader._scanner_params["time_zone"], "UTC");
    EXPECT_EQ(reader._scanner_params["hadoop.fs.defaultFS"], "hdfs://iceberg");
}

TEST_F(JniReaderTest, MaxComputeReaderBuildsScannerParamsFromDescriptorAndScanRange) {
    auto slots = build_file_slot_descs({{"id", std::make_shared<DataTypeInt32>()},
                                        {"name", std::make_shared<DataTypeString>()}});

    TTableDescriptor table_desc;
    table_desc.id = 1;
    table_desc.tableType = TTableType::MAX_COMPUTE_TABLE;
    table_desc.numCols = 2;
    table_desc.numClusteringCols = 0;
    table_desc.tableName = "mc_tbl";
    table_desc.dbName = "mc_db";
    table_desc.__isset.mcTable = true;
    table_desc.mcTable.__set_project("mc_project");
    table_desc.mcTable.__set_table("mc_table");
    table_desc.mcTable.__set_endpoint("service.cn.maxcompute.aliyun.com");
    table_desc.mcTable.__set_quota("quota_a");
    table_desc.mcTable.__set_properties({{"mc.access_key", "ak"}, {"mc.secret_key", "sk"}});

    MaxComputeTableDescriptor mc_desc(table_desc);
    ASSERT_TRUE(mc_desc.init_status().ok()) << mc_desc.init_status();

    TFileRangeDesc range;
    range.start_offset = 128;
    range.size = 256;

    TMaxComputeFileDesc max_compute_params;
    max_compute_params.__set_session_id("session-1");
    max_compute_params.__set_table_batch_read_session("scan-serializer");
    max_compute_params.__set_connect_timeout(30);
    max_compute_params.__set_read_timeout(60);
    max_compute_params.__set_retry_times(3);

    MaxComputeJniReader reader(&mc_desc, max_compute_params, slots, range, _runtime_state.get(),
                               &_profile);

    EXPECT_EQ(reader._connector_class, "org/apache/doris/maxcompute/MaxComputeJniScanner");
    EXPECT_EQ(reader._connector_name, "MaxComputeJniScanner");
    EXPECT_EQ(reader._scanner_params["endpoint"], "service.cn.maxcompute.aliyun.com");
    EXPECT_EQ(reader._scanner_params["quota"], "quota_a");
    EXPECT_EQ(reader._scanner_params["project"], "mc_project");
    EXPECT_EQ(reader._scanner_params["table"], "mc_table");
    EXPECT_EQ(reader._scanner_params["session_id"], "session-1");
    EXPECT_EQ(reader._scanner_params["scan_serializer"], "scan-serializer");
    EXPECT_EQ(reader._scanner_params["start_offset"], "128");
    EXPECT_EQ(reader._scanner_params["split_size"], "256");
    EXPECT_EQ(reader._scanner_params["required_fields"], "id,name");
    EXPECT_EQ(reader._scanner_params["columns_types"], "int#string");
    EXPECT_EQ(reader._scanner_params["connect_timeout"], "30");
    EXPECT_EQ(reader._scanner_params["read_timeout"], "60");
    EXPECT_EQ(reader._scanner_params["retry_count"], "3");
    EXPECT_EQ(reader._scanner_params["mc.access_key"], "ak");
    EXPECT_EQ(reader._scanner_params["mc.secret_key"], "sk");
}

} // namespace doris
