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

#include <map>
#include <string>

#include "io/file_factory.h"
#include "io/fs/file_reader.h"
#include "io/io_common.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "vec/exec/format/table/paimon_reader.h"

namespace doris::vectorized {

class PaimonMockReader final : public PaimonReader {
public:
    PaimonMockReader(std::unique_ptr<GenericReader> file_format_reader, RuntimeProfile* profile,
                     RuntimeState* state, const TFileScanRangeParams& params,
                     const TFileRangeDesc& range, io::IOContext* io_ctx)
            : PaimonReader(std::move(file_format_reader), profile, state, params, range, io_ctx) {};
    ~PaimonMockReader() final = default;

    void set_delete_rows() final {
        (reinterpret_cast<OrcReader*>(_file_format_reader.get()))
                ->set_position_delete_rowids(&_delete_rows);
    }

    void check() {
        ASSERT_TRUE(_has_schema_change == true);
        ASSERT_TRUE(_new_colname_to_value_range.empty());
        std::unordered_map<std::string, std::string> table_col_to_file_col_ans;
        table_col_to_file_col_ans["b"] = "map_col";
        table_col_to_file_col_ans["e"] = "array_col";
        table_col_to_file_col_ans["d"] = "struct_col";
        table_col_to_file_col_ans["a"] = "vvv";
        table_col_to_file_col_ans["c"] = "k";
        table_col_to_file_col_ans["nonono"] = "nonono";
        for (auto [table_col, file_col] : table_col_to_file_col_ans) {
            ASSERT_TRUE(_table_col_to_file_col[table_col] == file_col);
            ASSERT_TRUE(_file_col_to_table_col[file_col] == table_col);
        }
    }
};

class PaimonReaderTest : public ::testing::Test {
protected:
    void SetUp() override {
        _profile = new RuntimeProfile("test_profile");
        _state = new RuntimeState(TQueryGlobals());
        _io_ctx = new io::IOContext();
        _schema_file_path = "./be/test/exec/test_data/paimon_scanner/schema-0";
    }

    void TearDown() override {
        delete _profile;
        delete _state;
        delete _io_ctx;
    }

    RuntimeProfile* _profile;
    RuntimeState* _state;
    io::IOContext* _io_ctx;
    std::string _schema_file_path;
};

TEST_F(PaimonReaderTest, ReadSchemaFile) {
    std::map<int64_t, std::string> file_id_to_name;
    file_id_to_name[0] = "k";
    file_id_to_name[1] = "vvv";
    file_id_to_name[2] = "array_col";
    file_id_to_name[3] = "struct_col";
    file_id_to_name[6] = "map_col";

    TFileScanRangeParams params;
    params.file_type = TFileType::FILE_LOCAL;
    params.properties = {};
    params.hdfs_params = {};
    params.__isset.paimon_schema_info = true;
    params.paimon_schema_info[0] = file_id_to_name;
    TFileRangeDesc range;
    range.table_format_params.paimon_params.schema_id = 0;

    PaimonMockReader reader(nullptr, _profile, _state, params, range, _io_ctx);

    //        create table tmp5 (
    //                k int,
    //                vVV string,
    //                array_col array<int>,
    //                struct_COL struct<a:int,b:string>,
    //                map_COL map<string,int>
    //        ) tblproperties (
    //                'primary-key' = 'k',
    //                "file.format" = "parquet"
    //        );

    std::vector<std::string> read_table_col_names;
    read_table_col_names.emplace_back("a");
    read_table_col_names.emplace_back("b");
    read_table_col_names.emplace_back("c");
    read_table_col_names.emplace_back("d");
    read_table_col_names.emplace_back("e");
    read_table_col_names.emplace_back("nonono");

    std::unordered_map<uint64_t, std::string> table_col_id_table_name_map;
    table_col_id_table_name_map[1] = "a";
    table_col_id_table_name_map[6] = "b";
    table_col_id_table_name_map[0] = "c";
    table_col_id_table_name_map[3] = "d";
    table_col_id_table_name_map[2] = "e";
    table_col_id_table_name_map[10] = "nonono";

    std::unordered_map<std::string, ColumnValueRangeType> table_col_name_to_value_range;
    Status status = reader.gen_file_col_name(read_table_col_names, table_col_id_table_name_map,
                                             &table_col_name_to_value_range);
    ASSERT_TRUE(status.ok());
    reader.check();
}

} // namespace doris::vectorized