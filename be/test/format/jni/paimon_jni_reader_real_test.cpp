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

#include <array>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <system_error>
#include <vector>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "format/table/paimon_jni_reader.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "testutil/desc_tbl_builder.h"
#include "util/jni-util.h"

namespace doris {

class PaimonJniReaderRealTest : public testing::Test {
protected:
    struct SerializedPaimonInputs {
        std::string serialized_table;
        std::string serialized_split;
        std::string serialized_predicate;
    };

    void SetUp() override {
        _query_options.__set_batch_size(16);
        _runtime_state = std::make_unique<RuntimeState>(_query_options, _query_globals);
        _runtime_state->set_timezone("UTC");
    }

    std::vector<SlotDescriptor*> build_file_slot_descs() {
        DescriptorTblBuilder builder(&_object_pool);
        builder.declare_tuple() << std::make_tuple(std::make_shared<DataTypeInt32>(), "id");
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

    static std::filesystem::path repo_root() {
        const char* root = std::getenv("ROOT");
        DORIS_CHECK(root != nullptr);
        return root;
    }

    static std::filesystem::path paimon_scanner_jar() {
        return repo_root() /
               "output/be/lib/java_extensions/paimon-scanner/"
               "paimon-scanner-jar-with-dependencies.jar";
    }

    static std::filesystem::path hadoop_deps_root() {
        return repo_root() / "output/be/lib/hadoop_hdfs";
    }

    static std::filesystem::path paimon_warehouse_root() {
        return repo_root() / "docker/thirdparties/docker-compose/hive/scripts/paimon1";
    }

    static bool is_regular_file(const std::filesystem::path& path) {
        std::error_code error_code;
        return std::filesystem::is_regular_file(path, error_code);
    }

    static bool is_directory(const std::filesystem::path& path) {
        std::error_code error_code;
        return std::filesystem::is_directory(path, error_code);
    }

    static std::string real_test_prerequisite_error() {
        const char* root = std::getenv("ROOT");
        if (root == nullptr || std::string(root).empty()) {
            return "ROOT is not set";
        }
        if (!is_directory(root)) {
            return "ROOT does not point to an existing directory: " + std::string(root);
        }

        const char* java_home = std::getenv("JAVA_HOME");
        if (java_home == nullptr || std::string(java_home).empty()) {
            return "JAVA_HOME is not set";
        }
        const auto java_bin = std::filesystem::path(java_home) / "bin/java";
        if (!is_regular_file(java_bin)) {
            return "JAVA_HOME does not contain bin/java: " + java_bin.string();
        }

        if (!is_regular_file(paimon_scanner_jar())) {
            return "Paimon scanner jar not found: " + paimon_scanner_jar().string();
        }
        if (!is_directory(hadoop_deps_root())) {
            return "Hadoop dependency directory not found: " + hadoop_deps_root().string();
        }
        if (!is_directory(paimon_warehouse_root())) {
            return "Paimon test warehouse not found: " + paimon_warehouse_root().string();
        }
        return "";
    }

    static std::string shell_quote(const std::string& value) {
        std::string quoted = "'";
        for (char ch : value) {
            if (ch == '\'') {
                quoted.append("'\"'\"'");
            } else {
                quoted.push_back(ch);
            }
        }
        quoted.push_back('\'');
        return quoted;
    }

    static std::string run_command_and_capture_output(const std::string& command) {
        std::array<char, 4096> buffer {};
        std::string output;
        FILE* pipe = popen(command.c_str(), "r");
        DORIS_CHECK(pipe != nullptr);
        while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe) != nullptr) {
            output.append(buffer.data());
        }
        const int exit_code = pclose(pipe);
        CHECK_EQ(exit_code, 0) << output;
        return output;
    }

    static void append_jars_from_dir(std::string& classpath, const std::filesystem::path& dir) {
        if (!is_directory(dir)) {
            return;
        }
        for (const auto& entry : std::filesystem::directory_iterator(dir)) {
            if (entry.is_regular_file() && entry.path().extension() == ".jar") {
                classpath.append(":").append(entry.path().string());
            }
        }
    }

    static SerializedPaimonInputs build_serialized_inputs() {
        const auto helper_dir =
                std::filesystem::temp_directory_path() / "paimon_jni_reader_real_test_helper";
        std::filesystem::remove_all(helper_dir);
        std::filesystem::create_directories(helper_dir);

        const auto helper_source = helper_dir / "PaimonSerializationHelper.java";
        std::ofstream out(helper_source);
        out << R"(import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.InstantiationUtil;

import java.util.Base64;
import java.util.Collections;
import java.util.List;

public class PaimonSerializationHelper {
    private static String encodeObject(Object value) throws Exception {
        return Base64.getEncoder().encodeToString(InstantiationUtil.serializeObject(value));
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.setString("warehouse", args[0]);
        Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
        Table table = catalog.getTable(Identifier.create(args[1], args[2]));
        List<Split> splits = table.newReadBuilder().newScan().plan().splits();
        if (splits.size() != 1) {
            throw new IllegalStateException("Expected exactly one split but got " + splits.size());
        }

        PredicateBuilder predicateBuilder = new PredicateBuilder(table.rowType());
        Predicate predicate = predicateBuilder.greaterOrEqual(predicateBuilder.indexOf("id"), 2);

        System.out.println("TABLE=" + encodeObject(table));
        System.out.println("SPLIT=" + encodeObject(splits.get(0)));
        System.out.println("PREDICATE=" + encodeObject(Collections.singletonList(predicate)));
        catalog.close();
    }
}
)";
        out.close();
        DORIS_CHECK(out.good());

        std::string classpath = paimon_scanner_jar().string();
        append_jars_from_dir(classpath, hadoop_deps_root());
        append_jars_from_dir(classpath, hadoop_deps_root() / "lib");

        const auto java_bin = std::filesystem::path(std::getenv("JAVA_HOME")) / "bin/java";
        DORIS_CHECK(std::filesystem::exists(java_bin));

        const std::string command =
                shell_quote(java_bin.string()) + " -cp " + shell_quote(classpath) + " " +
                shell_quote(helper_source.string()) + " " +
                shell_quote(paimon_warehouse_root().string()) + " db1 row_jni_test";
        const auto output = run_command_and_capture_output(command);

        SerializedPaimonInputs inputs;
        std::istringstream stream(output);
        std::string line;
        while (std::getline(stream, line)) {
            if (line.rfind("TABLE=", 0) == 0) {
                inputs.serialized_table = line.substr(sizeof("TABLE=") - 1);
            } else if (line.rfind("SPLIT=", 0) == 0) {
                inputs.serialized_split = line.substr(sizeof("SPLIT=") - 1);
            } else if (line.rfind("PREDICATE=", 0) == 0) {
                inputs.serialized_predicate = line.substr(sizeof("PREDICATE=") - 1);
            }
        }
        std::filesystem::remove_all(helper_dir);

        DORIS_CHECK(!inputs.serialized_table.empty());
        DORIS_CHECK(!inputs.serialized_split.empty());
        DORIS_CHECK(!inputs.serialized_predicate.empty());
        return inputs;
    }

    static Status init_jni_runtime_once() {
        static const Status init_status = Jni::Util::Init();
        return init_status;
    }

    static SerializedPaimonInputs build_serialized_inputs_once() {
        static const SerializedPaimonInputs inputs = build_serialized_inputs();
        return inputs;
    }

    static std::vector<int32_t> collect_ids(const Block& block, size_t read_rows) {
        const auto& column = block.get_by_position(block.get_position_by_name("id"));
        const auto* nullable = check_and_get_column<ColumnNullable>(column.column.get());
        DORIS_CHECK(nullable != nullptr);
        const auto& id_col = assert_cast<const ColumnInt32&>(nullable->get_nested_column());
        std::vector<int32_t> ids;
        ids.reserve(read_rows);
        for (size_t i = 0; i < read_rows; ++i) {
            DORIS_CHECK(!nullable->is_null_at(i));
            ids.emplace_back(id_col.get_element(i));
        }
        return ids;
    }

    TQueryOptions _query_options;
    TQueryGlobals _query_globals;
    ObjectPool _object_pool;
    std::unique_ptr<RuntimeState> _runtime_state;
    RuntimeProfile _profile {"paimon_jni_reader_real_test"};
};

TEST_F(PaimonJniReaderRealTest, ReadsFilteredRowsFromFilesystemTable) {
    const auto prerequisite_error = real_test_prerequisite_error();
    if (!prerequisite_error.empty()) {
        GTEST_SKIP() << prerequisite_error;
    }

    auto init_status = init_jni_runtime_once();
    ASSERT_TRUE(init_status.ok()) << init_status;

    const auto inputs = build_serialized_inputs_once();
    auto slots = build_file_slot_descs();

    TFileRangeDesc range;
    range.__isset.table_format_params = true;
    range.table_format_params.__isset.paimon_params = true;
    range.table_format_params.paimon_params.__set_paimon_split(inputs.serialized_split);

    TFileScanRangeParams range_params;
    range_params.__set_serialized_table(inputs.serialized_table);
    range_params.__set_paimon_predicate(inputs.serialized_predicate);

    PaimonJniReader reader(slots, _runtime_state.get(), &_profile, range, &range_params);

    auto open_status = reader.init_reader();
    ASSERT_TRUE(open_status.ok()) << open_status;

    Block block = make_block(slots);
    size_t read_rows = 0;
    bool eof = false;
    auto read_status = reader.get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(read_status.ok()) << read_status;
    EXPECT_EQ(2, read_rows);
    EXPECT_FALSE(eof);
    EXPECT_EQ(collect_ids(block, read_rows), (std::vector<int32_t> {2, 3}));

    block = make_block(slots);
    read_status = reader.get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(read_status.ok()) << read_status;
    EXPECT_EQ(0, read_rows);
    EXPECT_TRUE(eof);

    auto close_status = reader.close();
    ASSERT_TRUE(close_status.ok()) << close_status;
}

} // namespace doris
