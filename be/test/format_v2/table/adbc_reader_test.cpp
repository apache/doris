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

#include "format_v2/table/adbc_reader.h"

#include <arrow-adbc/adbc.h>
#include <arrow/api.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "format_v2/file_reader.h"
#include "gen_cpp/PlanNodes_types.h"
#include "io/file_factory.h"
#include "io/io_common.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "testutil/adbc_sqlite_driver.h"
#include "testutil/desc_tbl_builder.h"
#include "util/adbc_driver_registry.h"

namespace doris::format::adbc {
namespace {

// Skipping is only correct when thirdparty predates arrow-adbc; it must say so out loud rather
// than surface as an unrelated-looking dlopen failure.
#define SKIP_WITHOUT_SQLITE_DRIVER()                                                              \
    do {                                                                                          \
        if (!adbc_sqlite_driver_available()) {                                                    \
            GTEST_SKIP() << "ADBC SQLite driver not found at " << adbc_sqlite_driver_path()       \
                         << "; run 'cd thirdparty && ./build-thirdparty.sh arrow_adbc' to build " \
                            "it. End-to-end ADBC coverage is NOT being exercised.";               \
        }                                                                                         \
    } while (0)

class BatchAdbcStream final : public AdbcStream {
public:
    BatchAdbcStream(std::vector<std::shared_ptr<arrow::RecordBatch>> batches,
                    std::shared_ptr<int> close_count)
            : _batches(std::move(batches)), _close_count(std::move(close_count)) {}

    Status next(std::shared_ptr<arrow::RecordBatch>* batch) override {
        DORIS_CHECK(batch != nullptr);
        if (_next_batch >= _batches.size()) {
            *batch = nullptr;
            return Status::OK();
        }
        *batch = _batches[_next_batch++];
        return Status::OK();
    }

    Status close() override {
        ++(*_close_count);
        return Status::OK();
    }

private:
    std::vector<std::shared_ptr<arrow::RecordBatch>> _batches;
    std::shared_ptr<int> _close_count;
    size_t _next_batch = 0;
};

TFileRangeDesc adbc_range(std::map<std::string, std::string> params) {
    TTableFormatFileDesc table_desc;
    table_desc.__set_table_format_type("adbc");
    table_desc.__set_adbc_params(std::move(params));

    TFileRangeDesc range;
    range.__set_format_type(TFileFormatType::FORMAT_ARROW);
    range.__set_path("/dummyPath");
    range.__set_table_format_params(std::move(table_desc));
    return range;
}

// A range that passes validation but is never actually connected to; group A injects a fake stream.
TFileRangeDesc fake_adbc_range() {
    return adbc_range({{"driver_path", "/dummy/libadbc_driver_fake.so"},
                       {"uri", "file:/dummy.db"},
                       {"query_sql", "SELECT 1"}});
}

std::vector<SlotDescriptor*> string_slot(ObjectPool* pool, DescriptorTbl** desc_tbl) {
    DescriptorTblBuilder builder(pool);
    builder.declare_tuple() << std::make_tuple(std::make_shared<DataTypeString>(),
                                               std::string("c_str"));
    *desc_tbl = builder.build();
    return (*desc_tbl)->get_tuple_descriptor(0)->slots();
}

std::vector<SlotDescriptor*> int_array_slot(ObjectPool* pool, DescriptorTbl** desc_tbl) {
    DescriptorTblBuilder builder(pool);
    builder.declare_tuple() << std::make_tuple(
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>()),
            std::string("c_array"));
    *desc_tbl = builder.build();
    return (*desc_tbl)->get_tuple_descriptor(0)->slots();
}

std::vector<SlotDescriptor*> sqlite_slots(ObjectPool* pool, DescriptorTbl** desc_tbl) {
    DescriptorTblBuilder builder(pool);
    // SQLite stores integers as 64-bit and reals as doubles; the ADBC driver reports them as such.
    builder.declare_tuple()
            << std::make_tuple(std::make_shared<DataTypeInt64>(), std::string("id"))
            << std::make_tuple(std::make_shared<DataTypeInt64>(), std::string("v_int"))
            << std::make_tuple(std::make_shared<DataTypeFloat64>(), std::string("v_dbl"))
            << std::make_tuple(std::make_shared<DataTypeString>(), std::string("v_txt"));
    *desc_tbl = builder.build();
    return (*desc_tbl)->get_tuple_descriptor(0)->slots();
}

// large_utf8 is what Go-based drivers emit and what the string serde refuses; feeding it through
// proves the reader normalizes before materializing.
std::shared_ptr<arrow::RecordBatch> make_named_large_string_batch(const std::string& column_name) {
    arrow::LargeStringBuilder b;
    EXPECT_TRUE(b.Append("doris").ok());
    EXPECT_TRUE(b.AppendNull().ok());
    std::shared_ptr<arrow::Array> arr;
    EXPECT_TRUE(b.Finish(&arr).ok());
    auto schema = arrow::schema({arrow::field(column_name, arrow::large_utf8())});
    return arrow::RecordBatch::Make(schema, 2, {arr});
}

std::shared_ptr<arrow::RecordBatch> make_large_string_batch() {
    return make_named_large_string_batch("c_str");
}

std::shared_ptr<arrow::RecordBatch> make_list_view_batch() {
    arrow::Int32Builder offsets_builder;
    EXPECT_TRUE(offsets_builder.AppendValues({2, 0}).ok());
    std::shared_ptr<arrow::Array> offsets;
    EXPECT_TRUE(offsets_builder.Finish(&offsets).ok());

    arrow::Int32Builder sizes_builder;
    EXPECT_TRUE(sizes_builder.AppendValues({1, 2}).ok());
    std::shared_ptr<arrow::Array> sizes;
    EXPECT_TRUE(sizes_builder.Finish(&sizes).ok());

    arrow::Int32Builder values_builder;
    EXPECT_TRUE(values_builder.AppendValues({10, 20, 30}).ok());
    std::shared_ptr<arrow::Array> values;
    EXPECT_TRUE(values_builder.Finish(&values).ok());

    auto array = arrow::ListViewArray::FromArrays(*offsets, *sizes, *values).ValueOrDie();
    auto schema = arrow::schema({arrow::field("c_array", array->type())});
    return arrow::RecordBatch::Make(schema, 2, {array});
}

std::unique_ptr<AdbcFileReader> create_reader(RuntimeProfile* profile, const TFileRangeDesc& range,
                                              const std::vector<SlotDescriptor*>& slots,
                                              AdbcStreamFactory factory) {
    auto system_properties = std::make_shared<io::FileSystemProperties>();
    auto file_description = std::make_unique<io::FileDescription>();
    file_description->path = "/dummyPath";
    return std::make_unique<AdbcFileReader>(system_properties, file_description, nullptr, profile,
                                            range, slots, std::move(factory));
}

Block make_request_block(const std::vector<ColumnDefinition>& schema,
                         const std::vector<int32_t>& local_ids) {
    Block block;
    for (const auto local_id : local_ids) {
        const auto it = std::find_if(schema.begin(), schema.end(), [&](const auto& column) {
            return column.local_id == local_id;
        });
        DORIS_CHECK(it != schema.end());
        block.insert({it->type->create_column(), it->type, it->name});
    }
    return block;
}

std::string nullable_string_at(const IColumn& column, size_t row) {
    const auto& nullable = assert_cast<const ColumnNullable&>(column);
    const auto& nested = assert_cast<const ColumnString&>(nullable.get_nested_column());
    return nested.get_data_at(row).to_string();
}

int64_t nullable_int64_at(const IColumn& column, size_t row) {
    const auto& nullable = assert_cast<const ColumnNullable&>(column);
    return assert_cast<const ColumnInt64&>(nullable.get_nested_column()).get_data()[row];
}

double nullable_double_at(const IColumn& column, size_t row) {
    const auto& nullable = assert_cast<const ColumnNullable&>(column);
    return assert_cast<const ColumnFloat64&>(nullable.get_nested_column()).get_data()[row];
}

bool is_null_at(const IColumn& column, size_t row) {
    return assert_cast<const ColumnNullable&>(column).is_null_at(row);
}

// Runs DDL/DML through ADBC so the fixture needs neither the sqlite3 CLI nor a sqlite dev package.
::testing::AssertionResult run_sqlite_ddl(const std::string& uri, const std::string& sql) {
    const AdbcDriver* driver = nullptr;
    Status st = AdbcDriverRegistry::instance().get_or_load(adbc_sqlite_driver_path(), "", &driver);
    if (!st.ok()) {
        return ::testing::AssertionFailure() << "load driver: " << st.to_string();
    }

    AdbcError error = ADBC_ERROR_INIT;
    AdbcDatabase database {};
    AdbcConnection connection {};
    AdbcStatement statement {};
    auto fail = [&](const char* what, AdbcStatusCode code) {
        std::string message = error.message != nullptr ? error.message : "";
        if (error.release != nullptr) {
            error.release(&error);
        }
        return ::testing::AssertionFailure() << what << " failed (" << code << "): " << message;
    };

    if (auto code = driver->DatabaseNew(&database, &error); code != ADBC_STATUS_OK) {
        return fail("DatabaseNew", code);
    }
    if (auto code = driver->DatabaseSetOption(&database, ADBC_OPTION_URI, uri.c_str(), &error);
        code != ADBC_STATUS_OK) {
        return fail("DatabaseSetOption", code);
    }
    if (auto code = driver->DatabaseInit(&database, &error); code != ADBC_STATUS_OK) {
        return fail("DatabaseInit", code);
    }
    if (auto code = driver->ConnectionNew(&connection, &error); code != ADBC_STATUS_OK) {
        return fail("ConnectionNew", code);
    }
    if (auto code = driver->ConnectionInit(&connection, &database, &error);
        code != ADBC_STATUS_OK) {
        return fail("ConnectionInit", code);
    }
    if (auto code = driver->StatementNew(&connection, &statement, &error); code != ADBC_STATUS_OK) {
        return fail("StatementNew", code);
    }
    if (auto code = driver->StatementSetSqlQuery(&statement, sql.c_str(), &error);
        code != ADBC_STATUS_OK) {
        return fail("StatementSetSqlQuery", code);
    }
    int64_t rows_affected = -1;
    if (auto code = driver->StatementExecuteQuery(&statement, nullptr, &rows_affected, &error);
        code != ADBC_STATUS_OK) {
        return fail("StatementExecuteQuery", code);
    }
    if (error.release != nullptr) {
        error.release(&error);
    }
    static_cast<void>(driver->StatementRelease(&statement, &error));
    static_cast<void>(driver->ConnectionRelease(&connection, &error));
    static_cast<void>(driver->DatabaseRelease(&database, &error));
    if (error.release != nullptr) {
        error.release(&error);
    }
    return ::testing::AssertionSuccess();
}

} // namespace

// Group A: the materialization path, driven from a RecordBatch so no database is involved.

// The key case: without normalize_arrow_array the serde hits its unsupported-type branch and this
// fails. Everything else in the reader can be right and the data still would not land.
TEST(AdbcReaderTest, NormalizesLargeStringBeforeMaterializing) {
    ObjectPool pool;
    DescriptorTbl* desc_tbl = nullptr;
    const auto slots = string_slot(&pool, &desc_tbl);
    RuntimeState state;
    RuntimeProfile profile("adbc_reader_normalize_test");
    auto close_count = std::make_shared<int>(0);

    auto reader =
            create_reader(&profile, fake_adbc_range(), slots,
                          [close_count](const TFileRangeDesc&, std::unique_ptr<AdbcStream>* out) {
                              *out = std::make_unique<BatchAdbcStream>(
                                      std::vector<std::shared_ptr<arrow::RecordBatch>> {
                                              make_large_string_batch()},
                                      close_count);
                              return Status::OK();
                          });
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 1);

    auto request = std::make_shared<FileScanRequest>();
    FileScanRequestBuilder builder(request.get());
    ASSERT_TRUE(builder.add_non_predicate_column(LocalColumnId(0)).ok());
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_request_block(schema, {0});
    size_t rows = 0;
    bool eof = false;
    const auto status = reader->get_block(&block, &rows, &eof);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(rows, 2);
    EXPECT_FALSE(eof);
    EXPECT_EQ(nullable_string_at(*block.get_by_position(0).column, 0), "doris");
    EXPECT_TRUE(is_null_at(*block.get_by_position(0).column, 1));

    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_EQ(rows, 0);
    EXPECT_TRUE(eof);
    ASSERT_TRUE(reader->close().ok());
    EXPECT_EQ(*close_count, 1);
}

TEST(AdbcReaderTest, NormalizesListViewBeforeMaterializing) {
    ObjectPool pool;
    DescriptorTbl* desc_tbl = nullptr;
    const auto slots = int_array_slot(&pool, &desc_tbl);
    RuntimeState state;
    RuntimeProfile profile("adbc_reader_list_view_test");
    auto close_count = std::make_shared<int>(0);

    auto reader = create_reader(
            &profile, fake_adbc_range(), slots,
            [close_count](const TFileRangeDesc&, std::unique_ptr<AdbcStream>* out) {
                *out = std::make_unique<BatchAdbcStream>(
                        std::vector<std::shared_ptr<arrow::RecordBatch>> {make_list_view_batch()},
                        close_count);
                return Status::OK();
            });
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<FileScanRequest>();
    FileScanRequestBuilder builder(request.get());
    ASSERT_TRUE(builder.add_non_predicate_column(LocalColumnId(0)).ok());
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_request_block(schema, {0});
    size_t rows = 0;
    bool eof = false;
    const auto status = reader->get_block(&block, &rows, &eof);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(rows, 2);
    const auto& nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    const auto& array = assert_cast<const ColumnArray&>(nullable.get_nested_column());
    ASSERT_EQ(array.get_offsets(), ColumnArray::Offsets64({1, 3}));
    const auto& elements = assert_cast<const ColumnNullable&>(array.get_data());
    const auto& data = assert_cast<const ColumnInt32&>(elements.get_nested_column()).get_data();
    EXPECT_EQ(data, ColumnInt32::Container({30, 10, 20}));

    ASSERT_TRUE(reader->close().ok());
    EXPECT_EQ(*close_count, 1);
}

// An all-null int64 array standing in for a TEXT column, which is what a source that infers Arrow
// types from the values it returns sends when a filter leaves only nulls.
std::shared_ptr<arrow::RecordBatch> make_all_null_int64_batch(const std::string& column_name) {
    arrow::Int64Builder b;
    EXPECT_TRUE(b.AppendNull().ok());
    EXPECT_TRUE(b.AppendNull().ok());
    std::shared_ptr<arrow::Array> arr;
    EXPECT_TRUE(b.Finish(&arr).ok());
    auto schema = arrow::schema({arrow::field(column_name, arrow::int64())});
    return arrow::RecordBatch::Make(schema, 2, {arr});
}

// A column whose values are ALL null arrives with a type that says nothing about the column: a
// source inferring Arrow types from values has nothing to infer from. Measured on the SQLite driver,
// the same TEXT column is utf8 for `SELECT id, name FROM t1` and int64 for the same query plus
// `WHERE name IS NULL`. Without the all-null branch this reaches the string serde and fails with
// "Unsupported arrow type for string column: 9", and FE cannot prevent it -- it cannot know which
// rows a filter will leave.
TEST(AdbcReaderTest, MaterializesAnAllNullColumnWhateverTypeTheSourceClaims) {
    ObjectPool pool;
    DescriptorTbl* desc_tbl = nullptr;
    const auto slots = string_slot(&pool, &desc_tbl);
    RuntimeState state;
    RuntimeProfile profile("adbc_reader_all_null_test");
    auto close_count = std::make_shared<int>(0);

    auto reader =
            create_reader(&profile, fake_adbc_range(), slots,
                          [close_count](const TFileRangeDesc&, std::unique_ptr<AdbcStream>* out) {
                              *out = std::make_unique<BatchAdbcStream>(
                                      std::vector<std::shared_ptr<arrow::RecordBatch>> {
                                              make_all_null_int64_batch("c_str")},
                                      close_count);
                              return Status::OK();
                          });
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<FileScanRequest>();
    FileScanRequestBuilder builder(request.get());
    ASSERT_TRUE(builder.add_non_predicate_column(LocalColumnId(0)).ok());
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_request_block(schema, {0});
    size_t rows = 0;
    bool eof = false;
    const auto status = reader->get_block(&block, &rows, &eof);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(rows, 2);
    EXPECT_TRUE(is_null_at(*block.get_by_position(0).column, 0));
    EXPECT_TRUE(is_null_at(*block.get_by_position(0).column, 1));
}

// The other side of that branch: a column carrying real values keeps its real type, so a genuine
// FE/source schema disagreement still fails rather than being papered over as nulls.
TEST(AdbcReaderTest, StillRejectsATypeMismatchOnAColumnThatHasValues) {
    ObjectPool pool;
    DescriptorTbl* desc_tbl = nullptr;
    const auto slots = string_slot(&pool, &desc_tbl);
    RuntimeState state;
    RuntimeProfile profile("adbc_reader_type_mismatch_test");
    auto close_count = std::make_shared<int>(0);

    arrow::Int64Builder values;
    EXPECT_TRUE(values.Append(7).ok());
    EXPECT_TRUE(values.AppendNull().ok());
    std::shared_ptr<arrow::Array> arr;
    EXPECT_TRUE(values.Finish(&arr).ok());
    auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("c_str", arrow::int64())}), 2,
                                          {arr});

    auto reader = create_reader(
            &profile, fake_adbc_range(), slots,
            [close_count, batch](const TFileRangeDesc&, std::unique_ptr<AdbcStream>* out) {
                *out = std::make_unique<BatchAdbcStream>(
                        std::vector<std::shared_ptr<arrow::RecordBatch>> {batch}, close_count);
                return Status::OK();
            });
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<FileScanRequest>();
    FileScanRequestBuilder builder(request.get());
    ASSERT_TRUE(builder.add_non_predicate_column(LocalColumnId(0)).ok());
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_request_block(schema, {0});
    size_t rows = 0;
    bool eof = false;
    EXPECT_FALSE(reader->get_block(&block, &rows, &eof).ok());
}

// A pushed-down COUNT(*) projects nothing, so every column the source returns is unrequested by
// definition. Without the empty-projection branch the unknown-column check below rejects the first
// one and a query asking for nothing but a number fails.
TEST(AdbcReaderTest, CountsRowsWhenTheScanProjectsNoColumns) {
    RuntimeState state;
    RuntimeProfile profile("adbc_reader_count_only_test");
    auto close_count = std::make_shared<int>(0);
    const std::vector<SlotDescriptor*> no_slots;

    auto reader =
            create_reader(&profile, fake_adbc_range(), no_slots,
                          [close_count](const TFileRangeDesc&, std::unique_ptr<AdbcStream>* out) {
                              *out = std::make_unique<BatchAdbcStream>(
                                      std::vector<std::shared_ptr<arrow::RecordBatch>> {
                                              make_named_large_string_batch("1")},
                                      close_count);
                              return Status::OK();
                          });
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_TRUE(schema.empty());

    auto request = std::make_shared<FileScanRequest>();
    ASSERT_TRUE(reader->open(request).ok());

    Block block;
    size_t rows = 0;
    bool eof = false;
    const auto status = reader->get_block(&block, &rows, &eof);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(rows, 2);
    EXPECT_FALSE(eof);
    EXPECT_EQ(block.columns(), 0);

    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_EQ(rows, 0);
    EXPECT_TRUE(eof);
    ASSERT_TRUE(reader->close().ok());
}

// The other half of the branch above: an unrequested column arriving ALONGSIDE requested ones still
// fails. That state means FE and this reader disagree about the projection, and this check is the
// only signal the disagreement exists -- relaxing it to tolerate the count case would remove it.
TEST(AdbcReaderTest, RejectsAColumnTheScanDidNotRequest) {
    ObjectPool pool;
    DescriptorTbl* desc_tbl = nullptr;
    const auto slots = string_slot(&pool, &desc_tbl);
    RuntimeState state;
    RuntimeProfile profile("adbc_reader_unknown_column_test");
    auto close_count = std::make_shared<int>(0);

    auto reader =
            create_reader(&profile, fake_adbc_range(), slots,
                          [close_count](const TFileRangeDesc&, std::unique_ptr<AdbcStream>* out) {
                              *out = std::make_unique<BatchAdbcStream>(
                                      std::vector<std::shared_ptr<arrow::RecordBatch>> {
                                              make_named_large_string_batch("not_requested")},
                                      close_count);
                              return Status::OK();
                          });
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    auto request = std::make_shared<FileScanRequest>();
    FileScanRequestBuilder builder(request.get());
    ASSERT_TRUE(builder.add_non_predicate_column(LocalColumnId(0)).ok());
    ASSERT_TRUE(reader->open(request).ok());

    auto block = make_request_block(schema, {0});
    size_t rows = 0;
    bool eof = false;
    const auto status = reader->get_block(&block, &rows, &eof);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("not_requested"), std::string::npos) << status.to_string();
}

// A range missing a required parameter must be rejected up front, not at connect time where the
// error would come back as an opaque driver message.
TEST(AdbcReaderTest, RejectsIncompleteAdbcParams) {
    ObjectPool pool;
    DescriptorTbl* desc_tbl = nullptr;
    const auto slots = string_slot(&pool, &desc_tbl);
    RuntimeState state;
    RuntimeProfile profile("adbc_reader_bad_range_test");

    for (const auto* missing : {"driver_path", "uri", "query_sql"}) {
        auto range = fake_adbc_range();
        range.table_format_params.adbc_params.erase(missing);
        auto reader = create_reader(&profile, range, slots,
                                    [](const TFileRangeDesc&, std::unique_ptr<AdbcStream>* out) {
                                        *out = nullptr;
                                        return Status::OK();
                                    });
        EXPECT_FALSE(reader->init(&state).ok()) << "missing " << missing << " was accepted";
    }

    auto range = fake_adbc_range();
    range.table_format_params.__isset.adbc_params = false;
    auto reader = create_reader(&profile, range, slots, {});
    EXPECT_FALSE(reader->init(&state).ok());
}

// A range says either "run this statement" or "read this partition of a statement already run".
// Accepting one that says both would let this reader execute a query the source has already
// executed, depending only on which branch the code happened to take first.
TEST(AdbcReaderTest, RejectsARangeThatSaysBothOrNeitherKindOfWork) {
    ObjectPool pool;
    DescriptorTbl* desc_tbl = nullptr;
    const auto slots = string_slot(&pool, &desc_tbl);
    RuntimeState state;
    RuntimeProfile profile("adbc_reader_exclusive_work_test");

    auto both = fake_adbc_range();
    both.table_format_params.adbc_params["partition_descriptor"] = "Zm9vYmFy";
    auto both_reader = create_reader(&profile, both, slots, {});
    const auto both_status = both_reader->init(&state);
    ASSERT_FALSE(both_status.ok());
    EXPECT_NE(both_status.to_string().find("both"), std::string::npos) << both_status.to_string();

    auto neither = fake_adbc_range();
    neither.table_format_params.adbc_params.erase("query_sql");
    auto neither_reader = create_reader(&profile, neither, slots, {});
    const auto neither_status = neither_reader->init(&state);
    ASSERT_FALSE(neither_status.ok());
    EXPECT_NE(neither_status.to_string().find("neither"), std::string::npos)
            << neither_status.to_string();
}

// Group B: the real ADBC C API call sequence, against the SQLite driver thirdparty builds.

class AdbcSqliteReaderTest : public ::testing::Test {
protected:
    void SetUp() override {
        SKIP_WITHOUT_SQLITE_DRIVER();
        _db_path = std::filesystem::temp_directory_path() /
                   ("doris_adbc_reader_test_" + std::to_string(::getpid()) + ".db");
        std::filesystem::remove(_db_path);
        const std::string uri = "file:" + _db_path.string();
        ASSERT_TRUE(run_sqlite_ddl(
                uri, "CREATE TABLE t (id INTEGER, v_int INTEGER, v_dbl REAL, v_txt TEXT)"));
        ASSERT_TRUE(run_sqlite_ddl(uri,
                                   "INSERT INTO t VALUES (1, 10, 1.5, 'alpha'), "
                                   "(2, NULL, NULL, NULL), (3, 30, 3.5, 'gamma')"));
    }

    void TearDown() override {
        if (!_db_path.empty()) {
            std::filesystem::remove(_db_path);
        }
    }

    std::string uri() const { return "file:" + _db_path.string(); }

    std::filesystem::path _db_path;
};

// Group A cannot prove the ADBC call sequence is right, nor that the driver manager is usable at
// run time. SQLite makes that testable without a server or a container.
TEST_F(AdbcSqliteReaderTest, ReadsFromRealSqliteDriverEndToEnd) {
    ObjectPool pool;
    DescriptorTbl* desc_tbl = nullptr;
    const auto slots = sqlite_slots(&pool, &desc_tbl);
    RuntimeState state;
    RuntimeProfile profile("adbc_reader_sqlite_e2e_test");

    auto range = adbc_range({
            {"driver_path", adbc_sqlite_driver_path()},
            {"uri", uri()},
            {"query_sql", "SELECT id, v_int, v_dbl, v_txt FROM t ORDER BY id"},
    });

    // No injected factory: this goes through the real ADBC stream.
    auto reader = create_reader(&profile, range, slots, {});
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 4);

    auto request = std::make_shared<FileScanRequest>();
    FileScanRequestBuilder builder(request.get());
    for (int32_t id = 0; id < 4; ++id) {
        ASSERT_TRUE(builder.add_non_predicate_column(LocalColumnId(id)).ok());
    }
    const auto open_status = reader->open(request);
    ASSERT_TRUE(open_status.ok()) << open_status.to_string();

    auto block = make_request_block(schema, {0, 1, 2, 3});
    size_t rows = 0;
    bool eof = false;
    const auto status = reader->get_block(&block, &rows, &eof);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(rows, 3);

    const auto& id_col = *block.get_by_position(0).column;
    const auto& int_col = *block.get_by_position(1).column;
    const auto& dbl_col = *block.get_by_position(2).column;
    const auto& txt_col = *block.get_by_position(3).column;

    EXPECT_EQ(nullable_int64_at(id_col, 0), 1);
    EXPECT_EQ(nullable_int64_at(id_col, 2), 3);

    EXPECT_EQ(nullable_int64_at(int_col, 0), 10);
    EXPECT_TRUE(is_null_at(int_col, 1));
    EXPECT_EQ(nullable_int64_at(int_col, 2), 30);

    EXPECT_DOUBLE_EQ(nullable_double_at(dbl_col, 0), 1.5);
    EXPECT_TRUE(is_null_at(dbl_col, 1));
    EXPECT_DOUBLE_EQ(nullable_double_at(dbl_col, 2), 3.5);

    EXPECT_EQ(nullable_string_at(txt_col, 0), "alpha");
    EXPECT_TRUE(is_null_at(txt_col, 1));
    EXPECT_EQ(nullable_string_at(txt_col, 2), "gamma");

    ASSERT_TRUE(reader->close().ok());
}

// A driver path that is not there is the most likely user error, so it must not look like an
// internal failure.
TEST_F(AdbcSqliteReaderTest, MissingDriverFailsWithThePathInTheMessage) {
    ObjectPool pool;
    DescriptorTbl* desc_tbl = nullptr;
    const auto slots = sqlite_slots(&pool, &desc_tbl);
    RuntimeState state;
    RuntimeProfile profile("adbc_reader_missing_driver_test");

    auto range = adbc_range({
            {"driver_path", "/nonexistent/libadbc_driver_nope.so"},
            {"uri", uri()},
            {"query_sql", "SELECT 1"},
    });
    auto reader = create_reader(&profile, range, slots, {});
    ASSERT_TRUE(reader->init(&state).ok());

    auto request = std::make_shared<FileScanRequest>();
    FileScanRequestBuilder builder(request.get());
    ASSERT_TRUE(builder.add_non_predicate_column(LocalColumnId(0)).ok());
    const auto status = reader->open(request);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("/nonexistent/libadbc_driver_nope.so"), std::string::npos);
}

// A bad query has to surface the driver's own message, otherwise SQL problems are undiagnosable.
TEST_F(AdbcSqliteReaderTest, InvalidQuerySurfacesTheDriverMessage) {
    ObjectPool pool;
    DescriptorTbl* desc_tbl = nullptr;
    const auto slots = sqlite_slots(&pool, &desc_tbl);
    RuntimeState state;
    RuntimeProfile profile("adbc_reader_bad_query_test");

    auto range = adbc_range({
            {"driver_path", adbc_sqlite_driver_path()},
            {"uri", uri()},
            {"query_sql", "SELECT * FROM no_such_table"},
    });
    auto reader = create_reader(&profile, range, slots, {});
    ASSERT_TRUE(reader->init(&state).ok());

    auto request = std::make_shared<FileScanRequest>();
    FileScanRequestBuilder builder(request.get());
    ASSERT_TRUE(builder.add_non_predicate_column(LocalColumnId(0)).ok());
    const auto status = reader->open(request);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("no_such_table"), std::string::npos)
            << "driver message was lost: " << status.to_string();
}

// A partition descriptor is FE's base64 of driver-private bytes. Garbage there has to be named as
// such: handed to the driver undecoded it would come back as an opaque parse failure from inside a
// protobuf, with nothing pointing at the parameter that was wrong.
TEST_F(AdbcSqliteReaderTest, RejectsAPartitionDescriptorThatIsNotBase64) {
    ObjectPool pool;
    DescriptorTbl* desc_tbl = nullptr;
    const auto slots = sqlite_slots(&pool, &desc_tbl);
    RuntimeState state;
    RuntimeProfile profile("adbc_reader_bad_partition_test");

    auto range = adbc_range({
            {"driver_path", adbc_sqlite_driver_path()},
            {"uri", uri()},
            {"partition_descriptor", "not base64 at all!!"},
    });
    auto reader = create_reader(&profile, range, slots, {});
    ASSERT_TRUE(reader->init(&state).ok());

    auto request = std::make_shared<FileScanRequest>();
    FileScanRequestBuilder builder(request.get());
    ASSERT_TRUE(builder.add_non_predicate_column(LocalColumnId(0)).ok());
    const auto status = reader->open(request);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("partition_descriptor"), std::string::npos)
            << status.to_string();
}

// The reader must actually take the partition branch, not fall through to running a statement it
// was not given. SQLite has no partitioned execution, so the proof that the call was made is the
// driver's own refusal of it -- naming ConnectionReadPartition, the entry point only this branch
// reaches. Reading a partition successfully needs a source that produces one, which is the Flight
// SQL regression suite, not a unit test.
TEST_F(AdbcSqliteReaderTest, ReadsAPartitionThroughTheDriverInsteadOfRunningAStatement) {
    ObjectPool pool;
    DescriptorTbl* desc_tbl = nullptr;
    const auto slots = sqlite_slots(&pool, &desc_tbl);
    RuntimeState state;
    RuntimeProfile profile("adbc_reader_partition_branch_test");

    auto range = adbc_range({
            {"driver_path", adbc_sqlite_driver_path()},
            {"uri", uri()},
            // Valid base64; the bytes are meaningless to the driver, which never gets to look at
            // them because it has no partition support at all.
            {"partition_descriptor", "Zm9vYmFy"},
    });
    auto reader = create_reader(&profile, range, slots, {});
    ASSERT_TRUE(reader->init(&state).ok());

    auto request = std::make_shared<FileScanRequest>();
    FileScanRequestBuilder builder(request.get());
    ASSERT_TRUE(builder.add_non_predicate_column(LocalColumnId(0)).ok());
    const auto status = reader->open(request);
    ASSERT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("ConnectionReadPartition"), std::string::npos)
            << "the partition branch was not taken: " << status.to_string();
}

// ---- the release contract Arrow aborts the process over ----

namespace {

// A stream shaped like the one the Flight SQL driver hands out: its release callback runs, but
// leaves `release` set. The Arrow C data interface forbids that, and Arrow C++ does not merely
// complain -- ArrowArrayStreamRelease calls abort(), taking the whole BE with it.
struct MisbehavingDriverStream {
    int release_calls = 0;
    int get_next_calls = 0;
};

MisbehavingDriverStream& state_of(ArrowArrayStream* self) {
    return *static_cast<MisbehavingDriverStream*>(self->private_data);
}

int misbehaving_get_schema(ArrowArrayStream* /*self*/, ArrowSchema* /*out*/) {
    return 0;
}

int misbehaving_get_next(ArrowArrayStream* self, ArrowArray* /*out*/) {
    state_of(self).get_next_calls++;
    return 0;
}

const char* misbehaving_get_last_error(ArrowArrayStream* /*self*/) {
    return "driver said so";
}

void misbehaving_release(ArrowArrayStream* self) {
    state_of(self).release_calls++;
    // Deliberately does NOT clear self->release. This is the bug being defended against.
}

ArrowArrayStream misbehaving_stream(MisbehavingDriverStream* state) {
    ArrowArrayStream stream {};
    stream.get_schema = misbehaving_get_schema;
    stream.get_next = misbehaving_get_next;
    stream.get_last_error = misbehaving_get_last_error;
    stream.release = misbehaving_release;
    stream.private_data = state;
    return stream;
}

} // namespace

TEST(AdbcStreamReleaseContractTest, ClearsReleaseEvenWhenTheDriverDoesNot) {
    MisbehavingDriverStream state;
    ArrowArrayStream stream = misbehaving_stream(&state);

    enforce_stream_release_contract(&stream);
    ASSERT_NE(stream.release, nullptr);
    stream.release(&stream);

    // The invariant Arrow asserts on, and the one a scan against Flight SQL used to break.
    EXPECT_EQ(stream.release, nullptr);
    // The driver still gets released, exactly once: the wrapper must not leak the real stream.
    EXPECT_EQ(state.release_calls, 1);
}

TEST(AdbcStreamReleaseContractTest, StillDelegatesEveryCallbackToTheDriver) {
    // A wrapper that swallowed calls would turn a crash into silently empty results, which is
    // worse: the scan would report success on rows it never read.
    MisbehavingDriverStream state;
    ArrowArrayStream stream = misbehaving_stream(&state);
    enforce_stream_release_contract(&stream);

    ArrowArray array {};
    EXPECT_EQ(stream.get_next(&stream, &array), 0);
    EXPECT_EQ(state.get_next_calls, 1);
    EXPECT_STREQ(stream.get_last_error(&stream), "driver said so");

    stream.release(&stream);
}

TEST(AdbcStreamReleaseContractTest, LeavesAnAlreadyReleasedStreamAlone) {
    // Wrapping one would hand Arrow callbacks that dereference a delegate with nothing behind it.
    ArrowArrayStream stream {};
    enforce_stream_release_contract(&stream);
    EXPECT_EQ(stream.release, nullptr);
    EXPECT_EQ(stream.private_data, nullptr);
}

} // namespace doris::format::adbc
