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

// Standalone Lance FFI test — links only against libdoris_ffi.a and arrow.
// Does NOT depend on the full Doris BE build.
// Build: see Makefile target below or CMake standalone_lance_test target.

#include <arrow/array.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <string>

// FFI declarations (matching lance_ffi.h without Doris deps)
extern "C" {
int32_t rust_echo(int32_t x);
int32_t lance_test_create_dataset(const uint8_t* path_ptr, size_t path_len);
int32_t lance_reader_open(const uint8_t* uri_ptr, size_t uri_len,
                          const uint8_t* const* column_names_ptr,
                          const size_t* column_names_len_ptr, size_t num_columns, size_t batch_size,
                          void** handle_out);
int32_t lance_reader_next_batch(void* handle, ArrowSchema* schema_out, ArrowArray* array_out,
                                bool* eof_out, int64_t* bytes_out);
int32_t lance_reader_get_schema(void* handle, ArrowSchema* schema_out);
void lance_reader_close(void* handle);
size_t lance_reader_last_error(uint8_t* buf, size_t buf_len);
int32_t lance_reader_open_json(const uint8_t* config_json_ptr, size_t config_json_len,
                               void** handle_out);
}

static std::string get_last_error() {
    uint8_t buf[1024];
    size_t len = lance_reader_last_error(buf, sizeof(buf));
    return len > 0 ? std::string(reinterpret_cast<char*>(buf), len) : "(no error)";
}

#define ASSERT_EQ(a, b, msg)                                                           \
    do {                                                                               \
        if ((a) != (b)) {                                                              \
            std::cerr << "FAIL: " << msg << ": " << (a) << " != " << (b) << std::endl; \
            return 1;                                                                  \
        }                                                                              \
    } while (0)
#define ASSERT_TRUE(a, msg)                            \
    do {                                               \
        if (!(a)) {                                    \
            std::cerr << "FAIL: " << msg << std::endl; \
            return 1;                                  \
        }                                              \
    } while (0)
#define ASSERT_OK(rc, msg)                                                         \
    do {                                                                           \
        if ((rc) < 0) {                                                            \
            std::cerr << "FAIL: " << msg << ": " << get_last_error() << std::endl; \
            return 1;                                                              \
        }                                                                          \
    } while (0)

int test_echo() {
    std::cout << "  test_echo... ";
    ASSERT_EQ(rust_echo(42), 42, "echo 42");
    ASSERT_EQ(rust_echo(0), 0, "echo 0");
    ASSERT_EQ(rust_echo(-1), -1, "echo -1");
    std::cout << "OK" << std::endl;
    return 0;
}

int test_open_close(const std::string& uri) {
    std::cout << "  test_open_close... ";
    void* handle = nullptr;
    int32_t rc = lance_reader_open(reinterpret_cast<const uint8_t*>(uri.data()), uri.size(),
                                   nullptr, nullptr, 0, 1024, &handle);
    ASSERT_OK(rc, "open");
    ASSERT_TRUE(handle != nullptr, "handle not null");
    lance_reader_close(handle);
    std::cout << "OK" << std::endl;
    return 0;
}

int test_get_schema(const std::string& uri) {
    std::cout << "  test_get_schema... ";
    void* handle = nullptr;
    int32_t rc = lance_reader_open(reinterpret_cast<const uint8_t*>(uri.data()), uri.size(),
                                   nullptr, nullptr, 0, 1024, &handle);
    ASSERT_OK(rc, "open");

    ArrowSchema c_schema {};
    rc = lance_reader_get_schema(handle, &c_schema);
    ASSERT_OK(rc, "get_schema");
    ASSERT_EQ(c_schema.n_children, 3L, "3 columns");
    ASSERT_TRUE(strcmp(c_schema.children[0]->name, "id") == 0, "col0=id");
    ASSERT_TRUE(strcmp(c_schema.children[1]->name, "name") == 0, "col1=name");
    ASSERT_TRUE(strcmp(c_schema.children[2]->name, "score") == 0, "col2=score");

    if (c_schema.release) c_schema.release(&c_schema);
    lance_reader_close(handle);
    std::cout << "OK" << std::endl;
    return 0;
}

int test_read_batches(const std::string& uri) {
    std::cout << "  test_read_batches... ";
    void* handle = nullptr;
    int32_t rc = lance_reader_open(reinterpret_cast<const uint8_t*>(uri.data()), uri.size(),
                                   nullptr, nullptr, 0, 1024, &handle);
    ASSERT_OK(rc, "open");

    int64_t total_rows = 0;
    int batch_count = 0;
    while (true) {
        ArrowSchema c_schema {};
        ArrowArray c_array {};
        bool eof = false;
        int64_t bytes = 0;
        rc = lance_reader_next_batch(handle, &c_schema, &c_array, &eof, &bytes);
        if (rc == 1 || eof) break; // FFI_EOF = 1
        ASSERT_OK(rc, "next_batch");
        ASSERT_TRUE(bytes > 0, "bytes > 0");

        // Import via Arrow C Data Interface and verify
        auto import_result = arrow::ImportRecordBatch(&c_array, &c_schema);
        ASSERT_TRUE(import_result.ok(), "ImportRecordBatch");
        auto batch = import_result.ValueUnsafe();
        total_rows += batch->num_rows();
        batch_count++;
    }
    ASSERT_EQ(total_rows, 5L, "5 total rows");
    ASSERT_TRUE(batch_count >= 1, "at least 1 batch");

    lance_reader_close(handle);
    std::cout << "OK (" << batch_count << " batch, " << total_rows << " rows)" << std::endl;
    return 0;
}

int test_column_projection(const std::string& uri) {
    std::cout << "  test_column_projection... ";
    const uint8_t* col = reinterpret_cast<const uint8_t*>("name");
    const uint8_t* cols[] = {col};
    size_t lens[] = {4};

    void* handle = nullptr;
    int32_t rc = lance_reader_open(reinterpret_cast<const uint8_t*>(uri.data()), uri.size(), cols,
                                   lens, 1, 1024, &handle);
    ASSERT_OK(rc, "open with projection");

    ArrowSchema c_schema {};
    rc = lance_reader_get_schema(handle, &c_schema);
    ASSERT_OK(rc, "get_schema");
    ASSERT_EQ(c_schema.n_children, 1L, "1 projected column");
    ASSERT_TRUE(strcmp(c_schema.children[0]->name, "name") == 0, "col=name");
    if (c_schema.release) c_schema.release(&c_schema);

    lance_reader_close(handle);
    std::cout << "OK" << std::endl;
    return 0;
}

int test_error_path() {
    std::cout << "  test_error_path... ";
    std::string bad = "/nonexistent/path.lance";
    void* handle = nullptr;
    int32_t rc = lance_reader_open(reinterpret_cast<const uint8_t*>(bad.data()), bad.size(),
                                   nullptr, nullptr, 0, 1024, &handle);
    ASSERT_TRUE(rc < 0, "error code negative");
    ASSERT_TRUE(handle == nullptr, "handle null on error");
    std::string err = get_last_error();
    ASSERT_TRUE(!err.empty(), "error message non-empty");

    // null handle close should not crash
    lance_reader_close(nullptr);
    std::cout << "OK" << std::endl;
    return 0;
}

int test_json_config(const std::string& uri) {
    std::cout << "  test_json_config... ";
    // Build JSON config with storage_options (empty for local) and version=0
    std::string config =
            R"({"uri":")" + uri +
            R"(","columns":["id","score"],"batch_size":4096,"version":0,"storage_options":{}})";

    void* handle = nullptr;
    int32_t rc = lance_reader_open_json(reinterpret_cast<const uint8_t*>(config.data()),
                                        config.size(), &handle);
    ASSERT_OK(rc, "open_json");
    ASSERT_TRUE(handle != nullptr, "handle not null");

    // Verify 2 projected columns
    ArrowSchema c_schema {};
    rc = lance_reader_get_schema(handle, &c_schema);
    ASSERT_OK(rc, "get_schema");
    ASSERT_EQ(c_schema.n_children, 2L, "2 projected columns");
    ASSERT_TRUE(strcmp(c_schema.children[0]->name, "id") == 0, "col0=id");
    ASSERT_TRUE(strcmp(c_schema.children[1]->name, "score") == 0, "col1=score");
    if (c_schema.release) c_schema.release(&c_schema);

    // Read data
    ArrowSchema batch_schema {};
    ArrowArray batch_array {};
    bool eof = false;
    int64_t bytes = 0;
    rc = lance_reader_next_batch(handle, &batch_schema, &batch_array, &eof, &bytes);
    ASSERT_OK(rc, "next_batch");
    ASSERT_EQ(batch_array.length, 5L, "5 rows");
    ASSERT_EQ(batch_array.n_children, 2L, "2 columns in batch");
    if (batch_array.release) batch_array.release(&batch_array);
    if (batch_schema.release) batch_schema.release(&batch_schema);

    lance_reader_close(handle);
    std::cout << "OK" << std::endl;
    return 0;
}

// Simulates the full TVF query path:
//   1. fetch_table_schema: open with no columns → get schema → return col names/types
//   2. FileScanner: open with projected columns → read batches → verify data values
int test_tvf_simulation(const std::string& uri) {
    std::cout << "  test_tvf_simulation... " << std::flush;

    // ========== Phase 1: Schema Inference (fetch_table_schema RPC) ==========
    // FE sends PFetchTableSchemaRequest to BE. BE opens dataset, reads schema, returns it.
    {
        std::string config = R"({"uri":")" + uri +
                             R"(","columns":[],"batch_size":1,"version":0,"storage_options":{}})";
        void* handle = nullptr;
        int32_t rc = lance_reader_open_json(reinterpret_cast<const uint8_t*>(config.data()),
                                            config.size(), &handle);
        ASSERT_OK(rc, "schema: open");

        ArrowSchema c_schema {};
        rc = lance_reader_get_schema(handle, &c_schema);
        ASSERT_OK(rc, "schema: get_schema");

        // Verify schema: 3 columns (id:int32, name:utf8, score:float64)
        ASSERT_EQ(c_schema.n_children, 3L, "schema: 3 columns");

        // id column - Arrow int32 format "i"
        ASSERT_TRUE(strcmp(c_schema.children[0]->name, "id") == 0, "schema: col0=id");
        ASSERT_TRUE(strcmp(c_schema.children[0]->format, "i") == 0, "schema: id is int32");

        // name column - Arrow utf8 format "u"
        ASSERT_TRUE(strcmp(c_schema.children[1]->name, "name") == 0, "schema: col1=name");
        ASSERT_TRUE(strcmp(c_schema.children[1]->format, "u") == 0, "schema: name is utf8");

        // score column - Arrow float64 format "g"
        ASSERT_TRUE(strcmp(c_schema.children[2]->name, "score") == 0, "schema: col2=score");
        ASSERT_TRUE(strcmp(c_schema.children[2]->format, "g") == 0, "schema: score is float64");

        if (c_schema.release) c_schema.release(&c_schema);
        lance_reader_close(handle);
    }

    // ========== Phase 2: Data Scan (FileScanner::get_next_block) ==========
    // FE plans query with schema from Phase 1, sends scan range to BE.
    // BE opens dataset with projected columns, reads batches, converts to Block.
    {
        std::string config =
                R"({"uri":")" + uri +
                R"(","columns":["id","name","score"],"batch_size":4096,"version":0,"storage_options":{}})";
        void* handle = nullptr;
        int32_t rc = lance_reader_open_json(reinterpret_cast<const uint8_t*>(config.data()),
                                            config.size(), &handle);
        ASSERT_OK(rc, "scan: open");

        // Read first (and only) batch
        ArrowSchema c_schema {};
        ArrowArray c_array {};
        bool eof = false;
        int64_t bytes = 0;
        rc = lance_reader_next_batch(handle, &c_schema, &c_array, &eof, &bytes);
        ASSERT_OK(rc, "scan: next_batch");
        ASSERT_TRUE(!eof, "scan: not eof on first batch");
        ASSERT_TRUE(bytes > 0, "scan: bytes > 0");

        // Import via Arrow C Data Interface (same as LanceRustReader::get_next_block)
        auto import_result = arrow::ImportRecordBatch(&c_array, &c_schema);
        ASSERT_TRUE(import_result.ok(), "scan: ImportRecordBatch");
        auto batch = import_result.ValueUnsafe();

        ASSERT_EQ(batch->num_rows(), 5L, "scan: 5 rows");
        ASSERT_EQ(batch->num_columns(), 3L, "scan: 3 columns");

        // Verify actual data values (this is what Doris Block would contain)
        // Column 0: id (int32) = [1, 2, 3, 4, 5]
        auto id_array = std::dynamic_pointer_cast<arrow::Int32Array>(batch->column(0));
        ASSERT_TRUE(id_array != nullptr, "scan: id is Int32Array");
        ASSERT_EQ(id_array->Value(0), 1, "scan: id[0]=1");
        ASSERT_EQ(id_array->Value(1), 2, "scan: id[1]=2");
        ASSERT_EQ(id_array->Value(4), 5, "scan: id[4]=5");

        // Column 1: name (utf8) = ["alice", "bob", "carol", "dave", "eve"]
        auto name_array = std::dynamic_pointer_cast<arrow::StringArray>(batch->column(1));
        ASSERT_TRUE(name_array != nullptr, "scan: name is StringArray");
        ASSERT_TRUE(name_array->GetString(0) == "alice", "scan: name[0]=alice");
        ASSERT_TRUE(name_array->GetString(1) == "bob", "scan: name[1]=bob");
        ASSERT_TRUE(name_array->GetString(4) == "eve", "scan: name[4]=eve");

        // Column 2: score (float64) = [90.5, 85.0, 92.3, 78.1, 88.7]
        auto score_array = std::dynamic_pointer_cast<arrow::DoubleArray>(batch->column(2));
        ASSERT_TRUE(score_array != nullptr, "scan: score is DoubleArray");
        ASSERT_TRUE(std::abs(score_array->Value(0) - 90.5) < 0.01, "scan: score[0]=90.5");
        ASSERT_TRUE(std::abs(score_array->Value(1) - 85.0) < 0.01, "scan: score[1]=85.0");
        ASSERT_TRUE(std::abs(score_array->Value(4) - 88.7) < 0.01, "scan: score[4]=88.7");

        // Verify EOF on next call
        ArrowSchema eof_schema {};
        ArrowArray eof_array {};
        bool is_eof = false;
        int64_t eof_bytes = 0;
        rc = lance_reader_next_batch(handle, &eof_schema, &eof_array, &is_eof, &eof_bytes);
        ASSERT_TRUE(rc == 1 || is_eof, "scan: EOF after last batch");

        lance_reader_close(handle);
    }

    std::cout << "OK (schema inference + full data scan verified)" << std::endl;
    return 0;
}

int main() {
    // Create test dataset
    auto tmpdir = std::filesystem::temp_directory_path() / "lance_e2e_test";
    std::filesystem::create_directories(tmpdir);
    std::string dataset_path = (tmpdir / "test.lance").string();

    std::cout << "Creating test dataset at " << dataset_path << std::endl;
    int32_t rc = lance_test_create_dataset(reinterpret_cast<const uint8_t*>(dataset_path.data()),
                                           dataset_path.size());
    if (rc != 0) {
        std::cerr << "Failed to create dataset: " << get_last_error() << std::endl;
        return 1;
    }

    std::cout << "Running Lance FFI E2E tests:" << std::endl;
    int failures = 0;
    failures += test_echo();
    failures += test_open_close(dataset_path);
    failures += test_get_schema(dataset_path);
    failures += test_read_batches(dataset_path);
    failures += test_column_projection(dataset_path);
    failures += test_error_path();
    failures += test_json_config(dataset_path);
    failures += test_tvf_simulation(dataset_path);

    // Cleanup
    std::filesystem::remove_all(tmpdir);

    if (failures == 0) {
        std::cout << "\nAll 8 tests PASSED!" << std::endl;
    } else {
        std::cerr << "\n" << failures << " test(s) FAILED!" << std::endl;
    }
    return failures;
}
