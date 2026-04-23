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

#ifdef BUILD_RUST_READERS

#include <gtest/gtest.h>

#include <cstdio>
#include <filesystem>
#include <string>

#include "format/lance/lance_ffi.h"

namespace doris {

class LanceFfiTest : public testing::Test {
protected:
    void SetUp() override {
        // Create a unique temp directory for each test
        _test_dir = std::filesystem::temp_directory_path() / "doris_lance_test_XXXXXX";
        _test_dir = std::filesystem::path(mkdtemp(const_cast<char*>(_test_dir.string().c_str())));

        // Create a test Lance dataset via Rust FFI
        std::string dataset_path = (_test_dir / "test.lance").string();
        int32_t rc = lance_test_create_dataset(
                reinterpret_cast<const uint8_t*>(dataset_path.data()), dataset_path.size());
        ASSERT_EQ(rc, lance_ffi::LANCE_FFI_OK)
                << "Failed to create test dataset: " << _get_last_error();

        _dataset_path = dataset_path;
    }

    void TearDown() override {
        std::error_code ec;
        std::filesystem::remove_all(_test_dir, ec);
    }

    std::string _get_last_error() {
        uint8_t buf[1024];
        size_t len = lance_reader_last_error(buf, sizeof(buf));
        if (len > 0) {
            return std::string(reinterpret_cast<const char*>(buf), len);
        }
        return "(no error)";
    }

    std::filesystem::path _test_dir;
    std::string _dataset_path;
};

// ==================== Raw FFI tests ====================

TEST_F(LanceFfiTest, EchoRoundTrip) {
    EXPECT_EQ(rust_echo(42), 42);
    EXPECT_EQ(rust_echo(0), 0);
    EXPECT_EQ(rust_echo(-1), -1);
}

TEST_F(LanceFfiTest, OpenAndClose) {
    LanceReaderHandle handle = nullptr;
    int32_t rc = lance_reader_open(reinterpret_cast<const uint8_t*>(_dataset_path.data()),
                                   _dataset_path.size(), nullptr, nullptr, 0, 1024, &handle);
    ASSERT_EQ(rc, lance_ffi::LANCE_FFI_OK) << _get_last_error();
    ASSERT_NE(handle, nullptr);

    lance_reader_close(handle);
}

TEST_F(LanceFfiTest, OpenNonexistentPath) {
    std::string bad_path = "/nonexistent/path/dataset.lance";
    LanceReaderHandle handle = nullptr;
    int32_t rc = lance_reader_open(reinterpret_cast<const uint8_t*>(bad_path.data()),
                                   bad_path.size(), nullptr, nullptr, 0, 1024, &handle);
    EXPECT_LT(rc, 0);
    EXPECT_EQ(handle, nullptr);

    // Error message should be available
    std::string err = _get_last_error();
    EXPECT_FALSE(err.empty());
}

TEST_F(LanceFfiTest, CloseNullHandle) {
    // Should not crash
    lance_reader_close(nullptr);
}

TEST_F(LanceFfiTest, GetSchema) {
    LanceReaderHandle handle = nullptr;
    int32_t rc = lance_reader_open(reinterpret_cast<const uint8_t*>(_dataset_path.data()),
                                   _dataset_path.size(), nullptr, nullptr, 0, 1024, &handle);
    ASSERT_EQ(rc, lance_ffi::LANCE_FFI_OK);

    ArrowSchema c_schema {};
    rc = lance_reader_get_schema(handle, &c_schema);
    ASSERT_EQ(rc, lance_ffi::LANCE_FFI_OK) << _get_last_error();

    // The test dataset has 3 columns: id, name, score
    // Arrow struct schema: n_children = number of columns
    EXPECT_EQ(c_schema.n_children, 3);
    EXPECT_STREQ(c_schema.children[0]->name, "id");
    EXPECT_STREQ(c_schema.children[1]->name, "name");
    EXPECT_STREQ(c_schema.children[2]->name, "score");

    // Release schema
    if (c_schema.release) c_schema.release(&c_schema);
    lance_reader_close(handle);
}

TEST_F(LanceFfiTest, ReadAllBatches) {
    LanceReaderHandle handle = nullptr;
    int32_t rc = lance_reader_open(reinterpret_cast<const uint8_t*>(_dataset_path.data()),
                                   _dataset_path.size(), nullptr, nullptr, 0, 1024, &handle);
    ASSERT_EQ(rc, lance_ffi::LANCE_FFI_OK);

    int64_t total_rows = 0;
    int batch_count = 0;

    while (true) {
        ArrowSchema c_schema {};
        ArrowArray c_array {};
        bool eof = false;
        int64_t bytes = 0;

        rc = lance_reader_next_batch(handle, &c_schema, &c_array, &eof, &bytes);

        if (rc == lance_ffi::LANCE_FFI_EOF || eof) {
            break;
        }
        ASSERT_EQ(rc, lance_ffi::LANCE_FFI_OK) << _get_last_error();
        ASSERT_GT(bytes, 0);

        total_rows += c_array.length;
        batch_count++;

        // Release Arrow C ABI ownership
        if (c_array.release) c_array.release(&c_array);
        if (c_schema.release) c_schema.release(&c_schema);
    }

    EXPECT_EQ(total_rows, 5);
    EXPECT_GE(batch_count, 1);

    lance_reader_close(handle);
}

TEST_F(LanceFfiTest, ReadWithColumnProjection) {
    // Project only "name" column
    const uint8_t* col_name = reinterpret_cast<const uint8_t*>("name");
    const uint8_t* col_ptrs[] = {col_name};
    size_t col_lens[] = {4};

    LanceReaderHandle handle = nullptr;
    int32_t rc = lance_reader_open(reinterpret_cast<const uint8_t*>(_dataset_path.data()),
                                   _dataset_path.size(), col_ptrs, col_lens, 1, 1024, &handle);
    ASSERT_EQ(rc, lance_ffi::LANCE_FFI_OK) << _get_last_error();

    // Verify schema has only 1 column
    ArrowSchema c_schema {};
    rc = lance_reader_get_schema(handle, &c_schema);
    ASSERT_EQ(rc, lance_ffi::LANCE_FFI_OK);
    EXPECT_EQ(c_schema.n_children, 1);
    EXPECT_STREQ(c_schema.children[0]->name, "name");
    if (c_schema.release) c_schema.release(&c_schema);

    // Read batch — should have 1 column, 5 rows
    ArrowSchema batch_schema {};
    ArrowArray batch_array {};
    bool eof = false;
    int64_t bytes = 0;

    rc = lance_reader_next_batch(handle, &batch_schema, &batch_array, &eof, &bytes);
    ASSERT_EQ(rc, lance_ffi::LANCE_FFI_OK);
    EXPECT_EQ(batch_array.length, 5);
    EXPECT_EQ(batch_array.n_children, 1);

    if (batch_array.release) batch_array.release(&batch_array);
    if (batch_schema.release) batch_schema.release(&batch_schema);

    lance_reader_close(handle);
}

TEST_F(LanceFfiTest, ErrorMessageRetrieval) {
    // Trigger an error
    std::string bad_path = "/does/not/exist.lance";
    LanceReaderHandle handle = nullptr;
    lance_reader_open(reinterpret_cast<const uint8_t*>(bad_path.data()), bad_path.size(), nullptr,
                      nullptr, 0, 1024, &handle);

    // Retrieve error message
    uint8_t buf[1024];
    size_t len = lance_reader_last_error(buf, sizeof(buf));
    EXPECT_GT(len, 0u);

    std::string msg(reinterpret_cast<const char*>(buf), len);
    // Should contain something about the path not existing
    EXPECT_FALSE(msg.empty());

    // Null buffer should return 0
    EXPECT_EQ(lance_reader_last_error(nullptr, 0), 0u);
}

} // namespace doris

#endif // BUILD_RUST_READERS
