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

#include "io/fs/packed_file_system.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "cloud/config.h"
#include "common/config.h"
#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/path.h"
#include "util/slice.h"

namespace doris::io {

using doris::Status;

// Mock FileReader for testing PackedFileSystem
class MockFileReader : public FileReader {
public:
    explicit MockFileReader(std::string content) : _content(std::move(content)) {}

    Status close() override {
        _closed = true;
        return Status::OK();
    }

    const Path& path() const override { return _path; }

    size_t size() const override { return _content.size(); }

    bool closed() const override { return _closed; }

    int64_t mtime() const override { return 0; }

protected:
    Status read_at_impl(size_t offset, Slice result, size_t* bytes_read,
                        const IOContext* /*io_ctx*/) override {
        if (offset >= _content.size()) {
            *bytes_read = 0;
            return Status::OK();
        }
        size_t available = _content.size() - offset;
        size_t to_read = std::min(result.get_size(), available);
        std::memcpy(result.mutable_data(), _content.data() + offset, to_read);
        *bytes_read = to_read;
        return Status::OK();
    }

private:
    Path _path = Path("mock_file");
    std::string _content;
    bool _closed = false;
};

// Mock FileWriter for testing PackedFileSystem
class MockFileWriterForMerge : public FileWriter {
public:
    explicit MockFileWriterForMerge(std::string path) : _path(std::move(path)) {}

    Status close(bool non_block = false) override {
        if (_state == FileWriter::State::CLOSED) {
            return Status::OK();
        }
        if (non_block) {
            _state = FileWriter::State::ASYNC_CLOSING;
            return Status::OK();
        }
        _state = FileWriter::State::CLOSED;
        return Status::OK();
    }

    Status appendv(const Slice* data, size_t data_cnt) override {
        for (size_t i = 0; i < data_cnt; ++i) {
            _written.append(reinterpret_cast<const char*>(data[i].data), data[i].size);
            _bytes_appended += data[i].size;
        }
        ++_append_calls;
        return Status::OK();
    }

    const Path& path() const override { return _path; }

    size_t bytes_appended() const override { return _bytes_appended; }

    FileWriter::State state() const override { return _state; }

private:
    Path _path;
    size_t _bytes_appended = 0;
    size_t _append_calls = 0; // Add to match packed_file_writer_test.cpp
    std::string _written;
    Status _append_status = Status::OK(); // Add to match packed_file_writer_test.cpp
    Status _close_status = Status::OK();  // Add to match packed_file_writer_test.cpp
    FileWriter::State _state = FileWriter::State::OPENED;
};

// Mock FileSystem for testing PackedFileSystem
class MockFileSystemForMerge : public FileSystem {
public:
    MockFileSystemForMerge() : FileSystem("mock_fs", FileSystemType::LOCAL) {}

    void set_create_status(Status st) { _create_status = std::move(st); }
    void set_open_status(Status st) { _open_status = std::move(st); }
    void set_exists_result(bool exists) { _exists_result = exists; }
    void set_file_size(int64_t size) { _file_size = size; }

    MockFileWriterForMerge* last_writer() const { return _last_writer; }
    MockFileReader* last_reader() const { return _last_reader; }

protected:
    Status create_file_impl(const Path& file, FileWriterPtr* writer,
                            const FileWriterOptions* /*opts*/) override {
        if (!_create_status.ok()) {
            return _create_status;
        }
        auto mock_writer = std::make_unique<MockFileWriterForMerge>(file.native());
        _last_writer = mock_writer.get();
        *writer = std::move(mock_writer);
        return Status::OK();
    }

    Status open_file_impl(const Path& file, FileReaderSPtr* reader,
                          const FileReaderOptions* /*opts*/) override {
        if (!_open_status.ok()) {
            return _open_status;
        }
        // Create a mock reader with some content
        auto mock_reader = std::make_shared<MockFileReader>("mock_content");
        _last_reader = mock_reader.get();
        *reader = std::move(mock_reader);
        return Status::OK();
    }

    Status exists_impl(const Path& /*path*/, bool* res) const override {
        *res = _exists_result;
        return Status::OK();
    }

    Status file_size_impl(const Path& /*file*/, int64_t* size) const override {
        *size = _file_size;
        return Status::OK();
    }

    Status create_directory_impl(const Path& /*dir*/, bool /*failed_if_exists*/) override {
        return Status::OK();
    }

    Status delete_file_impl(const Path& /*file*/) override { return Status::OK(); }

    Status batch_delete_impl(const std::vector<Path>& /*files*/) override { return Status::OK(); }

    Status delete_directory_impl(const Path& /*dir*/) override { return Status::OK(); }

    Status list_impl(const Path& /*dir*/, bool /*only_file*/, std::vector<FileInfo>* /*files*/,
                     bool* exists) override {
        *exists = true;
        return Status::OK();
    }

    Status rename_impl(const Path& /*orig_name*/, const Path& /*new_name*/) override {
        return Status::OK();
    }

    Status absolute_path(const Path& path, Path& abs_path) const override {
        abs_path = path;
        return Status::OK();
    }

private:
    Status _create_status = Status::OK();
    Status _open_status = Status::OK();
    bool _exists_result = true;
    int64_t _file_size = 0;
    MockFileWriterForMerge* _last_writer = nullptr;
    MockFileReader* _last_reader = nullptr;
};

// Test fixture for PackedFileSystem
class PackedFileSystemTest : public testing::Test {
protected:
    void SetUp() override {
        _old_small_threshold = config::small_file_threshold_bytes;
        config::small_file_threshold_bytes = 100;
        _inner_fs = std::make_shared<MockFileSystemForMerge>();
        _append_info.resource_id = "test_resource";
        _append_info.tablet_id = 12345;
        _append_info.rowset_id = "rowset_1";
        _append_info.txn_id = 6789;
    }

    void TearDown() override { config::small_file_threshold_bytes = _old_small_threshold; }

    int64_t _old_small_threshold = 0;
    std::shared_ptr<MockFileSystemForMerge> _inner_fs;
    PackedAppendContext _append_info;
};

TEST_F(PackedFileSystemTest, CreateFileWrapsWithPackedFileWriter) {
    PackedFileSystem merge_fs(_inner_fs, _append_info);

    Path file_path("test_file");
    FileWriterPtr writer;
    Status st = merge_fs.create_file(file_path, &writer, nullptr);
    EXPECT_TRUE(st.ok());
    EXPECT_NE(writer, nullptr);

    // Writer should be a PackedFileWriter (we can't easily check type, but it should work)
    std::string data = "test";
    Slice slice(data);
    st = writer->appendv(&slice, 1);
    EXPECT_TRUE(st.ok());
}

TEST_F(PackedFileSystemTest, OpenFileNotInMergeFile) {
    PackedFileSystem merge_fs(_inner_fs, _append_info);

    Path file_path("regular_file");
    FileReaderSPtr reader;
    Status st = merge_fs.open_file(file_path, &reader, nullptr);
    EXPECT_TRUE(st.ok());
    EXPECT_NE(reader, nullptr);
}

TEST_F(PackedFileSystemTest, OpenFileInMergeFile) {
    std::unordered_map<std::string, PackedSliceLocation> index_map;
    PackedSliceLocation index;
    index.packed_file_path = "packed_file_path";
    index.offset = 10;
    index.size = 20;
    index_map["test_file"] = index;

    PackedFileSystem merge_fs(_inner_fs, index_map, _append_info);

    Path file_path("test_file");
    FileReaderSPtr reader;
    Status st = merge_fs.open_file(file_path, &reader, nullptr);
    EXPECT_TRUE(st.ok());
    EXPECT_NE(reader, nullptr);
    EXPECT_EQ(reader->size(), index.size);
}

TEST_F(PackedFileSystemTest, ExistsForMergeFile) {
    std::unordered_map<std::string, PackedSliceLocation> index_map;
    PackedSliceLocation index;
    index.packed_file_path = "packed_file_path";
    index.offset = 10;
    index.size = 20;
    index_map["test_file"] = index;

    PackedFileSystem merge_fs(_inner_fs, index_map, _append_info);
    _inner_fs->set_exists_result(true);

    Path file_path("test_file");
    bool exists = false;
    Status st = merge_fs.exists(file_path, &exists);
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(exists);
}

TEST_F(PackedFileSystemTest, ExistsForRegularFile) {
    std::unordered_map<std::string, PackedSliceLocation> index_map;
    PackedFileSystem merge_fs(_inner_fs, index_map, _append_info);
    _inner_fs->set_exists_result(true);

    Path file_path("regular_file");
    bool exists = false;
    Status st = merge_fs.exists(file_path, &exists);
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(exists);
}

TEST_F(PackedFileSystemTest, ExistsForNonExistentFile) {
    std::unordered_map<std::string, PackedSliceLocation> index_map;
    PackedFileSystem merge_fs(_inner_fs, index_map, _append_info);
    _inner_fs->set_exists_result(false);

    Path file_path("non_existent_file");
    bool exists = true;
    Status st = merge_fs.exists(file_path, &exists);
    EXPECT_TRUE(st.ok());
    EXPECT_FALSE(exists);
}

TEST_F(PackedFileSystemTest, FileSizeForMergeFile) {
    std::unordered_map<std::string, PackedSliceLocation> index_map;
    PackedSliceLocation index;
    index.packed_file_path = "packed_file_path";
    index.offset = 10;
    index.size = 30;
    index_map["test_file"] = index;

    PackedFileSystem merge_fs(_inner_fs, index_map, _append_info);

    Path file_path("test_file");
    int64_t file_size = 0;
    Status st = merge_fs.file_size(file_path, &file_size);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(file_size, index.size);
}

TEST_F(PackedFileSystemTest, FileSizeForRegularFile) {
    std::unordered_map<std::string, PackedSliceLocation> index_map;
    PackedFileSystem merge_fs(_inner_fs, index_map, _append_info);
    _inner_fs->set_file_size(100);

    Path file_path("regular_file");
    int64_t file_size = 0;
    Status st = merge_fs.file_size(file_path, &file_size);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(file_size, 100);
}

TEST_F(PackedFileSystemTest, UnsupportedCreateDirectory) {
    PackedFileSystem merge_fs(_inner_fs, _append_info);

    Path dir_path("test_dir");
    Status st = merge_fs.create_directory(dir_path);
    EXPECT_FALSE(st.ok());
}

TEST_F(PackedFileSystemTest, UnsupportedDeleteFile) {
    PackedFileSystem merge_fs(_inner_fs, _append_info);

    Path file_path("test_file");
    Status st = merge_fs.delete_file(file_path);
    EXPECT_FALSE(st.ok());
}

TEST_F(PackedFileSystemTest, UnsupportedDeleteDirectory) {
    PackedFileSystem merge_fs(_inner_fs, _append_info);

    Path dir_path("test_dir");
    Status st = merge_fs.delete_directory(dir_path);
    EXPECT_FALSE(st.ok());
}

TEST_F(PackedFileSystemTest, UnsupportedList) {
    PackedFileSystem merge_fs(_inner_fs, _append_info);

    Path dir_path("test_dir");
    std::vector<FileInfo> files;
    bool exists = false;
    Status st = merge_fs.list(dir_path, false, &files, &exists);
    EXPECT_FALSE(st.ok());
}

TEST_F(PackedFileSystemTest, UnsupportedRename) {
    PackedFileSystem merge_fs(_inner_fs, _append_info);

    Path orig_path("orig");
    Path new_path("new");
    Status st = merge_fs.rename(orig_path, new_path);
    EXPECT_FALSE(st.ok());
}

TEST_F(PackedFileSystemTest, AbsolutePath) {
    PackedFileSystem merge_fs(_inner_fs, _append_info);

    Path path("test_path");
    Path abs_path;
    Status st = merge_fs.absolute_path(path, abs_path);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(abs_path.native(), path.native());
}

TEST_F(PackedFileSystemTest, MultipleFilesInMergeFile) {
    std::unordered_map<std::string, PackedSliceLocation> index_map;

    PackedSliceLocation index1;
    index1.packed_file_path = "packed_file_path";
    index1.offset = 0;
    index1.size = 10;
    index_map["file1"] = index1;

    PackedSliceLocation index2;
    index2.packed_file_path = "packed_file_path";
    index2.offset = 10;
    index2.size = 20;
    index_map["file2"] = index2;

    PackedFileSystem merge_fs(_inner_fs, index_map, _append_info);

    // Test file1
    Path file1_path("file1");
    int64_t file1_size = 0;
    Status st = merge_fs.file_size(file1_path, &file1_size);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(file1_size, index1.size);

    // Test file2
    Path file2_path("file2");
    int64_t file2_size = 0;
    st = merge_fs.file_size(file2_path, &file2_size);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(file2_size, index2.size);
}

TEST_F(PackedFileSystemTest, ExistsWithoutIndexMapInitialized) {
    PackedFileSystem merge_fs(_inner_fs, _append_info);

    Path file_path("test_file");
    bool exists = false;
    Status st = merge_fs.exists(file_path, &exists);
    // Should fail because index map is not initialized
    EXPECT_FALSE(st.ok());
}

TEST_F(PackedFileSystemTest, FileSizeWithoutIndexMapInitialized) {
    PackedFileSystem merge_fs(_inner_fs, _append_info);

    Path file_path("test_file");
    int64_t file_size = 0;
    Status st = merge_fs.file_size(file_path, &file_size);
    // Should fail because index map is not initialized
    EXPECT_FALSE(st.ok());
}

} // namespace doris::io
