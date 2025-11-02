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

#include <chrono>
#include <ctime>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/path.h"
#include "util/slice.h"

#include "cloud/config.h"

#include "io/fs/merge_file_manager.h"

namespace doris::io {

using doris::Status;
using std::chrono::seconds;

class MockFileWriter : public FileWriter {
public:
    explicit MockFileWriter(std::string path) : _path(std::move(path)) {}

    void set_append_status(Status st) { _append_status = std::move(st); }
    void set_close_status(Status st) { _close_status = std::move(st); }

    size_t append_calls() const { return _append_calls; }
    bool closed() const { return _state == State::CLOSED; }
    size_t bytes_appended() const override { return _bytes_appended; }
    const std::string& written_data() const { return _written; }

    Status close(bool /*non_block*/ = false) override {
        if (!_close_status.ok()) {
            return _close_status;
        }
        _state = State::CLOSED;
        return Status::OK();
    }

    Status appendv(const Slice* data, size_t data_cnt) override {
        if (!_append_status.ok()) {
            return _append_status;
        }
        for (size_t i = 0; i < data_cnt; ++i) {
            _written.append(reinterpret_cast<const char*>(data[i].data), data[i].size);
            _bytes_appended += data[i].size;
        }
        ++_append_calls;
        return Status::OK();
    }

    const Path& path() const override { return _path; }

    State state() const override { return _state; }

private:
    Path _path;
    size_t _bytes_appended = 0;
    size_t _append_calls = 0;
    std::string _written;
    Status _append_status = Status::OK();
    Status _close_status = Status::OK();
    State _state = State::OPENED;
};

class MockFileSystem : public FileSystem {
public:
    MockFileSystem() : FileSystem("mock_fs", FileSystemType::LOCAL) {}

    void set_create_status(Status st) { _create_status = std::move(st); }
    void set_writer_append_status(Status st) { _writer_append_status = std::move(st); }
    void set_writer_close_status(Status st) { _writer_close_status = std::move(st); }

    MockFileWriter* last_writer() const { return _last_writer; }
    const std::vector<std::string>& created_paths() const { return _created_paths; }

protected:
    Status create_file_impl(const Path& file, FileWriterPtr* writer,
                            const FileWriterOptions* /*opts*/) override {
        if (!_create_status.ok()) {
            return _create_status;
        }
        auto mock_writer = std::make_unique<MockFileWriter>(file.native());
        mock_writer->set_append_status(_writer_append_status);
        mock_writer->set_close_status(_writer_close_status);
        _last_writer = mock_writer.get();
        *writer = std::move(mock_writer);
        _created_paths.push_back(file.native());
        return Status::OK();
    }

    Status open_file_impl(const Path& /*file*/, FileReaderSPtr* /*reader*/,
                          const FileReaderOptions* /*opts*/) override {
        return Status::NotSupported("not implemented");
    }

    Status create_directory_impl(const Path& /*dir*/, bool /*failed_if_exists*/) override {
        return Status::OK();
    }

    Status delete_file_impl(const Path& /*file*/) override { return Status::OK(); }

    Status batch_delete_impl(const std::vector<Path>& /*files*/) override {
        return Status::OK();
    }

    Status delete_directory_impl(const Path& /*dir*/) override { return Status::OK(); }

    Status exists_impl(const Path& /*path*/, bool* res) const override {
        *res = false;
        return Status::OK();
    }

    Status file_size_impl(const Path& /*file*/, int64_t* size) const override {
        *size = 0;
        return Status::OK();
    }

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
    Status _writer_append_status = Status::OK();
    Status _writer_close_status = Status::OK();
    MockFileWriter* _last_writer = nullptr;
    std::vector<std::string> _created_paths;
};

class MergeFileManagerTest : public testing::Test {
protected:
    void SetUp() override {
        _old_merge_threshold = config::merge_file_size_threshold_bytes;
        _old_small_threshold = config::small_file_threshold_bytes;
        _old_retention = config::uploaded_file_retention_seconds;
        _old_deploy_mode = config::deploy_mode;
        _old_cloud_id = config::cloud_unique_id;

        config::merge_file_size_threshold_bytes = 1024;
        config::small_file_threshold_bytes = 1024;
        config::uploaded_file_retention_seconds = 60;
        config::deploy_mode.clear();
        config::cloud_unique_id.clear();

        file_system = std::make_shared<MockFileSystem>();
        manager = std::make_unique<MergeFileManager>();
        manager->_file_system = file_system;
        ASSERT_TRUE(manager->init().ok());
        ASSERT_TRUE(manager->create_new_merge_file_state(manager->_current_merge_file).ok());
        ASSERT_NE(file_system->last_writer(), nullptr);
    }

    void TearDown() override {
        manager.reset();
        file_system.reset();

        config::merge_file_size_threshold_bytes = _old_merge_threshold;
        config::small_file_threshold_bytes = _old_small_threshold;
        config::uploaded_file_retention_seconds = _old_retention;
        config::deploy_mode = _old_deploy_mode;
        config::cloud_unique_id = _old_cloud_id;
    }

    std::shared_ptr<MockFileSystem> file_system;
    std::unique_ptr<MergeFileManager> manager;

private:
    int64_t _old_merge_threshold = 0;
    int64_t _old_small_threshold = 0;
    int64_t _old_retention = 0;
    std::string _old_deploy_mode;
    std::string _old_cloud_id;
};

TEST_F(MergeFileManagerTest, CreateNewMergeFileStateFailure) {
    auto failing_fs = std::make_shared<MockFileSystem>();
    failing_fs->set_create_status(Status::IOError("create failed"));
    MergeFileManager local_manager;
    local_manager._file_system = failing_fs;
    EXPECT_FALSE(local_manager.create_new_merge_file_state(local_manager._current_merge_file).ok());
}

TEST_F(MergeFileManagerTest, AppendSmallFileSuccess) {
    auto writer = file_system->last_writer();
    ASSERT_NE(writer, nullptr);
    std::string payload = "abc";
    Slice slice(payload);

    Status st = manager->append("s/path", slice);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(writer->append_calls(), 1);
    EXPECT_EQ(writer->bytes_appended(), payload.size());

    auto it = manager->_global_index_map.find("s/path");
    ASSERT_NE(it, manager->_global_index_map.end());
    EXPECT_EQ(it->second.size, payload.size());
    EXPECT_EQ(it->second.offset, 0);
}

TEST_F(MergeFileManagerTest, AppendLargeFileSkipped) {
    config::small_file_threshold_bytes = 4;
    auto writer = file_system->last_writer();
    ASSERT_NE(writer, nullptr);

    std::string payload = "0123456789";
    Slice slice(payload);
    Status st = manager->append("large", slice);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(writer->append_calls(), 0);
    EXPECT_EQ(manager->_global_index_map.count("large"), 0);
}

TEST_F(MergeFileManagerTest, AppendWriterFailure) {
    auto writer = file_system->last_writer();
    ASSERT_NE(writer, nullptr);
    writer->set_append_status(Status::IOError("append fail"));

    std::string payload = "data";
    Slice slice(payload);
    Status st = manager->append("broken", slice);
    EXPECT_FALSE(st.ok());
    EXPECT_EQ(manager->_global_index_map.count("broken"), 0);
}

TEST_F(MergeFileManagerTest, AppendTriggersRotationWhenThresholdReached) {
    config::merge_file_size_threshold_bytes = 8;
    std::string payload1 = "aaaa";
    std::string payload2 = "bbbb";
    Slice slice1(payload1);
    Slice slice2(payload2);

    EXPECT_TRUE(manager->append("file1", slice1).ok());

    size_t create_before = file_system->created_paths().size();
    EXPECT_TRUE(manager->append("file2", slice2).ok());
    EXPECT_EQ(file_system->created_paths().size(), create_before + 1);
    EXPECT_EQ(manager->_uploading_merge_files.size(), 1);
}

TEST_F(MergeFileManagerTest, MarkCurrentMergeFileForUploadMovesState) {
    auto current_path = manager->_current_merge_file->merge_file_path;
    EXPECT_TRUE(manager->mark_current_merge_file_for_upload().ok());
    ASSERT_EQ(manager->_uploading_merge_files.size(), 1);
    auto uploading = manager->_uploading_merge_files.begin()->second;
    EXPECT_EQ(uploading->merge_file_path, current_path);
    EXPECT_EQ(uploading->state.load(), MergeFileManager::MergeFileStateEnum::UPLOADING);
    EXPECT_NE(manager->_current_merge_file, nullptr);
    EXPECT_NE(manager->_current_merge_file->merge_file_path, current_path);
}

TEST_F(MergeFileManagerTest, GetMergeFileIndexForUnknownPathReturnsNotFound) {
    std::vector<MergeFileSegmentIndex> indices;
    auto status = manager->get_merge_file_index("missing", &indices);
    EXPECT_TRUE(status.is<doris::ErrorCode::NOT_FOUND>());
    EXPECT_TRUE(indices.empty());
}

TEST_F(MergeFileManagerTest, GetMergeFileIndexReturnsStoredValue) {
    std::string payload = "abc";
    Slice slice(payload);
    EXPECT_TRUE(manager->append("stored", slice).ok());
    std::vector<MergeFileSegmentIndex> indices;
    EXPECT_TRUE(manager->get_merge_file_index("stored", &indices).ok());
    ASSERT_EQ(indices.size(), 1);
    EXPECT_EQ(indices[0].size, payload.size());
    EXPECT_EQ(indices[0].offset, 0);
    EXPECT_EQ(indices[0].merge_file_path, manager->_current_merge_file->merge_file_path);
}

TEST_F(MergeFileManagerTest, WaitWriteDoneReturnsOkWhenAlreadyUploaded) {
    std::string payload = "abc";
    Slice slice(payload);
    ASSERT_TRUE(manager->append("path", slice).ok());
    ASSERT_TRUE(manager->mark_current_merge_file_for_upload().ok());
    ASSERT_EQ(manager->_uploading_merge_files.size(), 1);

    auto uploading = manager->_uploading_merge_files.begin()->second;
    {
        std::lock_guard lock(uploading->upload_mutex);
        uploading->state = MergeFileManager::MergeFileStateEnum::UPLOADED;
        uploading->upload_time = std::time(nullptr);
    }
    manager->_uploaded_merge_files[uploading->merge_file_path] = uploading;
    manager->_uploading_merge_files.clear();
    uploading->upload_cv.notify_all();

    EXPECT_TRUE(manager->wait_write_done("path").ok());
}

TEST_F(MergeFileManagerTest, WaitWriteDoneReturnsErrorWhenFailed) {
    std::string payload = "abc";
    Slice slice(payload);
    ASSERT_TRUE(manager->append("bad", slice).ok());
    ASSERT_TRUE(manager->mark_current_merge_file_for_upload().ok());

    auto uploading = manager->_uploading_merge_files.begin()->second;
    {
        std::lock_guard lock(uploading->upload_mutex);
        uploading->state = MergeFileManager::MergeFileStateEnum::FAILED;
        uploading->last_error = "meta failed";
        uploading->upload_time = std::time(nullptr);
    }
    manager->_uploaded_merge_files[uploading->merge_file_path] = uploading;
    manager->_uploading_merge_files.clear();
    uploading->upload_cv.notify_all();

    auto status = manager->wait_write_done("bad");
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("meta failed"), std::string::npos);
}

TEST_F(MergeFileManagerTest, WaitWriteDoneReturnsErrorWhenPathMissing) {
    auto indices = manager->_global_index_map.find("ghost");
    ASSERT_EQ(indices, manager->_global_index_map.end());
    auto status = manager->wait_write_done("ghost");
    EXPECT_FALSE(status.ok());
}

TEST_F(MergeFileManagerTest, WaitWriteDoneBlocksUntilUploadCompletes) {
    std::string payload = "abc";
    Slice slice(payload);
    ASSERT_TRUE(manager->append("waiting", slice).ok());

    auto current_state = manager->_current_merge_file.get();
    {
        std::lock_guard lock(current_state->upload_mutex);
        current_state->state = MergeFileManager::MergeFileStateEnum::UPLOADING;
    }

    std::promise<void> ready;
    std::future<void> ready_future = ready.get_future();
    std::thread waiter([&] {
        ready.set_value();
        auto status = manager->wait_write_done("waiting");
        EXPECT_TRUE(status.ok());
    });

    ready_future.wait();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    {
        std::lock_guard lock(current_state->upload_mutex);
        current_state->state = MergeFileManager::MergeFileStateEnum::UPLOADED;
    }
    current_state->upload_cv.notify_all();

    waiter.join();
}

TEST_F(MergeFileManagerTest, UploadMergeFileReturnsErrorWhenWriterNull) {
    auto status = manager->upload_merge_file("path", nullptr);
    EXPECT_FALSE(status.ok());
}

TEST_F(MergeFileManagerTest, UploadMergeFilePropagatesWriterCloseFailure) {
    auto writer = file_system->last_writer();
    ASSERT_NE(writer, nullptr);
    writer->set_close_status(Status::IOError("close fail"));
    auto status = manager->upload_merge_file(writer->path().native(), writer);
    EXPECT_FALSE(status.ok());
}

TEST_F(MergeFileManagerTest, UploadMergeFileSucceedsWhenWriterCloses) {
    auto writer = file_system->last_writer();
    ASSERT_NE(writer, nullptr);
    auto status = manager->upload_merge_file(writer->path().native(), writer);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(writer->closed());
}

TEST_F(MergeFileManagerTest, ProcessUploadingFilesSetsFailedWhenMetaUpdateFails) {
    std::string payload = "abc";
    Slice slice(payload);
    ASSERT_TRUE(manager->append("meta_fail", slice).ok());
    ASSERT_TRUE(manager->mark_current_merge_file_for_upload().ok());
    ASSERT_EQ(manager->_uploading_merge_files.size(), 1);

    manager->process_uploading_files();

    ASSERT_EQ(manager->_uploading_merge_files.size(), 0);
    ASSERT_EQ(manager->_uploaded_merge_files.size(), 1);
    auto uploaded = manager->_uploaded_merge_files.begin()->second;
    EXPECT_EQ(uploaded->state.load(), MergeFileManager::MergeFileStateEnum::FAILED);
    EXPECT_FALSE(uploaded->last_error.empty());
}

TEST_F(MergeFileManagerTest, CleanupExpiredDataRemovesOldEntries) {
    config::uploaded_file_retention_seconds = 0;
    auto state = std::make_shared<MergeFileManager::MergeFileState>();
    state->merge_file_path = "old";
    state->state = MergeFileManager::MergeFileStateEnum::UPLOADED;
    state->upload_time = 0;
    manager->_uploaded_merge_files[state->merge_file_path] = state;

    manager->cleanup_expired_data();

    EXPECT_TRUE(manager->_uploaded_merge_files.empty());
}

} // namespace doris::io
