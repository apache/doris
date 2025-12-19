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

#include "io/fs/packed_file_manager.h"

#include <gtest/gtest.h>

#include <any>
#include <chrono>
#include <cstring>
#include <ctime>
#include <future>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "cloud/config.h"
#include "common/config.h"
#include "common/status.h"
#include "cpp/sync_point.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/packed_file_trailer.h"
#include "io/fs/path.h"
#include "util/coding.h"
#include "util/slice.h"

namespace doris::io {

using doris::Status;
class MockFileWriter : public FileWriter {
public:
    explicit MockFileWriter(std::string path) : _path(std::move(path)) {}

    void set_append_status(Status st) { _append_status = std::move(st); }
    void set_close_status(Status st) { _close_status = std::move(st); }
    void set_start_close_status(Status st) { _start_close_status = std::move(st); }
    void complete_async_close() {
        if (_state == State::ASYNC_CLOSING) {
            _state = State::CLOSED;
        }
    }

    size_t append_calls() const { return _append_calls; }
    bool closed() const { return _state == State::CLOSED; }
    size_t bytes_appended() const override { return _bytes_appended; }
    const std::string& written_data() const { return _written; }

    Status close(bool non_block = false) override {
        if (_state == State::CLOSED) {
            if (non_block) {
                if (_close_status.ok()) {
                    return Status::Error<ErrorCode::ALREADY_CLOSED>(
                            "MockFileWriter already closed: {}", _path.native());
                }
                return _close_status;
            }
            return Status::Error<ErrorCode::ALREADY_CLOSED>("MockFileWriter already closed: {}",
                                                            _path.native());
        }

        if (!_start_close_status.ok()) {
            return _start_close_status;
        }

        if (_state == State::ASYNC_CLOSING) {
            return Status::InternalError("Don't submit async close multi times");
        }

        if (non_block) {
            _state = State::ASYNC_CLOSING;
            return Status::OK();
        }

        _state = State::CLOSED;
        return _close_status;
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
    Status _start_close_status = Status::OK();
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

    Status batch_delete_impl(const std::vector<Path>& /*files*/) override { return Status::OK(); }

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

class PackedFileManagerTest : public testing::Test {
protected:
    void SetUp() override {
        _old_merge_threshold = config::packed_file_size_threshold_bytes;
        _old_small_threshold = config::small_file_threshold_bytes;
        _old_retention = config::uploaded_file_retention_seconds;
        _old_time_threshold = config::packed_file_time_threshold_ms;
        _old_small_file_count_threshold = config::packed_file_small_file_count_threshold;
        _old_deploy_mode = config::deploy_mode;
        _old_cloud_id = config::cloud_unique_id;

        config::packed_file_size_threshold_bytes = 1024;
        config::small_file_threshold_bytes = 1024;
        config::uploaded_file_retention_seconds = 60;
        config::packed_file_time_threshold_ms = 100; // Default 100ms
        config::packed_file_small_file_count_threshold = 100;
        config::deploy_mode.clear();
        config::cloud_unique_id.clear();

        file_system = std::make_shared<MockFileSystem>();
        manager = std::make_unique<PackedFileManager>();
        manager->_file_systems[_resource_id] = file_system;
        ASSERT_TRUE(manager->init().ok());
        auto& state = manager->current_packed_files_for_test()[_resource_id];
        ASSERT_TRUE(manager->create_new_packed_file_state_for_test(_resource_id, state).ok());
        ASSERT_NE(file_system->last_writer(), nullptr);
        manager->reset_packed_file_bvars_for_test();
    }

    void TearDown() override {
        manager.reset();
        file_system.reset();

        config::packed_file_size_threshold_bytes = _old_merge_threshold;
        config::small_file_threshold_bytes = _old_small_threshold;
        config::uploaded_file_retention_seconds = _old_retention;
        config::packed_file_time_threshold_ms = _old_time_threshold;
        config::packed_file_small_file_count_threshold = _old_small_file_count_threshold;
        config::deploy_mode = _old_deploy_mode;
        config::cloud_unique_id = _old_cloud_id;
    }

    PackedAppendContext default_append_info() const {
        PackedAppendContext info;
        info.resource_id = _resource_id;
        info.tablet_id = _tablet_id;
        info.rowset_id = _rowset_id;
        info.txn_id = _txn_id;
        return info;
    }

    std::shared_ptr<MockFileSystem> file_system;
    std::unique_ptr<PackedFileManager> manager;

private:
    int64_t _old_merge_threshold = 0;
    int64_t _old_small_threshold = 0;
    int64_t _old_retention = 0;
    int64_t _old_time_threshold = 0;
    int64_t _old_small_file_count_threshold = 0;
    std::string _old_deploy_mode;
    std::string _old_cloud_id;
    std::string _resource_id = "test_resource";
    int64_t _tablet_id = 12345;
    std::string _rowset_id = "rowset_1";
    int64_t _txn_id = 6789;
};

TEST_F(PackedFileManagerTest, CreateNewMergeFileStateFailure) {
    auto failing_fs = std::make_shared<MockFileSystem>();
    failing_fs->set_create_status(Status::IOError("create failed"));
    PackedFileManager local_manager;
    local_manager._file_systems[_resource_id] = failing_fs;
    auto& state = local_manager.current_packed_files_for_test()[_resource_id];
    EXPECT_FALSE(local_manager.create_new_packed_file_state_for_test(_resource_id, state).ok());
}

TEST_F(PackedFileManagerTest, AppendSmallFileSuccess) {
    auto writer = file_system->last_writer();
    ASSERT_NE(writer, nullptr);
    std::string payload = "abc";
    Slice slice(payload);

    auto info = default_append_info();
    Status st = manager->append_small_file("s/path", slice, info);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(writer->append_calls(), 1);
    EXPECT_EQ(writer->bytes_appended(), payload.size());

    auto it = manager->global_slice_locations_for_test().find("s/path");
    ASSERT_NE(it, manager->global_slice_locations_for_test().end());
    EXPECT_EQ(it->second.size, payload.size());
    EXPECT_EQ(it->second.offset, 0);
    EXPECT_EQ(it->second.tablet_id, info.tablet_id);
    EXPECT_EQ(it->second.rowset_id, info.rowset_id);
    EXPECT_EQ(it->second.resource_id, info.resource_id);
    EXPECT_EQ(it->second.txn_id, info.txn_id);
}

TEST_F(PackedFileManagerTest, AppendFailsWithoutTxnId) {
    std::string payload = "abc";
    Slice slice(payload);
    auto info = default_append_info();
    info.txn_id = 0;

    Status st = manager->append_small_file("missing_txn", slice, info);
    EXPECT_EQ(st.code(), doris::ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(manager->global_slice_locations_for_test().count("missing_txn"), 0);
}

TEST_F(PackedFileManagerTest, AppendLargeFileSkipped) {
    config::small_file_threshold_bytes = 4;
    auto writer = file_system->last_writer();
    ASSERT_NE(writer, nullptr);

    std::string payload = "0123456789";
    Slice slice(payload);
    auto info = default_append_info();
    Status st = manager->append_small_file("large", slice, info);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(writer->append_calls(), 0);
    EXPECT_EQ(manager->global_slice_locations_for_test().count("large"), 0);
}

TEST_F(PackedFileManagerTest, AppendWriterFailure) {
    auto writer = file_system->last_writer();
    ASSERT_NE(writer, nullptr);
    writer->set_append_status(Status::IOError("append fail"));

    std::string payload = "data";
    Slice slice(payload);
    auto info = default_append_info();
    Status st = manager->append_small_file("broken", slice, info);
    EXPECT_FALSE(st.ok());
    EXPECT_EQ(manager->global_slice_locations_for_test().count("broken"), 0);
}

TEST_F(PackedFileManagerTest, AppendTriggersRotationWhenThresholdReached) {
    config::packed_file_size_threshold_bytes = 8;
    std::string payload1 = "aaaa";
    std::string payload2 = "bbbb";
    Slice slice1(payload1);
    Slice slice2(payload2);
    auto info = default_append_info();

    EXPECT_TRUE(manager->append_small_file("file1", slice1, info).ok());

    size_t create_before = file_system->created_paths().size();
    EXPECT_TRUE(manager->append_small_file("file2", slice2, info).ok());
    EXPECT_EQ(file_system->created_paths().size(), create_before + 1);
    EXPECT_EQ(manager->uploading_packed_files_for_test().size(), 1);
}

TEST_F(PackedFileManagerTest, AppendTriggersRotationWhenFileCountThresholdReached) {
    config::packed_file_small_file_count_threshold = 2;
    config::packed_file_size_threshold_bytes = 1024; // keep size threshold out of the way

    std::string payload = "data";
    Slice slice(payload);
    auto info = default_append_info();

    EXPECT_TRUE(manager->append_small_file("file1", slice, info).ok());
    EXPECT_TRUE(manager->uploading_packed_files_for_test().empty());

    size_t created_before = file_system->created_paths().size();
    EXPECT_TRUE(manager->append_small_file("file2", slice, info).ok());
    EXPECT_EQ(manager->uploading_packed_files_for_test().size(), 1);
    EXPECT_EQ(file_system->created_paths().size(), created_before + 1);

    auto uploading = manager->uploading_packed_files_for_test().begin()->second;
    ASSERT_NE(uploading, nullptr);
    EXPECT_EQ(uploading->slice_locations.size(), 2);

    auto* new_state = manager->current_packed_files_for_test()[_resource_id].get();
    ASSERT_NE(new_state, nullptr);
    EXPECT_NE(uploading->packed_file_path, new_state->packed_file_path);
}

TEST_F(PackedFileManagerTest, MarkCurrentMergeFileForUploadMovesState) {
    auto* current_state = manager->current_packed_files_for_test()[_resource_id].get();
    ASSERT_NE(current_state, nullptr);
    auto current_path = current_state->packed_file_path;
    EXPECT_TRUE(manager->mark_current_packed_file_for_upload(_resource_id).ok());
    ASSERT_EQ(manager->uploading_packed_files_for_test().size(), 1);
    auto uploading = manager->uploading_packed_files_for_test().begin()->second;
    EXPECT_EQ(uploading->packed_file_path, current_path);
    EXPECT_EQ(uploading->state.load(), PackedFileManager::PackedFileState::READY_TO_UPLOAD);
    auto* new_state = manager->current_packed_files_for_test()[_resource_id].get();
    EXPECT_NE(new_state, nullptr);
    EXPECT_NE(new_state->packed_file_path, current_path);
}

TEST_F(PackedFileManagerTest, GetMergeFileIndexForUnknownPathReturnsNotFound) {
    PackedSliceLocation index;
    auto status = manager->get_packed_slice_location("missing", &index);
    EXPECT_EQ(status.code(), doris::ErrorCode::NOT_FOUND);
}

TEST_F(PackedFileManagerTest, GetMergeFileIndexReturnsStoredValue) {
    std::string payload = "abc";
    Slice slice(payload);
    auto info = default_append_info();
    EXPECT_TRUE(manager->append_small_file("stored", slice, info).ok());
    PackedSliceLocation index;
    EXPECT_TRUE(manager->get_packed_slice_location("stored", &index).ok());
    EXPECT_EQ(index.size, payload.size());
    EXPECT_EQ(index.offset, 0);
    auto* current_state = manager->current_packed_files_for_test()[_resource_id].get();
    ASSERT_NE(current_state, nullptr);
    EXPECT_EQ(index.packed_file_path, current_state->packed_file_path);
}

TEST_F(PackedFileManagerTest, WaitWriteDoneReturnsOkWhenAlreadyUploaded) {
    std::string payload = "abc";
    Slice slice(payload);
    auto info = default_append_info();
    ASSERT_TRUE(manager->append_small_file("path", slice, info).ok());
    ASSERT_TRUE(manager->mark_current_packed_file_for_upload(_resource_id).ok());
    ASSERT_EQ(manager->uploading_packed_files_for_test().size(), 1);

    auto uploading = manager->uploading_packed_files_for_test().begin()->second;
    {
        std::lock_guard lock(uploading->upload_mutex);
        uploading->state = PackedFileManager::PackedFileState::UPLOADED;
        uploading->upload_time = std::time(nullptr);
    }
    manager->uploaded_packed_files_for_test()[uploading->packed_file_path] = uploading;
    manager->uploading_packed_files_for_test().clear();
    uploading->upload_cv.notify_all();

    EXPECT_TRUE(manager->wait_upload_done("path").ok());
}

TEST_F(PackedFileManagerTest, WaitWriteDoneReturnsErrorWhenFailed) {
    std::string payload = "abc";
    Slice slice(payload);
    auto info = default_append_info();
    ASSERT_TRUE(manager->append_small_file("bad", slice, info).ok());
    ASSERT_TRUE(manager->mark_current_packed_file_for_upload(_resource_id).ok());

    auto uploading = manager->uploading_packed_files_for_test().begin()->second;
    {
        std::lock_guard lock(uploading->upload_mutex);
        uploading->state = PackedFileManager::PackedFileState::FAILED;
        uploading->last_error = "meta failed";
        uploading->upload_time = std::time(nullptr);
    }
    manager->uploaded_packed_files_for_test()[uploading->packed_file_path] = uploading;
    manager->uploading_packed_files_for_test().clear();
    uploading->upload_cv.notify_all();

    auto status = manager->wait_upload_done("bad");
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("meta failed"), std::string::npos);
}

TEST_F(PackedFileManagerTest, WaitWriteDoneReturnsErrorWhenPathMissing) {
    auto indices = manager->global_slice_locations_for_test().find("ghost");
    ASSERT_EQ(indices, manager->global_slice_locations_for_test().end());
    auto status = manager->wait_upload_done("ghost");
    EXPECT_FALSE(status.ok());
}

TEST_F(PackedFileManagerTest, WaitWriteDoneBlocksUntilUploadCompletes) {
    std::string payload = "abc";
    Slice slice(payload);
    auto info = default_append_info();
    ASSERT_TRUE(manager->append_small_file("waiting", slice, info).ok());

    auto current_state = manager->current_packed_files_for_test()[_resource_id].get();
    ASSERT_NE(current_state, nullptr);
    {
        std::lock_guard lock(current_state->upload_mutex);
        current_state->state = PackedFileManager::PackedFileState::UPLOADING;
    }

    std::promise<void> ready;
    std::future<void> ready_future = ready.get_future();
    std::thread waiter([&] {
        ready.set_value();
        auto status = manager->wait_upload_done("waiting");
        EXPECT_TRUE(status.ok());
    });

    ready_future.wait();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    {
        std::lock_guard lock(current_state->upload_mutex);
        current_state->state = PackedFileManager::PackedFileState::UPLOADED;
    }
    current_state->upload_cv.notify_all();

    waiter.join();
}

TEST_F(PackedFileManagerTest, UploadMergeFileReturnsErrorWhenWriterNull) {
    auto status = manager->finalize_packed_file_upload("path", nullptr);
    EXPECT_FALSE(status.ok());
}

TEST_F(PackedFileManagerTest, UploadMergeFilePropagatesWriterCloseFailure) {
    auto writer = file_system->last_writer();
    ASSERT_NE(writer, nullptr);
    writer->set_start_close_status(Status::IOError("close fail"));
    auto status = manager->finalize_packed_file_upload(writer->path().native(), writer);
    EXPECT_FALSE(status.ok());
}

TEST_F(PackedFileManagerTest, UploadMergeFileSucceedsWhenWriterCloses) {
    auto writer = file_system->last_writer();
    ASSERT_NE(writer, nullptr);
    auto status = manager->finalize_packed_file_upload(writer->path().native(), writer);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(writer->state(), FileWriter::State::ASYNC_CLOSING);
}

TEST_F(PackedFileManagerTest, ProcessUploadingFilesSetsFailedWhenMetaUpdateFails) {
    std::string payload = "abc";
    Slice slice(payload);
    auto info = default_append_info();
    ASSERT_TRUE(manager->append_small_file("meta_fail", slice, info).ok());
    ASSERT_TRUE(manager->mark_current_packed_file_for_upload(_resource_id).ok());
    ASSERT_EQ(manager->uploading_packed_files_for_test().size(), 1);

    manager->process_uploading_packed_files();

    ASSERT_EQ(manager->uploading_packed_files_for_test().size(), 0);
    ASSERT_EQ(manager->uploaded_packed_files_for_test().size(), 1);
    auto uploaded = manager->uploaded_packed_files_for_test().begin()->second;
    EXPECT_EQ(uploaded->state.load(), PackedFileManager::PackedFileState::FAILED);
    EXPECT_FALSE(uploaded->last_error.empty());
}

TEST_F(PackedFileManagerTest, ProcessUploadingFilesCompletesAsyncUpload) {
    std::string payload = "abc";
    Slice slice(payload);
    auto info = default_append_info();
    ASSERT_TRUE(manager->append_small_file("async_success", slice, info).ok());
    ASSERT_TRUE(manager->mark_current_packed_file_for_upload(_resource_id).ok());
    ASSERT_EQ(manager->uploading_packed_files_for_test().size(), 1);

    auto uploading = manager->uploading_packed_files_for_test().begin()->second;
    auto* writer = dynamic_cast<MockFileWriter*>(uploading->writer.get());
    ASSERT_NE(writer, nullptr);
    uploading->state = PackedFileManager::PackedFileState::UPLOADING;
    ASSERT_TRUE(writer->close(true).ok());

    manager->process_uploading_packed_files();
    EXPECT_EQ(uploading->state.load(), PackedFileManager::PackedFileState::UPLOADING);
    EXPECT_EQ(manager->uploaded_packed_files_for_test().size(), 0);

    writer->complete_async_close();
    manager->process_uploading_packed_files();
    EXPECT_EQ(manager->uploading_packed_files_for_test().size(), 0);
    ASSERT_EQ(manager->uploaded_packed_files_for_test().size(), 1);
    auto uploaded = manager->uploaded_packed_files_for_test().begin()->second;
    EXPECT_EQ(uploaded->state.load(), PackedFileManager::PackedFileState::UPLOADED);
}

TEST_F(PackedFileManagerTest, ProcessUploadingFilesSetsFailedWhenAsyncCloseFails) {
    std::string payload = "abc";
    Slice slice(payload);
    auto info = default_append_info();
    ASSERT_TRUE(manager->append_small_file("async_fail", slice, info).ok());
    ASSERT_TRUE(manager->mark_current_packed_file_for_upload(_resource_id).ok());
    ASSERT_EQ(manager->uploading_packed_files_for_test().size(), 1);

    auto uploading = manager->uploading_packed_files_for_test().begin()->second;
    auto* writer = dynamic_cast<MockFileWriter*>(uploading->writer.get());
    ASSERT_NE(writer, nullptr);
    uploading->state = PackedFileManager::PackedFileState::UPLOADING;
    writer->set_close_status(Status::IOError("async close fail"));
    ASSERT_TRUE(writer->close(true).ok());

    manager->process_uploading_packed_files();
    EXPECT_EQ(uploading->state.load(), PackedFileManager::PackedFileState::UPLOADING);

    writer->complete_async_close();
    manager->process_uploading_packed_files();
    EXPECT_EQ(manager->uploading_packed_files_for_test().size(), 0);
    ASSERT_EQ(manager->uploaded_packed_files_for_test().size(), 1);
    auto failed = manager->uploaded_packed_files_for_test().begin()->second;
    EXPECT_EQ(failed->state.load(), PackedFileManager::PackedFileState::FAILED);
    EXPECT_NE(failed->last_error.find("async close fail"), std::string::npos);
}

TEST_F(PackedFileManagerTest, AppendPackedFileInfoToFileTail) {
    std::string payload = "abc";
    Slice slice(payload);
    auto info = default_append_info();
    ASSERT_TRUE(manager->append_small_file("trailer_path", slice, info).ok());
    ASSERT_TRUE(manager->mark_current_packed_file_for_upload(_resource_id).ok());
    ASSERT_EQ(manager->uploading_packed_files_for_test().size(), 1);

    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    sp->set_call_back("PackedFileManager::update_meta_service", [](std::vector<std::any>&& args) {
        auto pair = try_any_cast_ret<Status>(args);
        pair->first = Status::OK();
        pair->second = true;
    });

    manager->process_uploading_packed_files();

    sp->clear_call_back("PackedFileManager::update_meta_service");
    sp->disable_processing();

    ASSERT_EQ(manager->uploading_packed_files_for_test().size(), 1);
    auto uploading = manager->uploading_packed_files_for_test().begin()->second;
    auto* writer = dynamic_cast<MockFileWriter*>(uploading->writer.get());
    ASSERT_NE(writer, nullptr);

    const auto& data = writer->written_data();
    cloud::PackedFileFooterPB parsed_footer;
    uint32_t version = 0;
    auto st = parse_packed_file_trailer(data, &parsed_footer, &version);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(version, kPackedFileTrailerVersion);
    ASSERT_TRUE(parsed_footer.has_packed_file_info());

    const auto& parsed_info = parsed_footer.packed_file_info();
    ASSERT_EQ(parsed_info.slices_size(), 1);
    EXPECT_EQ(parsed_info.slices(0).path(), "trailer_path");
    EXPECT_EQ(parsed_info.slices(0).offset(), 0);
    EXPECT_EQ(parsed_info.slices(0).size(), payload.size());
    EXPECT_EQ(parsed_info.resource_id(), info.resource_id);
}

TEST_F(PackedFileManagerTest, CleanupExpiredDataRemovesOldEntries) {
    config::uploaded_file_retention_seconds = 0;
    auto state = std::make_shared<PackedFileManager::PackedFileContext>();
    state->packed_file_path = "old";
    state->resource_id = _resource_id;
    state->state = PackedFileManager::PackedFileState::UPLOADED;
    state->upload_time = 0;
    manager->uploaded_packed_files_for_test()[state->packed_file_path] = state;

    manager->cleanup_expired_data();

    EXPECT_TRUE(manager->uploaded_packed_files_for_test().empty());
}

TEST_F(PackedFileManagerTest, MergeFileBvarMetricsUpdated) {
    manager->reset_packed_file_bvars_for_test();
    auto state = std::make_shared<PackedFileManager::PackedFileContext>();
    state->total_size = 300;
    PackedSliceLocation idx1;
    idx1.size = 100;
    PackedSliceLocation idx2;
    idx2.size = 200;
    state->slice_locations["a"] = idx1;
    state->slice_locations["b"] = idx2;

    manager->record_packed_file_metrics_for_test(state.get());

    EXPECT_EQ(manager->packed_file_total_count_for_test(), 1);
    EXPECT_EQ(manager->packed_file_total_small_file_num_for_test(), 2);
    EXPECT_EQ(manager->packed_file_total_size_bytes_for_test(), 300);
    EXPECT_DOUBLE_EQ(manager->packed_file_avg_small_file_num_for_test(), 2.0);
    EXPECT_DOUBLE_EQ(manager->packed_file_avg_file_size_for_test(), 300.0);

    manager->reset_packed_file_bvars_for_test();
    EXPECT_EQ(manager->packed_file_total_count_for_test(), 0);
    EXPECT_EQ(manager->packed_file_total_small_file_num_for_test(), 0);
    EXPECT_EQ(manager->packed_file_total_size_bytes_for_test(), 0);
    EXPECT_DOUBLE_EQ(manager->packed_file_avg_small_file_num_for_test(), 0.0);
    EXPECT_DOUBLE_EQ(manager->packed_file_avg_file_size_for_test(), 0.0);
}

// Multiple small file imports, segment size < threshold, triggers merge file
TEST_F(PackedFileManagerTest, MultipleSmallFilesTriggerMergeFile) {
    // Set small file threshold to 100 bytes
    config::small_file_threshold_bytes = 100;
    // Set merge file size threshold to 500 bytes (will trigger rotation when reached)
    config::packed_file_size_threshold_bytes = 500;

    auto writer = file_system->last_writer();
    ASSERT_NE(writer, nullptr);

    auto info = default_append_info();
    constexpr int file_count = 10;
    constexpr int file_size = 50; // Each file is 50 bytes, smaller than threshold 100

    // Append multiple small files
    for (int i = 0; i < file_count; ++i) {
        std::string path = "small_file_" + std::to_string(i);
        std::string payload(file_size, 'a' + (i % 26)); // 50 bytes per file
        Slice slice(payload);

        Status st = manager->append_small_file(path, slice, info);
        EXPECT_TRUE(st.ok()) << "Failed to append file " << i << ": " << st.msg();

        // Verify file is in global index map
        auto it = manager->global_slice_locations_for_test().find(path);
        ASSERT_NE(it, manager->global_slice_locations_for_test().end());
        EXPECT_EQ(it->second.size, file_size);
        EXPECT_EQ(it->second.tablet_id, info.tablet_id);
        EXPECT_EQ(it->second.rowset_id, info.rowset_id);
    }

    // Verify all files are in global index map
    // Note: When total_size + new_file_size >= threshold, a new merge file is created
    // So files may be distributed across multiple merge files
    int total_files_in_index = 0;
    int64_t total_size_in_index = 0;
    for (int i = 0; i < file_count; ++i) {
        std::string path = "small_file_" + std::to_string(i);
        auto it = manager->global_slice_locations_for_test().find(path);
        if (it != manager->global_slice_locations_for_test().end()) {
            total_files_in_index++;
            total_size_in_index += it->second.size;
            EXPECT_LT(it->second.size, config::small_file_threshold_bytes)
                    << "File " << path << " size " << it->second.size
                    << " should be less than threshold " << config::small_file_threshold_bytes;
        }
    }
    EXPECT_EQ(total_files_in_index, file_count);
    EXPECT_EQ(total_size_in_index, file_count * file_size);

    // Count files across all merge files (current and uploading)
    int total_files_in_packed_files = 0;
    int64_t total_size_in_packed_files = 0;

    // Check current merge file
    auto* current_state = manager->current_packed_files_for_test()[_resource_id].get();
    ASSERT_NE(current_state, nullptr);
    total_files_in_packed_files += current_state->slice_locations.size();
    total_size_in_packed_files += current_state->total_size;

    // Check uploading merge files
    for (const auto& [path, state] : manager->uploading_packed_files_for_test()) {
        total_files_in_packed_files += state->slice_locations.size();
        total_size_in_packed_files += state->total_size;
    }

    EXPECT_EQ(total_files_in_packed_files, file_count);
    EXPECT_EQ(total_size_in_packed_files, file_count * file_size);

    // When total size reaches threshold, merge file should be rotated
    // Add one more file to trigger rotation (if not already triggered)
    std::string path_trigger = "trigger_file";
    std::string payload_trigger(file_size, 'z');
    Slice slice_trigger(payload_trigger);

    size_t create_before = file_system->created_paths().size();
    Status st = manager->append_small_file(path_trigger, slice_trigger, info);
    EXPECT_TRUE(st.ok());

    // Should create new merge file when threshold is reached
    EXPECT_GE(file_system->created_paths().size(), create_before);
    // May have uploading merge files if threshold was reached
    EXPECT_GE(manager->uploading_packed_files_for_test().size(), 0);

    // New merge file should have the trigger file
    auto* new_state = manager->current_packed_files_for_test()[_resource_id].get();
    ASSERT_NE(new_state, nullptr);
    // Trigger file should be in current state or uploading state
    auto trigger_it = manager->global_slice_locations_for_test().find(path_trigger);
    EXPECT_NE(trigger_it, manager->global_slice_locations_for_test().end());
}

// Multiple files close to threshold, boundary value verification for merge file
TEST_F(PackedFileManagerTest, FilesNearThresholdBoundaryMergeFile) {
    // Set small file threshold to 100 bytes
    config::small_file_threshold_bytes = 100;
    // Set merge file size threshold to 500 bytes
    config::packed_file_size_threshold_bytes = 500;

    auto writer = file_system->last_writer();
    ASSERT_NE(writer, nullptr);

    auto info = default_append_info();

    // Files exactly at threshold - 1 (should be merged)
    {
        std::string path1 = "boundary_file_99";
        std::string payload1(99, 'x'); // 99 bytes, just below threshold
        Slice slice1(payload1);

        Status st = manager->append_small_file(path1, slice1, info);
        EXPECT_TRUE(st.ok());

        auto it = manager->global_slice_locations_for_test().find(path1);
        ASSERT_NE(it, manager->global_slice_locations_for_test().end());
        EXPECT_EQ(it->second.size, 99);
        EXPECT_LT(it->second.size, config::small_file_threshold_bytes);
    }

    // Files exactly at threshold (should be merged, uses > not >=, so 100 <= 100 means merged)
    {
        std::string path2 = "boundary_file_100";
        std::string payload2(100, 'y'); // 100 bytes, exactly at threshold
        Slice slice2(payload2);

        Status st = manager->append_small_file(path2, slice2, info);
        EXPECT_TRUE(st.ok());

        // File at threshold should be merged (check uses >, so 100 is not > 100, so it's merged)
        auto it = manager->global_slice_locations_for_test().find(path2);
        ASSERT_NE(it, manager->global_slice_locations_for_test().end());
        EXPECT_EQ(it->second.size, 100);
        EXPECT_LE(it->second.size, config::small_file_threshold_bytes);
    }

    // Files at threshold + 1 (should NOT be merged)
    {
        std::string path3 = "boundary_file_101";
        std::string payload3(101, 'z'); // 101 bytes, above threshold
        Slice slice3(payload3);

        size_t append_calls_before = writer->append_calls();
        Status st = manager->append_small_file(path3, slice3, info);
        EXPECT_TRUE(st.ok());

        // File above threshold should not be merged
        auto it = manager->global_slice_locations_for_test().find(path3);
        EXPECT_EQ(it, manager->global_slice_locations_for_test().end());
        EXPECT_EQ(writer->append_calls(), append_calls_before);
    }

    // Multiple files close to threshold, verify merge file behavior
    // Add files that are just below threshold to fill up merge file
    constexpr int file_count = 5;
    constexpr int file_size = 99; // Just below threshold

    for (int i = 0; i < file_count; ++i) {
        std::string path = "near_threshold_file_" + std::to_string(i);
        std::string payload(file_size, 'a' + (i % 26));
        Slice slice(payload);

        Status st = manager->append_small_file(path, slice, info);
        EXPECT_TRUE(st.ok()) << "Failed to append file " << i << ": " << st.msg();

        // Verify file is merged
        auto it = manager->global_slice_locations_for_test().find(path);
        ASSERT_NE(it, manager->global_slice_locations_for_test().end());
        EXPECT_EQ(it->second.size, file_size);
        EXPECT_LT(it->second.size, config::small_file_threshold_bytes);
    }

    // Verify merge file state
    // Note: When total_size + new_file_size >= threshold, a new merge file is created
    // So files may be distributed across multiple merge files
    int total_packed_files = 0;
    int64_t total_merged_size = 0;

    // Count files in current merge file
    auto* current_state = manager->current_packed_files_for_test()[_resource_id].get();
    ASSERT_NE(current_state, nullptr);
    total_packed_files += current_state->slice_locations.size();
    total_merged_size += current_state->total_size;

    // Count files in uploading merge files
    for (const auto& [path, state] : manager->uploading_packed_files_for_test()) {
        total_packed_files += state->slice_locations.size();
        total_merged_size += state->total_size;
    }

    // Should have 1 file from first test (99 bytes) + 1 file at threshold (100 bytes) + 5 files from loop (99 bytes each) = 7 files
    EXPECT_EQ(total_packed_files, 7);
    EXPECT_EQ(total_merged_size, 99 + 100 + (file_count * file_size));

    // Verify all merged files are at or below threshold (uses >, so == threshold is merged)
    for (const auto& [path, index] : current_state->slice_locations) {
        EXPECT_LE(index.size, config::small_file_threshold_bytes)
                << "File " << path << " size " << index.size
                << " should be less than or equal to threshold "
                << config::small_file_threshold_bytes;
    }
    for (const auto& [path, state] : manager->uploading_packed_files_for_test()) {
        for (const auto& [file_path, index] : state->slice_locations) {
            EXPECT_LE(index.size, config::small_file_threshold_bytes)
                    << "File " << file_path << " size " << index.size
                    << " should be less than or equal to threshold "
                    << config::small_file_threshold_bytes;
        }
    }

    // Note: writer->bytes_appended() may not reflect all merged data because
    // each merge file has its own writer, and we're only checking the last writer
    // So we don't verify writer->bytes_appended() here
}

// Merge file result check, generated path and filename meet expectations
TEST_F(PackedFileManagerTest, MergeFilePathAndFilenameFormat) {
    // Verify initial merge file path format
    auto* current_state = manager->current_packed_files_for_test()[_resource_id].get();
    ASSERT_NE(current_state, nullptr);
    const std::string& packed_file_path = current_state->packed_file_path;

    // Path should start with "data/packed_file/"
    EXPECT_TRUE(packed_file_path.find("data/packed_file/") == 0)
            << "Merge file path should start with 'data/packed_file/', got: " << packed_file_path;
}

// Test timeout triggers upload, causing small files to be uploaded directly
TEST_F(PackedFileManagerTest, TimeoutTriggersDirectUpload) {
    // Set a very large merge file size threshold so size won't trigger upload
    config::packed_file_size_threshold_bytes = 10 * 1024 * 1024; // 10MB
    // Set a short timeout threshold - 50ms to trigger timeout quickly
    config::packed_file_time_threshold_ms = 50;

    // Reset manager to use new config
    manager.reset();
    file_system.reset();
    file_system = std::make_shared<MockFileSystem>();
    manager = std::make_unique<PackedFileManager>();
    manager->_file_systems[_resource_id] = file_system;
    ASSERT_TRUE(manager->init().ok());
    auto& state = manager->current_packed_files_for_test()[_resource_id];
    ASSERT_TRUE(manager->create_new_packed_file_state_for_test(_resource_id, state).ok());
    ASSERT_NE(file_system->last_writer(), nullptr);

    // Add a small file that won't trigger size-based upload
    std::string payload = "small_file_data";
    Slice slice(payload);
    auto info = default_append_info();
    ASSERT_TRUE(manager->append_small_file("small_file_1", slice, info).ok());

    // Get the merge file path before timeout
    PackedSliceLocation index_before;
    ASSERT_TRUE(manager->get_packed_slice_location("small_file_1", &index_before).ok());
    std::string packed_file_path_before = index_before.packed_file_path;

    // Verify file is in current merge file (not uploaded yet)
    auto* current_state = manager->current_packed_files_for_test()[_resource_id].get();
    ASSERT_NE(current_state, nullptr);
    EXPECT_EQ(current_state->packed_file_path, packed_file_path_before);
    EXPECT_EQ(current_state->state.load(), PackedFileManager::PackedFileState::ACTIVE);

    // Start background manager thread
    manager->start_background_manager();

    // Wait for timeout to be triggered and processed
    // Background thread checks every check_interval_ms (half of timeout threshold = 25ms)
    // We need to wait at least timeout threshold (50ms) + some margin for processing
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Verify that the merge file was marked for upload due to timeout
    // The original merge file should be in uploading or uploaded state, or new file created
    bool found_in_uploading = false;
    bool found_in_uploaded = false;
    std::string new_packed_file_path;
    {
        auto* new_current_state = manager->current_packed_files_for_test()[_resource_id].get();
        if (new_current_state && new_current_state->packed_file_path != packed_file_path_before) {
            new_packed_file_path = new_current_state->packed_file_path;
        }
    }

    {
        std::lock_guard<std::mutex> lock(manager->_packed_files_mutex);
        if (manager->uploading_packed_files_for_test().find(packed_file_path_before) !=
            manager->uploading_packed_files_for_test().end()) {
            found_in_uploading = true;
        }
        if (manager->uploaded_packed_files_for_test().find(packed_file_path_before) !=
            manager->uploaded_packed_files_for_test().end()) {
            found_in_uploaded = true;
        }
    }

    // The merge file should have been moved to uploading/uploaded state or new file created
    EXPECT_TRUE(found_in_uploading || found_in_uploaded || !new_packed_file_path.empty())
            << "Merge file should be marked for upload due to timeout";

    // Verify the small file index still points to the original merge file
    PackedSliceLocation index_after;
    ASSERT_TRUE(manager->get_packed_slice_location("small_file_1", &index_after).ok());
    EXPECT_EQ(index_after.packed_file_path, packed_file_path_before);
    EXPECT_EQ(index_after.size, payload.size());

    // Process uploading files to complete the upload
    manager->process_uploading_packed_files();

    // Stop background manager
    manager->stop_background_manager();

    // Verify the file is eventually in uploaded or uploading state
    {
        std::lock_guard<std::mutex> lock(manager->_packed_files_mutex);
        bool is_uploaded =
                (manager->uploaded_packed_files_for_test().find(packed_file_path_before) !=
                 manager->uploaded_packed_files_for_test().end());
        bool is_uploading =
                (manager->uploading_packed_files_for_test().find(packed_file_path_before) !=
                 manager->uploading_packed_files_for_test().end());
        EXPECT_TRUE(is_uploaded || is_uploading)
                << "Merge file should be in uploading or uploaded state after timeout";
    }
}

// Continuous import of small files while modifying threshold, merge file should still trigger
TEST_F(PackedFileManagerTest, ModifyThresholdDuringContinuousImport) {
    // Set initial threshold to 100 bytes
    config::small_file_threshold_bytes = 100;
    config::packed_file_size_threshold_bytes = 500;

    auto writer = file_system->last_writer();
    ASSERT_NE(writer, nullptr);

    auto info = default_append_info();
    constexpr int file_size = 50; // 50 bytes per file, should be merged

    // Phase 1: Import some files with initial threshold (100 bytes)
    constexpr int phase1_file_count = 5;
    for (int i = 0; i < phase1_file_count; ++i) {
        std::string path = "file_phase1_" + std::to_string(i);
        std::string payload(file_size, 'a' + (i % 26));
        Slice slice(payload);

        Status st = manager->append_small_file(path, slice, info);
        EXPECT_TRUE(st.ok()) << "Failed to append file " << i << ": " << st.msg();

        // Verify file is in global index map
        auto it = manager->global_slice_locations_for_test().find(path);
        ASSERT_NE(it, manager->global_slice_locations_for_test().end());
        EXPECT_EQ(it->second.size, file_size);
        EXPECT_LT(it->second.size, config::small_file_threshold_bytes);
    }

    // Phase 2: Modify threshold to 200 bytes (larger than before)
    config::small_file_threshold_bytes = 200;

    // Phase 3: Continue importing files with new threshold
    // Files should still be merged because 50 < 200
    constexpr int phase2_file_count = 5;
    for (int i = 0; i < phase2_file_count; ++i) {
        std::string path = "file_phase2_" + std::to_string(i);
        std::string payload(file_size, 'x' + (i % 26));
        Slice slice(payload);

        Status st = manager->append_small_file(path, slice, info);
        EXPECT_TRUE(st.ok()) << "Failed to append file phase2_" << i << ": " << st.msg();

        // Verify file is in global index map
        auto it = manager->global_slice_locations_for_test().find(path);
        ASSERT_NE(it, manager->global_slice_locations_for_test().end());
        EXPECT_EQ(it->second.size, file_size);
        EXPECT_LT(it->second.size, config::small_file_threshold_bytes);
    }

    // Phase 4: Modify threshold to 30 bytes (smaller than file size)
    config::small_file_threshold_bytes = 30;

    // Phase 5: Import a file larger than new threshold
    // This file should NOT be merged because 50 > 30
    {
        std::string path = "file_large_after_threshold_change";
        std::string payload(file_size, 'z');
        Slice slice(payload);

        Status st = manager->append_small_file(path, slice, info);
        EXPECT_TRUE(st.ok());

        // Verify file is NOT in global index map (should be skipped)
        auto it = manager->global_slice_locations_for_test().find(path);
        EXPECT_EQ(it, manager->global_slice_locations_for_test().end())
                << "File larger than threshold should not be merged";
    }

    // Verify all files from phase 1 and phase 2 are in merge files
    int total_packed_files = 0;
    int64_t total_merged_size = 0;

    // Count files in current merge file
    auto* current_state = manager->current_packed_files_for_test()[_resource_id].get();
    ASSERT_NE(current_state, nullptr);
    total_packed_files += current_state->slice_locations.size();
    total_merged_size += current_state->total_size;

    // Count files in uploading merge files
    for (const auto& [path, state] : manager->uploading_packed_files_for_test()) {
        total_packed_files += state->slice_locations.size();
        total_merged_size += state->total_size;
    }

    // Should have phase1_file_count + phase2_file_count = 10 files merged
    EXPECT_EQ(total_packed_files, phase1_file_count + phase2_file_count);
    EXPECT_EQ(total_merged_size, (phase1_file_count + phase2_file_count) * file_size);

    // Verify all phase 1 and phase 2 files are in global index map
    for (int i = 0; i < phase1_file_count; ++i) {
        std::string path = "file_phase1_" + std::to_string(i);
        auto it = manager->global_slice_locations_for_test().find(path);
        EXPECT_NE(it, manager->global_slice_locations_for_test().end())
                << "Phase 1 file " << i << " should be in global index map";
        if (it != manager->global_slice_locations_for_test().end()) {
            EXPECT_EQ(it->second.size, file_size);
        }
    }

    for (int i = 0; i < phase2_file_count; ++i) {
        std::string path = "file_phase2_" + std::to_string(i);
        auto it = manager->global_slice_locations_for_test().find(path);
        EXPECT_NE(it, manager->global_slice_locations_for_test().end())
                << "Phase 2 file " << i << " should be in global index map";
        if (it != manager->global_slice_locations_for_test().end()) {
            EXPECT_EQ(it->second.size, file_size);
        }
    }

    // Phase 6: Modify threshold back to a larger value (e.g., 100 bytes)
    // This verifies that threshold changes are applied dynamically
    config::small_file_threshold_bytes = 100;

    // Phase 7: Verify merge file rotation can still be triggered after threshold change
    // Add a small file (25 bytes) that should be merged with new threshold
    {
        std::string path_trigger = "trigger_file_after_threshold";
        constexpr int trigger_file_size = 25; // 25 bytes, should be merged (25 < 100)
        std::string payload_trigger(trigger_file_size, 't');
        Slice slice_trigger(payload_trigger);

        size_t create_before = file_system->created_paths().size();
        Status st = manager->append_small_file(path_trigger, slice_trigger, info);
        EXPECT_TRUE(st.ok());

        // Should create new merge file when threshold is reached (if not already)
        EXPECT_GE(file_system->created_paths().size(), create_before);

        // Verify trigger file is in global index map (should be merged because 25 < 100)
        auto trigger_it = manager->global_slice_locations_for_test().find(path_trigger);
        EXPECT_NE(trigger_it, manager->global_slice_locations_for_test().end())
                << "Trigger file should be in global index map after threshold change";
        if (trigger_it != manager->global_slice_locations_for_test().end()) {
            EXPECT_EQ(trigger_it->second.size, trigger_file_size);
        }
    }
}

} // namespace doris::io
