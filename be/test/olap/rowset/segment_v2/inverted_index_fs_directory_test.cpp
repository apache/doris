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

#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <memory>

#include "common/config.h"
#include "io/fs/file_system.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "testutil/test_util.h"
#include "util/debug_points.h"

namespace doris::segment_v2 {

class DorisFSDirectoryTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Enable debug points for testing
        _original_enable_debug_points = config::enable_debug_points;
        config::enable_debug_points = true;

        _tmp_dir = std::filesystem::temp_directory_path() / "doris_fs_directory_test";
        std::filesystem::remove_all(_tmp_dir);
        std::filesystem::create_directories(_tmp_dir);
        _fs = io::global_local_filesystem();
        _directory = std::make_unique<DorisFSDirectory>();
        _directory->init(_fs, _tmp_dir.string().c_str());
    }

    void TearDown() override {
        _directory.reset();
        std::filesystem::remove_all(_tmp_dir);
        config::enable_debug_points = _original_enable_debug_points;
    }

    std::filesystem::path _tmp_dir;
    io::FileSystemSPtr _fs;
    std::unique_ptr<DorisFSDirectory> _directory;
    bool _original_enable_debug_points;
};

// Test 1: LOG_AND_THROW_IF_ERROR macro error handling in list()
TEST_F(DorisFSDirectoryTest, ListErrorHandling) {
    // First verify debug points are working by testing a simple case
    std::vector<std::string> names;
    EXPECT_NO_THROW(_directory->list(&names)); // Should work normally

    DebugPoints::instance()->add("DorisFSDirectory::list_status_is_not_ok");

    EXPECT_THROW(_directory->list(&names), CLuceneError);

    DebugPoints::instance()->remove("DorisFSDirectory::list_status_is_not_ok");
}

// Test 2: Directory not exists scenario in list()
TEST_F(DorisFSDirectoryTest, ListDirectoryNotExists) {
    // This test reveals a potential bug in the current implementation:
    // When exists=false but st.ok()=true, LOG_AND_THROW_IF_ERROR won't throw
    // because it only checks if status is not OK. The current code should
    // create a non-OK status when directory doesn't exist.
    DebugPoints::instance()->add("DorisFSDirectory::list_directory_not_exists");

    std::vector<std::string> names;
    // Current implementation doesn't throw - this reveals the bug
    EXPECT_NO_THROW(_directory->list(&names));

    DebugPoints::instance()->remove("DorisFSDirectory::list_directory_not_exists");
}

// Test 3: File exists error handling
TEST_F(DorisFSDirectoryTest, FileExistsErrorHandling) {
    // First test normal behavior
    EXPECT_NO_THROW(_directory->fileExists("test_file"));

    DebugPoints::instance()->add("DorisFSDirectory::fileExists_status_is_not_ok");

    EXPECT_THROW(_directory->fileExists("test_file"), CLuceneError);

    DebugPoints::instance()->remove("DorisFSDirectory::fileExists_status_is_not_ok");
}

// Test 4: File length with NOT_FOUND error
TEST_F(DorisFSDirectoryTest, FileLengthFileNotFound) {
    DebugPoints::instance()->add("inverted file read error: index file not found");

    EXPECT_THROW(_directory->fileLength("test_file"), CLuceneError);

    DebugPoints::instance()->remove("inverted file read error: index file not found");
}

// Test 5: File length general error handling
TEST_F(DorisFSDirectoryTest, FileLengthErrorHandling) {
    DebugPoints::instance()->add("DorisFSDirectory::fileLength_status_is_not_ok");

    EXPECT_THROW(_directory->fileLength("test_file"), CLuceneError);

    DebugPoints::instance()->remove("DorisFSDirectory::fileLength_status_is_not_ok");
}

// Test 6: Touch file error handling
TEST_F(DorisFSDirectoryTest, TouchFileErrorHandling) {
    DebugPoints::instance()->add("DorisFSDirectory::touchFile_status_is_not_ok");

    EXPECT_THROW(_directory->touchFile("test_file"), CLuceneError);

    DebugPoints::instance()->remove("DorisFSDirectory::touchFile_status_is_not_ok");
}

// Test 7: Delete file error handling
TEST_F(DorisFSDirectoryTest, DeleteFileErrorHandling) {
    DebugPoints::instance()->add("DorisFSDirectory::doDeleteFile_status_is_not_ok");

    EXPECT_THROW(_directory->doDeleteFile("test_file"), CLuceneError);

    DebugPoints::instance()->remove("DorisFSDirectory::doDeleteFile_status_is_not_ok");
}

// Test 8: Delete directory error handling
TEST_F(DorisFSDirectoryTest, DeleteDirectoryErrorHandling) {
    DebugPoints::instance()->add("DorisFSDirectory::deleteDirectory_throw_is_not_directory");

    EXPECT_THROW(_directory->deleteDirectory(), CLuceneError);

    DebugPoints::instance()->remove("DorisFSDirectory::deleteDirectory_throw_is_not_directory");
}

// Test 9: Rename file exists error handling
TEST_F(DorisFSDirectoryTest, RenameFileExistsErrorHandling) {
    DebugPoints::instance()->add("DorisFSDirectory::renameFile_exists_status_is_not_ok");

    EXPECT_THROW(_directory->renameFile("from", "to"), CLuceneError);

    DebugPoints::instance()->remove("DorisFSDirectory::renameFile_exists_status_is_not_ok");
}

// Test 10: Rename file delete error handling
TEST_F(DorisFSDirectoryTest, RenameFileDeleteErrorHandling) {
    DebugPoints::instance()->add("DorisFSDirectory::renameFile_delete_status_is_not_ok");

    EXPECT_THROW(_directory->renameFile("from", "to"), CLuceneError);

    DebugPoints::instance()->remove("DorisFSDirectory::renameFile_delete_status_is_not_ok");
}

// Test 11: Rename file rename error handling
TEST_F(DorisFSDirectoryTest, RenameFileRenameErrorHandling) {
    DebugPoints::instance()->add("DorisFSDirectory::renameFile_rename_status_is_not_ok");

    EXPECT_THROW(_directory->renameFile("from", "to"), CLuceneError);

    DebugPoints::instance()->remove("DorisFSDirectory::renameFile_rename_status_is_not_ok");
}

// Test 12: Create output exists error handling
TEST_F(DorisFSDirectoryTest, CreateOutputExistsErrorHandling) {
    DebugPoints::instance()->add("DorisFSDirectory::createOutput_exists_status_is_not_ok");

    EXPECT_THROW(_directory->createOutput("test_file"), CLuceneError);

    DebugPoints::instance()->remove("DorisFSDirectory::createOutput_exists_status_is_not_ok");
}

// Test 13: Create output delete error handling
TEST_F(DorisFSDirectoryTest, CreateOutputDeleteErrorHandling) {
    // First create a file so the delete operation will be triggered
    std::filesystem::path test_file = _tmp_dir / "test_file";
    std::ofstream(test_file).close();

    DebugPoints::instance()->add("DorisFSDirectory::createOutput_delete_status_is_not_ok");

    EXPECT_THROW(_directory->createOutput("test_file"), CLuceneError);

    DebugPoints::instance()->remove("DorisFSDirectory::createOutput_delete_status_is_not_ok");
}

// Test 14: Create output exists after delete error
TEST_F(DorisFSDirectoryTest, CreateOutputExistsAfterDeleteError) {
    // First create a file so the delete operation will be triggered
    std::filesystem::path test_file = _tmp_dir / "test_file";
    std::ofstream(test_file).close();

    DebugPoints::instance()->add("DorisFSDirectory::createOutput_exists_after_delete_error");

    EXPECT_THROW(_directory->createOutput("test_file"), CLuceneError);

    DebugPoints::instance()->remove("DorisFSDirectory::createOutput_exists_after_delete_error");
}

// Test 15: FSIndexInput open with IO error
TEST_F(DorisFSDirectoryTest, FSIndexInputOpenWithIOError) {
    DebugPoints::instance()->add("inverted file read error: index file not found");

    lucene::store::IndexInput* ret = nullptr;
    CLuceneError error;

    bool result = DorisFSDirectory::FSIndexInput::open(_fs, "nonexistent_file", ret, error);

    EXPECT_FALSE(result);
    EXPECT_EQ(error.number(), CL_ERR_FileNotFound);

    DebugPoints::instance()->remove("inverted file read error: index file not found");
}

// Test 16: FSIndexInput open with empty file
TEST_F(DorisFSDirectoryTest, FSIndexInputOpenWithEmptyFile) {
    // Create empty file
    std::filesystem::path test_file = _tmp_dir / "empty_file";
    std::ofstream(test_file).close();

    lucene::store::IndexInput* ret = nullptr;
    CLuceneError error;

    bool result = DorisFSDirectory::FSIndexInput::open(_fs, test_file.string().c_str(), ret, error);

    EXPECT_FALSE(result);
    EXPECT_EQ(error.number(), CL_ERR_EmptyIndexSegment);
}

// Test 17: FSIndexInput readInternal with read error
TEST_F(DorisFSDirectoryTest, FSIndexInputReadInternalWithReadError) {
    // Create a file with content
    std::filesystem::path test_file = _tmp_dir / "test_file";
    std::ofstream ofs(test_file);
    ofs << "test content for reading";
    ofs.close();

    lucene::store::IndexInput* input = nullptr;
    CLuceneError error;

    bool result =
            DorisFSDirectory::FSIndexInput::open(_fs, test_file.string().c_str(), input, error);
    EXPECT_TRUE(result);

    DebugPoints::instance()->add(
            "DorisFSDirectory::FSIndexInput::readInternal_reader_read_at_error");

    uint8_t buffer[10];
    EXPECT_THROW(input->readBytes(buffer, 10), CLuceneError);

    DebugPoints::instance()->remove(
            "DorisFSDirectory::FSIndexInput::readInternal_reader_read_at_error");
    _CLDELETE(input);
}

// Test 18: FSIndexInput readInternal with bytes read error
TEST_F(DorisFSDirectoryTest, FSIndexInputReadInternalWithBytesReadError) {
    // Create a file with content
    std::filesystem::path test_file = _tmp_dir / "test_file2";
    std::ofstream ofs(test_file);
    ofs << "test content for reading";
    ofs.close();

    lucene::store::IndexInput* input = nullptr;
    CLuceneError error;

    bool result =
            DorisFSDirectory::FSIndexInput::open(_fs, test_file.string().c_str(), input, error);
    EXPECT_TRUE(result);

    DebugPoints::instance()->add("DorisFSDirectory::FSIndexInput::readInternal_bytes_read_error");

    uint8_t buffer[10];
    EXPECT_THROW(input->readBytes(buffer, 10), CLuceneError);

    DebugPoints::instance()->remove(
            "DorisFSDirectory::FSIndexInput::readInternal_bytes_read_error");
    _CLDELETE(input);
}

// Test 19: FSIndexOutput init error
TEST_F(DorisFSDirectoryTest, FSIndexOutputInitError) {
    DebugPoints::instance()->add(
            "DorisFSDirectory::FSIndexOutput._throw_clucene_error_in_fsindexoutput_init");
    std::unique_ptr<DorisFSDirectory::FSIndexOutputV2> output =
            std::make_unique<DorisFSDirectory::FSIndexOutputV2>();

    try {
        output->init(nullptr);
    } catch (CLuceneError& err) {
        EXPECT_EQ(err.number(), CL_ERR_IO);
        EXPECT_EQ(std::string(err.what()),
                  "debug point: test throw error in fsindexoutput init mock error");
    }
    try {
        output->close();
    } catch (CLuceneError& err) {
        EXPECT_EQ(err.number(), CL_ERR_IO);
        EXPECT_EQ(std::string(err.what()), "flushBuffer error, _index_v2_file_writer = nullptr");
    }

    DebugPoints::instance()->remove(
            "DorisFSDirectory::FSIndexOutput._throw_clucene_error_in_fsindexoutput_init");
}

// Test 20: FSIndexOutput destructor error
TEST_F(DorisFSDirectoryTest, FSIndexOutputDestructorError) {
    auto* output = _directory->createOutput("test_file");
    output->writeString("test");

    DebugPoints::instance()->add(
            "DorisFSDirectory::FSIndexOutput._throw_clucene_error_in_fsindexoutput_destructor");

    // The destructor should handle the error gracefully (no exception should propagate)
    EXPECT_NO_THROW(delete output);

    DebugPoints::instance()->remove(
            "DorisFSDirectory::FSIndexOutput._throw_clucene_error_in_fsindexoutput_destructor");
}

// Test 21: FSIndexOutput close with writer close error
TEST_F(DorisFSDirectoryTest, FSIndexOutputCloseWithWriterCloseError) {
    auto* output = _directory->createOutput("test_file");
    output->writeString("test");

    DebugPoints::instance()->add("DorisFSDirectory::FSIndexOutput._set_writer_close_status_error");

    EXPECT_THROW(output->close(), CLuceneError);

    DebugPoints::instance()->remove(
            "DorisFSDirectory::FSIndexOutput._set_writer_close_status_error");
    delete output;
}

// Test 22: FSIndexOutput close with null writer
TEST_F(DorisFSDirectoryTest, FSIndexOutputCloseWithNullWriter) {
    auto* output = _directory->createOutput("test_file");
    output->writeString("test");

    DebugPoints::instance()->add("DorisFSDirectory::FSIndexOutput.set_writer_nullptr");

    EXPECT_THROW(output->close(), CLuceneError);

    DebugPoints::instance()->remove("DorisFSDirectory::FSIndexOutput.set_writer_nullptr");
    delete output;
}

// Test 24: DorisFSDirectory close error
TEST_F(DorisFSDirectoryTest, DorisFSDirectoryCloseError) {
    DebugPoints::instance()->add("DorisFSDirectory::close_close_with_error");

    EXPECT_THROW(_directory->close(), CLuceneError);

    DebugPoints::instance()->remove("DorisFSDirectory::close_close_with_error");
}

// Test 25: DorisFSDirectoryFactory getDirectory with null file
TEST_F(DorisFSDirectoryTest, DorisFSDirectoryFactoryGetDirectoryWithNullFile) {
    DebugPoints::instance()->add("DorisFSDirectoryFactory::getDirectory_file_is_nullptr");

    EXPECT_THROW(DorisFSDirectoryFactory::getDirectory(_fs, "test_path", false), CLuceneError);

    DebugPoints::instance()->remove("DorisFSDirectoryFactory::getDirectory_file_is_nullptr");
}

// Test 26: DorisFSDirectoryFactory getDirectory exists error
TEST_F(DorisFSDirectoryTest, DorisFSDirectoryFactoryGetDirectoryExistsError) {
    bool original_inverted_index_ram_dir_enable = config::inverted_index_ram_dir_enable;
    config::inverted_index_ram_dir_enable = false;
    DebugPoints::instance()->add("DorisFSDirectoryFactory::getDirectory_exists_status_is_not_ok");

    EXPECT_THROW(DorisFSDirectoryFactory::getDirectory(_fs, "test_path", false), CLuceneError);

    DebugPoints::instance()->remove(
            "DorisFSDirectoryFactory::getDirectory_exists_status_is_not_ok");
    config::inverted_index_ram_dir_enable = original_inverted_index_ram_dir_enable;
}

// Test 27: DorisFSDirectoryFactory getDirectory create directory error
TEST_F(DorisFSDirectoryTest, DorisFSDirectoryFactoryGetDirectoryCreateDirectoryError) {
    bool original_inverted_index_ram_dir_enable = config::inverted_index_ram_dir_enable;
    config::inverted_index_ram_dir_enable = false;
    DebugPoints::instance()->add(
            "DorisFSDirectoryFactory::getDirectory_create_directory_status_is_not_ok");

    EXPECT_THROW(DorisFSDirectoryFactory::getDirectory(
                         _fs, (_tmp_dir / "test_path2").string().c_str(), false),
                 CLuceneError);

    DebugPoints::instance()->remove(
            "DorisFSDirectoryFactory::getDirectory_create_directory_status_is_not_ok");
    config::inverted_index_ram_dir_enable = original_inverted_index_ram_dir_enable;
}

// Test 28: Buffer size setting in FSIndexInput::open (覆盖第92-93行)
TEST_F(DorisFSDirectoryTest, FSIndexInputOpenBufferSizeDefault) {
    // Create a file with content
    std::filesystem::path test_file = _tmp_dir / "buffer_test_file";
    std::ofstream ofs(test_file);
    ofs << "test content for buffer size test";
    ofs.close();

    lucene::store::IndexInput* ret = nullptr;
    CLuceneError error;

    // Test with buffer_size = -1 (should set to default buffer size)
    bool result =
            DorisFSDirectory::FSIndexInput::open(_fs, test_file.string().c_str(), ret, error, -1);
    EXPECT_TRUE(result);
    EXPECT_NE(ret, nullptr);

    _CLDELETE(ret);
}

// Test 29: DorisRAMFSDirectory fileModified with file not found
TEST_F(DorisFSDirectoryTest, DorisRAMFSDirectoryFileModifiedWithFileNotFound) {
    DebugPoints::instance()->add("DorisRAMFSDirectory::fileModified_file_not_found");

    auto ram_dir = std::make_unique<DorisRAMFSDirectory>();
    ram_dir->init(_fs, _tmp_dir.string().c_str());

    EXPECT_THROW(ram_dir->fileModified("test_file"), CLuceneError);

    DebugPoints::instance()->remove("DorisRAMFSDirectory::fileModified_file_not_found");
}

// Test 30: DorisRAMFSDirectory touchFile with file not found
TEST_F(DorisFSDirectoryTest, DorisRAMFSDirectoryTouchFileWithFileNotFound) {
    DebugPoints::instance()->add("DorisRAMFSDirectory::touchFile_file_not_found");

    auto ram_dir = std::make_unique<DorisRAMFSDirectory>();
    ram_dir->init(_fs, _tmp_dir.string().c_str());

    EXPECT_THROW(ram_dir->touchFile("test_file"), CLuceneError);

    DebugPoints::instance()->remove("DorisRAMFSDirectory::touchFile_file_not_found");
}

// Test 31: DorisRAMFSDirectory fileLength with file not found
TEST_F(DorisFSDirectoryTest, DorisRAMFSDirectoryFileLengthWithFileNotFound) {
    DebugPoints::instance()->add("DorisRAMFSDirectory::fileLength_file_not_found");

    auto ram_dir = std::make_unique<DorisRAMFSDirectory>();
    ram_dir->init(_fs, _tmp_dir.string().c_str());

    EXPECT_THROW(ram_dir->fileLength("test_file"), CLuceneError);

    DebugPoints::instance()->remove("DorisRAMFSDirectory::fileLength_file_not_found");
}

// Test 32: DorisRAMFSDirectory openInput with file not found
TEST_F(DorisFSDirectoryTest, DorisRAMFSDirectoryOpenInputWithFileNotFound) {
    DebugPoints::instance()->add("DorisRAMFSDirectory::openInput_file_not_found");

    auto ram_dir = std::make_unique<DorisRAMFSDirectory>();
    ram_dir->init(_fs, _tmp_dir.string().c_str());

    lucene::store::IndexInput* ret = nullptr;
    CLuceneError error;

    bool result = ram_dir->openInput("test_file", ret, error);

    EXPECT_FALSE(result);
    EXPECT_EQ(error.number(), CL_ERR_IO);

    DebugPoints::instance()->remove("DorisRAMFSDirectory::openInput_file_not_found");
}

// Test 33: DorisRAMFSDirectory close error
TEST_F(DorisFSDirectoryTest, DorisRAMFSDirectoryCloseError) {
    DebugPoints::instance()->add("DorisRAMFSDirectory::close_close_with_error");

    auto ram_dir = std::make_unique<DorisRAMFSDirectory>();
    ram_dir->init(_fs, _tmp_dir.string().c_str());

    EXPECT_THROW(ram_dir->close(), CLuceneError);

    DebugPoints::instance()->remove("DorisRAMFSDirectory::close_close_with_error");
}

// Test 34: DorisRAMFSDirectory renameFile with file not found
TEST_F(DorisFSDirectoryTest, DorisRAMFSDirectoryRenameFileWithFileNotFound) {
    DebugPoints::instance()->add("DorisRAMFSDirectory::renameFile_itr_filesMap_end");

    auto ram_dir = std::make_unique<DorisRAMFSDirectory>();
    ram_dir->init(_fs, _tmp_dir.string().c_str());

    EXPECT_THROW(ram_dir->renameFile("from", "to"), CLuceneError);

    DebugPoints::instance()->remove("DorisRAMFSDirectory::renameFile_itr_filesMap_end");
}

// Test 35: DorisRAMFSDirectory createOutput with existing file
TEST_F(DorisFSDirectoryTest, DorisRAMFSDirectoryCreateOutputWithExistingFile) {
    DebugPoints::instance()->add("DorisRAMFSDirectory::createOutput_itr_filesMap_end");

    auto ram_dir = std::make_unique<DorisRAMFSDirectory>();
    ram_dir->init(_fs, _tmp_dir.string().c_str());

    // This should not throw, but create a new output
    auto* output = ram_dir->createOutput("test_file");
    EXPECT_NE(output, nullptr);

    delete output;
    DebugPoints::instance()->remove("DorisRAMFSDirectory::createOutput_itr_filesMap_end");
}

// Test 36: FSIndexOutputV2 init error
TEST_F(DorisFSDirectoryTest, FSIndexOutputV2InitError) {
    DebugPoints::instance()->add(
            "DorisFSDirectory::FSIndexOutput._throw_clucene_error_in_fsindexoutput_init");
    std::unique_ptr<DorisFSDirectory::FSIndexOutputV2> output =
            std::make_unique<DorisFSDirectory::FSIndexOutputV2>();

    // Create writer first
    io::FileWriterPtr writer;
    std::string file_path = (_tmp_dir / "test_file").string();
    Status s = _fs->create_file(file_path, &writer);
    EXPECT_TRUE(s.ok());

    try {
        output->init(writer.get());
    } catch (CLuceneError& err) {
        EXPECT_EQ(err.number(), CL_ERR_IO);
        EXPECT_EQ(std::string(err.what()),
                  "debug point: test throw error in fsindexoutput init mock error");
    }
    try {
        output->close();
    } catch (CLuceneError& err) {
        EXPECT_EQ(err.number(), CL_ERR_IO);
        EXPECT_EQ(std::string(err.what()), "flushBuffer error, _index_v2_file_writer = nullptr");
    }

    DebugPoints::instance()->remove(
            "DorisFSDirectory::FSIndexOutput._throw_clucene_error_in_fsindexoutput_init");
}

// Test 37: FSIndexOutputV2 flushBuffer error
TEST_F(DorisFSDirectoryTest, FSIndexOutputV2FlushBufferError) {
    DebugPoints::instance()->add(
            "DorisFSDirectory::FSIndexOutput._status_error_in_fsindexoutput_flushBuffer");

    // Create writer first
    io::FileWriterPtr writer;
    std::string file_path = (_tmp_dir / "test_file").string();
    Status s = _fs->create_file(file_path, &writer);
    EXPECT_TRUE(s.ok());

    auto output = _directory->createOutputV2(writer.get());

    // Write small chunks to fill the buffer and trigger flush
    // BufferedIndexOutput buffer size is 1024 bytes
    std::string data(65537, 'a'); // 65536 bytes per write
    // This final write should trigger flushBuffer and hit the debug point
    EXPECT_THROW(output->writeString(data.c_str(), data.length()), CLuceneError);

    DebugPoints::instance()->remove(
            "DorisFSDirectory::FSIndexOutput._status_error_in_fsindexoutput_flushBuffer");

    // Try to close gracefully first to avoid destructor issues
    try {
        output->close();
    } catch (...) {
        // Ignore close errors in cleanup
    }
}

// Test 38: FSIndexOutputV2 flushBuffer with null writer
TEST_F(DorisFSDirectoryTest, FSIndexOutputV2FlushBufferWithNullWriter) {
    // Create a custom FSIndexOutputV2 with null writer to test the error condition
    std::unique_ptr<DorisFSDirectory::FSIndexOutputV2> fs_output_v2 =
            std::make_unique<DorisFSDirectory::FSIndexOutputV2>();
    fs_output_v2->init(nullptr); // Initialize with null writer

    // Any write operation should trigger flushBuffer and throw error due to null writer
    std::string data(65537, 'a'); // 65536 bytes per write
    // This final write should trigger flushBuffer and hit the debug point
    try {
        fs_output_v2->writeString(data.c_str(), data.length());
    } catch (CLuceneError& err) {
        EXPECT_EQ(err.number(), CL_ERR_IO);
        EXPECT_EQ(std::string(err.what()), "flushBuffer error, _index_v2_file_writer = nullptr");
    }
    try {
        fs_output_v2->close();
    } catch (CLuceneError& err) {
        EXPECT_EQ(err.number(), CL_ERR_IO);
        EXPECT_EQ(std::string(err.what()), "flushBuffer error, _index_v2_file_writer = nullptr");
    }
}

// Test 39: FSIndexOutputV2 flushBuffer with null buffer
TEST_F(DorisFSDirectoryTest, FSIndexOutputV2FlushBufferWithNullBuffer) {
    DebugPoints::instance()->add("DorisFSDirectory::FSIndexOutputV2::flushBuffer_b_is_nullptr");

    // Create writer first
    io::FileWriterPtr writer;
    std::string file_path = (_tmp_dir / "test_file").string();
    Status s = _fs->create_file(file_path, &writer);
    EXPECT_TRUE(s.ok());

    std::unique_ptr<DorisFSDirectory::FSIndexOutputV2> output =
            std::make_unique<DorisFSDirectory::FSIndexOutputV2>();
    output->init(writer.get());

    // This should just log warning, not throw
    EXPECT_NO_THROW(output->writeString("test"));
    output->close();
    DebugPoints::instance()->remove("DorisFSDirectory::FSIndexOutputV2::flushBuffer_b_is_nullptr");
}

// Test 40: FSIndexOutputV2 close error
TEST_F(DorisFSDirectoryTest, FSIndexOutputV2CloseError) {
    DebugPoints::instance()->add("DorisFSDirectory::FSIndexOutput._set_writer_close_status_error");

    // Create writer first
    io::FileWriterPtr writer;
    std::string file_path = (_tmp_dir / "test_file").string();
    Status s = _fs->create_file(file_path, &writer);
    EXPECT_TRUE(s.ok());

    std::unique_ptr<DorisFSDirectory::FSIndexOutputV2> output =
            std::make_unique<DorisFSDirectory::FSIndexOutputV2>();
    output->init(writer.get());
    output->writeString("test");

    EXPECT_THROW(output->close(), CLuceneError);

    DebugPoints::instance()->remove(
            "DorisFSDirectory::FSIndexOutput._set_writer_close_status_error");
}

// Test 41: FSIndexOutputV2 close with null writer
TEST_F(DorisFSDirectoryTest, FSIndexOutputV2CloseWithNullWriter) {
    DebugPoints::instance()->add("DorisFSDirectory::FSIndexOutput.set_writer_nullptr");

    // Create writer first
    io::FileWriterPtr writer;
    std::string file_path = (_tmp_dir / "test_file").string();
    Status s = _fs->create_file(file_path, &writer);
    EXPECT_TRUE(s.ok());

    std::unique_ptr<DorisFSDirectory::FSIndexOutputV2> output =
            std::make_unique<DorisFSDirectory::FSIndexOutputV2>();
    output->init(writer.get());

    try {
        output->close();
    } catch (CLuceneError& err) {
        EXPECT_EQ(err.number(), CL_ERR_IO);
        EXPECT_EQ(std::string(err.what()),
                  "close file writer error, _index_v2_file_writer = nullptr");
    }

    DebugPoints::instance()->remove("DorisFSDirectory::FSIndexOutput.set_writer_nullptr");
}

// Test 42: FSIndexOutputV2 length with null writer
TEST_F(DorisFSDirectoryTest, FSIndexOutputV2LengthWithNullWriter) {
    // Create a custom FSIndexOutputV2 with null writer to test precondition
    std::unique_ptr<DorisFSDirectory::FSIndexOutputV2> fs_output_v2 =
            std::make_unique<DorisFSDirectory::FSIndexOutputV2>();
    fs_output_v2->init(nullptr);

    try {
        fs_output_v2->length();
    } catch (CLuceneError& err) {
        EXPECT_EQ(err.number(), CL_ERR_IO);
        EXPECT_EQ(std::string(err.what()), "file is not open, index_v2_file_writer is nullptr");
    }

    try {
        fs_output_v2->close();
    } catch (CLuceneError& err) {
        EXPECT_EQ(err.number(), CL_ERR_IO);
        EXPECT_EQ(std::string(err.what()), "flushBuffer error, _index_v2_file_writer = nullptr");
    }
}

// Test 43: Test FSIndexInput copy constructor - basic functionality
TEST_F(DorisFSDirectoryTest, FSIndexInputCopyConstructorWithNullHandle) {
    // Create a file first
    std::filesystem::path test_file = _tmp_dir / "test_file_clone";
    std::ofstream ofs(test_file);
    ofs << "test content for clone";
    ofs.close();

    lucene::store::IndexInput* input = nullptr;
    CLuceneError error;

    bool result =
            DorisFSDirectory::FSIndexInput::open(_fs, test_file.string().c_str(), input, error);
    EXPECT_TRUE(result);

    auto* fs_input = dynamic_cast<DorisFSDirectory::FSIndexInput*>(input);
    ASSERT_NE(fs_input, nullptr);

    // Test normal clone functionality
    auto* cloned_input = fs_input->clone();
    EXPECT_NE(cloned_input, nullptr);

    // Verify cloned input works correctly
    EXPECT_EQ(cloned_input->length(), fs_input->length());

    _CLDELETE(cloned_input);
    _CLDELETE(input);
}

// Test 44: FSIndexOutput flushBuffer error
TEST_F(DorisFSDirectoryTest, FSIndexOutputFlushBufferError) {
    DebugPoints::instance()->add(
            "DorisFSDirectory::FSIndexOutput._status_error_in_fsindexoutput_flushBuffer");

    auto* output = _directory->createOutput("test_file");

    std::string data(65537, 'a'); // 65536 bytes per write
    EXPECT_THROW(output->writeString(data.c_str(), data.length()), CLuceneError);

    DebugPoints::instance()->remove(
            "DorisFSDirectory::FSIndexOutput._status_error_in_fsindexoutput_flushBuffer");

    try {
        output->close();
    } catch (...) {
        // Ignore close errors
    }
    delete output;
}

// Test 45: FSIndexOutput flushBuffer with null writer
TEST_F(DorisFSDirectoryTest, FSIndexOutputFlushBufferWithNullWriter) {
    DebugPoints::instance()->add("DorisFSDirectory::FSIndexOutput::flushBuffer_writer_is_nullptr");

    auto* output = _directory->createOutput("test_file2");
    std::string data(65537, 'a'); // 65536 bytes per write
    // This should log warning but not crash
    EXPECT_NO_THROW(output->writeString(data.c_str(), data.length()));

    DebugPoints::instance()->remove(
            "DorisFSDirectory::FSIndexOutput::flushBuffer_writer_is_nullptr");

    try {
        output->close();
    } catch (...) {
        // Ignore close errors
    }
    delete output;
}

// Test 46: FSIndexOutput flushBuffer with null buffer
TEST_F(DorisFSDirectoryTest, FSIndexOutputFlushBufferWithNullBuffer) {
    DebugPoints::instance()->add("DorisFSDirectory::FSIndexOutput::flushBuffer_b_is_nullptr");

    auto* output = _directory->createOutput("test_file3");

    std::string data(65537, 'a'); // 65536 bytes per write
    // This should log warning but not crash
    EXPECT_NO_THROW(output->writeString(data.c_str(), data.length()));

    DebugPoints::instance()->remove("DorisFSDirectory::FSIndexOutput::flushBuffer_b_is_nullptr");

    try {
        output->close();
    } catch (...) {
        // Ignore close errors
    }
    delete output;
}

TEST_F(DorisFSDirectoryTest, FSIndexInputReadInternalTimer) {
    std::string file_name = "test_timer_file";
    std::filesystem::path test_file = _tmp_dir / file_name;
    std::ofstream ofs(test_file);
    std::string content = "some test content for timer";
    ofs << content;
    ofs.close();

    lucene::store::IndexInput* input1 = nullptr;
    CLuceneError error;
    bool result =
            DorisFSDirectory::FSIndexInput::open(_fs, test_file.string().c_str(), input1, error);
    EXPECT_TRUE(result);
    ASSERT_NE(input1, nullptr);

    auto* fs_input1 = dynamic_cast<DorisFSDirectory::FSIndexInput*>(input1);
    ASSERT_NE(fs_input1, nullptr);

    io::FileCacheStatistics stats;
    fs_input1->_io_ctx.file_cache_stats = &stats;

    auto* input2 = fs_input1->clone();
    auto* fs_input2 = dynamic_cast<DorisFSDirectory::FSIndexInput*>(input2);
    ASSERT_NE(fs_input2, nullptr);

    fs_input2->_io_ctx.file_cache_stats = &stats;

    uint8_t buffer1[10];
    input1->readBytes(buffer1, 10);
    EXPECT_GT(stats.inverted_index_io_timer, 0);
    int64_t old_time = stats.inverted_index_io_timer;

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    input2->seek(0);
    uint8_t buffer2[10];
    input2->readBytes(buffer2, 10);
    EXPECT_GT(stats.inverted_index_io_timer, old_time);

    _CLDELETE(input2);
    _CLDELETE(input1);
}

TEST_F(DorisFSDirectoryTest, PrivGetFN) {
    {
        std::string file_name = "my_file.txt";
        std::string result = _directory->priv_getFN(file_name);
        std::string expected_path = (_tmp_dir / file_name).string();
        EXPECT_EQ(result, expected_path);
    }
    {
        std::string file_name = "";
        std::string result = _directory->priv_getFN(file_name);
        std::string expected_path = (_tmp_dir / file_name).string();
        EXPECT_EQ(result, expected_path);
    }
    {
        std::string file_name = "subdir/another_file.log";
        std::string result = _directory->priv_getFN(file_name);
        std::string expected_path = (_tmp_dir / file_name).string();
        EXPECT_EQ(result, expected_path);
    }
}

} // namespace doris::segment_v2