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

#include "olap/rowset/segment_v2/inverted_index_compound_reader.h"

#include <CLucene.h>
#include <CLucene/config/repl_wchar.h>
#include <CLucene/index/IndexReader.h>
#include <CLucene/store/RAMDirectory.h>
#include <CLucene/util/Misc.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <cstring>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest_pred_impl.h"
#include "io/fs/local_file_system.h"
#include "olap/rowset/segment_v2/index_file_reader.h"
#include "olap/rowset/segment_v2/index_file_writer.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"
#include "olap/tablet_schema.h"
#include "olap/tablet_schema_helper.h"
#include "runtime/exec_env.h"
#include "util/slice.h"

using namespace lucene::index;
using doris::segment_v2::IndexFileWriter;

namespace doris::segment_v2 {

class DorisCompoundReaderTest : public testing::Test {
public:
    const std::string kTestDir = "./ut_dir/inverted_index_compound_reader_test";

    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        std::vector<StorePath> paths;
        paths.emplace_back(kTestDir, 1024);
        auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
        st = tmp_file_dirs->init();
        if (!st.ok()) {
            return;
        }
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));
    }

    void TearDown() override {}

    CL_NS(store)::IndexInput* create_mock_index_input(const std::string& file_path,
                                                      const std::vector<std::string>& file_names,
                                                      const std::vector<int64_t>& lengths) {
        std::filesystem::path parent_path = std::filesystem::path(file_path).parent_path();
        if (!std::filesystem::exists(parent_path)) {
            std::filesystem::create_directories(parent_path);
        }

        DorisFSDirectory* dir_ptr = DorisFSDirectoryFactory::getDirectory(
                io::global_local_filesystem(), parent_path.string().c_str());
        if (dir_ptr == nullptr) {
            EXPECT_TRUE(false);
            return nullptr;
        }
        std::unique_ptr<lucene::store::Directory, DirectoryDeleter> directory(dir_ptr);

        lucene::store::IndexOutput* output = directory->createOutput(
                std::filesystem::path(file_path).filename().string().c_str());
        if (output == nullptr) {
            EXPECT_TRUE(false);
            return nullptr;
        }
        std::unique_ptr<lucene::store::IndexOutput> output_guard(output);

        std::vector<int> header_files;
        int64_t header_data_size = 0;
        const int64_t MAX_HEADER_DATA_SIZE = 16 * 1024 * 1024;

        output->writeVInt(file_names.size());

        int64_t header_length = output->getFilePointer();
        for (size_t i = 0; i < file_names.size(); ++i) {
            int64_t entry_size = 0;
            entry_size += 2 + file_names[i].length();
            entry_size += 8;
            entry_size += 8;

            if (header_data_size + lengths[i] <= MAX_HEADER_DATA_SIZE) {
                header_files.push_back(i);
                header_data_size += lengths[i];
                header_length += entry_size + lengths[i];
            } else {
                header_length += entry_size;
            }
        }

        int64_t data_offset = header_length;

        for (size_t i = 0; i < file_names.size(); ++i) {
            output->writeString(file_names[i]);

            if (std::find(header_files.begin(), header_files.end(), i) != header_files.end()) {
                output->writeLong(-1);
            } else {
                output->writeLong(data_offset);
                data_offset += lengths[i];
            }

            output->writeLong(lengths[i]);

            if (std::find(header_files.begin(), header_files.end(), i) != header_files.end()) {
                std::unique_ptr<uint8_t[]> data(new uint8_t[lengths[i]]);
                for (int64_t j = 0; j < lengths[i]; ++j) {
                    data[j] = static_cast<uint8_t>('A');
                }
                output->writeBytes(data.get(), lengths[i]);
            }
        }

        for (size_t i = 0; i < file_names.size(); ++i) {
            if (std::find(header_files.begin(), header_files.end(), i) == header_files.end()) {
                std::unique_ptr<uint8_t[]> data(new uint8_t[lengths[i]]);
                for (int64_t j = 0; j < lengths[i]; ++j) {
                    data[j] = static_cast<uint8_t>('Z' - i % 26);
                }
                output->writeBytes(data.get(), lengths[i]);
            }
        }

        output_guard->close();
        directory->close();

        CL_NS(store)::IndexInput* index_input = nullptr;
        CLuceneError err;
        auto ok = DorisFSDirectory::FSIndexInput::open(io::global_local_filesystem(),
                                                       file_path.c_str(), index_input, err, 4096);
        EXPECT_TRUE(ok) << err.what();
        return index_input;
    }

    void verify_file_exists(DorisCompoundReader* reader, const std::string& file_name) {
        EXPECT_TRUE(reader->fileExists(file_name.c_str()))
                << "File " << file_name << " should exist in the compound reader";
    }

    void verify_file_length(DorisCompoundReader* reader, const std::string& file_name,
                            int64_t expected_length) {
        EXPECT_EQ(reader->fileLength(file_name.c_str()), expected_length)
                << "File " << file_name << " should have length " << expected_length;
    }

    void verify_file_list(DorisCompoundReader* reader,
                          const std::vector<std::string>& expected_files) {
        std::vector<std::string> actual_files;
        reader->list(&actual_files);

        EXPECT_EQ(actual_files.size(), expected_files.size())
                << "Number of files in the compound reader does not match";

        for (const auto& file : expected_files) {
            EXPECT_TRUE(std::find(actual_files.begin(), actual_files.end(), file) !=
                        actual_files.end())
                    << "File " << file << " should be in the file list";
        }
    }

    void verify_file_can_be_opened(DorisCompoundReader* reader, const std::string& file_name,
                                   int64_t expected_length) {
        CLuceneError err;
        lucene::store::IndexInput* input = nullptr;
        EXPECT_TRUE(reader->openInput(file_name.c_str(), input, err, 4096))
                << "Failed to open file " << file_name << ": " << err.what();

        EXPECT_EQ(input->length(), expected_length)
                << "Opened file " << file_name << " should have length " << expected_length;

        input->close();
        _CLLDELETE(input);
    }

    EntriesType create_test_entries(const std::vector<std::string>& file_names,
                                    const std::vector<int64_t>& offsets,
                                    const std::vector<int64_t>& lengths) {
        EntriesType entries;

        for (size_t i = 0; i < file_names.size(); ++i) {
            std::unique_ptr<ReaderFileEntry> entry = std::make_unique<ReaderFileEntry>();
            entry->file_name = file_names[i];
            entry->offset = offsets[i];
            entry->length = lengths[i];
            entries.emplace(file_names[i], std::move(entry));
        }

        return entries;
    }

    std::string local_segment_path(std::string base, std::string rowset_id, int64_t seg_id) {
        return base + "/" + rowset_id + "/" + std::to_string(seg_id) + ".dat";
    }
};

TEST_F(DorisCompoundReaderTest, BasicConstruction) {
    std::string index_path = kTestDir + "/test_compound_file.idx";

    std::vector<std::string> file_names = {"file1.dat", "file2.dat", "file3.dat"};
    std::vector<int64_t> lengths = {50, 60, 70};

    CL_NS(store)::IndexInput* index_input =
            create_mock_index_input(index_path, file_names, lengths);

    DorisCompoundReader reader(index_input, 4096, nullptr);

    for (const auto& file : file_names) {
        verify_file_exists(&reader, file);
    }

    for (size_t i = 0; i < file_names.size(); ++i) {
        verify_file_length(&reader, file_names[i], lengths[i]);
    }

    verify_file_list(&reader, file_names);

    for (size_t i = 0; i < file_names.size(); ++i) {
        verify_file_can_be_opened(&reader, file_names[i], lengths[i]);
    }

    reader.close();
}

TEST_F(DorisCompoundReaderTest, ConstructFromStream) {
    std::string index_path = kTestDir + "/test_compound_file2.idx";

    std::vector<std::string> file_names = {"file1.dat", "file2.dat", "file3.dat"};
    std::vector<int64_t> lengths = {50, 60, 20};

    CL_NS(store)::IndexInput* index_input =
            create_mock_index_input(index_path, file_names, lengths);

    DorisCompoundReader reader(index_input, 4096, nullptr);

    for (const auto& file : file_names) {
        verify_file_exists(&reader, file);
    }

    for (size_t i = 0; i < file_names.size(); ++i) {
        verify_file_length(&reader, file_names[i], lengths[i]);
    }

    verify_file_list(&reader, file_names);

    for (size_t i = 0; i < file_names.size(); ++i) {
        verify_file_can_be_opened(&reader, file_names[i], lengths[i]);
    }

    reader.close();
}

TEST_F(DorisCompoundReaderTest, CloneCompoundReader) {
    std::string index_path = kTestDir + "/test_compound_file3.idx";

    std::vector<std::string> file_names = {"file1.dat", "file2.dat"};
    std::vector<int64_t> offsets = {100, 200};
    std::vector<int64_t> lengths = {50, 60};

    CL_NS(store)::IndexInput* index_input =
            create_mock_index_input(index_path, file_names, lengths);

    EntriesType entries = create_test_entries(file_names, offsets, lengths);

    DorisCompoundReader original_reader(index_input, entries);

    CL_NS(store)::IndexInput* cloned_input = original_reader.getDorisIndexInput()->clone();

    EntriesType cloned_entries;
    for (const auto& it : entries) {
        auto* origin_entry = it.second.get();
        std::unique_ptr<ReaderFileEntry> entry = std::make_unique<ReaderFileEntry>();
        entry->file_name = origin_entry->file_name;
        entry->offset = origin_entry->offset;
        entry->length = origin_entry->length;
        cloned_entries.emplace(it.first, std::move(entry));
    }

    DorisCompoundReader cloned_reader(cloned_input, cloned_entries);

    verify_file_list(&cloned_reader, file_names);

    for (size_t i = 0; i < file_names.size(); ++i) {
        verify_file_exists(&cloned_reader, file_names[i]);
        verify_file_length(&cloned_reader, file_names[i], lengths[i]);
        verify_file_can_be_opened(&cloned_reader, file_names[i], lengths[i]);
    }

    original_reader.close();
    cloned_reader.close();
}

TEST_F(DorisCompoundReaderTest, ErrorHandling) {
    std::string index_path = kTestDir + "/test_compound_file5.idx";

    std::vector<std::string> file_names = {"file1.dat"};
    std::vector<int64_t> lengths = {50};

    CL_NS(store)::IndexInput* index_input =
            create_mock_index_input(index_path, file_names, lengths);

    DorisCompoundReader reader(index_input);

    CLuceneError err;
    lucene::store::IndexInput* input = nullptr;
    EXPECT_FALSE(reader.openInput("non_existent_file.dat", input, err))
            << "Opening a non-existent file should fail";

    reader.close();
}

TEST_F(DorisCompoundReaderTest, IntegrationWithFileWriter) {
    std::string rowset_id = "test_rowset";
    int seg_id = 0;

    std::string index_path_prefix = std::string(InvertedIndexDescriptor::get_index_file_path_prefix(
            local_segment_path(kTestDir, rowset_id, seg_id)));

    int index_id = 12345;
    auto index_meta_pb = std::make_unique<TabletIndexPB>();
    index_meta_pb->set_index_type(IndexType::INVERTED);
    index_meta_pb->set_index_id(index_id);
    index_meta_pb->set_index_name("test_index");
    index_meta_pb->clear_col_unique_id();
    index_meta_pb->add_col_unique_id(0);

    TabletIndex idx_meta;
    idx_meta.init_from_pb(*index_meta_pb.get());

    std::vector<std::string> test_terms = {"term1", "term2", "term3"};

    {
        std::string index_path =
                InvertedIndexDescriptor::get_index_file_path_v1(index_path_prefix, index_id, "");

        auto index_file_writer = std::make_unique<IndexFileWriter>(
                io::global_local_filesystem(), index_path_prefix, rowset_id, seg_id,
                InvertedIndexStorageFormatPB::V1);

        auto result = index_file_writer->open(&idx_meta);
        ASSERT_TRUE(result.has_value()) << "Failed to open writer directory";

        auto dir = result.value();
        lucene::store::IndexOutput* out = dir->createOutput("test_file.dat");
        for (const auto& term : test_terms) {
            out->writeString(term);
        }
        out->close();
        _CLLDELETE(out);

        auto st = index_file_writer->close();
        ASSERT_TRUE(st.ok()) << st;

        auto file_reader = std::make_unique<IndexFileReader>(
                io::global_local_filesystem(), index_path_prefix, InvertedIndexStorageFormatPB::V1);

        st = file_reader->init();
        ASSERT_TRUE(st.ok()) << st;

        auto reader_result = file_reader->open(&idx_meta);
        ASSERT_TRUE(reader_result.has_value())
                << "Failed to open compound reader: " << reader_result.error();
        auto compound_reader = std::move(reader_result.value());

        std::vector<std::string> files;
        compound_reader->list(&files);
        ASSERT_FALSE(files.empty()) << "Compound reader should contain files";

        for (const auto& file : files) {
            verify_file_exists(compound_reader.get(), file);
        }

        compound_reader->close();
    }

    {
        io::FileWriterPtr file_writer;
        std::string index_path = InvertedIndexDescriptor::get_index_file_path_v2(index_path_prefix);
        io::FileWriterOptions opts;
        Status st = io::global_local_filesystem()->create_file(index_path, &file_writer, &opts);
        ASSERT_TRUE(st.ok()) << st;

        auto index_file_writer = std::make_unique<IndexFileWriter>(
                io::global_local_filesystem(), index_path_prefix, rowset_id, seg_id,
                InvertedIndexStorageFormatPB::V2, std::move(file_writer));

        auto result = index_file_writer->open(&idx_meta);
        ASSERT_TRUE(result.has_value()) << "Failed to open writer directory";

        auto dir = result.value();
        lucene::store::IndexOutput* out = dir->createOutput("test_file.dat");
        for (const auto& term : test_terms) {
            out->writeString(term);
        }
        out->close();
        _CLLDELETE(out);

        st = index_file_writer->close();
        ASSERT_TRUE(st.ok()) << st;

        auto file_reader = std::make_unique<IndexFileReader>(
                io::global_local_filesystem(), index_path_prefix, InvertedIndexStorageFormatPB::V2);

        st = file_reader->init();
        ASSERT_TRUE(st.ok()) << st;

        auto reader_result = file_reader->open(&idx_meta);
        ASSERT_TRUE(reader_result.has_value())
                << "Failed to open compound reader: " << reader_result.error();
        auto compound_reader = std::move(reader_result.value());

        std::vector<std::string> files;
        compound_reader->list(&files);
        ASSERT_FALSE(files.empty()) << "Compound reader should contain files";

        for (const auto& file : files) {
            verify_file_exists(compound_reader.get(), file);
        }

        compound_reader->close();
    }
}

TEST_F(DorisCompoundReaderTest, FileCopyCorrectness) {
    std::string index_path = kTestDir + "/test_file_copy.idx";

    std::string file_name = "test_copy.dat";
    std::vector<std::string> file_names = {file_name};
    std::vector<int64_t> lengths = {256};

    EntriesType entries;
    std::unique_ptr<ReaderFileEntry> entry = std::make_unique<ReaderFileEntry>();
    entry->file_name = file_name;
    entry->offset = -1;
    entry->length = lengths[0];
    entries.emplace(file_name, std::move(entry));

    CL_NS(store)::IndexInput* index_input =
            create_mock_index_input(index_path, file_names, lengths);

    class TestCompoundReader : public DorisCompoundReader {
    public:
        TestCompoundReader(CL_NS(store)::IndexInput* stream, const EntriesType& entries)
                : DorisCompoundReader(stream, entries) {}

        using DorisCompoundReader::_copyFile;

        bool testFileCopy(const char* file, int32_t file_length) {
            _ram_dir = std::make_unique<lucene::store::RAMDirectory>();
            const int32_t buffer_size = 64;
            uint8_t buffer[4096];

            try {
                processFileEntries(buffer, buffer_size);
                return verifyFileCopy(file, file_length);
            } catch (CLuceneError& error) {
                ADD_FAILURE() << "Exception during file copy test: " << error.what();
                return false;
            }
        }

    private:
        void processFileEntries(uint8_t* buffer, int32_t buffer_size) {
            int32_t count = _stream->readVInt();
            for (int32_t i = 0; i < count; i++) {
                std::unique_ptr<ReaderFileEntry> entry = std::make_unique<ReaderFileEntry>();
                std::wstring tid;
                int32_t string_length = _stream->readVInt();
                tid.resize(string_length);
                // Read the string characters directly
                _stream->readChars(tid.data(), 0, string_length);
                entry->file_name = std::string(tid.begin(), tid.end());
                entry->offset = _stream->readLong();
                entry->length = _stream->readLong();
                if (entry->offset < 0) {
                    _copyFile(entry->file_name.c_str(), static_cast<int32_t>(entry->length), buffer,
                              buffer_size);
                }
            }
        }

        bool verifyFileCopy(const char* file, int32_t file_length) {
            EXPECT_TRUE(_ram_dir->fileExists(file)) << "File should exist after copy";

            lucene::store::IndexInput* input = nullptr;
            CLuceneError err;
            bool opened = _ram_dir->openInput(file, input, err);
            EXPECT_TRUE(opened) << "Failed to open copied file: " << err.what();

            verifyFileContent(input, file_length);
            verifyDirectoryState(file);
            testEmptyFile();

            return true;
        }

        void verifyFileContent(lucene::store::IndexInput* input, int32_t file_length) {
            std::unique_ptr<lucene::store::IndexInput,
                            std::function<void(lucene::store::IndexInput*)>>
                    input_guard(input, [](lucene::store::IndexInput* p) {
                        if (p) {
                            p->close();
                            _CLLDELETE(p);
                        }
                    });

            EXPECT_EQ(input_guard->length(), file_length) << "File length should match";
            input_guard->seek(0);
            std::vector<uint8_t> full_buffer(file_length);
            input_guard->readBytes(full_buffer.data(), file_length);
            for (int32_t i = 0; i < file_length; ++i) {
                EXPECT_EQ(full_buffer[i], 65) << "File content should match";
            }
        }

        void verifyDirectoryState(const char* file) {
            std::vector<std::string> dir_files;
            _ram_dir->list(&dir_files);
            EXPECT_EQ(dir_files.size(), 1U) << "RAM directory should contain exactly one file";
            EXPECT_EQ(dir_files[0], file) << "File name in RAM directory should match";

            CLuceneError err;
            bool deleted = _ram_dir->deleteFile(file);
            EXPECT_TRUE(deleted) << "Failed to delete file: " << err.what();
            EXPECT_FALSE(_ram_dir->fileExists(file)) << "File should not exist after deletion";
        }

        void testEmptyFile() {
            const char* empty_file = "empty_test.dat";
            const int32_t buffer_size = 64;
            uint8_t buffer[4096];
            _copyFile(empty_file, 0, buffer, buffer_size);
            EXPECT_TRUE(_ram_dir->fileExists(empty_file)) << "Empty file should exist after copy";

            lucene::store::IndexInput* empty_input = nullptr;
            CLuceneError err;
            bool empty_opened = _ram_dir->openInput(empty_file, empty_input, err);
            EXPECT_TRUE(empty_opened) << "Failed to open empty file: " << err.what();

            std::unique_ptr<lucene::store::IndexInput,
                            std::function<void(lucene::store::IndexInput*)>>
                    empty_guard(empty_input, [](lucene::store::IndexInput* p) {
                        if (p) {
                            p->close();
                            _CLLDELETE(p);
                        }
                    });

            EXPECT_EQ(empty_guard->length(), 0) << "Empty file length should be 0";
        }
    };

    TestCompoundReader test_reader(index_input, entries);

    EXPECT_TRUE(test_reader.testFileCopy(file_name.c_str(), static_cast<int32_t>(lengths[0])));

    test_reader.close();
}

TEST_F(DorisCompoundReaderTest, CSIndexInputClone) {
    std::string index_path = kTestDir + "/test_csinput_clone.idx";
    std::vector<std::string> file_names = {"clone_test.dat"};
    std::vector<int64_t> lengths = {17 * 1024 * 1024};

    CL_NS(store)::IndexInput* index_input =
            create_mock_index_input(index_path, file_names, lengths);

    DorisCompoundReader reader(index_input, 4096, nullptr);

    CLuceneError err;
    lucene::store::IndexInput* input = nullptr;
    EXPECT_TRUE(reader.openInput("clone_test.dat", input, err, 4096));

    // Test CSIndexInput clone functionality
    lucene::store::IndexInput* cloned_input = input->clone();
    EXPECT_NE(cloned_input, nullptr);
    EXPECT_EQ(cloned_input->length(), input->length());

    // Test CSIndexInput methods
    EXPECT_STREQ(cloned_input->getDirectoryType(), "DorisCompoundReader");
    EXPECT_STREQ(cloned_input->getObjectName(), "CSIndexInput");

    // Test setIoContext
    io::IOContext io_ctx;
    cloned_input->setIoContext(&io_ctx);

    input->close();
    cloned_input->close();
    _CLLDELETE(input);
    _CLLDELETE(cloned_input);

    reader.close();
}

TEST_F(DorisCompoundReaderTest, CSIndexInputReadPastEOF) {
    std::string index_path = kTestDir + "/test_read_past_eof.idx";
    std::vector<std::string> file_names = {"small_file.dat"};
    std::vector<int64_t> lengths = {10};

    CL_NS(store)::IndexInput* index_input =
            create_mock_index_input(index_path, file_names, lengths);

    DorisCompoundReader reader(index_input, 4096, nullptr);

    CLuceneError err;
    lucene::store::IndexInput* input = nullptr;
    EXPECT_TRUE(reader.openInput("small_file.dat", input, err, 4096));

    // Try to read past EOF - should throw exception
    uint8_t buffer[20];
    bool exception_thrown = false;
    try {
        input->readBytes(buffer, 15); // Try to read more than file length (10)
    } catch (CLuceneError& e) {
        exception_thrown = true;
        EXPECT_EQ(e.number(), CL_ERR_IO);
    }
    EXPECT_TRUE(exception_thrown);

    input->close();
    _CLLDELETE(input);
    reader.close();
}

TEST_F(DorisCompoundReaderTest, UnsupportedOperations) {
    std::string index_path = kTestDir + "/test_unsupported.idx";
    std::vector<std::string> file_names = {"test.dat"};
    std::vector<int64_t> lengths = {50};

    CL_NS(store)::IndexInput* index_input =
            create_mock_index_input(index_path, file_names, lengths);

    DorisCompoundReader reader(index_input, 4096, nullptr);

    // Test unsupported operations - all should throw exceptions
    bool exception_thrown = false;

    // Test doDeleteFile
    try {
        reader.doDeleteFile("test.dat");
    } catch (CLuceneError& e) {
        exception_thrown = true;
        EXPECT_EQ(e.number(), CL_ERR_UnsupportedOperation);
    }
    EXPECT_TRUE(exception_thrown);

    // Test renameFile
    exception_thrown = false;
    try {
        reader.renameFile("old.dat", "new.dat");
    } catch (CLuceneError& e) {
        exception_thrown = true;
        EXPECT_EQ(e.number(), CL_ERR_UnsupportedOperation);
    }
    EXPECT_TRUE(exception_thrown);

    // Test touchFile
    exception_thrown = false;
    try {
        reader.touchFile("test.dat");
    } catch (CLuceneError& e) {
        exception_thrown = true;
        EXPECT_EQ(e.number(), CL_ERR_UnsupportedOperation);
    }
    EXPECT_TRUE(exception_thrown);

    // Test createOutput
    exception_thrown = false;
    try {
        reader.createOutput("output.dat");
    } catch (CLuceneError& e) {
        exception_thrown = true;
        EXPECT_EQ(e.number(), CL_ERR_UnsupportedOperation);
    }
    EXPECT_TRUE(exception_thrown);

    reader.close();
}

TEST_F(DorisCompoundReaderTest, FileModifiedAndToString) {
    std::string index_path = kTestDir + "/test_misc_methods.idx";
    std::vector<std::string> file_names = {"test.dat"};
    std::vector<int64_t> lengths = {50};

    CL_NS(store)::IndexInput* index_input =
            create_mock_index_input(index_path, file_names, lengths);

    DorisCompoundReader reader(index_input, 4096, nullptr);

    // Test fileModified - should return 0
    EXPECT_EQ(reader.fileModified("test.dat"), 0);

    // Test toString
    std::string str_rep = reader.toString();
    EXPECT_TRUE(str_rep.find("DorisCompoundReader") != std::string::npos);

    reader.close();
}

TEST_F(DorisCompoundReaderTest, OpenInputWithNullStream) {
    std::string index_path = kTestDir + "/test_null_stream.idx";
    std::vector<std::string> file_names = {"test.dat"};
    std::vector<int64_t> lengths = {50};

    CL_NS(store)::IndexInput* index_input =
            create_mock_index_input(index_path, file_names, lengths);

    DorisCompoundReader reader(index_input, 4096, nullptr);

    // Close the reader first to simulate null stream
    reader.close();

    // Now try to open input - should fail
    CLuceneError err;
    lucene::store::IndexInput* input = nullptr;
    EXPECT_FALSE(reader.openInput("test.dat", input, err));
    EXPECT_EQ(err.number(), CL_ERR_IO);
}

TEST_F(DorisCompoundReaderTest, ClosedReaderOperations) {
    std::string index_path = kTestDir + "/test_closed_reader.idx";
    std::vector<std::string> file_names = {"test.dat"};
    std::vector<int64_t> lengths = {50};

    CL_NS(store)::IndexInput* index_input =
            create_mock_index_input(index_path, file_names, lengths);

    DorisCompoundReader reader(index_input, 4096, nullptr);
    reader.close();

    // Test operations on closed reader - all should throw exceptions
    bool exception_thrown = false;

    // Test list
    try {
        std::vector<std::string> files;
        reader.list(&files);
    } catch (CLuceneError& e) {
        exception_thrown = true;
        EXPECT_EQ(e.number(), CL_ERR_IO);
    }
    EXPECT_TRUE(exception_thrown);

    // Test fileExists
    exception_thrown = false;
    try {
        reader.fileExists("test.dat");
    } catch (CLuceneError& e) {
        exception_thrown = true;
        EXPECT_EQ(e.number(), CL_ERR_IO);
    }
    EXPECT_TRUE(exception_thrown);

    // Test fileLength
    exception_thrown = false;
    try {
        reader.fileLength("test.dat");
    } catch (CLuceneError& e) {
        exception_thrown = true;
        EXPECT_EQ(e.number(), CL_ERR_IO);
    }
    EXPECT_TRUE(exception_thrown);
}

TEST_F(DorisCompoundReaderTest, FileCopyErrorHandling) {
    std::string index_path = kTestDir + "/test_file_copy_error.idx";

    // Create a compound file with header data that will test _copyFile error paths
    std::vector<std::string> file_names = {"large_header.dat"};
    std::vector<int64_t> lengths = {1000}; // Large enough to be in header

    CL_NS(store)::IndexInput* index_input =
            create_mock_index_input(index_path, file_names, lengths);

    // Test normal _copyFile operation
    DorisCompoundReader reader(index_input, 4096, nullptr);

    // Verify the file was copied to RAM directory correctly
    CLuceneError err;
    lucene::store::IndexInput* input = nullptr;
    EXPECT_TRUE(reader.openInput("large_header.dat", input, err));
    EXPECT_EQ(input->length(), 1000);

    input->close();
    _CLLDELETE(input);
    reader.close();
}

TEST_F(DorisCompoundReaderTest, OpenInputBufferSizeHandling) {
    std::string index_path = kTestDir + "/test_buffer_size.idx";
    std::vector<std::string> file_names = {"test.dat"};
    std::vector<int64_t> lengths = {50};

    CL_NS(store)::IndexInput* index_input =
            create_mock_index_input(index_path, file_names, lengths);

    DorisCompoundReader reader(index_input, 4096, nullptr);

    // Test openInput with buffer size < 1 (should use default)
    CLuceneError err;
    lucene::store::IndexInput* input = nullptr;
    EXPECT_TRUE(reader.openInput("test.dat", input, err, 0)); // buffer size 0
    EXPECT_NE(input, nullptr);

    input->close();
    _CLLDELETE(input);

    // Test openInput with custom buffer size
    EXPECT_TRUE(reader.openInput("test.dat", input, err, 2048));
    EXPECT_NE(input, nullptr);

    input->close();
    _CLLDELETE(input);
    reader.close();
}

TEST_F(DorisCompoundReaderTest, GetDorisIndexInput) {
    std::string index_path = kTestDir + "/test_get_doris_input.idx";
    std::vector<std::string> file_names = {"test.dat"};
    std::vector<int64_t> lengths = {50};

    CL_NS(store)::IndexInput* index_input =
            create_mock_index_input(index_path, file_names, lengths);

    DorisCompoundReader reader(index_input, 4096, nullptr);

    // Test getDorisIndexInput
    CL_NS(store)::IndexInput* retrieved_input = reader.getDorisIndexInput();
    EXPECT_NE(retrieved_input, nullptr);
    EXPECT_EQ(retrieved_input, index_input);

    reader.close();
}

TEST_F(DorisCompoundReaderTest, UniquePointerOpenInput) {
    std::string index_path = kTestDir + "/test_unique_ptr.idx";
    std::vector<std::string> file_names = {"test.dat"};
    std::vector<int64_t> lengths = {50};

    CL_NS(store)::IndexInput* index_input =
            create_mock_index_input(index_path, file_names, lengths);

    DorisCompoundReader reader(index_input, 4096, nullptr);

    // Test openInput with unique_ptr
    std::unique_ptr<lucene::store::IndexInput> input_ptr;
    CLuceneError err;
    EXPECT_TRUE(reader.openInput("test.dat", input_ptr, err, 4096));
    EXPECT_NE(input_ptr, nullptr);

    // Test with non-existent file
    std::unique_ptr<lucene::store::IndexInput> failed_input_ptr;
    EXPECT_FALSE(reader.openInput("non_existent.dat", failed_input_ptr, err, 4096));
    EXPECT_EQ(failed_input_ptr, nullptr);

    input_ptr->close();
    reader.close();
}

TEST_F(DorisCompoundReaderTest, DestructorWithErrorHandling) {
    std::string index_path = kTestDir + "/test_destructor_error.idx";
    std::vector<std::string> file_names = {"test.dat"};
    std::vector<int64_t> lengths = {50};

    CL_NS(store)::IndexInput* index_input =
            create_mock_index_input(index_path, file_names, lengths);

    {
        DorisCompoundReader reader(index_input, 4096, nullptr);
        // Reader will be destroyed when going out of scope
        // This tests the destructor path with error handling
    }
    // No explicit assertions needed - just ensuring no crashes
}

TEST_F(DorisCompoundReaderTest, FileNotFoundInFileLength) {
    std::string index_path = kTestDir + "/test_file_not_found.idx";
    std::vector<std::string> file_names = {"existing_file.dat"};
    std::vector<int64_t> lengths = {50};

    CL_NS(store)::IndexInput* index_input =
            create_mock_index_input(index_path, file_names, lengths);

    DorisCompoundReader reader(index_input, 4096, nullptr);

    // Test fileLength with non-existent file - should throw exception
    bool exception_thrown = false;
    try {
        reader.fileLength("non_existent_file.dat");
    } catch (CLuceneError& e) {
        exception_thrown = true;
        EXPECT_EQ(e.number(), CL_ERR_IO);
        std::string error_msg = e.what();
        EXPECT_TRUE(error_msg.find("does not exist") != std::string::npos);
    }
    EXPECT_TRUE(exception_thrown);

    reader.close();
}

TEST_F(DorisCompoundReaderTest, ConstructorExceptionHandling) {
    std::string index_path = kTestDir + "/test_constructor_exception.idx";

    // Test construction that should trigger debug point if available
    std::vector<std::string> file_names = {"debug_test.dat"};
    std::vector<int64_t> lengths = {100};

    CL_NS(store)::IndexInput* index_input =
            create_mock_index_input(index_path, file_names, lengths);

    // Normal construction should work
    bool construction_successful = true;
    try {
        DorisCompoundReader reader(index_input, 4096, nullptr);
        reader.close();
    } catch (CLuceneError&) {
        construction_successful = false;
    }

    // In normal cases without debug points, construction should succeed
    EXPECT_TRUE(construction_successful);
}

TEST_F(DorisCompoundReaderTest, CopyFileWithZeroRemainder) {
    std::string index_path = kTestDir + "/test_copy_file_zero_remainder.idx";

    // Create files that will test exact division scenarios in _copyFile
    std::vector<std::string> file_names = {"exact_size.dat"};
    std::vector<int64_t> lengths = {16384}; // Buffer size from compound reader source

    CL_NS(store)::IndexInput* index_input =
            create_mock_index_input(index_path, file_names, lengths);

    DorisCompoundReader reader(index_input, 4096, nullptr);

    // Verify the file was copied correctly with zero remainder
    CLuceneError err;
    lucene::store::IndexInput* input = nullptr;
    EXPECT_TRUE(reader.openInput("exact_size.dat", input, err));
    EXPECT_EQ(input->length(), 16384);

    input->close();
    _CLLDELETE(input);
    reader.close();
}

TEST_F(DorisCompoundReaderTest, OpenInputFromRAMDirectory) {
    std::string index_path = kTestDir + "/test_ram_directory.idx";

    // Create a small file that will be stored in RAM directory
    std::vector<std::string> file_names = {"ram_file.dat"};
    std::vector<int64_t> lengths = {50}; // Small enough to be in header/RAM

    CL_NS(store)::IndexInput* index_input =
            create_mock_index_input(index_path, file_names, lengths);

    DorisCompoundReader reader(index_input, 4096, nullptr);

    // The file should be accessible from RAM directory
    CLuceneError err;
    lucene::store::IndexInput* input = nullptr;
    EXPECT_TRUE(reader.openInput("ram_file.dat", input, err, 4096));
    EXPECT_NE(input, nullptr);
    EXPECT_EQ(input->length(), 50);

    input->close();
    _CLLDELETE(input);
    reader.close();
}

} // namespace doris::segment_v2