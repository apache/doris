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

#include "olap/byte_buffer.h"
#include "olap/in_stream.h"
#include "olap/out_stream.h"
#include "olap/rowset/bit_field_reader.h"
#include "olap/rowset/bit_field_writer.h"
#include "util/logging.h"

namespace doris {

class TestBitField : public testing::Test {
public:
    TestBitField() {}

    virtual ~TestBitField() {}

    void SetUp() {
        ASSERT_EQ(system("mkdir -p ./ut_dir/"), 0);
        ASSERT_EQ(system("rm ./ut_dir/tmp_file"), 0);
        _out_stream = new (std::nothrow) OutStream(OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE, nullptr);
        ASSERT_TRUE(_out_stream != nullptr);
        _writer = new (std::nothrow) BitFieldWriter(_out_stream);
        ASSERT_TRUE(_writer != nullptr);
        _writer->init();
    }

    void TearDown() {
        SAFE_DELETE(_reader);
        SAFE_DELETE(_out_stream);
        SAFE_DELETE(_writer);
        SAFE_DELETE(_shared_buffer);
        SAFE_DELETE(_stream);
    }

    void CreateReader() {
        ASSERT_EQ(OLAP_SUCCESS,
                  _helper.open_with_mode(_file_path.c_str(), O_CREAT | O_EXCL | O_WRONLY,
                                         S_IRUSR | S_IWUSR));
        _out_stream->write_to_file(&_helper, 0);
        _helper.close();

        ASSERT_EQ(OLAP_SUCCESS,
                  _helper.open_with_mode(_file_path.c_str(), O_RDONLY, S_IRUSR | S_IWUSR));

        _shared_buffer = StorageByteBuffer::create(OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE +
                                                   sizeof(StreamHead));
        ASSERT_TRUE(_shared_buffer != nullptr);

        _stream = new (std::nothrow)
                ReadOnlyFileStream(&_helper, &_shared_buffer, 0, _helper.length(), nullptr,
                                   OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE, &_stats);
        ASSERT_EQ(OLAP_SUCCESS, _stream->init());

        _reader = new (std::nothrow) BitFieldReader(_stream);
        ASSERT_TRUE(_reader != nullptr);
        _reader->init();
    }

    BitFieldReader* _reader;
    OutStream* _out_stream;
    BitFieldWriter* _writer;
    FileHandler _helper;
    StorageByteBuffer* _shared_buffer;
    ReadOnlyFileStream* _stream;
    OlapReaderStatistics _stats;

    std::string _file_path = "./ut_dir/tmp_file";
};

TEST_F(TestBitField, ReadWriteOneBit) {
    // write data
    ASSERT_EQ(OLAP_SUCCESS, _writer->write(true));
    ASSERT_EQ(OLAP_SUCCESS, _writer->flush());

    // read data
    CreateReader();

    char value = 0;
    ASSERT_EQ(OLAP_SUCCESS, _reader->next(&value));
    ASSERT_EQ(value, 1);
}

TEST_F(TestBitField, ReadWriteMultiBits) {
    // write data
    for (int32_t i = 0; i < 100; i++) {
        if (0 == i % 2) {
            ASSERT_EQ(OLAP_SUCCESS, _writer->write(true));
        } else {
            ASSERT_EQ(OLAP_SUCCESS, _writer->write(false));
        }
    }
    ASSERT_EQ(OLAP_SUCCESS, _writer->flush());

    // read data
    CreateReader();

    char value = 0;
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(OLAP_SUCCESS, _reader->next(&value));
        if (0 == i % 2) {
            ASSERT_EQ(value, 1);
        } else {
            ASSERT_EQ(value, 0);
        }
    }
}

TEST_F(TestBitField, Seek) {
    // write data
    for (int32_t i = 0; i < 100; i++) {
        if (0 == i % 2) {
            ASSERT_EQ(OLAP_SUCCESS, _writer->write(true));
        } else {
            ASSERT_EQ(OLAP_SUCCESS, _writer->write(false));
        }
    }
    PositionEntryWriter index_entry;
    _writer->get_position(&index_entry);

    ASSERT_EQ(OLAP_SUCCESS, _writer->write(true));

    ASSERT_EQ(OLAP_SUCCESS, _writer->flush());

    // read data
    CreateReader();

    char value = 0;
    PositionEntryReader entry;
    entry._positions = index_entry._positions;
    entry._positions_count = index_entry._positions_count;
    entry._statistics.init(OLAP_FIELD_TYPE_TINYINT, false);

    PositionProvider position(&entry);
    _reader->seek(&position);
    ASSERT_EQ(OLAP_SUCCESS, _reader->next(&value));
    ASSERT_EQ(value, 1);
}

TEST_F(TestBitField, Skip) {
    // write data
    for (int32_t i = 0; i < 100; i++) {
        if (0 == i % 2) {
            ASSERT_EQ(OLAP_SUCCESS, _writer->write(true));
        } else {
            ASSERT_EQ(OLAP_SUCCESS, _writer->write(false));
        }
    }

    ASSERT_EQ(OLAP_SUCCESS, _writer->write(true));

    ASSERT_EQ(OLAP_SUCCESS, _writer->flush());

    // read data
    CreateReader();

    char value = 0;
    _reader->skip(100);
    ASSERT_EQ(OLAP_SUCCESS, _reader->next(&value));
    ASSERT_EQ(value, 1);
}

} // namespace doris

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    doris::init_glog("be-test");
    int ret = doris::OLAP_SUCCESS;
    testing::InitGoogleTest(&argc, argv);
    ret = RUN_ALL_TESTS();
    google::protobuf::ShutdownProtobufLibrary();
    return ret;
}
