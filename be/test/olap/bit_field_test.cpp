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
        EXPECT_EQ(system("mkdir -p ./ut_dir/"), 0);
        EXPECT_EQ(system("rm ./ut_dir/tmp_file"), 0);
        _out_stream = new (std::nothrow) OutStream(OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE, nullptr);
        EXPECT_TRUE(_out_stream != nullptr);
        _writer = new (std::nothrow) BitFieldWriter(_out_stream);
        EXPECT_TRUE(_writer != nullptr);
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
        EXPECT_EQ(Status::OK(),
                  _helper.open_with_mode(_file_path.c_str(), O_CREAT | O_EXCL | O_WRONLY,
                                         S_IRUSR | S_IWUSR));
        _out_stream->write_to_file(&_helper, 0);
        _helper.close();

        EXPECT_EQ(Status::OK(),
                  _helper.open_with_mode(_file_path.c_str(), O_RDONLY, S_IRUSR | S_IWUSR));

        _shared_buffer = StorageByteBuffer::create(OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE +
                                                   sizeof(StreamHead));
        EXPECT_TRUE(_shared_buffer != nullptr);

        _stream = new (std::nothrow)
                ReadOnlyFileStream(&_helper, &_shared_buffer, 0, _helper.length(), nullptr,
                                   OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE, &_stats);
        EXPECT_EQ(Status::OK(), _stream->init());

        _reader = new (std::nothrow) BitFieldReader(_stream);
        EXPECT_TRUE(_reader != nullptr);
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
    EXPECT_EQ(Status::OK(), _writer->write(true));
    EXPECT_EQ(Status::OK(), _writer->flush());

    // read data
    CreateReader();

    char value = 0;
    EXPECT_EQ(Status::OK(), _reader->next(&value));
    EXPECT_EQ(value, 1);
}

TEST_F(TestBitField, ReadWriteMultiBits) {
    // write data
    for (int32_t i = 0; i < 100; i++) {
        if (0 == i % 2) {
            EXPECT_EQ(Status::OK(), _writer->write(true));
        } else {
            EXPECT_EQ(Status::OK(), _writer->write(false));
        }
    }
    EXPECT_EQ(Status::OK(), _writer->flush());

    // read data
    CreateReader();

    char value = 0;
    for (int32_t i = 0; i < 100; i++) {
        EXPECT_EQ(Status::OK(), _reader->next(&value));
        if (0 == i % 2) {
            EXPECT_EQ(value, 1);
        } else {
            EXPECT_EQ(value, 0);
        }
    }
}

TEST_F(TestBitField, Seek) {
    // write data
    for (int32_t i = 0; i < 100; i++) {
        if (0 == i % 2) {
            EXPECT_EQ(Status::OK(), _writer->write(true));
        } else {
            EXPECT_EQ(Status::OK(), _writer->write(false));
        }
    }
    PositionEntryWriter index_entry;
    _writer->get_position(&index_entry);

    EXPECT_EQ(Status::OK(), _writer->write(true));

    EXPECT_EQ(Status::OK(), _writer->flush());

    // read data
    CreateReader();

    char value = 0;
    PositionEntryReader entry;
    entry._positions = index_entry._positions;
    entry._positions_count = index_entry._positions_count;
    entry._statistics.init(OLAP_FIELD_TYPE_TINYINT, false);

    PositionProvider position(&entry);
    _reader->seek(&position);
    EXPECT_EQ(Status::OK(), _reader->next(&value));
    EXPECT_EQ(value, 1);
}

TEST_F(TestBitField, Skip) {
    // write data
    for (int32_t i = 0; i < 100; i++) {
        if (0 == i % 2) {
            EXPECT_EQ(Status::OK(), _writer->write(true));
        } else {
            EXPECT_EQ(Status::OK(), _writer->write(false));
        }
    }

    EXPECT_EQ(Status::OK(), _writer->write(true));

    EXPECT_EQ(Status::OK(), _writer->flush());

    // read data
    CreateReader();

    char value = 0;
    _reader->skip(100);
    EXPECT_EQ(Status::OK(), _reader->next(&value));
    EXPECT_EQ(value, 1);
}

} // namespace doris
