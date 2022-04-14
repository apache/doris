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
#include "olap/rowset/run_length_integer_reader.h"
#include "olap/rowset/run_length_integer_writer.h"
#include "olap/stream_index_reader.h"
#include "olap/stream_index_writer.h"
#include "util/logging.h"

namespace doris {

class TestRunLengthUnsignInteger : public testing::Test {
public:
    TestRunLengthUnsignInteger() {}

    virtual ~TestRunLengthUnsignInteger() {}

    virtual void SetUp() {
        EXPECT_EQ(system("mkdir -p ./ut_dir"), 0);
        EXPECT_EQ(system("rm -rf ./ut_dir/tmp_file"), 0);
        _out_stream = new (std::nothrow) OutStream(OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE, nullptr);
        EXPECT_TRUE(_out_stream != nullptr);
        _writer = new (std::nothrow) RunLengthIntegerWriter(_out_stream, false);
        EXPECT_TRUE(_writer != nullptr);
    }

    virtual void TearDown() {
        SAFE_DELETE(_reader);
        SAFE_DELETE(_out_stream);
        SAFE_DELETE(_writer);
        SAFE_DELETE(_shared_buffer);
        SAFE_DELETE(_stream);
    }

    void CreateReader() {
        EXPECT_EQ(Status::OK(),
                  helper.open_with_mode(_file_path.c_str(), O_CREAT | O_EXCL | O_WRONLY,
                                        S_IRUSR | S_IWUSR));
        _out_stream->write_to_file(&helper, 0);
        helper.close();

        EXPECT_EQ(Status::OK(),
                  helper.open_with_mode(_file_path.c_str(), O_RDONLY, S_IRUSR | S_IWUSR));

        _shared_buffer = StorageByteBuffer::create(OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE +
                                                   sizeof(StreamHead));
        EXPECT_TRUE(_shared_buffer != nullptr);

        _stream = new (std::nothrow)
                ReadOnlyFileStream(&helper, &_shared_buffer, 0, helper.length(), nullptr,
                                   OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE, &_stats);
        EXPECT_EQ(Status::OK(), _stream->init());

        _reader = new (std::nothrow) RunLengthIntegerReader(_stream, false);
        EXPECT_TRUE(_reader != nullptr);
    }

    RunLengthIntegerReader* _reader;
    OutStream* _out_stream;
    RunLengthIntegerWriter* _writer;
    FileHandler helper;
    StorageByteBuffer* _shared_buffer;
    ReadOnlyFileStream* _stream;
    OlapReaderStatistics _stats;

    std::string _file_path = "./ut_dir/tmp_file";
};

TEST_F(TestRunLengthUnsignInteger, ReadWriteOneInteger) {
    // write data
    EXPECT_EQ(Status::OK(), _writer->write(100));
    EXPECT_EQ(Status::OK(), _writer->flush());

    // read data
    CreateReader();

    EXPECT_TRUE(_reader->has_next());
    int64_t value = 0;
    EXPECT_EQ(Status::OK(), _reader->next(&value));
    EXPECT_EQ(value, 100);

    EXPECT_FALSE(_reader->has_next());
}

TEST_F(TestRunLengthUnsignInteger, ReadWriteMultiInteger) {
    // write data
    int64_t write_data[] = {100, 102, 105, 106};
    for (int32_t i = 0; i < 4; i++) {
        EXPECT_EQ(Status::OK(), _writer->write(write_data[i]));
    }

    EXPECT_EQ(Status::OK(), _writer->flush());

    // read data
    CreateReader();

    for (int32_t i = 0; i < 4; i++) {
        EXPECT_TRUE(_reader->has_next());
        int64_t value = 0;
        EXPECT_EQ(Status::OK(), _reader->next(&value));
        EXPECT_EQ(value, write_data[i]);
    }

    EXPECT_FALSE(_reader->has_next());
}

TEST_F(TestRunLengthUnsignInteger, seek) {
    // write data
    int64_t write_data[] = {100, 102, 105, 106};
    for (int32_t i = 0; i < 4; i++) {
        EXPECT_EQ(Status::OK(), _writer->write(write_data[i]));
    }

    PositionEntryWriter index_entry;
    _writer->get_position(&index_entry, false);
    _writer->write(107);

    _writer->write(108);
    _writer->write(109);
    _writer->write(110);

    EXPECT_EQ(Status::OK(), _writer->flush());

    // read data
    CreateReader();

    PositionEntryReader entry;
    entry._positions = index_entry._positions;
    entry._positions_count = index_entry._positions_count;
    entry._statistics.init(OLAP_FIELD_TYPE_INT, false);

    PositionProvider position(&entry);
    _reader->seek(&position);
    int64_t value = 0;
    EXPECT_EQ(Status::OK(), _reader->next(&value));
    EXPECT_EQ(value, 107);
    EXPECT_EQ(Status::OK(), _reader->next(&value));
    EXPECT_EQ(value, 108);
    EXPECT_EQ(Status::OK(), _reader->next(&value));
    EXPECT_EQ(value, 109);
    EXPECT_EQ(Status::OK(), _reader->next(&value));
    EXPECT_EQ(value, 110);
}

TEST_F(TestRunLengthUnsignInteger, skip) {
    // write data
    int64_t write_data[] = {100, 102, 105, 106};
    for (int32_t i = 0; i < 4; i++) {
        EXPECT_EQ(Status::OK(), _writer->write(write_data[i]));
    }

    EXPECT_EQ(Status::OK(), _writer->flush());

    // read data
    CreateReader();

    _reader->skip(2);
    int64_t value = 0;
    EXPECT_EQ(Status::OK(), _reader->next(&value));
    EXPECT_EQ(value, 105);
    EXPECT_EQ(Status::OK(), _reader->next(&value));
    EXPECT_EQ(value, 106);
    EXPECT_NE(Status::OK(), _reader->next(&value));
}

TEST_F(TestRunLengthUnsignInteger, ShortRepeatEncoding) {
    // write data
    int64_t write_data[] = {100, 100, 100, 100};
    for (int32_t i = 0; i < 4; i++) {
        EXPECT_EQ(Status::OK(), _writer->write(write_data[i]));
    }
    EXPECT_EQ(Status::OK(), _writer->flush());

    // read data
    CreateReader();

    for (int32_t i = 0; i < 4; i++) {
        EXPECT_TRUE(_reader->has_next());
        int64_t value = 0;
        EXPECT_EQ(Status::OK(), _reader->next(&value));
        EXPECT_EQ(value, write_data[i]);
    }

    EXPECT_FALSE(_reader->has_next());
}

TEST_F(TestRunLengthUnsignInteger, ShortRepeatEncoding2) {
    // write data
    int64_t write_data[] = {876012345678912, 876012345678912, 876012345678912, 876012345678912};
    for (int32_t i = 0; i < 4; i++) {
        EXPECT_EQ(Status::OK(), _writer->write(write_data[i]));
    }
    EXPECT_EQ(Status::OK(), _writer->flush());

    // read data
    CreateReader();

    for (int32_t i = 0; i < 4; i++) {
        EXPECT_TRUE(_reader->has_next());
        int64_t value = 0;
        EXPECT_EQ(Status::OK(), _reader->next(&value));
        EXPECT_EQ(value, write_data[i]);
    }

    EXPECT_FALSE(_reader->has_next());
}

TEST_F(TestRunLengthUnsignInteger, ShortRepeatEncoding3) {
    // write data
    int64_t write_data[] = {876012345678912};
    for (int32_t i = 0; i < 1; i++) {
        EXPECT_EQ(Status::OK(), _writer->write(write_data[i]));
    }
    EXPECT_EQ(Status::OK(), _writer->flush());

    // read data
    CreateReader();

    for (int32_t i = 0; i < 1; i++) {
        EXPECT_TRUE(_reader->has_next());
        int64_t value = 0;
        EXPECT_EQ(Status::OK(), _reader->next(&value));
        EXPECT_EQ(value, write_data[i]);
    }

    EXPECT_FALSE(_reader->has_next());
}

TEST_F(TestRunLengthUnsignInteger, DirectEncoding) {
    // write data
    int64_t write_data[] = {1703, 6054, 8760, 902};
    for (int32_t i = 0; i < 4; i++) {
        EXPECT_EQ(Status::OK(), _writer->write(write_data[i]));
    }
    EXPECT_EQ(Status::OK(), _writer->flush());

    // read data
    CreateReader();

    for (int32_t i = 0; i < 4; i++) {
        EXPECT_TRUE(_reader->has_next());
        int64_t value = 0;
        EXPECT_EQ(Status::OK(), _reader->next(&value));
        EXPECT_EQ(value, write_data[i]);
    }

    EXPECT_FALSE(_reader->has_next());
}

TEST_F(TestRunLengthUnsignInteger, DirectEncoding2) {
    // write data
    int64_t write_data[] = {1703, 6054, 876012345678912, 902, 9292, 184932, 873624, 827364, 999, 8};
    for (int32_t i = 0; i < 10; i++) {
        EXPECT_EQ(Status::OK(), _writer->write(write_data[i]));
    }
    EXPECT_EQ(Status::OK(), _writer->flush());

    // read data
    CreateReader();

    for (int32_t i = 0; i < 10; i++) {
        EXPECT_TRUE(_reader->has_next());
        int64_t value = 0;
        EXPECT_EQ(Status::OK(), _reader->next(&value));
        EXPECT_EQ(value, write_data[i]);
    }

    EXPECT_FALSE(_reader->has_next());
}

TEST_F(TestRunLengthUnsignInteger, PatchedBaseEncoding1) {
    // write data
    int64_t write_data[] = {
            1703, 6054, 876012345678912, 902,   9292, 184932, 873624, 827364, 999, 8,
            1,    3323, 432232523,       90982, 9,    223234, 5,      44,     5,   3};
    for (int32_t i = 0; i < 20; i++) {
        EXPECT_EQ(Status::OK(), _writer->write(write_data[i]));
    }
    EXPECT_EQ(Status::OK(), _writer->flush());

    // read data
    CreateReader();

    for (int32_t i = 0; i < 20; i++) {
        EXPECT_TRUE(_reader->has_next());
        int64_t value = 0;
        EXPECT_EQ(Status::OK(), _reader->next(&value));
        EXPECT_EQ(value, write_data[i]);
    }

    EXPECT_FALSE(_reader->has_next());
}

TEST_F(TestRunLengthUnsignInteger, PatchedBaseEncoding2) {
    // write data
    int64_t write_data[] = {
            1703, 6054,      902,   9292, 184932, 873624, 827364,          999, 8, 1,
            3323, 432232523, 90982, 9,    223234, 5,      876012345678912, 44,  5, 3};
    for (int32_t i = 0; i < 20; i++) {
        EXPECT_EQ(Status::OK(), _writer->write(write_data[i]));
    }
    EXPECT_EQ(Status::OK(), _writer->flush());

    // read data
    CreateReader();

    for (int32_t i = 0; i < 20; i++) {
        EXPECT_TRUE(_reader->has_next());
        int64_t value = 0;
        EXPECT_EQ(Status::OK(), _reader->next(&value));
        EXPECT_EQ(value, write_data[i]);
    }

    EXPECT_FALSE(_reader->has_next());
}

class TestRunLengthSignInteger : public testing::Test {
public:
    TestRunLengthSignInteger() {}

    virtual ~TestRunLengthSignInteger() {}

    virtual void SetUp() {
        EXPECT_EQ(system("mkdir -p ./ut_dir"), 0);
        EXPECT_EQ(system("rm ./ut_dir/tmp_file"), 0);
        _out_stream = new (std::nothrow) OutStream(OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE, nullptr);
        EXPECT_TRUE(_out_stream != nullptr);
        _writer = new (std::nothrow) RunLengthIntegerWriter(_out_stream, true);
        EXPECT_TRUE(_writer != nullptr);
    }

    virtual void TearDown() {
        SAFE_DELETE(_reader);
        SAFE_DELETE(_out_stream);
        SAFE_DELETE(_writer);
        SAFE_DELETE(_shared_buffer);
        SAFE_DELETE(_stream);
    }

    void CreateReader() {
        EXPECT_EQ(Status::OK(),
                  helper.open_with_mode(_file_path.c_str(), O_CREAT | O_EXCL | O_WRONLY,
                                        S_IRUSR | S_IWUSR));
        _out_stream->write_to_file(&helper, 0);
        helper.close();

        EXPECT_EQ(Status::OK(),
                  helper.open_with_mode(_file_path.c_str(), O_RDONLY, S_IRUSR | S_IWUSR));

        _shared_buffer = StorageByteBuffer::create(OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE +
                                                   sizeof(StreamHead));
        EXPECT_TRUE(_shared_buffer != nullptr);

        _stream = new (std::nothrow)
                ReadOnlyFileStream(&helper, &_shared_buffer, 0, helper.length(), nullptr,
                                   OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE, &_stats);
        EXPECT_EQ(Status::OK(), _stream->init());

        _reader = new (std::nothrow) RunLengthIntegerReader(_stream, true);
        EXPECT_TRUE(_reader != nullptr);
    }

    RunLengthIntegerReader* _reader;
    OutStream* _out_stream;
    RunLengthIntegerWriter* _writer;
    FileHandler helper;
    StorageByteBuffer* _shared_buffer;
    ReadOnlyFileStream* _stream;
    OlapReaderStatistics _stats;
    std::string _file_path = "./ut_dir/tmp_file";
};

TEST_F(TestRunLengthSignInteger, ReadWriteOneInteger) {
    // write data
    EXPECT_EQ(Status::OK(), _writer->write(100));
    EXPECT_EQ(Status::OK(), _writer->flush());

    // read data
    CreateReader();

    EXPECT_TRUE(_reader->has_next());
    int64_t value = 0;
    EXPECT_EQ(Status::OK(), _reader->next(&value));
    EXPECT_EQ(value, 100);

    EXPECT_FALSE(_reader->has_next());
}

TEST_F(TestRunLengthSignInteger, ReadWriteOneInteger2) {
    // write data
    EXPECT_EQ(Status::OK(), _writer->write(1234567800));
    EXPECT_EQ(Status::OK(), _writer->flush());

    // read data
    CreateReader();

    EXPECT_TRUE(_reader->has_next());
    int64_t value = 0;
    EXPECT_EQ(Status::OK(), _reader->next(&value));
    EXPECT_EQ(value, 1234567800);

    EXPECT_FALSE(_reader->has_next());
}

TEST_F(TestRunLengthSignInteger, ReadWriteMultiInteger) {
    // write data
    int64_t write_data[] = {100, 101, 104, 107};
    for (int32_t i = 0; i < 4; i++) {
        EXPECT_EQ(Status::OK(), _writer->write(write_data[i]));
    }

    EXPECT_EQ(Status::OK(), _writer->flush());

    // read data
    CreateReader();

    for (int32_t i = 0; i < 4; i++) {
        EXPECT_TRUE(_reader->has_next());
        int64_t value = 0;
        EXPECT_EQ(Status::OK(), _reader->next(&value));
        EXPECT_EQ(value, write_data[i]);
    }

    EXPECT_FALSE(_reader->has_next());
}

TEST_F(TestRunLengthSignInteger, seek) {
    // write data
    int64_t write_data[] = {100, 102, 105, 106};
    for (int32_t i = 0; i < 4; i++) {
        EXPECT_EQ(Status::OK(), _writer->write(write_data[i]));
    }

    PositionEntryWriter index_entry;
    _writer->get_position(&index_entry, false);
    _writer->write(107);

    _writer->write(108);
    _writer->write(109);
    _writer->write(110);

    EXPECT_EQ(Status::OK(), _writer->flush());

    // read data
    CreateReader();

    PositionEntryReader entry;
    entry._positions = index_entry._positions;
    entry._positions_count = index_entry._positions_count;
    entry._statistics.init(OLAP_FIELD_TYPE_INT, false);

    PositionProvider position(&entry);
    _reader->seek(&position);
    int64_t value = 0;
    EXPECT_EQ(Status::OK(), _reader->next(&value));
    EXPECT_EQ(value, 107);
    EXPECT_EQ(Status::OK(), _reader->next(&value));
    EXPECT_EQ(value, 108);
    EXPECT_EQ(Status::OK(), _reader->next(&value));
    EXPECT_EQ(value, 109);
    EXPECT_EQ(Status::OK(), _reader->next(&value));
    EXPECT_EQ(value, 110);
}

TEST_F(TestRunLengthSignInteger, skip) {
    // write data
    int64_t write_data[] = {100, 102, 105, 106};
    for (int32_t i = 0; i < 4; i++) {
        EXPECT_EQ(Status::OK(), _writer->write(write_data[i]));
    }

    EXPECT_EQ(Status::OK(), _writer->flush());

    // read data
    CreateReader();

    _reader->skip(2);
    int64_t value = 0;
    EXPECT_EQ(Status::OK(), _reader->next(&value));
    EXPECT_EQ(value, 105);
    EXPECT_EQ(Status::OK(), _reader->next(&value));
    EXPECT_EQ(value, 106);
    EXPECT_NE(Status::OK(), _reader->next(&value));
}

TEST_F(TestRunLengthSignInteger, ShortRepeatEncoding) {
    // write data
    int64_t write_data[] = {-100, -100, -100, -100};
    for (int32_t i = 0; i < 4; i++) {
        EXPECT_EQ(Status::OK(), _writer->write(write_data[i]));
    }
    EXPECT_EQ(Status::OK(), _writer->flush());

    // read data
    CreateReader();

    for (int32_t i = 0; i < 4; i++) {
        EXPECT_TRUE(_reader->has_next());
        int64_t value = 0;
        EXPECT_EQ(Status::OK(), _reader->next(&value));
        EXPECT_EQ(value, write_data[i]);
    }

    EXPECT_FALSE(_reader->has_next());
}

TEST_F(TestRunLengthSignInteger, DirectEncoding) {
    // write data
    int64_t write_data[] = {-1703, -6054, -8760, -902};
    for (int32_t i = 0; i < 4; i++) {
        EXPECT_EQ(Status::OK(), _writer->write(write_data[i]));
    }
    EXPECT_EQ(Status::OK(), _writer->flush());

    // read data
    CreateReader();

    for (int32_t i = 0; i < 4; i++) {
        EXPECT_TRUE(_reader->has_next());
        int64_t value = 0;
        EXPECT_EQ(Status::OK(), _reader->next(&value));
        EXPECT_EQ(value, write_data[i]);
    }

    EXPECT_FALSE(_reader->has_next());
}

TEST_F(TestRunLengthUnsignInteger, ReadWriteMassInteger) {
    // write data
    for (int64_t i = 0; i < 100000; i++) {
        EXPECT_EQ(Status::OK(), _writer->write(i));
    }

    EXPECT_EQ(Status::OK(), _writer->flush());

    // read data
    CreateReader();

    for (int64_t i = 0; i < 100000; i++) {
        EXPECT_TRUE(_reader->has_next());
        int64_t value = 0;
        EXPECT_EQ(Status::OK(), _reader->next(&value));
        EXPECT_EQ(value, i);
    }
}

TEST_F(TestRunLengthSignInteger, PatchedBaseEncoding1) {
    // write data
    int64_t write_data[] = {
            1703, 6054, -876012345678912, 902,    9292, 184932, 873624, 827364, 999, 8,
            1,    3323, 432232523,        -90982, 9,    223234, 5,      44,     5,   3};
    for (int32_t i = 0; i < 20; i++) {
        EXPECT_EQ(Status::OK(), _writer->write(write_data[i]));
    }
    EXPECT_EQ(Status::OK(), _writer->flush());

    // read data
    CreateReader();

    for (int32_t i = 0; i < 20; i++) {
        EXPECT_TRUE(_reader->has_next());
        int64_t value = 0;
        EXPECT_EQ(Status::OK(), _reader->next(&value));
        EXPECT_EQ(value, write_data[i]);
    }

    EXPECT_FALSE(_reader->has_next());
}

TEST_F(TestRunLengthSignInteger, PatchedBaseEncoding2) {
    // write data
    int64_t write_data[] = {
            -1703, -6054, -876012345678912, -902,   -9292, -184932, -873624, -827364, -999, -8,
            -1,    -3323, -432232523,       -90982, -9,    -223234, -5,      -44,     -5,   -3};
    for (int32_t i = 0; i < 20; i++) {
        EXPECT_EQ(Status::OK(), _writer->write(write_data[i]));
    }
    EXPECT_EQ(Status::OK(), _writer->flush());

    // read data
    CreateReader();

    for (int32_t i = 0; i < 20; i++) {
        EXPECT_TRUE(_reader->has_next());
        int64_t value = 0;
        EXPECT_EQ(Status::OK(), _reader->next(&value));
        EXPECT_EQ(value, write_data[i]);
    }

    EXPECT_FALSE(_reader->has_next());
}

// case for T in [if ((base & mask) != 0)]
TEST_F(TestRunLengthSignInteger, PatchedBaseEncoding3) {
    // write data
    int64_t write_data[] = {
            1703, 6054, 876012345678912, 902,    9292, 184932, 873624, 827364, 999, 888,
            -300, 3323, 432232523,       -90982, 450,  223234, 690,    444,    555, 333};
    for (int32_t i = 0; i < 20; i++) {
        EXPECT_EQ(Status::OK(), _writer->write(write_data[i]));
    }
    EXPECT_EQ(Status::OK(), _writer->flush());

    // read data
    CreateReader();

    for (int32_t i = 0; i < 20; i++) {
        EXPECT_TRUE(_reader->has_next());
        int64_t value = 0;
        EXPECT_EQ(Status::OK(), _reader->next(&value));
        EXPECT_EQ(value, write_data[i]);
    }

    EXPECT_FALSE(_reader->has_next());
}

// test fo gap > 255
TEST_F(TestRunLengthSignInteger, PatchedBaseEncoding4) {
    // write data
    int64_t write_data[] = {300,
                            6054,
                            902,
                            9292,
                            184932,
                            873624,
                            827364,
                            999,
                            888,
                            1703,
                            3323,
                            432232523,
                            90982,
                            450,
                            223234,
                            690,
                            444,
                            555,
                            333,
                            232,
                            300,
                            6054,
                            902,
                            9292,
                            184932,
                            873624,
                            827364,
                            999,
                            888,
                            1703,
                            3323,
                            432232523,
                            90982,
                            450,
                            223234,
                            690,
                            444,
                            555,
                            333,
                            232,
                            300,
                            6054,
                            902,
                            9292,
                            184932,
                            873624,
                            827364,
                            999,
                            888,
                            1703,
                            3323,
                            432232523,
                            90982,
                            450,
                            223234,
                            690,
                            444,
                            555,
                            333,
                            232,
                            300,
                            6054,
                            902,
                            9292,
                            184932,
                            873624,
                            827364,
                            999,
                            888,
                            1703,
                            3323,
                            432232523,
                            90982,
                            450,
                            223234,
                            690,
                            444,
                            555,
                            333,
                            232,
                            300,
                            6054,
                            902,
                            9292,
                            184932,
                            873624,
                            827364,
                            999,
                            888,
                            1703,
                            3323,
                            432232523,
                            90982,
                            450,
                            223234,
                            690,
                            444,
                            555,
                            333,
                            232,
                            300,
                            6054,
                            902,
                            9292,
                            184932,
                            873624,
                            827364,
                            999,
                            888,
                            1703,
                            3323,
                            432232523,
                            90982,
                            450,
                            223234,
                            690,
                            444,
                            555,
                            333,
                            232,
                            300,
                            6054,
                            902,
                            9292,
                            184932,
                            873624,
                            827364,
                            999,
                            888,
                            1703,
                            3323,
                            432232523,
                            90982,
                            450,
                            223234,
                            690,
                            444,
                            555,
                            333,
                            232,
                            300,
                            6054,
                            902,
                            9292,
                            184932,
                            873624,
                            827364,
                            999,
                            888,
                            1703,
                            3323,
                            432232523,
                            90982,
                            450,
                            223234,
                            690,
                            444,
                            555,
                            333,
                            232,
                            300,
                            6054,
                            902,
                            9292,
                            184932,
                            873624,
                            827364,
                            999,
                            888,
                            1703,
                            3323,
                            432232523,
                            90982,
                            450,
                            223234,
                            690,
                            444,
                            555,
                            333,
                            232,
                            300,
                            6054,
                            902,
                            9292,
                            184932,
                            873624,
                            827364,
                            999,
                            888,
                            1703,
                            3323,
                            432232523,
                            90982,
                            450,
                            223234,
                            690,
                            444,
                            555,
                            333,
                            232,
                            300,
                            6054,
                            902,
                            9292,
                            184932,
                            873624,
                            827364,
                            999,
                            888,
                            1703,
                            3323,
                            432232523,
                            90982,
                            450,
                            223234,
                            690,
                            444,
                            555,
                            333,
                            232,
                            300,
                            6054,
                            902,
                            9292,
                            184932,
                            873624,
                            827364,
                            999,
                            888,
                            1703,
                            3323,
                            432232523,
                            90982,
                            450,
                            223234,
                            690,
                            444,
                            555,
                            333,
                            232,
                            300,
                            6054,
                            902,
                            9292,
                            184932,
                            873624,
                            827364,
                            999,
                            888,
                            1703,
                            3323,
                            432232523,
                            90982,
                            450,
                            223234,
                            690,
                            444,
                            555,
                            333,
                            232,
                            300,
                            6054,
                            902,
                            9292,
                            184932,
                            873624,
                            827364,
                            999,
                            888,
                            1703,
                            3323,
                            432232523,
                            90982,
                            450,
                            223234,
                            690,
                            444,
                            555,
                            333,
                            232,
                            876012345678912};

    for (int32_t i = 0; i < 281; i++) {
        EXPECT_EQ(Status::OK(), _writer->write(write_data[i]));
    }
    EXPECT_EQ(Status::OK(), _writer->flush());

    // read data
    CreateReader();

    for (int32_t i = 0; i < 281; i++) {
        EXPECT_TRUE(_reader->has_next());
        int64_t value = 0;
        EXPECT_EQ(Status::OK(), _reader->next(&value));
        EXPECT_EQ(value, write_data[i]);
    }

    EXPECT_FALSE(_reader->has_next());
}

// test for [if (max_gap == 511)]
TEST_F(TestRunLengthSignInteger, PatchedBaseEncoding5) {
    // write data
    int64_t write_data[] = {
            300,  6054,           902,   9292, 184932, 873624, 827364, 999, 888, 1703,
            3323, 432232523,      90982, 450,  223234, 690,    444,    555, 333, 232,
            300,  6054,           902,   9292, 184932, 873624, 827364, 999, 888, 1703,
            3323, 432232523,      90982, 450,  223234, 690,    444,    555, 333, 232,
            300,  6054,           902,   9292, 184932, 873624, 827364, 999, 888, 1703,
            3323, 432232523,      90982, 450,  223234, 690,    444,    555, 333, 232,
            300,  6054,           902,   9292, 184932, 873624, 827364, 999, 888, 1703,
            3323, 432232523,      90982, 450,  223234, 690,    444,    555, 333, 232,
            300,  6054,           902,   9292, 184932, 873624, 827364, 999, 888, 1703,
            3323, 432232523,      90982, 450,  223234, 690,    444,    555, 333, 232,
            300,  6054,           902,   9292, 184932, 873624, 827364, 999, 888, 1703,
            3323, 432232523,      90982, 450,  223234, 690,    444,    555, 333, 232,
            300,  6054,           902,   9292, 184932, 873624, 827364, 999, 888, 1703,
            3323, 432232523,      90982, 450,  223234, 690,    444,    555, 333, 232,
            300,  6054,           902,   9292, 184932, 873624, 827364, 999, 888, 1703,
            3323, 432232523,      90982, 450,  223234, 690,    444,    555, 333, 232,
            300,  6054,           902,   9292, 184932, 873624, 827364, 999, 888, 1703,
            3323, 432232523,      90982, 450,  223234, 690,    444,    555, 333, 232,
            300,  6054,           902,   9292, 184932, 873624, 827364, 999, 888, 1703,
            3323, 432232523,      90982, 450,  223234, 690,    444,    555, 333, 232,
            300,  6054,           902,   9292, 184932, 873624, 827364, 999, 888, 1703,
            3323, 432232523,      90982, 450,  223234, 690,    444,    555, 333, 232,
            300,  6054,           902,   9292, 184932, 873624, 827364, 999, 888, 1703,
            3323, 432232523,      90982, 450,  223234, 690,    444,    555, 333, 232,
            300,  6054,           902,   9292, 184932, 873624, 827364, 999, 888, 1703,
            3323, 432232523,      90982, 450,  223234, 690,    444,    555, 333, 232,
            300,  6054,           902,   9292, 184932, 873624, 827364, 999, 888, 1703,
            3323, 432232523,      90982, 450,  223234, 690,    444,    555, 333, 232,
            300,  6054,           902,   9292, 184932, 873624, 827364, 999, 888, 1703,
            3323, 432232523,      90982, 450,  223234, 690,    444,    555, 333, 232,
            300,  6054,           902,   9292, 184932, 873624, 827364, 999, 888, 1703,
            3323, 432232523,      90982, 450,  223234, 690,    444,    555, 333, 232,
            300,  6054,           902,   9292, 184932, 873624, 827364, 999, 888, 1703,
            3323, 432232523,      90982, 450,  223234, 690,    444,    555, 333, 232,
            300,  6054,           902,   9292, 184932, 873624, 827364, 999, 888, 1703,
            3323, 432232523,      90982, 450,  223234, 690,    444,    555, 333, 232,
            300,  6054,           902,   9292, 184932, 873624, 827364, 999, 888, 1703,
            3323, 432232523,      90982, 450,  223234, 690,    444,    555, 333, 232,
            300,  6054,           902,   9292, 184932, 873624, 827364, 999, 888, 1703,
            3323, 432232523,      90982, 450,  223234, 690,    444,    555, 333, 232,
            300,  6054,           902,   9292, 184932, 873624, 827364, 999, 888, 1703,
            3323, 432232523,      90982, 450,  223234, 690,    444,    555, 333, 232,
            300,  6054,           902,   9292, 184932, 873624, 827364, 999, 888, 1703,
            3323, 432232523,      90982, 450,  223234, 690,    444,    555, 333, 232,
            300,  6054,           902,   9292, 184932, 873624, 827364, 999, 888, 1703,
            3323, 432232523,      90982, 450,  223234, 690,    444,    555, 333, 232,
            300,  6054,           902,   9292, 184932, 873624, 827364, 999, 888, 1703,
            3323, 432232523,      90982, 450,  223234, 690,    444,    555, 333, 232,
            300,  6054,           902,   9292, 184932, 873624, 827364, 999, 888, 1703,
            3323, 432232523,      90982, 450,  223234, 690,    444,    555, 333, 232,
            300,  6054,           902,   9292, 184932, 873624, 827364, 999, 888, 1703,
            3323, 876012345678912};

    for (int32_t i = 0; i < 512; i++) {
        EXPECT_EQ(Status::OK(), _writer->write(write_data[i]));
    }
    EXPECT_EQ(Status::OK(), _writer->flush());

    // read data
    CreateReader();

    for (int32_t i = 0; i < 512; i++) {
        EXPECT_TRUE(_reader->has_next());
        int64_t value = 0;
        EXPECT_EQ(Status::OK(), _reader->next(&value));
        EXPECT_EQ(value, write_data[i]);
    }

    EXPECT_FALSE(_reader->has_next());
}

// this case use to test large negative number.
// The minimum of data sequence is -84742859065569280,
// the positive form is 84742859065569280.
// It is a 57 bit width integer and used 8 byte to encoding it.
// The byte number is encoding as (8-1) = 7, in 111 binary form.
TEST_F(TestRunLengthSignInteger, PatchedBaseEncoding6) {
    // write data
    int64_t write_data[] = {
            -17887939293638656, -15605417571528704, -15605417571528704, -13322895849418752,
            -13322895849418752, -84742859065569280, -15605417571528704, -13322895849418752,
            -13322895849418752, -15605417571528704, -13322895849418752, -13322895849418752,
            -15605417571528704, -15605417571528704, -13322895849418752, -13322895849418752,
            -15605417571528704, -15605417571528704, -13322895849418752, -13322895849418752,
            -11040374127308800, -15605417571528704, -13322895849418752, -13322895849418752,
            -15605417571528704, -15605417571528704, -13322895849418752, -13322895849418752,
            -15605417571528704, -13322895849418752};
    for (int32_t i = 0; i < 30; i++) {
        EXPECT_EQ(Status::OK(), _writer->write(write_data[i]));
    }
    EXPECT_EQ(Status::OK(), _writer->flush());

    // read data
    CreateReader();

    for (int32_t i = 0; i < 30; i++) {
        EXPECT_TRUE(_reader->has_next());
        int64_t value = 0;
        EXPECT_EQ(Status::OK(), _reader->next(&value));
        EXPECT_EQ(value, write_data[i]);
    }

    EXPECT_FALSE(_reader->has_next());
}

TEST_F(TestRunLengthSignInteger, DirectEncodingForDeltaOverflows1) {
    // write data
    int64_t write_data[] = {4513343538618202711, 2911390882471569739, -9181829309989854913};
    for (int32_t i = 0; i < 3; i++) {
        EXPECT_EQ(Status::OK(), _writer->write(write_data[i]));
    }

    EXPECT_EQ(Status::OK(), _writer->flush());

    // read data
    CreateReader();

    for (int32_t i = 0; i < 3; i++) {
        EXPECT_TRUE(_reader->has_next());
        int64_t value = 0;
        EXPECT_EQ(Status::OK(), _reader->next(&value));
        EXPECT_EQ(value, write_data[i]);
    }

    EXPECT_FALSE(_reader->has_next());
}

TEST_F(TestRunLengthSignInteger, DirectEncodingForDeltaOverflows2) {
    // write data
    int64_t write_data[388] = {-10655366027330,
                               -8,
                               -8,
                               17062988027150,
                               33791751739027,
                               11226586316144,
                               28085455533984,
                               3809037113342,
                               4519464927340,
                               7057442420685,
                               14594401530747,
                               51297588056932,
                               22756638885750,
                               33503880074496,
                               37462526094279,
                               2768357503914,
                               10013218679000,
                               9782540355504,
                               14191567385952,
                               7500449144168,
                               52986427404273,
                               34061223404544,
                               41158355187480,
                               30231130071318,
                               59156080720623,
                               5229947056790,
                               15537038856150,
                               63126198469920,
                               3285383272432,
                               35686846610326,
                               1408668367602,
                               3506863240250,
                               38486027140122,
                               74694461322614,
                               5631517623023,
                               1071278551640,
                               60280539267507,
                               55998488499421,
                               15148183576420,
                               9345266527488,
                               92610339730326,
                               4760991023496,
                               5809785829925,
                               4727943945615,
                               45024286170204,
                               9430751906642,
                               9104898240000,
                               1310787469233,
                               19485266345637,
                               5211360444059,
                               13671501255270,
                               29025205412982,
                               16589693255688,
                               5550737611815,
                               55740247673420,
                               4416290119582,
                               5285248047529,
                               36719742782763,
                               25526635256587,
                               469124468760,
                               23302498025820,
                               15213356460134,
                               39178120907751,
                               3663022204884,
                               12383207229348,
                               26476189044888,
                               3016879997537,
                               40542785494057,
                               27113842491675,
                               22895214591884,
                               10417880862400,
                               41668786286070,
                               9444820760988,
                               4868267611350,
                               95075547166521,
                               31102600536052,
                               3984468754330,
                               18604973210352,
                               14995789569131,
                               31920766994781,
                               1683419132059,
                               23882256010288,
                               31188556951272,
                               21582097820895,
                               3392728705744,
                               49134949965522,
                               71017521225855,
                               2954016986940,
                               39104716002146,
                               849942233098,
                               13757131707844,
                               134503055610,
                               3609965787162,
                               5092516507104,
                               15030981322468,
                               13161768499404,
                               21216129648768,
                               37101803372970,
                               37101803372970,
                               2847403596440,
                               8709398658448,
                               17409277451720,
                               1765292210160,
                               12794111172864,
                               59395030132284,
                               25700056122869,
                               3617518482435,
                               10096266209660,
                               603359572452,
                               14638639926830,
                               47440669759700,
                               541260539624,
                               30978788863282,
                               696405704526,
                               18657155830038,
                               8940665549457,
                               48888857992884,
                               61600635324852,
                               62348216229282,
                               36737996974800,
                               23412718588072,
                               39733566705126,
                               24349931484564,
                               8726400511488,
                               53728914318592,
                               33230740766440,
                               5598927206656,
                               62807536003975,
                               59361934257815,
                               12281154081596,
                               60614594920391,
                               55896003656892,
                               12410592819768,
                               26900986612464,
                               27212849083404,
                               4081661384017,
                               62807128313880,
                               10390915208885,
                               19067862634200,
                               54951638814317,
                               6813715526592,
                               55668241923840,
                               10385258308992,
                               449478352872,
                               5773207377695,
                               7951085473750,
                               8075739133609,
                               3474440407650,
                               12804432172290,
                               54059206249468,
                               41060766380882,
                               32370112924240,
                               14089711019310,
                               20756475257231,
                               12027575432880,
                               18022828219993,
                               2876759174844,
                               3428645976585,
                               10428044444020,
                               83396752046512,
                               53899236887790,
                               2436412807160,
                               58240905226752,
                               49366625937140,
                               37105861727280,
                               8612969542385,
                               73033645852629,
                               43369958815872,
                               7261355608605,
                               39209192629850,
                               52609810587480,
                               43476360891080,
                               5041062521194,
                               38576540661093,
                               68017667314375,
                               13432761019896,
                               5464942964090,
                               43050750861745,
                               19350242905500,
                               75097467074637,
                               2556614854921,
                               37718643408480,
                               213620237510,
                               6724311130250,
                               25457500052952,
                               489516376431,
                               11514465298138,
                               717063515668,
                               26446350217560,
                               6756064528605,
                               33085247961173,
                               42923416365802,
                               24304783775635,
                               38572198977000,
                               30768510004640,
                               15169698546850,
                               11126988953456,
                               19972411935195,
                               3128825910344,
                               41008728739248,
                               9593284980500,
                               71039982484735,
                               32594940903700,
                               1833067856312,
                               30457700317302,
                               1581398691678,
                               20232754466668,
                               1176823804850,
                               15276320639355,
                               8945917123286,
                               18919341601824,
                               21678108350498,
                               11552006037852,
                               23919732805330,
                               11335293921846,
                               42481509203406,
                               2122540078032,
                               12644802041058,
                               57724114297590,
                               4260375471943,
                               22854679188447,
                               1679805810144,
                               20920530725100,
                               79680508970004,
                               4822078070025,
                               21021229791528,
                               51167989737960,
                               165090524909,
                               25873285104027,
                               46513810563360,
                               3575825256938,
                               34059047115978,
                               19806749124512,
                               78800618138484,
                               28322771215728,
                               1706728554744,
                               3278090395233,
                               23320617333774,
                               12703326279678,
                               2607629313708,
                               2539395442752,
                               28015716713200,
                               10300326647150,
                               30128043086820,
                               54595159345500,
                               13060160042787,
                               10655366027330,
                               17062988027150,
                               57029084625870,
                               7756250581905,
                               76771656574558,
                               4510750676015,
                               21874347427140,
                               3200391993042,
                               44103009370141,
                               57277463673026,
                               1018187154816,
                               46506687921519,
                               34421816854665,
                               2676955231764,
                               47301429307040,
                               3547510780001,
                               46639125075628,
                               17055804956334,
                               1646477501284,
                               2478078885625,
                               54398069002768,
                               31340183949950,
                               49221902712600,
                               36419457248232,
                               13122452465580,
                               10185432189606,
                               74024622661290,
                               17301062652176,
                               76970388164508,
                               62373552781812,
                               6841468343550,
                               59793356788000,
                               19225236462716,
                               32388898413606,
                               6187897297975,
                               6187897297975,
                               17420000862880,
                               53547183179125,
                               28894759120032,
                               24398977402333,
                               24813328904455,
                               28037560402170,
                               8374290597420,
                               4350223211799,
                               17455283786332,
                               63688413624321,
                               57562099045760,
                               33975883005192,
                               18354742508025,
                               16639404261926,
                               58997385417308,
                               6803575242456,
                               37764812164226,
                               84538481394123,
                               4965525257607,
                               121662980560,
                               7029409232880,
                               31220248346100,
                               68137270368096,
                               26037399196380,
                               44447384771070,
                               10695793870605,
                               25448023325214,
                               92664298126772,
                               7339073401512,
                               8807332093230,
                               10372293259380,
                               54867075540795,
                               497219901781,
                               69741834972537,
                               18310069153200,
                               3089795662998,
                               2555983074092,
                               27136123982275,
                               45588802330695,
                               784447977666,
                               8592621213364,
                               16042005794780,
                               48914398341200,
                               39352870745600,
                               13745169930000,
                               22896230019150,
                               3674063255112,
                               6936424673580,
                               20715482731050,
                               962371484361,
                               59719596050289,
                               24836189653956,
                               27890432977132,
                               10416225687015,
                               37371930733881,
                               16641855346480,
                               1243415213082,
                               7024777702423,
                               15056326461072,
                               237522450780,
                               8654097255216,
                               1935692634400,
                               48149720268700,
                               22018394580560,
                               4353414553674,
                               233342798248,
                               1689195367328,
                               73349430633813,
                               16579193709760,
                               47254775444604,
                               5751774863052,
                               4168272591177,
                               56466991266759,
                               10403807615696,
                               30368152985625,
                               23805779365764,
                               1751347115141,
                               686411646452,
                               10942582463904,
                               37051912941344,
                               66573514373520,
                               5193629914880,
                               10276936700310,
                               45333683755358,
                               5369872937250,
                               19870592423202,
                               44901818802729,
                               1984995522276,
                               38210789175216,
                               13140877390832,
                               36472949862774,
                               11003144677000,
                               34026969817136,
                               68246909413281,
                               9480783308290,
                               8927412760982,
                               44662728145200,
                               4559499822921,
                               13378660270086,
                               50432177305629,
                               33536986417717,
                               8548419242610,
                               43216481118384,
                               37549778463076,
                               2,
                               9223372036854775807};

    for (int32_t i = 0; i < 388; i++) {
        EXPECT_EQ(Status::OK(), _writer->write(write_data[i]));
    }
    EXPECT_EQ(Status::OK(), _writer->flush());

    // read data
    CreateReader();

    for (int32_t i = 0; i < 388; i++) {
        EXPECT_TRUE(_reader->has_next());
        int64_t value = 0;
        EXPECT_EQ(Status::OK(), _reader->next(&value));
        EXPECT_EQ(value, write_data[i]);
    }

    EXPECT_FALSE(_reader->has_next());
}

} // namespace doris
