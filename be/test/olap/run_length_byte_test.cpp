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
#include "olap/file_stream.h"
#include "olap/in_stream.h"
#include "olap/out_stream.h"
#include "olap/rowset/column_reader.h"
#include "olap/rowset/run_length_byte_reader.h"
#include "olap/rowset/run_length_byte_writer.h"
#include "olap/stream_index_reader.h"
#include "olap/stream_index_writer.h"
#include "util/logging.h"

namespace doris {

using namespace testing;

TEST(TestStream, UncompressOutStream) {
    // write data
    OutStream* out_stream =
            new (std::nothrow) OutStream(OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE, nullptr);
    ASSERT_TRUE(out_stream != nullptr);
    ASSERT_TRUE(out_stream->_compressor == nullptr);

    out_stream->write(0x5a);
    out_stream->flush();

    ASSERT_EQ(out_stream->get_stream_length(), sizeof(StreamHead) + 1);

    ASSERT_EQ(out_stream->output_buffers().size(), 1);

    std::vector<StorageByteBuffer*>::const_iterator it = out_stream->output_buffers().begin();
    ASSERT_EQ((*it)->position(), 0);
    StreamHead head;
    (*it)->get((char*)&head, sizeof(head));
    ASSERT_EQ(head.type, StreamHead::UNCOMPRESSED);
    ASSERT_EQ(head.length, 1);
    char data;
    ASSERT_EQ(OLAP_SUCCESS, (*it)->get((char*)&data));
    ASSERT_EQ(0x5A, data);
    ASSERT_NE(OLAP_SUCCESS, (*it)->get((char*)&data));

    SAFE_DELETE(out_stream);
}

TEST(TestStream, UncompressOutStream2) {
    // write data
    OutStream* out_stream =
            new (std::nothrow) OutStream(OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE, nullptr);
    ASSERT_TRUE(out_stream != nullptr);
    ASSERT_TRUE(out_stream->_compressor == nullptr);

    for (int32_t i = 0; i < OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE; i++) {
        out_stream->write(0x5a);
    }
    out_stream->write(0x5a);
    out_stream->flush();

    uint64_t stream_length = sizeof(StreamHead) * 2 + OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE + 1;
    ASSERT_EQ(out_stream->get_stream_length(), stream_length);

    ASSERT_EQ(out_stream->output_buffers().size(), 2);

    std::vector<StorageByteBuffer*> inputs;
    for (const auto& it : out_stream->output_buffers()) {
        inputs.push_back(StorageByteBuffer::reference_buffer(it, 0, it->limit()));
    }

    std::vector<uint64_t> offsets;
    offsets.push_back(0);
    offsets.push_back(sizeof(StreamHead) + OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE);
    InStream* in_stream =
            new (std::nothrow) InStream(&inputs, offsets, out_stream->get_stream_length(), nullptr,
                                        out_stream->get_total_buffer_size());

    char data;
    for (int32_t i = 0; i < OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE - 1; i++) {
        ASSERT_EQ(in_stream->read(&data), OLAP_SUCCESS);
        ASSERT_EQ(data, 0x5a);
    }
    ASSERT_EQ(in_stream->read(&data), OLAP_SUCCESS);
    ASSERT_EQ(data, 0x5a);
    ASSERT_EQ(in_stream->read(&data), OLAP_SUCCESS);
    ASSERT_EQ(data, 0x5a);

    ASSERT_NE(in_stream->read(&data), OLAP_SUCCESS);

    SAFE_DELETE(out_stream);
    SAFE_DELETE(in_stream);
    for (auto input : inputs) {
        delete input;
    }
}

TEST(TestStream, UncompressOutStream3) {
    // write data
    OutStream* out_stream =
            new (std::nothrow) OutStream(OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE, nullptr);
    ASSERT_TRUE(out_stream != nullptr);
    ASSERT_TRUE(out_stream->_compressor == nullptr);

    for (int32_t i = 0; i < OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE; i++) {
        out_stream->write(0x5a);
    }
    char write_data[2] = {0x5a, 0x5a};
    out_stream->write(write_data, 2);
    out_stream->flush();

    uint64_t stream_length = sizeof(StreamHead) * 2 + OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE + 2;
    ASSERT_EQ(out_stream->get_stream_length(), stream_length);

    ASSERT_EQ(out_stream->output_buffers().size(), 2);

    std::vector<StorageByteBuffer*> inputs;
    for (const auto& it : out_stream->output_buffers()) {
        inputs.push_back(StorageByteBuffer::reference_buffer(it, 0, it->limit()));
    }

    std::vector<uint64_t> offsets;
    offsets.push_back(0);
    offsets.push_back(sizeof(StreamHead) + OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE);
    InStream* in_stream =
            new (std::nothrow) InStream(&inputs, offsets, out_stream->get_stream_length(), nullptr,
                                        out_stream->get_total_buffer_size());

    char data;
    for (int32_t i = 0; i < OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE; i++) {
        ASSERT_EQ(in_stream->read(&data), OLAP_SUCCESS);
        ASSERT_EQ(data, 0x5a);
    }
    ASSERT_EQ(in_stream->read(&data), OLAP_SUCCESS);
    ASSERT_EQ(data, 0x5a);

    ASSERT_EQ(in_stream->read(&data), OLAP_SUCCESS);
    ASSERT_EQ(data, 0x5a);

    ASSERT_NE(in_stream->read(&data), OLAP_SUCCESS);

    SAFE_DELETE(in_stream);
    SAFE_DELETE(out_stream);
    for (auto input : inputs) {
        delete input;
    }
}

TEST(TestStream, UncompressInStream) {
    // write data
    OutStream* out_stream =
            new (std::nothrow) OutStream(OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE, nullptr);
    ASSERT_TRUE(out_stream != nullptr);
    ASSERT_TRUE(out_stream->_compressor == nullptr);

    out_stream->write(0x5a);
    out_stream->flush();

    // read data
    std::vector<StorageByteBuffer*> inputs;
    const auto& it = out_stream->output_buffers().begin();
    ASSERT_NE(it, out_stream->output_buffers().end());
    inputs.push_back(StorageByteBuffer::reference_buffer(*it, 0, (*it)->capacity()));

    std::vector<uint64_t> offsets;
    offsets.assign(inputs.size(), 0);
    InStream* in_stream =
            new (std::nothrow) InStream(&inputs, offsets, out_stream->get_stream_length(), nullptr,
                                        out_stream->get_total_buffer_size());
    SAFE_DELETE(out_stream);

    ASSERT_EQ(in_stream->available(), 1);
    char data;
    ASSERT_EQ(in_stream->read(&data), OLAP_SUCCESS);
    ASSERT_EQ(data, 0x5a);

    SAFE_DELETE(in_stream);
    for (auto input : inputs) {
        delete input;
    }
}

// the length after compress must be smaller than original stream, then the compressor will be called.
TEST(TestStream, CompressOutStream) {
    // write data
    OutStream* out_stream =
            new (std::nothrow) OutStream(OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE, lz4_compress);
    ASSERT_TRUE(out_stream != nullptr);
    ASSERT_TRUE(out_stream->_compressor != nullptr);

    char* write_data = new char[OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE];
    memset(write_data, 0x5a, OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE);

    out_stream->write(write_data, OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE);
    out_stream->flush();

    //ASSERT_EQ(out_stream->get_stream_length(), sizeof(StreamHead) + 2);

    //ASSERT_EQ(out_stream->output_buffers().size(), 1);

    std::vector<StorageByteBuffer*>::const_iterator it = out_stream->output_buffers().begin();

    StreamHead head;
    (*it)->get((char*)&head, sizeof(head));
    ASSERT_EQ(head.type, StreamHead::COMPRESSED);
    // if lzo, this should be 49
    ASSERT_EQ(51, head.length);

    SAFE_DELETE_ARRAY(write_data);
    SAFE_DELETE(out_stream);
}

TEST(TestStream, CompressOutStream2) {
    // write data
    OutStream* out_stream =
            new (std::nothrow) OutStream(OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE, lz4_compress);
    ASSERT_TRUE(out_stream != nullptr);
    ASSERT_TRUE(out_stream->_compressor != nullptr);

    for (int32_t i = 0; i < OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE; i++) {
        out_stream->write(0x5a);
    }
    out_stream->write(0x5a);
    out_stream->flush();

    std::vector<StorageByteBuffer*> inputs;
    for (const auto& it : out_stream->output_buffers()) {
        inputs.push_back(StorageByteBuffer::reference_buffer(it, 0, it->limit()));
    }

    std::vector<uint64_t> offsets;
    offsets.push_back(0);
    offsets.push_back(59); // if lzo, this should be 57
    InStream* in_stream =
            new (std::nothrow) InStream(&inputs, offsets, out_stream->get_stream_length(),
                                        lz4_decompress, out_stream->get_total_buffer_size());

    char data;
    for (int32_t i = 0; i < OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE; i++) {
        ASSERT_EQ(in_stream->read(&data), OLAP_SUCCESS);
        ASSERT_EQ(data, 0x5a);
    }
    ASSERT_EQ(in_stream->read(&data), OLAP_SUCCESS);
    ASSERT_EQ(data, 0x5a);

    ASSERT_NE(in_stream->read(&data), OLAP_SUCCESS);

    SAFE_DELETE(in_stream);
    SAFE_DELETE(out_stream);
    for (auto input : inputs) {
        delete input;
    }
}

TEST(TestStream, CompressOutStream3) {
    // write data
    OutStream* out_stream =
            new (std::nothrow) OutStream(OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE, lz4_compress);
    ASSERT_TRUE(out_stream != nullptr);
    ASSERT_TRUE(out_stream->_compressor != nullptr);

    for (int32_t i = 0; i < OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE; i++) {
        out_stream->write(0x5a);
    }
    char write_data[100];
    for (int32_t i = 0; i < sizeof(write_data); i++) {
        write_data[i] = 0x5a;
    }
    out_stream->write(write_data, sizeof(write_data));
    out_stream->flush();

    std::vector<StorageByteBuffer*> inputs;
    for (const auto& it : out_stream->output_buffers()) {
        inputs.push_back(StorageByteBuffer::reference_buffer(it, 0, it->limit()));
    }

    std::vector<uint64_t> offsets;
    offsets.push_back(0);
    offsets.push_back(57);
    InStream* in_stream =
            new (std::nothrow) InStream(&inputs, offsets, out_stream->get_stream_length(),
                                        lz4_decompress, out_stream->get_total_buffer_size());

    char data;
    for (int32_t i = 0; i < OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE; i++) {
        ASSERT_EQ(in_stream->read(&data), OLAP_SUCCESS);
        ASSERT_EQ(data, 0x5a);
    }
    for (int32_t i = 0; i < sizeof(write_data); i++) {
        ASSERT_EQ(in_stream->read(&data), OLAP_SUCCESS);
        ASSERT_EQ(data, write_data[i]);
    }

    ASSERT_NE(in_stream->read(&data), OLAP_SUCCESS);

    SAFE_DELETE(in_stream);
    SAFE_DELETE(out_stream);
    for (auto input : inputs) {
        delete input;
    }
}

//test for _slice() in [while (len > 0 && m_current_range < m_inputs.size())]
TEST(TestStream, CompressOutStream4) {
    // write data
    OutStream* out_stream = new (std::nothrow) OutStream(18, lz4_compress);
    ASSERT_TRUE(out_stream != nullptr);
    ASSERT_TRUE(out_stream->_compressor != nullptr);

    for (int32_t i = 0; i < 15; i++) {
        out_stream->write(0x5a);
    }
    out_stream->_spill();

    for (int32_t i = 0; i < 12; i++) {
        out_stream->write(0x5a);
    }
    for (int32_t i = 0; i < 6; i++) {
        out_stream->write(i);
    }
    out_stream->flush();

    std::vector<StorageByteBuffer*> inputs;
    for (const auto& it : out_stream->output_buffers()) {
        inputs.push_back(StorageByteBuffer::reference_buffer(it, 0, it->limit()));
    }

    std::vector<uint64_t> offsets;
    offsets.push_back(0);
    offsets.push_back(16);
    InStream* in_stream =
            new (std::nothrow) InStream(&inputs, offsets, out_stream->get_stream_length(),
                                        lz4_decompress, out_stream->get_total_buffer_size());

    char data;
    for (int32_t i = 0; i < 15; i++) {
        ASSERT_EQ(in_stream->read(&data), OLAP_SUCCESS);
        ASSERT_EQ(data, 0x5a);
    }

    for (int32_t i = 0; i < 12; i++) {
        ASSERT_EQ(in_stream->read(&data), OLAP_SUCCESS);
        ASSERT_EQ(data, 0x5a);
    }

    for (int32_t i = 0; i < 6; i++) {
        ASSERT_EQ(in_stream->read(&data), OLAP_SUCCESS);
        ASSERT_EQ(data, i);
    }

    ASSERT_NE(in_stream->read(&data), OLAP_SUCCESS);

    SAFE_DELETE(in_stream);
    SAFE_DELETE(out_stream);
    for (auto input : inputs) {
        delete input;
    }
}

TEST(TestStream, CompressMassOutStream) {
    // write data
    OutStream* out_stream = new (std::nothrow) OutStream(100, lz4_compress);
    ASSERT_TRUE(out_stream != nullptr);
    ASSERT_TRUE(out_stream->_compressor != nullptr);

    for (int32_t i = 0; i < 100; i++) {
        out_stream->write(0x5a);
    }
    //out_stream->write(0);

    for (int32_t i = 0; i < 100; i++) {
        out_stream->write(i);
    }
    //out_stream->write(100);
    out_stream->flush();

    std::vector<StorageByteBuffer*> inputs;
    for (const auto& it : out_stream->output_buffers()) {
        inputs.push_back(StorageByteBuffer::reference_buffer(it, 0, it->limit()));
    }
    std::vector<uint64_t> offsets;
    offsets.push_back(0);
    offsets.push_back(19); // if lzo, this should be 17
    InStream* in_stream =
            new (std::nothrow) InStream(&inputs, offsets, out_stream->get_stream_length(),
                                        lz4_decompress, out_stream->get_total_buffer_size());
    SAFE_DELETE(out_stream);

    char data;
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(in_stream->read(&data), OLAP_SUCCESS);
        ASSERT_EQ(data, 0x5a);
    }
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(in_stream->read(&data), OLAP_SUCCESS);
        ASSERT_EQ(data, i);
    }

    ASSERT_NE(in_stream->read(&data), OLAP_SUCCESS);

    SAFE_DELETE(in_stream);
    for (auto input : inputs) {
        delete input;
    }
}

TEST(TestStream, CompressInStream) {
    // write data
    OutStream* out_stream =
            new (std::nothrow) OutStream(OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE, lz4_compress);
    ASSERT_TRUE(out_stream != nullptr);
    ASSERT_TRUE(out_stream->_compressor != nullptr);

    char* write_data = new char[OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE];
    memset(write_data, 0x5a, OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE);

    out_stream->write(write_data, OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE);
    out_stream->flush();

    // read data
    std::vector<StorageByteBuffer*> inputs;
    std::vector<StorageByteBuffer*>::const_iterator it = out_stream->output_buffers().begin();
    ASSERT_NE(it, out_stream->output_buffers().end());
    StorageByteBuffer* tmp_byte_buffer =
            StorageByteBuffer::reference_buffer(*it, 0, (*it)->capacity());
    inputs.push_back(tmp_byte_buffer);

    std::vector<uint64_t> offsets;
    offsets.assign(inputs.size(), 0);
    InStream* in_stream =
            new (std::nothrow) InStream(&inputs, offsets, out_stream->get_stream_length(),
                                        lz4_decompress, out_stream->get_total_buffer_size());
    ASSERT_EQ(in_stream->available(), OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE);
    char data;
    for (int32_t i = 0; i < OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE - 1; ++i) {
        ASSERT_EQ(in_stream->read(&data), OLAP_SUCCESS);
        ASSERT_EQ(data, 0x5a);
    }
    ASSERT_EQ(in_stream->read(&data), OLAP_SUCCESS);
    ASSERT_EQ(data, 0x5a);
    ASSERT_NE(in_stream->read(&data), OLAP_SUCCESS);

    SAFE_DELETE_ARRAY(write_data);
    SAFE_DELETE(out_stream);
    SAFE_DELETE(in_stream);
    for (auto input : inputs) {
        delete input;
    }
}

TEST(TestStream, SeekUncompress) {
    // write data
    OutStream* out_stream =
            new (std::nothrow) OutStream(OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE, nullptr);
    ASSERT_TRUE(out_stream != nullptr);
    ASSERT_TRUE(out_stream->_compressor == nullptr);

    out_stream->write(0x5a);

    PositionEntryWriter index_entry;
    out_stream->get_position(&index_entry);
    out_stream->write(0x5b);
    ASSERT_EQ(index_entry.positions_count(), 2);
    ASSERT_EQ(index_entry.positions(0), 0);
    ASSERT_EQ(index_entry.positions(1), 1);
    out_stream->flush();

    // read data
    std::vector<StorageByteBuffer*> inputs;
    std::vector<StorageByteBuffer*>::const_iterator it = out_stream->output_buffers().begin();
    ASSERT_NE(it, out_stream->output_buffers().end());
    StorageByteBuffer* tmp_byte_buffer =
            StorageByteBuffer::reference_buffer(*it, 0, (*it)->capacity());
    inputs.push_back(tmp_byte_buffer);

    std::vector<uint64_t> offsets;
    offsets.assign(inputs.size(), 0);
    InStream* in_stream =
            new (std::nothrow) InStream(&inputs, offsets, out_stream->get_stream_length(), nullptr,
                                        out_stream->get_total_buffer_size());
    ASSERT_EQ(in_stream->available(), 2);

    char buffer[256];
    index_entry.write_to_buffer(buffer);
    StreamIndexHeader header;
    header.position_format = index_entry.positions_count();
    header.statistic_format = OLAP_FIELD_TYPE_TINYINT;
    PositionEntryReader entry;
    entry.init(&header, OLAP_FIELD_TYPE_TINYINT, false);
    entry.attach(buffer);
    PositionProvider position(&entry);

    ASSERT_EQ(entry.positions_count(), 2);
    ASSERT_EQ(entry.positions(0), 0);
    ASSERT_EQ(entry.positions(1), 1);

    in_stream->seek(&position);
    char data;
    ASSERT_EQ(in_stream->read(&data), OLAP_SUCCESS);
    ASSERT_EQ(data, 0x5b);
    SAFE_DELETE(out_stream);
    SAFE_DELETE(in_stream);
    for (auto input : inputs) {
        delete input;
    }
}

TEST(TestStream, SkipUncompress) {
    // write data
    OutStream* out_stream =
            new (std::nothrow) OutStream(OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE, nullptr);
    ASSERT_TRUE(out_stream != nullptr);
    ASSERT_TRUE(out_stream->_compressor == nullptr);

    char write_data[] = {0x5a, 0x5b, 0x5c, 0x5d};
    for (int32_t i = 0; i < sizeof(write_data); i++) {
        out_stream->write(write_data[i]);
    }
    out_stream->write(0x5e);
    out_stream->flush();

    // read data
    std::vector<StorageByteBuffer*> inputs;
    std::vector<StorageByteBuffer*>::const_iterator it = out_stream->output_buffers().begin();
    ASSERT_NE(it, out_stream->output_buffers().end());
    StorageByteBuffer* tmp_byte_buffer =
            StorageByteBuffer::reference_buffer(*it, 0, (*it)->capacity());
    inputs.push_back(tmp_byte_buffer);

    std::vector<uint64_t> offsets;
    offsets.assign(inputs.size(), 0);
    InStream* in_stream =
            new (std::nothrow) InStream(&inputs, offsets, out_stream->get_stream_length(), nullptr,
                                        out_stream->get_total_buffer_size());
    ASSERT_EQ(in_stream->available(), sizeof(write_data) + 1);
    in_stream->skip(sizeof(write_data) - 1);
    char data;
    ASSERT_EQ(in_stream->read(&data), OLAP_SUCCESS);
    ASSERT_EQ(data, write_data[sizeof(write_data) - 1]);
    SAFE_DELETE(out_stream);
    SAFE_DELETE(in_stream);
    for (auto input : inputs) {
        delete input;
    }
}

TEST(TestStream, SeekCompress) {
    // write data
    OutStream* out_stream =
            new (std::nothrow) OutStream(OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE, lz4_compress);
    ASSERT_TRUE(out_stream != nullptr);

    for (int32_t i = 0; i < 10; i++) {
        out_stream->write(0x5a);
    }

    PositionEntryWriter index_entry;
    out_stream->get_position(&index_entry);
    out_stream->write(0x5b);
    ASSERT_EQ(index_entry.positions_count(), 2);
    ASSERT_EQ(index_entry.positions(0), 0);
    ASSERT_EQ(index_entry.positions(1), 10);
    out_stream->flush();

    // read data
    std::vector<StorageByteBuffer*> inputs;
    std::vector<StorageByteBuffer*>::const_iterator it = out_stream->output_buffers().begin();
    ASSERT_NE(it, out_stream->output_buffers().end());
    StorageByteBuffer* tmp_byte_buffer =
            StorageByteBuffer::reference_buffer(*it, 0, (*it)->capacity());
    inputs.push_back(tmp_byte_buffer);

    std::vector<uint64_t> offsets;
    offsets.assign(inputs.size(), 0);
    InStream* in_stream =
            new (std::nothrow) InStream(&inputs, offsets, out_stream->get_stream_length(),
                                        lz4_decompress, out_stream->get_total_buffer_size());
    //ASSERT_EQ(in_stream->available(), 2);
    char buffer[256];
    index_entry.write_to_buffer(buffer);
    StreamIndexHeader header;
    header.position_format = index_entry.positions_count();
    header.statistic_format = OLAP_FIELD_TYPE_TINYINT;
    PositionEntryReader entry;
    entry.init(&header, OLAP_FIELD_TYPE_TINYINT, false);
    entry.attach(buffer);

    PositionProvider position(&entry);
    in_stream->seek(&position);
    char data;
    ASSERT_EQ(in_stream->read(&data), OLAP_SUCCESS);
    ASSERT_EQ(data, 0x5b);
    SAFE_DELETE(out_stream);
    SAFE_DELETE(in_stream);
    for (auto input : inputs) {
        delete input;
    }
}

TEST(TestStream, SkipCompress) {
    // write data
    OutStream* out_stream =
            new (std::nothrow) OutStream(OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE, lz4_compress);
    ASSERT_TRUE(out_stream != nullptr);

    for (int32_t i = 0; i < 10; i++) {
        out_stream->write(0x5a);
    }
    out_stream->write(0x5e);
    out_stream->flush();

    // read data
    std::vector<StorageByteBuffer*> inputs;
    std::vector<StorageByteBuffer*>::const_iterator it = out_stream->output_buffers().begin();
    ASSERT_NE(it, out_stream->output_buffers().end());
    StorageByteBuffer* tmp_byte_buffer =
            StorageByteBuffer::reference_buffer(*it, 0, (*it)->capacity());
    inputs.push_back(tmp_byte_buffer);

    std::vector<uint64_t> offsets;
    offsets.assign(inputs.size(), 0);
    InStream* in_stream =
            new (std::nothrow) InStream(&inputs, offsets, out_stream->get_stream_length(),
                                        lz4_decompress, out_stream->get_total_buffer_size());

    in_stream->skip(10);
    char data;
    ASSERT_EQ(in_stream->read(&data), OLAP_SUCCESS);
    ASSERT_EQ(data, 0x5e);

    SAFE_DELETE(out_stream);
    SAFE_DELETE(in_stream);
    for (auto input : inputs) {
        delete input;
    }
}

class TestRunLengthByte : public testing::Test {
public:
    TestRunLengthByte() {}

    virtual ~TestRunLengthByte() {}

    virtual void SetUp() {
        ASSERT_EQ(system("mkdir -p ./ut_dir"), 0);
        ASSERT_EQ(system("rm -rf ./ut_dir/tmp_file"), 0);
        _out_stream = new (std::nothrow) OutStream(OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE, nullptr);
        ASSERT_TRUE(_out_stream != nullptr);
        _writer = new (std::nothrow) RunLengthByteWriter(_out_stream);
        ASSERT_TRUE(_writer != nullptr);
    }

    virtual void TearDown() {
        SAFE_DELETE(_reader);
        SAFE_DELETE(_out_stream);
        SAFE_DELETE(_writer);
        SAFE_DELETE(_shared_buffer);
        SAFE_DELETE(_stream);
    }

    void CreateReader() {
        ASSERT_EQ(OLAP_SUCCESS,
                  helper.open_with_mode(_file_path.c_str(), O_CREAT | O_EXCL | O_WRONLY,
                                        S_IRUSR | S_IWUSR));
        _out_stream->write_to_file(&helper, 0);
        helper.close();

        ASSERT_EQ(OLAP_SUCCESS,
                  helper.open_with_mode(_file_path.c_str(), O_RDONLY, S_IRUSR | S_IWUSR));

        _shared_buffer = StorageByteBuffer::create(OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE +
                                                   sizeof(StreamHead));
        ASSERT_TRUE(_shared_buffer != nullptr);

        _stream = new (std::nothrow)
                ReadOnlyFileStream(&helper, &_shared_buffer, 0, helper.length(), nullptr,
                                   OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE, &_stats);
        ASSERT_EQ(OLAP_SUCCESS, _stream->init());

        _reader = new (std::nothrow) RunLengthByteReader(_stream);
        ASSERT_TRUE(_reader != nullptr);
    }

    RunLengthByteReader* _reader;
    OutStream* _out_stream;
    RunLengthByteWriter* _writer;
    FileHandler helper;
    StorageByteBuffer* _shared_buffer;
    ReadOnlyFileStream* _stream;
    OlapReaderStatistics _stats;

    std::string _file_path = "./ut_dir/tmp_file";
};

TEST_F(TestRunLengthByte, ReadWriteOneByte) {
    _writer->write(0x5a);
    _writer->flush();
    CreateReader();

    ASSERT_TRUE(_reader->has_next());
    char value = 0xff;
    ASSERT_EQ(OLAP_SUCCESS, _reader->next(&value));
    ASSERT_EQ(value, 0X5A);

    ASSERT_FALSE(_reader->has_next());
}

TEST_F(TestRunLengthByte, ReadWriteMultiBytes) {
    // write data
    char write_data[] = {0x5a, 0x5b, 0x5c, 0x5d};
    for (int32_t i = 0; i < sizeof(write_data); i++) {
        _writer->write(write_data[i]);
    }

    _writer->flush();

    // the stream contain head, control byte and four byte literal
    ASSERT_EQ(_out_stream->get_stream_length(), sizeof(StreamHead) + 1 + 4);

    // read data
    CreateReader();

    for (int32_t i = 0; i < sizeof(write_data); i++) {
        ASSERT_TRUE(_reader->has_next());
        char value = 0xff;
        ASSERT_EQ(OLAP_SUCCESS, _reader->next(&value));
        ASSERT_EQ(value, write_data[i]);
    }

    ASSERT_FALSE(_reader->has_next());
}

TEST_F(TestRunLengthByte, ReadWriteSameBytes) {
    // write data
    char write_data[] = {0x5a, 0x5a, 0x5a, 0x5a};
    for (int32_t i = 0; i < sizeof(write_data); i++) {
        _writer->write(write_data[i]);
    }

    _writer->flush();

    // the stream contain head, control byte(4-3) and one byte literal
    ASSERT_EQ(_out_stream->get_stream_length(), sizeof(StreamHead) + 1 + 1);

    // read data
    CreateReader();

    for (int32_t i = 0; i < sizeof(write_data); i++) {
        ASSERT_TRUE(_reader->has_next());
        char value = 0xff;
        ASSERT_EQ(OLAP_SUCCESS, _reader->next(&value));
        ASSERT_EQ(value, write_data[i]);
    }

    ASSERT_FALSE(_reader->has_next());
}

TEST_F(TestRunLengthByte, Seek) {
    // write data
    char write_data[] = {0x5a, 0x5b, 0x5c, 0x5d};
    for (int32_t i = 0; i < sizeof(write_data); i++) {
        _writer->write(write_data[i]);
    }

    PositionEntryWriter index_entry;
    _writer->get_position(&index_entry);
    _writer->write(0x5e);

    _writer->write(0x5f);
    _writer->write(0x60);
    _writer->write(0x61);

    _writer->flush();

    // read data
    CreateReader();

    char buffer[256];
    index_entry.write_to_buffer(buffer);
    StreamIndexHeader header;
    header.position_format = index_entry.positions_count();
    header.statistic_format = OLAP_FIELD_TYPE_TINYINT;
    PositionEntryReader entry;
    entry.init(&header, OLAP_FIELD_TYPE_TINYINT, false);
    entry.attach(buffer);

    PositionProvider position(&entry);
    _reader->seek(&position);
    char value = 0xff;
    ASSERT_EQ(OLAP_SUCCESS, _reader->next(&value));
    ASSERT_EQ(value, 0x5e);
    ASSERT_EQ(OLAP_SUCCESS, _reader->next(&value));
    ASSERT_EQ(value, 0x5f);
    ASSERT_EQ(OLAP_SUCCESS, _reader->next(&value));
    ASSERT_EQ(value, 0x60);
    ASSERT_EQ(OLAP_SUCCESS, _reader->next(&value));
    ASSERT_EQ(value, 0x61);
}

TEST_F(TestRunLengthByte, Skip) {
    // write data
    char write_data[] = {0x5a, 0x5b, 0x5c, 0x5d};
    for (int32_t i = 0; i < sizeof(write_data); i++) {
        _writer->write(write_data[i]);
    }
    _writer->write(0x5e);
    _writer->flush();

    // read data
    CreateReader();

    _reader->skip(sizeof(write_data) - 1);
    char value = 0xff;
    ASSERT_EQ(OLAP_SUCCESS, _reader->next(&value));
    ASSERT_EQ(value, write_data[sizeof(write_data) - 1]);
    ASSERT_EQ(OLAP_SUCCESS, _reader->next(&value));
    ASSERT_EQ(value, 0x5e);
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
