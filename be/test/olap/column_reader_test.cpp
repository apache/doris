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

#include "olap/rowset/column_reader.h"

#include <gtest/gtest.h>

#include "olap/byte_buffer.h"
#include "olap/field.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/row_block.h"
#include "olap/row_cursor.h"
#include "olap/rowset/column_writer.h"
#include "olap/stream_name.h"
#include "runtime/mem_pool.h"
#include "runtime/string_value.hpp"
#include "runtime/vectorized_row_batch.h"
#include "util/logging.h"

using std::string;

namespace doris {

class TestColumn : public testing::Test {
public:
    TestColumn() : _column_writer(nullptr), _column_reader(nullptr), _stream_factory(nullptr) {
        _offsets.clear();
        _mem_tracker.reset(new MemTracker(-1));
        _mem_pool.reset(new MemPool(_mem_tracker.get()));
    }

    virtual ~TestColumn() {
        SAFE_DELETE(_column_writer);
        SAFE_DELETE(_column_reader);
        SAFE_DELETE(_stream_factory);
    }

    virtual void SetUp() {
        _offsets.push_back(0);

        _stream_factory = new (std::nothrow)
                OutStreamFactory(COMPRESS_LZ4, OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE);
        ASSERT_TRUE(_stream_factory != nullptr);
        config::column_dictionary_key_ratio_threshold = 30;
        config::column_dictionary_key_size_threshold = 1000;
    }

    virtual void TearDown() {
        SAFE_DELETE(_column_writer);
        SAFE_DELETE(_column_reader);
        SAFE_DELETE(_stream_factory);
        SAFE_DELETE(_shared_buffer);

        _offsets.clear();
        for (auto in_stream : _map_in_streams) {
            delete in_stream.second;
        }
        _map_in_streams.clear();
        _present_buffers.clear();
        _data_buffers.clear();
        _second_buffers.clear();
        _dictionary_buffers.clear();
        _length_buffers.clear();
    }

    void create_column_writer(const TabletSchema& tablet_schema) {
        _column_writer = ColumnWriter::create(0, tablet_schema, _stream_factory, 1024,
                                              BLOOM_FILTER_DEFAULT_FPP);

        ASSERT_TRUE(_column_writer != nullptr);
        ASSERT_EQ(_column_writer->init(), OLAP_SUCCESS);
    }

    void create_column_reader(const TabletSchema& tablet_schema) {
        UniqueIdEncodingMap encodings;
        encodings[0] = ColumnEncodingMessage();
        encodings[0].set_kind(ColumnEncodingMessage::DIRECT);
        encodings[0].set_dictionary_size(1);
        create_column_reader(tablet_schema, encodings);
    }

    void create_column_reader(const TabletSchema& tablet_schema, UniqueIdEncodingMap& encodings) {
        UniqueIdToColumnIdMap included;
        included[0] = 0;
        UniqueIdToColumnIdMap segment_included;
        segment_included[0] = 0;

        _column_reader =
                ColumnReader::create(0, tablet_schema, included, segment_included, encodings);

        ASSERT_TRUE(_column_reader != nullptr);

        ASSERT_EQ(system("mkdir -p ./ut_dir"), 0);
        ASSERT_EQ(system("rm ./ut_dir/tmp_file"), 0);

        ASSERT_EQ(OLAP_SUCCESS,
                  helper.open_with_mode("./ut_dir/tmp_file", O_CREAT | O_EXCL | O_WRONLY,
                                        S_IRUSR | S_IWUSR));
        std::vector<int> off;
        std::vector<int> length;
        std::vector<int> buffer_size;
        std::vector<StreamName> name;

        std::map<StreamName, OutStream*>::const_iterator it = _stream_factory->streams().begin();
        for (; it != _stream_factory->streams().end(); ++it) {
            StreamName stream_name = it->first;
            OutStream* out_stream = it->second;
            std::vector<StorageByteBuffer*>* buffers = nullptr;

            if (out_stream->is_suppressed()) {
                continue;
            }

            if (stream_name.kind() == StreamInfoMessage::ROW_INDEX) {
                continue;
            } else if (stream_name.kind() == StreamInfoMessage::PRESENT) {
                buffers = &_present_buffers;
            } else if (stream_name.kind() == StreamInfoMessage::DATA) {
                buffers = &_data_buffers;
            } else if (stream_name.kind() == StreamInfoMessage::SECONDARY) {
                buffers = &_second_buffers;
            } else if (stream_name.kind() == StreamInfoMessage::DICTIONARY_DATA) {
                buffers = &_dictionary_buffers;
            } else if (stream_name.kind() == StreamInfoMessage::LENGTH) {
                buffers = &_length_buffers;
            } else {
                ASSERT_TRUE(false);
            }

            ASSERT_TRUE(buffers != nullptr);
            off.push_back(helper.tell());
            out_stream->write_to_file(&helper, 0);
            length.push_back(out_stream->get_stream_length());
            buffer_size.push_back(out_stream->get_total_buffer_size());
            name.push_back(stream_name);
        }
        helper.close();

        ASSERT_EQ(OLAP_SUCCESS,
                  helper.open_with_mode("./ut_dir/tmp_file", O_RDONLY, S_IRUSR | S_IWUSR));

        _shared_buffer = StorageByteBuffer::create(OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE +
                                                   sizeof(StreamHead));
        ASSERT_TRUE(_shared_buffer != nullptr);

        for (int i = 0; i < off.size(); ++i) {
            ReadOnlyFileStream* in_stream = new (std::nothrow)
                    ReadOnlyFileStream(&helper, &_shared_buffer, off[i], length[i], lz4_decompress,
                                       buffer_size[i], &_stats);
            ASSERT_EQ(OLAP_SUCCESS, in_stream->init());

            _map_in_streams[name[i]] = in_stream;
        }

        ASSERT_EQ(_column_reader->init(&_map_in_streams, 1024, _mem_pool.get(), &_stats),
                  OLAP_SUCCESS);
    }

    void set_tablet_schema_with_one_column(std::string name, std::string type,
                                           std::string aggregation, uint32_t length,
                                           bool is_allow_null, bool is_key,
                                           TabletSchema* tablet_schema) {
        TabletSchemaPB tablet_schema_pb;
        ColumnPB* column = tablet_schema_pb.add_column();
        column->set_unique_id(0);
        column->set_name(name);
        column->set_name(name);
        column->set_type(type);
        column->set_is_key(is_key);
        column->set_is_nullable(is_allow_null);
        column->set_length(length);
        column->set_aggregation(aggregation);
        column->set_precision(1000);
        column->set_frac(1000);
        column->set_is_bf_column(false);
        tablet_schema->init_from_pb(tablet_schema_pb);
    }

    void create_and_save_last_position() {
        ASSERT_EQ(_column_writer->create_row_index_entry(), OLAP_SUCCESS);
    }

    ColumnWriter* _column_writer;
    ColumnReader* _column_reader;
    std::shared_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<MemPool> _mem_pool;
    std::unique_ptr<ColumnVector> _col_vector;

    OutStreamFactory* _stream_factory;

    std::vector<size_t> _offsets;
    std::vector<StorageByteBuffer*> _present_buffers;
    std::vector<StorageByteBuffer*> _data_buffers;
    std::vector<StorageByteBuffer*> _second_buffers;
    std::vector<StorageByteBuffer*> _dictionary_buffers;
    std::vector<StorageByteBuffer*> _length_buffers;

    StorageByteBuffer* _shared_buffer;
    std::map<StreamName, ReadOnlyFileStream*> _map_in_streams;
    FileHandler helper;
    OlapReaderStatistics _stats;
};

TEST_F(TestColumn, VectorizedTinyColumnWithoutPresent) {
    // write data
    TabletSchema tablet_schema;
    set_tablet_schema_with_one_column("TinyColumn", "TINYINT", "REPLACE", 1, false, true,
                                      &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 1024;
    block.init(block_info);

    char value = 1;
    write_row.set_field_content(0, &value, _mem_pool.get());
    block.set_row(0, write_row);

    value = 3;
    write_row.set_field_content(0, &value, _mem_pool.get());
    block.set_row(1, write_row);
    block.finalize(2);

    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 2, _mem_pool.get()), OLAP_SUCCESS);
    char* data = reinterpret_cast<char*>(_col_vector->col_data());

    value = *reinterpret_cast<char*>(data);
    ASSERT_EQ(value, 1);

    data++;
    value = *reinterpret_cast<char*>(data);
    ASSERT_EQ(value, 3);
}

TEST_F(TestColumn, SeekTinyColumnWithoutPresent) {
    // write data
    TabletSchema tablet_schema;
    set_tablet_schema_with_one_column("TinyColumn", "TINYINT", "REPLACE", 1, false, true,
                                      &tablet_schema);

    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 1024;
    block.init(block_info);

    char value = 1;
    write_row.set_field_content(0, &value, _mem_pool.get());
    block.set_row(0, write_row);

    value = 2;
    write_row.set_field_content(0, &value, _mem_pool.get());
    block.set_row(1, write_row);
    block.finalize(2);

    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    create_and_save_last_position();

    value = 3;
    write_row.set_field_content(0, &value, _mem_pool.get());
    block.set_row(0, write_row);
    block.finalize(1);

    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    create_and_save_last_position();

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    PositionEntryReader entry1;
    entry1._positions = _column_writer->index()->mutable_entry(0)->_positions;
    entry1._positions_count = _column_writer->index()->mutable_entry(0)->_positions_count;
    entry1._statistics.init(OLAP_FIELD_TYPE_TINYINT, false);

    PositionEntryReader entry2;
    entry2._positions = _column_writer->index()->mutable_entry(1)->_positions;
    entry2._positions_count = _column_writer->index()->mutable_entry(1)->_positions_count;
    entry2._statistics.init(OLAP_FIELD_TYPE_TINYINT, false);

    PositionProvider position0(&entry1);
    PositionProvider position1(&entry2);

    ASSERT_EQ(_column_reader->seek(&position0), OLAP_SUCCESS);
    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 2, _mem_pool.get()), OLAP_SUCCESS);
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    value = *reinterpret_cast<char*>(data);
    ASSERT_EQ(value, 1);
    data++;
    value = *reinterpret_cast<char*>(data);
    ASSERT_EQ(value, 2);

    ASSERT_EQ(_column_reader->seek(&position1), OLAP_SUCCESS);
    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
    data = reinterpret_cast<char*>(_col_vector->col_data());
    value = *reinterpret_cast<char*>(data);
    ASSERT_EQ(value, 3);
}

TEST_F(TestColumn, SkipTinyColumnWithoutPresent) {
    // write data
    TabletSchema tablet_schema;
    set_tablet_schema_with_one_column("TinyColumn", "TINYINT", "REPLACE", 1, false, true,
                                      &tablet_schema);

    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 1024;
    block.init(block_info);

    char value = 1;
    write_row.set_field_content(0, &value, _mem_pool.get());
    block.set_row(0, write_row);

    value = 2;
    write_row.set_field_content(0, &value, _mem_pool.get());
    block.set_row(1, write_row);

    value = 3;
    write_row.set_field_content(0, &value, _mem_pool.get());
    block.set_row(2, write_row);
    block.finalize(3);

    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    ASSERT_EQ(_column_reader->skip(2), OLAP_SUCCESS);
    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    value = *reinterpret_cast<char*>(data);
    ASSERT_EQ(value, 3);
}

TEST_F(TestColumn, VectorizedTinyColumnWithPresent) {
    // write data
    TabletSchema tablet_schema;
    set_tablet_schema_with_one_column("TinyColumn", "TINYINT", "REPLACE", 1, true, true,
                                      &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 1024;
    block.init(block_info);

    write_row.set_null(0);
    block.set_row(0, write_row);

    write_row.set_not_null(0);
    char value = 3;
    write_row.set_field_content(0, &value, _mem_pool.get());
    block.set_row(1, write_row);
    block.finalize(2);

    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 2, _mem_pool.get()), OLAP_SUCCESS);
    bool* is_null = _col_vector->is_null();
    ASSERT_EQ(is_null[0], true);

    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    ASSERT_EQ(is_null[1], false);
    value = *reinterpret_cast<char*>(data + 1);
}

TEST_F(TestColumn, TinyColumnIndex) {
    // write data
    TabletSchema tablet_schema;
    set_tablet_schema_with_one_column("TinyColumn", "TINYINT", "REPLACE", 1, true, true,
                                      &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 1024;
    block.init(block_info);

    char value = 1;
    write_row.set_field_content(0, &value, _mem_pool.get());
    block.set_row(0, write_row);

    value = 3;
    write_row.set_field_content(0, &value, _mem_pool.get());
    block.set_row(1, write_row);
    block.finalize(2);

    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 2, _mem_pool.get()), OLAP_SUCCESS);
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    value = *reinterpret_cast<char*>(data);
    ASSERT_EQ(value, 1);

    value = *reinterpret_cast<char*>(data + 1);
    ASSERT_EQ(value, 3);
}

TEST_F(TestColumn, SeekTinyColumnWithPresent) {
    // write data
    TabletSchema tablet_schema;
    set_tablet_schema_with_one_column("TinyColumn", "TINYINT", "REPLACE", 1, true, true,
                                      &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 1024;
    block.init(block_info);

    char value = 1;
    write_row.set_field_content(0, &value, _mem_pool.get());
    block.set_row(0, write_row);

    value = 2;
    write_row.set_field_content(0, &value, _mem_pool.get());
    block.set_row(1, write_row);
    block.finalize(2);

    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    create_and_save_last_position();

    value = 3;
    write_row.set_field_content(0, &value, _mem_pool.get());
    block.set_row(0, write_row);
    block.finalize(1);

    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    create_and_save_last_position();

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    PositionEntryReader entry1;
    entry1._positions = _column_writer->index()->mutable_entry(0)->_positions;
    entry1._positions_count = _column_writer->index()->mutable_entry(0)->_positions_count;
    entry1._statistics.init(OLAP_FIELD_TYPE_TINYINT, false);

    PositionEntryReader entry2;
    entry2._positions = _column_writer->index()->mutable_entry(1)->_positions;
    entry2._positions_count = _column_writer->index()->mutable_entry(1)->_positions_count;
    entry2._statistics.init(OLAP_FIELD_TYPE_TINYINT, false);

    PositionProvider position1(&entry1);
    PositionProvider position2(&entry2);

    ASSERT_EQ(_column_reader->seek(&position1), OLAP_SUCCESS);
    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 2, _mem_pool.get()), OLAP_SUCCESS);
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    value = *reinterpret_cast<char*>(data);
    ASSERT_EQ(value, 1);
    value = *reinterpret_cast<char*>(data + 1);
    ASSERT_EQ(value, 2);

    ASSERT_EQ(_column_reader->seek(&position2), OLAP_SUCCESS);
    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
    data = reinterpret_cast<char*>(_col_vector->col_data());
    value = *reinterpret_cast<char*>(data);
    ASSERT_EQ(value, 3);
}

TEST_F(TestColumn, SkipTinyColumnWithPresent) {
    // write data
    TabletSchema tablet_schema;
    set_tablet_schema_with_one_column("TinyColumn", "TINYINT", "REPLACE", 1, true, true,
                                      &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 1024;
    block.init(block_info);

    char value = 1;
    write_row.set_field_content(0, &value, _mem_pool.get());
    block.set_row(0, write_row);

    value = 2;
    write_row.set_field_content(0, &value, _mem_pool.get());
    block.set_row(1, write_row);

    value = 3;
    write_row.set_field_content(0, &value, _mem_pool.get());
    block.set_row(2, write_row);
    block.finalize(3);

    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    ASSERT_EQ(_column_reader->skip(2), OLAP_SUCCESS);
    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    value = *reinterpret_cast<char*>(data);
    ASSERT_EQ(value, 3);
}

TEST_F(TestColumn, VectorizedShortColumnWithoutPresent) {
    // write data
    TabletSchema tablet_schema;
    set_tablet_schema_with_one_column("ShortColumn", "SMALLINT", "REPLACE", 2, false, true,
                                      &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 1024;
    block.init(block_info);

    int16_t value = 1;
    write_row.set_field_content(0, reinterpret_cast<char*>(&value), _mem_pool.get());
    block.set_row(0, write_row);

    value = 3;
    write_row.set_field_content(0, reinterpret_cast<char*>(&value), _mem_pool.get());
    block.set_row(1, write_row);
    block.finalize(2);

    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 2, _mem_pool.get()), OLAP_SUCCESS);
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    value = *reinterpret_cast<int16_t*>(data);
    ASSERT_EQ(value, 1);

    value = *reinterpret_cast<int16_t*>(data + sizeof(int16_t));
    ASSERT_EQ(value, 3);
}

TEST_F(TestColumn, SeekShortColumnWithoutPresent) {
    // write data
    TabletSchema tablet_schema;
    set_tablet_schema_with_one_column("ShortColumn", "SMALLINT", "REPLACE", 2, false, true,
                                      &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 1024;
    block.init(block_info);

    int16_t value = 1;
    write_row.set_field_content(0, reinterpret_cast<char*>(&value), _mem_pool.get());
    block.set_row(0, write_row);

    value = 2;
    write_row.set_field_content(0, reinterpret_cast<char*>(&value), _mem_pool.get());
    block.set_row(1, write_row);
    block.finalize(2);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    create_and_save_last_position();

    value = 3;
    write_row.set_field_content(0, reinterpret_cast<char*>(&value), _mem_pool.get());
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    create_and_save_last_position();

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    PositionEntryReader entry1;
    entry1._positions = _column_writer->index()->mutable_entry(0)->_positions;
    entry1._positions_count = _column_writer->index()->mutable_entry(0)->_positions_count;
    entry1._statistics.init(OLAP_FIELD_TYPE_SMALLINT, false);

    PositionEntryReader entry2;
    entry2._positions = _column_writer->index()->mutable_entry(1)->_positions;
    entry2._positions_count = _column_writer->index()->mutable_entry(1)->_positions_count;
    entry2._statistics.init(OLAP_FIELD_TYPE_SMALLINT, false);

    PositionProvider position0(&entry1);
    PositionProvider position1(&entry2);

    ASSERT_EQ(_column_reader->seek(&position0), OLAP_SUCCESS);
    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 2, _mem_pool.get()), OLAP_SUCCESS);
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    value = *reinterpret_cast<char*>(data);
    ASSERT_EQ(value, 1);

    value = *reinterpret_cast<char*>(data + sizeof(int16_t));
    ASSERT_EQ(value, 2);

    ASSERT_EQ(_column_reader->seek(&position1), OLAP_SUCCESS);
    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
    data = reinterpret_cast<char*>(_col_vector->col_data());
    value = *reinterpret_cast<char*>(data);
    ASSERT_EQ(value, 3);
}

TEST_F(TestColumn, SkipShortColumnWithoutPresent) {
    // write data
    TabletSchema tablet_schema;
    set_tablet_schema_with_one_column("ShortColumn", "SMALLINT", "REPLACE", 2, false, true,
                                      &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 1024;
    block.init(block_info);

    int16_t value = 1;
    write_row.set_field_content(0, reinterpret_cast<char*>(&value), _mem_pool.get());
    block.set_row(0, write_row);

    value = 2;
    write_row.set_field_content(0, reinterpret_cast<char*>(&value), _mem_pool.get());
    block.set_row(1, write_row);

    value = 3;
    write_row.set_field_content(0, reinterpret_cast<char*>(&value), _mem_pool.get());
    block.set_row(2, write_row);
    block.finalize(3);

    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    ASSERT_EQ(_column_reader->skip(2), OLAP_SUCCESS);
    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    value = *reinterpret_cast<char*>(data);
    ASSERT_EQ(value, 3);
}

TEST_F(TestColumn, SeekShortColumnWithPresent) {
    // write data
    TabletSchema tablet_schema;
    set_tablet_schema_with_one_column("ShortColumn", "SMALLINT", "REPLACE", 2, true, true,
                                      &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 1024;
    block.init(block_info);

    int16_t value = 1;
    write_row.set_field_content(0, reinterpret_cast<char*>(&value), _mem_pool.get());
    block.set_row(0, write_row);

    value = 2;
    write_row.set_field_content(0, reinterpret_cast<char*>(&value), _mem_pool.get());
    block.set_row(1, write_row);
    block.finalize(2);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    create_and_save_last_position();

    value = 3;
    write_row.set_field_content(0, reinterpret_cast<char*>(&value), _mem_pool.get());
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    create_and_save_last_position();

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data

    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    PositionEntryReader entry1;
    entry1._positions = _column_writer->index()->mutable_entry(0)->_positions;
    entry1._positions_count = _column_writer->index()->mutable_entry(0)->_positions_count;
    entry1._statistics.init(OLAP_FIELD_TYPE_SMALLINT, false);

    PositionEntryReader entry2;
    entry2._positions = _column_writer->index()->mutable_entry(1)->_positions;
    entry2._positions_count = _column_writer->index()->mutable_entry(1)->_positions_count;
    entry2._statistics.init(OLAP_FIELD_TYPE_SMALLINT, false);

    PositionProvider position0(&entry1);
    PositionProvider position1(&entry2);

    ASSERT_EQ(_column_reader->seek(&position0), OLAP_SUCCESS);
    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    value = *reinterpret_cast<char*>(data);
    ASSERT_EQ(value, 1);

    ASSERT_EQ(_column_reader->seek(&position1), OLAP_SUCCESS);
    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
    data = reinterpret_cast<char*>(_col_vector->col_data());
    value = *reinterpret_cast<char*>(data);
    ASSERT_EQ(value, 3);
}

TEST_F(TestColumn, VectorizedShortColumnWithPresent) {
    // write data
    TabletSchema tablet_schema;

    set_tablet_schema_with_one_column("ShortColumn", "SMALLINT", "REPLACE", 2, true, true,
                                      &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 1024;
    block.init(block_info);

    write_row.set_null(0);
    block.set_row(0, write_row);

    int16_t value = 3;
    write_row.set_not_null(0);
    write_row.set_field_content(0, reinterpret_cast<char*>(&value), _mem_pool.get());
    block.set_row(1, write_row);
    block.finalize(2);

    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 2, _mem_pool.get()), OLAP_SUCCESS);
    bool* is_null = _col_vector->is_null();
    ASSERT_EQ(is_null[0], true);

    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    ASSERT_EQ(is_null[1], false);

    value = *reinterpret_cast<char*>(data + sizeof(int16_t));
    ASSERT_EQ(value, 3);
}

TEST_F(TestColumn, SkipShortColumnWithPresent) {
    // write data
    TabletSchema tablet_schema;

    set_tablet_schema_with_one_column("ShortColumn", "SMALLINT", "REPLACE", 2, true, true,
                                      &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 1024;
    block.init(block_info);

    int16_t value = 1;
    write_row.set_field_content(0, reinterpret_cast<char*>(&value), _mem_pool.get());
    block.set_row(0, write_row);

    value = 2;
    write_row.set_field_content(0, reinterpret_cast<char*>(&value), _mem_pool.get());
    block.set_row(1, write_row);

    value = 3;
    write_row.set_field_content(0, reinterpret_cast<char*>(&value), _mem_pool.get());
    block.set_row(2, write_row);
    block.finalize(3);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data

    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    ASSERT_EQ(_column_reader->skip(2), OLAP_SUCCESS);
    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    value = *reinterpret_cast<char*>(data);
    ASSERT_EQ(value, 3);
}

TEST_F(TestColumn, VectorizedIntColumnWithoutPresent) {
    // write data
    TabletSchema tablet_schema;

    set_tablet_schema_with_one_column("IntColumn", "INT", "REPLACE", 4, false, true,
                                      &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 1024;
    block.init(block_info);

    int32_t value = 1;
    write_row.set_field_content(0, reinterpret_cast<char*>(&value), _mem_pool.get());
    block.set_row(0, write_row);

    value = 3;
    write_row.set_field_content(0, reinterpret_cast<char*>(&value), _mem_pool.get());
    block.set_row(1, write_row);
    block.finalize(2);

    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data

    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 2, _mem_pool.get()), OLAP_SUCCESS);
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    value = *reinterpret_cast<int*>(data);
    ASSERT_EQ(value, 1);

    value = *reinterpret_cast<int*>(data + sizeof(int));
    ASSERT_EQ(value, 3);
}

TEST_F(TestColumn, VectorizedIntColumnMassWithoutPresent) {
    // write data
    TabletSchema tablet_schema;

    set_tablet_schema_with_one_column("IntColumn", "INT", "REPLACE", 4, false, true,
                                      &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    for (int32_t i = 0; i < 10000; i++) {
        write_row.set_field_content(0, reinterpret_cast<char*>(&i), _mem_pool.get());
        block.set_row(i, write_row);
    }
    block.finalize(10000);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data

    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    _col_vector.reset(new ColumnVector());

    char* data = nullptr;
    for (int32_t i = 0; i < 10000; ++i) {
        if (i % 1000 == 0) {
            ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 1000, _mem_pool.get()),
                      OLAP_SUCCESS);
            data = reinterpret_cast<char*>(_col_vector->col_data());
        }

        int32_t value = 0;
        value = *reinterpret_cast<int*>(data);
        ASSERT_EQ(value, i);
        data += sizeof(int32_t);
    }
}

TEST_F(TestColumn, VectorizedIntColumnWithPresent) {
    // write data
    TabletSchema tablet_schema;

    set_tablet_schema_with_one_column("IntColumn", "INT", "REPLACE", 4, true, true, &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    int32_t value = -1;
    write_row.set_field_content(0, reinterpret_cast<char*>(&value), _mem_pool.get());
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    write_row.set_null(0);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data

    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    _col_vector.reset(new ColumnVector());

    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 2, _mem_pool.get()), OLAP_SUCCESS);

    bool* is_null = _col_vector->is_null();
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    ASSERT_EQ(is_null[0], false);
    value = *reinterpret_cast<int*>(data);
    ASSERT_EQ(value, -1);

    ASSERT_EQ(is_null[1], true);
}

TEST_F(TestColumn, VectorizedLongColumnWithoutPresent) {
    // write data
    TabletSchema tablet_schema;

    set_tablet_schema_with_one_column("LongColumnWithoutPresent", "BIGINT", "REPLACE", 8, false,
                                      true, &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    int64_t value = 1;
    write_row.set_field_content(0, reinterpret_cast<char*>(&value), _mem_pool.get());
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    value = 3;
    write_row.set_field_content(0, reinterpret_cast<char*>(&value), _mem_pool.get());
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data

    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 2, _mem_pool.get()), OLAP_SUCCESS);
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    value = *reinterpret_cast<int64_t*>(data);
    ASSERT_EQ(value, 1);

    value = *reinterpret_cast<int64_t*>(data + sizeof(int64_t));
    ASSERT_EQ(value, 3);
}

TEST_F(TestColumn, VectorizedLongColumnWithPresent) {
    // write data
    TabletSchema tablet_schema;

    set_tablet_schema_with_one_column("LongColumnWithPresent", "BIGINT", "REPLACE", 8, true, true,
                                      &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    write_row.set_null(0);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    int64_t value = 3;
    write_row.set_not_null(0);
    write_row.set_field_content(0, reinterpret_cast<char*>(&value), _mem_pool.get());
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data

    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 2, _mem_pool.get()), OLAP_SUCCESS);
    bool* is_null = _col_vector->is_null();
    ASSERT_EQ(is_null[0], true);

    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    ASSERT_EQ(is_null[1], false);

    value = *reinterpret_cast<int64_t*>(data + sizeof(int64_t));
    ASSERT_EQ(value, 3);
}

TEST_F(TestColumn, VectorizedFloatColumnWithoutPresent) {
    // write data
    TabletSchema tablet_schema;

    set_tablet_schema_with_one_column("FloatColumnWithoutPresent", "FLOAT", "REPLACE", 4, false,
                                      true, &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    float value = 1.234;
    write_row.set_field_content(0, reinterpret_cast<char*>(&value), _mem_pool.get());
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    value = 3.234;
    write_row.set_field_content(0, reinterpret_cast<char*>(&value), _mem_pool.get());
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data

    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 2, _mem_pool.get()), OLAP_SUCCESS);
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    value = *reinterpret_cast<float*>(data);
    ASSERT_FLOAT_EQ(value, 1.234);

    data += sizeof(float);
    value = *reinterpret_cast<float*>(data);
    ASSERT_FLOAT_EQ(value, 3.234);
}

TEST_F(TestColumn, VectorizedFloatColumnWithPresent) {
    // write data
    TabletSchema tablet_schema;

    set_tablet_schema_with_one_column("FloatColumnWithPresent", "FLOAT", "REPLACE", 4, true, true,
                                      &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    write_row.set_null(0);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    float value = 3.234;
    write_row.set_not_null(0);
    write_row.set_field_content(0, reinterpret_cast<char*>(&value), _mem_pool.get());
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data

    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 2, _mem_pool.get()), OLAP_SUCCESS);
    bool* is_null = _col_vector->is_null();
    ASSERT_EQ(is_null[0], true);

    ASSERT_EQ(is_null[1], false);

    char* data = reinterpret_cast<char*>(_col_vector->col_data()) + sizeof(float);
    value = *reinterpret_cast<float*>(data);
    ASSERT_FLOAT_EQ(value, 3.234);
}

TEST_F(TestColumn, SeekFloatColumnWithPresent) {
    // write data
    TabletSchema tablet_schema;

    set_tablet_schema_with_one_column("FloatColumnWithPresent", "FLOAT", "REPLACE", 4, true, true,
                                      &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    float value = 1.234;
    write_row.set_field_content(0, reinterpret_cast<char*>(&value), _mem_pool.get());
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    create_and_save_last_position();

    value = 3.234;
    write_row.set_field_content(0, reinterpret_cast<char*>(&value), _mem_pool.get());
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    create_and_save_last_position();

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data

    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    PositionEntryReader entry1;
    entry1._positions = _column_writer->index()->mutable_entry(0)->_positions;
    entry1._positions_count = _column_writer->index()->mutable_entry(0)->_positions_count;
    entry1._statistics.init(OLAP_FIELD_TYPE_FLOAT, false);

    PositionEntryReader entry2;
    entry2._positions = _column_writer->index()->mutable_entry(1)->_positions;
    entry2._positions_count = _column_writer->index()->mutable_entry(1)->_positions_count;
    entry2._statistics.init(OLAP_FIELD_TYPE_FLOAT, false);

    PositionProvider position0(&entry1);
    PositionProvider position1(&entry2);

    ASSERT_EQ(_column_reader->seek(&position0), OLAP_SUCCESS);
    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 2, _mem_pool.get()), OLAP_SUCCESS);
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    value = *reinterpret_cast<float*>(data);
    ASSERT_FLOAT_EQ(value, 1.234);

    value = *reinterpret_cast<float*>(data + sizeof(float));
    ASSERT_FLOAT_EQ(value, 3.234);
}

TEST_F(TestColumn, SkipFloatColumnWithPresent) {
    // write data
    TabletSchema tablet_schema;

    set_tablet_schema_with_one_column("FloatColumnWithPresent", "FLOAT", "REPLACE", 4, true, true,
                                      &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    float value = 1.234;
    write_row.set_field_content(0, reinterpret_cast<char*>(&value), _mem_pool.get());
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    value = 3.234;
    write_row.set_field_content(0, reinterpret_cast<char*>(&value), _mem_pool.get());
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data

    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    ASSERT_EQ(_column_reader->skip(1), OLAP_SUCCESS);
    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    value = *reinterpret_cast<float*>(data);
    ASSERT_FLOAT_EQ(value, 3.234);
}

TEST_F(TestColumn, VectorizedDoubleColumnWithoutPresent) {
    // write data
    TabletSchema tablet_schema;

    set_tablet_schema_with_one_column("DoubleColumnWithoutPresent", "DOUBLE", "REPLACE", 8, false,
                                      true, &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    double value = 1.23456789;
    write_row.set_field_content(0, reinterpret_cast<char*>(&value), _mem_pool.get());
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    value = 3.23456789;
    write_row.set_field_content(0, reinterpret_cast<char*>(&value), _mem_pool.get());
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data

    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 2, _mem_pool.get()), OLAP_SUCCESS);
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    value = *reinterpret_cast<double*>(data);
    ASSERT_DOUBLE_EQ(value, 1.23456789);

    data += sizeof(double);
    value = *reinterpret_cast<double*>(data);
    ASSERT_DOUBLE_EQ(value, 3.23456789);
}

TEST_F(TestColumn, VectorizedDoubleColumnWithPresent) {
    // write data
    TabletSchema tablet_schema;

    set_tablet_schema_with_one_column("DoubleColumnWithPresent", "DOUBLE", "REPLACE", 8, true, true,
                                      &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    write_row.set_null(0);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    double value = 3.23456789;
    write_row.set_not_null(0);
    write_row.set_field_content(0, reinterpret_cast<char*>(&value), _mem_pool.get());
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data

    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 2, _mem_pool.get()), OLAP_SUCCESS);
    bool* is_null = _col_vector->is_null();
    ASSERT_EQ(is_null[0], true);

    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    ASSERT_EQ(is_null[1], false);

    data += sizeof(double);
    value = *reinterpret_cast<double*>(data);
    ASSERT_DOUBLE_EQ(value, 3.23456789);
}

TEST_F(TestColumn, VectorizedDatetimeColumnWithoutPresent) {
    // write data
    TabletSchema tablet_schema;

    set_tablet_schema_with_one_column("DatetimeColumnWithoutPresent", "DATETIME", "REPLACE", 8,
                                      false, true, &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    std::vector<string> val_string_array;
    val_string_array.push_back("2000-10-10 10:10:10");
    OlapTuple tuple(val_string_array);
    write_row.from_tuple(tuple);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data

    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    read_row.set_field_content(0, data, _mem_pool.get());
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), "0&2000-10-10 10:10:10",
                        strlen("0&2000-10-10 10:10:10")) == 0);
}

TEST_F(TestColumn, VectorizedDatetimeColumnWithPresent) {
    // write data
    TabletSchema tablet_schema;

    set_tablet_schema_with_one_column("DatetimeColumnWithoutPresent", "DATETIME", "REPLACE", 8,
                                      true, true, &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    write_row.set_null(0);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    std::vector<string> val_string_array;
    val_string_array.push_back("2000-10-10 10:10:10");
    OlapTuple tuple(val_string_array);
    write_row.from_tuple(tuple);
    write_row.set_not_null(0);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data

    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 2, _mem_pool.get()), OLAP_SUCCESS);
    bool* is_null = _col_vector->is_null();
    ASSERT_EQ(is_null[0], true);

    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    ASSERT_EQ(is_null[1], false);

    data += sizeof(uint64_t);
    read_row.set_field_content(0, data, _mem_pool.get());
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), "0&2000-10-10 10:10:10",
                        strlen("0&2000-10-10 10:10:10")) == 0);

    ASSERT_NE(_column_reader->next_vector(_col_vector.get(), 2, _mem_pool.get()), OLAP_SUCCESS);
}
TEST_F(TestColumn, VectorizedDatetimeColumnZero) {
    // write data
    TabletSchema tablet_schema;

    set_tablet_schema_with_one_column("DatetimeColumnWithoutPresent", "DATETIME", "REPLACE", 8,
                                      true, true, &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    write_row.set_null(0);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    std::vector<string> val_string_array;
    val_string_array.push_back("1000-01-01 00:00:00");
    OlapTuple tuple(val_string_array);
    write_row.from_tuple(tuple);
    write_row.set_not_null(0);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data

    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 2, _mem_pool.get()), OLAP_SUCCESS);
    bool* is_null = _col_vector->is_null();
    ASSERT_EQ(is_null[0], true);

    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    ASSERT_EQ(is_null[1], false);

    data += sizeof(uint64_t);
    read_row.set_field_content(0, data, _mem_pool.get());
    std::cout << read_row.to_string() << std::endl;
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), "0&1000-01-01 00:00:00",
                        strlen("0&1000-01-01 00:00:00")) == 0);

    ASSERT_NE(_column_reader->next_vector(_col_vector.get(), 2, _mem_pool.get()), OLAP_SUCCESS);
}

TEST_F(TestColumn, VectorizedDateColumnWithoutPresent) {
    // write data
    TabletSchema tablet_schema;

    set_tablet_schema_with_one_column("DateColumnWithoutoutPresent", "DATE", "REPLACE", 3, false,
                                      true, &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    std::vector<string> val_string_array;
    val_string_array.push_back("2000-10-10");
    OlapTuple tuple(val_string_array);
    write_row.from_tuple(tuple);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data

    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    read_row.set_field_content(0, data, _mem_pool.get());
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), "0&2000-10-10", strlen("0&2000-10-10")) == 0);
}

TEST_F(TestColumn, VectorizedDateColumnWithPresent) {
    // write data
    TabletSchema tablet_schema;

    set_tablet_schema_with_one_column("DateColumnWithoutoutPresent", "DATE", "REPLACE", 3, true,
                                      true, &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    write_row.set_null(0);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    std::vector<string> val_string_array;
    val_string_array.push_back("2000-10-10");
    OlapTuple tuple(val_string_array);
    write_row.from_tuple(tuple);
    for (uint32_t i = 0; i < 100; ++i) {
        write_row.set_not_null(0);
        block.set_row(0, write_row);
        block.finalize(1);
        ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);
    }

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data

    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 101, _mem_pool.get()), OLAP_SUCCESS);
    bool* is_null = _col_vector->is_null();
    ASSERT_EQ(is_null[0], true);

    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    for (uint32_t i = 0; i < 100; ++i) {
        data += sizeof(uint24_t);
        ASSERT_EQ(is_null[i + 1], false);
        read_row.set_field_content(0, data, _mem_pool.get());
        ASSERT_TRUE(strncmp(read_row.to_string().c_str(), "0&2000-10-10", strlen("0&2000-10-10")) ==
                    0);
    }
}

TEST_F(TestColumn, VectorizedDecimalColumnWithoutPresent) {
    // write data
    TabletSchema tablet_schema;

    set_tablet_schema_with_one_column("DecimalColumnWithoutoutPresent", "DECIMAL", "REPLACE", 12,
                                      false, true, &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    std::vector<string> val_string_array;
    val_string_array.push_back("1234.5678");
    OlapTuple tuple1(val_string_array);
    write_row.from_tuple(tuple1);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    val_string_array.clear();
    val_string_array.push_back("5678.1234");
    OlapTuple tuple2(val_string_array);
    write_row.from_tuple(tuple2);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data

    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 2, _mem_pool.get()), OLAP_SUCCESS);
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    read_row.set_field_content(0, data, _mem_pool.get());
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), "0&1234.5678", strlen("0&1234.5678")) == 0);

    data += sizeof(decimal12_t);
    read_row.set_field_content(0, data, _mem_pool.get());
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), "0&5678.1234", strlen("0&5678.1234")) == 0);
}

TEST_F(TestColumn, VectorizedDecimalColumnWithPresent) {
    // write data
    TabletSchema tablet_schema;

    set_tablet_schema_with_one_column("DecimalColumnWithoutoutPresent", "DECIMAL", "REPLACE", 12,
                                      true, true, &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    std::vector<string> val_string_array;
    write_row.set_null(0);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    val_string_array.clear();
    val_string_array.push_back("5678.1234");
    OlapTuple tuple(val_string_array);
    write_row.from_tuple(tuple);
    write_row.set_not_null(0);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data

    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 2, _mem_pool.get()), OLAP_SUCCESS);
    bool* is_null = _col_vector->is_null();
    ASSERT_EQ(is_null[0], true);

    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    data += sizeof(decimal12_t);
    ASSERT_EQ(is_null[1], false);
    read_row.set_field_content(0, data, _mem_pool.get());
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), "0&5678.1234", strlen("0&5678.1234")) == 0);
}

TEST_F(TestColumn, SkipDecimalColumnWithPresent) {
    // write data
    TabletSchema tablet_schema;

    set_tablet_schema_with_one_column("DecimalColumnWithPresent", "DECIMAL", "REPLACE", 12, true,
                                      true, &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    std::vector<string> val_string_array;
    val_string_array.push_back("1234.5678");
    write_row.from_tuple(val_string_array);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    val_string_array.clear();
    val_string_array.push_back("5678.1234");
    write_row.from_tuple(val_string_array);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data

    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    char read_value[20];
    memset(read_value, 0, 20);
    ASSERT_EQ(_column_reader->skip(1), OLAP_SUCCESS);
    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    read_row.set_field_content(0, data, _mem_pool.get());
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), "0&5678.1234", strlen("0&5678.1234")) == 0);
}

TEST_F(TestColumn, SeekDecimalColumnWithPresent) {
    // write data
    TabletSchema tablet_schema;

    set_tablet_schema_with_one_column("DecimalColumnWithPresent", "DECIMAL", "REPLACE", 12, true,
                                      true, &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    std::vector<string> val_string_array;
    val_string_array.push_back("1234.5678");
    OlapTuple tuple1(val_string_array);
    write_row.from_tuple(tuple1);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    create_and_save_last_position();

    val_string_array.clear();
    val_string_array.push_back("5678.1234");
    OlapTuple tuple2(val_string_array);
    write_row.from_tuple(tuple2);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    create_and_save_last_position();

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data

    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);
    PositionEntryReader entry1;
    entry1._positions = _column_writer->index()->mutable_entry(0)->_positions;
    entry1._positions_count = _column_writer->index()->mutable_entry(0)->_positions_count;
    entry1._statistics.init(OLAP_FIELD_TYPE_FLOAT, false);

    PositionEntryReader entry2;
    entry2._positions = _column_writer->index()->mutable_entry(1)->_positions;
    entry2._positions_count = _column_writer->index()->mutable_entry(1)->_positions_count;
    entry2._statistics.init(OLAP_FIELD_TYPE_FLOAT, false);

    PositionProvider position0(&entry1);
    PositionProvider position1(&entry2);

    char read_value[20];
    memset(read_value, 0, 20);
    ASSERT_EQ(_column_reader->seek(&position0), OLAP_SUCCESS);
    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    read_row.set_field_content(0, data, _mem_pool.get());
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), "0&1234.5678", strlen("0&1234.5678")) == 0);

    memset(read_value, 0, 20);
    ASSERT_EQ(_column_reader->seek(&position1), OLAP_SUCCESS);
    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
    data = reinterpret_cast<char*>(_col_vector->col_data());
    read_row.set_field_content(0, data, _mem_pool.get());
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), "0&5678.1234", strlen("0&5678.1234")) == 0);
}

TEST_F(TestColumn, VectorizedLargeIntColumnWithoutPresent) {
    // init tablet schema
    TabletSchema tablet_schema;

    set_tablet_schema_with_one_column("LargeIntColumnWithoutoutPresent", "LARGEINT", "SUM", 16,
                                      false, true, &tablet_schema);
    // test data
    string value1 = "100000000000000000000000000000000000000";
    string value2 = "-170141183460469231731687303715884105728";

    // write data

    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    std::vector<string> val_string_array;
    val_string_array.push_back(value1);
    OlapTuple tuple1(val_string_array);
    write_row.from_tuple(tuple1);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    val_string_array.clear();
    val_string_array.push_back(value2);
    OlapTuple tuple2(val_string_array);
    write_row.from_tuple(tuple2);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data

    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 2, _mem_pool.get()), OLAP_SUCCESS);
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    read_row.set_field_content(0, data, _mem_pool.get());
    value1 = "0&" + value1;
    value2 = "0&" + value2;
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), value1.c_str(), value1.size()) == 0);

    read_row.set_field_content(0, data + sizeof(int128_t), _mem_pool.get());
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), value2.c_str(), value2.size()) == 0);
}

TEST_F(TestColumn, VectorizedLargeIntColumnWithPresent) {
    // init tablet schema
    TabletSchema tablet_schema;

    set_tablet_schema_with_one_column("LargeIntColumnWithoutoutPresent", "LARGEINT", "SUM", 16,
                                      true, true, &tablet_schema);

    // test data
    string value1 = "100000000000000000000000000000000000000";
    string value2 = "-170141183460469231731687303715884105728";

    // write data

    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    write_row.set_null(0);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    std::vector<string> val_string_array;
    val_string_array.push_back(value1);
    OlapTuple tuple1(val_string_array);
    write_row.from_tuple(tuple1);
    write_row.set_not_null(0);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    val_string_array.clear();
    val_string_array.push_back(value2);
    OlapTuple tuple2(val_string_array);
    write_row.from_tuple(tuple2);
    write_row.set_not_null(0);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data

    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 3, _mem_pool.get()), OLAP_SUCCESS);
    bool* is_null = _col_vector->is_null();
    ASSERT_EQ(is_null[0], true);
    ASSERT_EQ(is_null[1], false);
    ASSERT_EQ(is_null[2], false);

    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    value1 = "0&" + value1;
    value2 = "0&" + value2;

    data += sizeof(int128_t);
    read_row.set_field_content(0, data, _mem_pool.get());
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), value1.c_str(), value1.size()) == 0);

    data += sizeof(int128_t);
    read_row.set_field_content(0, data, _mem_pool.get());
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), value2.c_str(), value2.size()) == 0);
}

TEST_F(TestColumn, SkipLargeIntColumnWithPresent) {
    // init tablet schema
    TabletSchema tablet_schema;

    set_tablet_schema_with_one_column("LargeIntColumnWithPresent", "LARGEINT", "SUM", 16, true,
                                      true, &tablet_schema);
    // test data
    string value1 = "100000000000000000000000000000000000000";
    string value2 = "-170141183460469231731687303715884105728";

    // write data

    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    std::vector<string> val_string_array;
    val_string_array.push_back(value1);
    OlapTuple tuple1(val_string_array);
    write_row.from_tuple(tuple1);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    val_string_array.clear();
    val_string_array.push_back(value2);
    OlapTuple tuple2(val_string_array);
    write_row.from_tuple(tuple2);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    create_column_reader(tablet_schema);
    RowCursor read_row;
    read_row.init(tablet_schema);

    value2 = "0&" + value2;
    ASSERT_EQ(_column_reader->skip(1), OLAP_SUCCESS);
    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    read_row.set_field_content(0, data, _mem_pool.get());
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), value2.c_str(), value2.size()) == 0);
}

TEST_F(TestColumn, VectorizedDirectVarcharColumnWithoutPresent) {
    // write data
    TabletSchema tablet_schema;

    set_tablet_schema_with_one_column("DirectVarcharColumnWithoutoutPresent", "VARCHAR", "REPLACE",
                                      10, false, true, &tablet_schema);

    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);
    write_row.allocate_memory_for_string_type(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    std::vector<string> val_string_array;
    val_string_array.push_back("YWJjZGU="); //"abcde" base_64_encode is "YWJjZGU="
    OlapTuple tuple1(val_string_array);
    write_row.from_tuple(tuple1);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);
    for (uint32_t i = 0; i < 2; i++) {
        block.set_row(0, write_row);
        block.finalize(1);
        ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);
    }
    val_string_array.clear();
    val_string_array.push_back("ZWRjYmE="); //"edcba" base_64_encode is "ZWRjYmE="
    OlapTuple tuple2(val_string_array);
    write_row.from_tuple(tuple2);
    for (uint32_t i = 0; i < 2; i++) {
        block.set_row(0, write_row);
        block.finalize(1);
        ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);
    }

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);
    read_row.allocate_memory_for_string_type(tablet_schema);

    _col_vector.reset(new ColumnVector());

    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 5, _mem_pool.get()), OLAP_SUCCESS);
    Slice* value = reinterpret_cast<Slice*>(_col_vector->col_data());
    ASSERT_TRUE(strncmp(value->data, "YWJjZGU=", value->size) == 0);
    for (uint32_t i = 0; i < 2; i++) {
        value++;
        ASSERT_TRUE(strncmp(value->data, "YWJjZGU=", value->size) == 0);
    }
    for (uint32_t i = 0; i < 2; i++) {
        value++;
        ASSERT_TRUE(strncmp(value->data, "ZWRjYmE=", value->size) == 0);
    }
    ASSERT_NE(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
}

TEST_F(TestColumn, VectorizedDirectVarcharColumnWithPresent) {
    // write data
    TabletSchema tablet_schema;
    set_tablet_schema_with_one_column("DirectVarcharColumnWithoutoutPresent", "VARCHAR", "REPLACE",
                                      10, true, true, &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);
    write_row.allocate_memory_for_string_type(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    write_row.set_null(0);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    std::vector<string> val_string_array;
    val_string_array.push_back("YWJjZGU="); //"abcde" base_64_encode is "YWJjZGU="
    OlapTuple tuple(val_string_array);
    write_row.from_tuple(tuple);
    write_row.set_not_null(0);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);
    read_row.allocate_memory_for_string_type(tablet_schema);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 2, _mem_pool.get()), OLAP_SUCCESS);
    bool* is_null = _col_vector->is_null();
    ASSERT_EQ(is_null[0], true);
    ASSERT_EQ(is_null[1], false);

    Slice* value = reinterpret_cast<Slice*>(_col_vector->col_data());
    value++;
    ASSERT_TRUE(strncmp(value->data, "YWJjZGU=", value->size) == 0);
}

TEST_F(TestColumn, SkipDirectVarcharColumnWithPresent) {
    // write data
    TabletSchema tablet_schema;

    set_tablet_schema_with_one_column("DirectVarcharColumnWithPresent", "VARCHAR", "REPLACE", 10,
                                      true, true, &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);
    write_row.allocate_memory_for_string_type(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    std::vector<string> val_string_array;
    val_string_array.push_back("YWJjZGU="); //"abcde" base_64_encode is "YWJjZGU="
    OlapTuple tuple1(val_string_array);
    write_row.from_tuple(tuple1);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    val_string_array.clear();
    val_string_array.push_back("YWFhYWE="); //"aaaaa" base_64_encode is "YWJjZGU="
    OlapTuple tuple2(val_string_array);
    write_row.from_tuple(tuple2);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);
    read_row.allocate_memory_for_string_type(tablet_schema);

    char read_value[20];
    memset(read_value, 0, 20);
    ASSERT_EQ(_column_reader->skip(1), OLAP_SUCCESS);
    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
    Slice* value = reinterpret_cast<Slice*>(_col_vector->col_data());
    ASSERT_TRUE(strncmp(value->data, "YWFhYWE=", value->size) == 0);
}

TEST_F(TestColumn, SeekDirectVarcharColumnWithoutPresent) {
    // write data
    TabletSchema tablet_schema;

    set_tablet_schema_with_one_column("DirectVarcharColumnWithPresent", "VARCHAR", "REPLACE", 10,
                                      false, true, &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);
    write_row.allocate_memory_for_string_type(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    std::vector<string> val_string_array;
    val_string_array.push_back("YWJjZGU="); //"abcde" base_64_encode is "YWJjZGU="
    OlapTuple tuple1(val_string_array);
    write_row.from_tuple(tuple1);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    _column_writer->create_row_index_entry();

    val_string_array.clear();
    val_string_array.push_back("YWFhYWE="); //"aaaaa" base_64_encode is "YWJjZGU="
    OlapTuple tuple2(val_string_array);
    write_row.from_tuple(tuple2);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    _column_writer->create_row_index_entry();

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);
    read_row.allocate_memory_for_string_type(tablet_schema);

    PositionEntryReader entry1;
    entry1._positions = _column_writer->index()->mutable_entry(0)->_positions;
    entry1._positions_count = _column_writer->index()->mutable_entry(0)->_positions_count;
    entry1._statistics.init(OLAP_FIELD_TYPE_VARCHAR, false);

    PositionEntryReader entry2;
    entry2._positions = _column_writer->index()->mutable_entry(1)->_positions;
    entry2._positions_count = _column_writer->index()->mutable_entry(1)->_positions_count;
    entry2._statistics.init(OLAP_FIELD_TYPE_VARCHAR, false);

    PositionProvider position0(&entry1);
    PositionProvider position1(&entry2);

    ASSERT_EQ(_column_reader->seek(&position0), OLAP_SUCCESS);
    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
    Slice* value = reinterpret_cast<Slice*>(_col_vector->col_data());
    ASSERT_TRUE(strncmp(value->data, "YWJjZGU=", value->size) == 0);

    ASSERT_EQ(_column_reader->seek(&position1), OLAP_SUCCESS);
    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
    value = reinterpret_cast<Slice*>(_col_vector->col_data());
    ASSERT_TRUE(strncmp(value->data, "YWFhYWE=", value->size) == 0);
}

TEST_F(TestColumn, SeekDirectVarcharColumnWithPresent) {
    // write data
    TabletSchema tablet_schema;

    set_tablet_schema_with_one_column("DirectVarcharColumnWithPresent", "VARCHAR", "REPLACE", 10,
                                      true, true, &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);
    write_row.allocate_memory_for_string_type(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    std::vector<string> val_string_array;
    val_string_array.push_back("YWJjZGU="); //"abcde" base_64_encode is "YWJjZGU="
    OlapTuple tuple1(val_string_array);
    write_row.from_tuple(tuple1);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    _column_writer->create_row_index_entry();

    val_string_array.clear();
    val_string_array.push_back("YWFhYWE="); //"aaaaa" base_64_encode is "YWJjZGU="
    OlapTuple tuple2(val_string_array);
    write_row.from_tuple(tuple2);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    _column_writer->create_row_index_entry();

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);
    read_row.allocate_memory_for_string_type(tablet_schema);

    PositionEntryReader entry1;
    entry1._positions = _column_writer->index()->mutable_entry(0)->_positions;
    entry1._positions_count = _column_writer->index()->mutable_entry(0)->_positions_count;
    entry1._statistics.init(OLAP_FIELD_TYPE_VARCHAR, false);

    PositionEntryReader entry2;
    entry2._positions = _column_writer->index()->mutable_entry(1)->_positions;
    entry2._positions_count = _column_writer->index()->mutable_entry(1)->_positions_count;
    entry2._statistics.init(OLAP_FIELD_TYPE_VARCHAR, false);

    PositionProvider position0(&entry1);
    PositionProvider position1(&entry2);

    ASSERT_EQ(_column_reader->seek(&position0), OLAP_SUCCESS);
    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
    Slice* value = reinterpret_cast<Slice*>(_col_vector->col_data());
    ASSERT_TRUE(strncmp(value->data, "YWJjZGU=", value->size) == 0);

    ASSERT_EQ(_column_reader->seek(&position1), OLAP_SUCCESS);
    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
    value = reinterpret_cast<Slice*>(_col_vector->col_data());
    ASSERT_TRUE(strncmp(value->data, "YWFhYWE=", value->size) == 0);
}

TEST_F(TestColumn, VectorizedCharColumnWithoutPresent) {
    // write data
    TabletSchema tablet_schema;
    set_tablet_schema_with_one_column("CharColumnWithoutoutPresent", "CHAR", "REPLACE",
                                      strlen("abcde"), false, true, &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);
    write_row.allocate_memory_for_string_type(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    std::vector<string> val_string_array;
    val_string_array.push_back("abcde"); //"abcde" base_64_encode is "YWJjZGU="
    OlapTuple tuple1(val_string_array);
    write_row.from_tuple(tuple1);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);
    for (uint32_t i = 0; i < 2; i++) {
        block.set_row(0, write_row);
        block.finalize(1);
        ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);
    }
    val_string_array.clear();
    val_string_array.push_back("edcba"); //"edcba" base_64_encode is "ZWRjYmE="
    OlapTuple tuple2(val_string_array);
    write_row.from_tuple(tuple2);
    for (uint32_t i = 0; i < 2; i++) {
        block.set_row(0, write_row);
        block.finalize(1);
        ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);
    }

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);
    read_row.allocate_memory_for_string_type(tablet_schema);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 5, _mem_pool.get()), OLAP_SUCCESS);
    Slice* value = reinterpret_cast<Slice*>(_col_vector->col_data());

    ASSERT_TRUE(strncmp(value->data, "abcde", value->size) == 0);
    for (uint32_t i = 0; i < 2; i++) {
        value++;
        ASSERT_TRUE(strncmp(value->data, "abcde", value->size) == 0);
    }
    for (uint32_t i = 0; i < 2; i++) {
        value++;
        ASSERT_TRUE(strncmp(value->data, "edcba", value->size) == 0);
    }
    ASSERT_NE(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
}

TEST_F(TestColumn, VectorizedCharColumnWithPresent) {
    // write data
    TabletSchema tablet_schema;
    set_tablet_schema_with_one_column("CharColumnWithoutoutPresent", "CHAR", "REPLACE",
                                      strlen("abcde"), true, true, &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);
    write_row.allocate_memory_for_string_type(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    write_row.set_null(0);
    block.set_row(0, write_row);

    std::vector<string> val_string_array;
    val_string_array.push_back("abcde"); //"abcde" base_64_encode is "YWJjZGU="
    OlapTuple tuple(val_string_array);
    write_row.from_tuple(tuple);
    write_row.set_not_null(0);
    block.set_row(1, write_row);
    block.finalize(2);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);
    read_row.allocate_memory_for_string_type(tablet_schema);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 2, _mem_pool.get()), OLAP_SUCCESS);
    bool* is_null = _col_vector->is_null();
    ASSERT_EQ(is_null[0], true);
    ASSERT_EQ(is_null[1], false);

    Slice* value = reinterpret_cast<Slice*>(_col_vector->col_data());
    value++;
    ASSERT_TRUE(strncmp(value->data, "abcde", value->size) == 0);
}

TEST_F(TestColumn, VectorizedCharColumnWithoutoutPresent2) {
    // write data
    TabletSchema tablet_schema;
    set_tablet_schema_with_one_column("CharColumnWithoutoutPresent", "CHAR", "REPLACE", 20, false,
                                      true, &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);
    write_row.allocate_memory_for_string_type(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    std::vector<string> val_string_array;
    val_string_array.push_back("abcde"); //"abcde" base_64_encode is "YWJjZGU="
    OlapTuple tuple1(val_string_array);
    write_row.from_tuple(tuple1);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    val_string_array.clear();
    val_string_array.push_back("aaaaa"); //"abcde" base_64_encode is "YWJjZGU="
    OlapTuple tuple2(val_string_array);
    write_row.from_tuple(tuple2);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    val_string_array.clear();
    val_string_array.push_back("bbbbb"); //"abcde" base_64_encode is "YWJjZGU="
    OlapTuple tuple3(val_string_array);
    write_row.from_tuple(tuple3);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    val_string_array.clear();
    val_string_array.push_back("ccccc"); //"abcde" base_64_encode is "YWJjZGU="
    OlapTuple tuple4(val_string_array);
    write_row.from_tuple(tuple4);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    val_string_array.clear();
    val_string_array.push_back("ddddd"); //"abcde" base_64_encode is "YWJjZGU="
    OlapTuple tuple5(val_string_array);
    write_row.from_tuple(tuple5);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);
    read_row.allocate_memory_for_string_type(tablet_schema);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 5, _mem_pool.get()), OLAP_SUCCESS);
    Slice* value = reinterpret_cast<Slice*>(_col_vector->col_data());

    ASSERT_TRUE(strncmp(value->data, "abcde", value->size) == 0);

    value++;
    ASSERT_TRUE(strncmp(value->data, "aaaaa", value->size) == 0);

    value++;
    ASSERT_TRUE(strncmp(value->data, "bbbbb", value->size) == 0);

    value++;
    ASSERT_TRUE(strncmp(value->data, "ccccc", value->size) == 0);

    value++;
    ASSERT_TRUE(strncmp(value->data, "ddddd", value->size) == 0);
}

TEST_F(TestColumn, VectorizedStringColumnWithoutPresent) {
    // write data
    TabletSchema tablet_schema;
    set_tablet_schema_with_one_column("StringColumnWithoutoutPresent", "STRING", "REPLACE", 0,
                                      false, true, &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);
    write_row.allocate_memory_for_string_type(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    std::vector<string> val_string_array;
    val_string_array.push_back("abcde"); //"abcde" base_64_encode is "YWJjZGU="
    OlapTuple tuple1(val_string_array);
    write_row.from_tuple(tuple1);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);
    for (uint32_t i = 0; i < 2; i++) {
        block.set_row(0, write_row);
        block.finalize(1);
        ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);
    }
    val_string_array.clear();
    val_string_array.push_back("edcba"); //"edcba" base_64_encode is "ZWRjYmE="
    OlapTuple tuple2(val_string_array);
    write_row.from_tuple(tuple2);
    for (uint32_t i = 0; i < 2; i++) {
        block.set_row(0, write_row);
        block.finalize(1);
        ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);
    }

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);
    read_row.allocate_memory_for_string_type(tablet_schema);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 5, _mem_pool.get()), OLAP_SUCCESS);
    Slice* value = reinterpret_cast<Slice*>(_col_vector->col_data());

    ASSERT_TRUE(strncmp(value->data, "abcde", value->size) == 0);
    for (uint32_t i = 0; i < 2; i++) {
        value++;
        ASSERT_TRUE(strncmp(value->data, "abcde", value->size) == 0);
    }
    for (uint32_t i = 0; i < 2; i++) {
        value++;
        ASSERT_TRUE(strncmp(value->data, "edcba", value->size) == 0);
    }
    ASSERT_NE(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
}

TEST_F(TestColumn, VectorizedStringColumnWithPresent) {
    // write data
    TabletSchema tablet_schema;
    set_tablet_schema_with_one_column("StringColumnWithoutoutPresent", "STRING", "REPLACE", 0, true,
                                      true, &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);
    write_row.allocate_memory_for_string_type(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    write_row.set_null(0);
    block.set_row(0, write_row);

    std::vector<string> val_string_array;
    val_string_array.push_back("abcde"); //"abcde" base_64_encode is "YWJjZGU="
    OlapTuple tuple(val_string_array);
    write_row.from_tuple(tuple);
    write_row.set_not_null(0);
    block.set_row(1, write_row);
    block.finalize(2);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);
    read_row.allocate_memory_for_string_type(tablet_schema);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 2, _mem_pool.get()), OLAP_SUCCESS);
    bool* is_null = _col_vector->is_null();
    ASSERT_EQ(is_null[0], true);
    ASSERT_EQ(is_null[1], false);

    Slice* value = reinterpret_cast<Slice*>(_col_vector->col_data());
    value++;
    ASSERT_TRUE(strncmp(value->data, "abcde", value->size) == 0);
}

TEST_F(TestColumn, VectorizedStringColumnWithoutoutPresent2) {
    // write data
    TabletSchema tablet_schema;
    set_tablet_schema_with_one_column("StringColumnWithoutoutPresent", "STRING", "REPLACE", 0,
                                      false, true, &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);
    write_row.allocate_memory_for_string_type(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    std::vector<string> val_string_array;
    val_string_array.push_back("abcde"); //"abcde" base_64_encode is "YWJjZGU="
    OlapTuple tuple1(val_string_array);
    write_row.from_tuple(tuple1);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    val_string_array.clear();
    val_string_array.push_back("aaaaa"); //"abcde" base_64_encode is "YWJjZGU="
    OlapTuple tuple2(val_string_array);
    write_row.from_tuple(tuple2);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    val_string_array.clear();
    val_string_array.push_back("bbbbb"); //"abcde" base_64_encode is "YWJjZGU="
    OlapTuple tuple3(val_string_array);
    write_row.from_tuple(tuple3);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    val_string_array.clear();
    val_string_array.push_back("ccccc"); //"abcde" base_64_encode is "YWJjZGU="
    OlapTuple tuple4(val_string_array);
    write_row.from_tuple(tuple4);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    val_string_array.clear();
    val_string_array.push_back("ddddd"); //"abcde" base_64_encode is "YWJjZGU="
    OlapTuple tuple5(val_string_array);
    write_row.from_tuple(tuple5);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);
    read_row.allocate_memory_for_string_type(tablet_schema);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 5, _mem_pool.get()), OLAP_SUCCESS);
    Slice* value = reinterpret_cast<Slice*>(_col_vector->col_data());

    ASSERT_TRUE(strncmp(value->data, "abcde", value->size) == 0);

    value++;
    ASSERT_TRUE(strncmp(value->data, "aaaaa", value->size) == 0);

    value++;
    ASSERT_TRUE(strncmp(value->data, "bbbbb", value->size) == 0);

    value++;
    ASSERT_TRUE(strncmp(value->data, "ccccc", value->size) == 0);

    value++;
    ASSERT_TRUE(strncmp(value->data, "ddddd", value->size) == 0);
}

TEST_F(TestColumn, VectorizedDirectVarcharColumnWith65533) {
    // write data
    TabletSchema tablet_schema;
    set_tablet_schema_with_one_column("DirectVarcharColumnWithoutoutPresent", "VARCHAR", "REPLACE",
                                      65535, false, true, &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);
    write_row.allocate_memory_for_string_type(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    std::vector<string> val_string_array;
    val_string_array.push_back(std::string(65533, 'a'));
    OlapTuple tuple1(val_string_array);
    ASSERT_EQ(OLAP_SUCCESS, write_row.from_tuple(tuple1));
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    val_string_array.clear();
    val_string_array.push_back("edcba"); //"edcba" base_64_encode is "ZWRjYmE="
    OlapTuple tuple2(val_string_array);
    write_row.from_tuple(tuple2);
    for (uint32_t i = 0; i < 2; i++) {
        block.set_row(0, write_row);
        block.finalize(1);
        ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);
    }

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);
    read_row.allocate_memory_for_string_type(tablet_schema);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 3, _mem_pool.get()), OLAP_SUCCESS);
    Slice* value = reinterpret_cast<Slice*>(_col_vector->col_data());

    for (uint32_t i = 0; i < 65533; i++) {
        ASSERT_TRUE(strncmp(value->data + i, "a", 1) == 0);
    }

    for (uint32_t i = 0; i < 2; i++) {
        value++;
        ASSERT_TRUE(strncmp(value->data, "edcba", value->size) == 0);
    }
}

} // namespace doris

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    doris::MemInfo::init();
    int ret = doris::OLAP_SUCCESS;
    testing::InitGoogleTest(&argc, argv);
    ret = RUN_ALL_TESTS();
    return ret;
}
