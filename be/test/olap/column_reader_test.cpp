// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

#include "olap/column_file/byte_buffer.h"
#include "olap/column_file/stream_name.h"
#include "olap/column_file/column_reader.h"
#include "olap/column_file/column_writer.h"
#include "olap/field.h"
#include "olap/olap_define.h"
#include "olap/olap_common.h"
#include "olap/row_cursor.h"
#include "util/logging.h"

using std::string;

namespace palo {
namespace column_file {

class TestColumn : public testing::Test {
public:
    TestColumn() : 
            _column_writer(NULL),
            _column_reader(NULL),
            _stream_factory(NULL) {
            _offsets.clear();
        _map_in_streams.clear();

        _present_buffers.clear();
        
        _data_buffers.clear();

        _second_buffers.clear();

        _dictionary_buffers.clear();

        _length_buffers.clear();
    }
    
    virtual ~TestColumn() {
        SAFE_DELETE(_column_writer);
        SAFE_DELETE(_column_reader);
        SAFE_DELETE(_stream_factory);
    }
    
    virtual void SetUp() {
        _offsets.push_back(0);

        _stream_factory = 
                new(std::nothrow) OutStreamFactory(COMPRESS_LZO,
                                                   OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE);
        ASSERT_TRUE(_stream_factory != NULL);
        config::column_dictionary_key_ration_threshold = 30;
        config::column_dictionary_key_size_threshold = 1000;
    }
    
    virtual void TearDown() {
        SAFE_DELETE(_column_writer);

        SAFE_DELETE(_column_reader);

        SAFE_DELETE(_stream_factory);
        
        SAFE_DELETE(_shared_buffer);
        
        _offsets.clear();

        _map_in_streams.clear();

        _present_buffers.clear();
        
        _data_buffers.clear();

        _second_buffers.clear();

        _dictionary_buffers.clear();

        _length_buffers.clear();
    }

    void CreateColumnWriter(const std::vector<FieldInfo> &tablet_schema) {
        _column_writer = ColumnWriter::create(
                0, tablet_schema, _stream_factory, 1024, BLOOM_FILTER_DEFAULT_FPP);
        
        ASSERT_TRUE(_column_writer != NULL);
        ASSERT_EQ(_column_writer->init(), OLAP_SUCCESS);
    }

    void CreateColumnReader(const std::vector<FieldInfo> &tablet_schema) {
        UniqueIdEncodingMap encodings;
        encodings[0] = ColumnEncodingMessage();
        encodings[0].set_kind(ColumnEncodingMessage::DIRECT);
        encodings[0].set_dictionary_size(1);
        CreateColumnReader(tablet_schema, encodings);
    }

    void CreateColumnReader(
            const std::vector<FieldInfo> &tablet_schema,
            UniqueIdEncodingMap &encodings) {
        UniqueIdToColumnIdMap included;
        included[0] = 0;
        UniqueIdToColumnIdMap segment_included;
        segment_included[0] = 0;
        //UniqueIdSet segment_columns;
        //segment_columns.insert(0); 

        _column_reader = ColumnReader::create(0,
                                               tablet_schema,
                                               included,
                                               segment_included,
                                               encodings);
        
        ASSERT_TRUE(_column_reader != NULL);

        system("rm ./tmp_file");

        ASSERT_EQ(OLAP_SUCCESS, 
                  helper.open_with_mode("tmp_file", 
                                        O_CREAT | O_EXCL | O_WRONLY, 
                                        S_IRUSR | S_IWUSR));
        std::vector<int> off;
        std::vector<int> length;
        std::vector<int> buffer_size;
        std::vector<StreamName> name;

        for (std::map<StreamName, OutStream*>::const_iterator it = _stream_factory->streams().begin();
                it != _stream_factory->streams().end(); ++it) {
            StreamName stream_name = it->first;
            OutStream *out_stream = it->second;
            std::vector<ByteBuffer*> *buffers;

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
            
            ASSERT_TRUE(buffers != NULL);
            off.push_back(helper.tell());
            out_stream->write_to_file(&helper, 0);
            length.push_back(out_stream->get_stream_length());
            buffer_size.push_back(out_stream->get_total_buffer_size());
            name.push_back(stream_name);
        }
        helper.close();

        ASSERT_EQ(OLAP_SUCCESS, helper.open_with_mode("tmp_file", 
                O_RDONLY, S_IRUSR | S_IWUSR)); 

        _shared_buffer = ByteBuffer::create(
                OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE + sizeof(StreamHead));
        ASSERT_TRUE(_shared_buffer != NULL);

        for (int i = 0; i < off.size(); ++i) {
            ReadOnlyFileStream* in_stream = new (std::nothrow) ReadOnlyFileStream(
                    &helper, 
                    &_shared_buffer,
                    off[i], 
                    length[i], 
                    lzo_decompress, 
                    buffer_size[i]);
            ASSERT_EQ(OLAP_SUCCESS, in_stream->init());

            _map_in_streams[name[i]] = in_stream;
        }

        ASSERT_EQ(_column_reader->init(&_map_in_streams), OLAP_SUCCESS);
    }

    void SetFieldInfo(FieldInfo &field_info,
                      std::string name,
                      FieldType type,
                      FieldAggregationMethod aggregation,
                      uint32_t length,
                      bool is_allow_null,
                      bool is_key) {
        field_info.name = name;
        field_info.type = type;
        field_info.aggregation = aggregation;
        field_info.length = length;
        field_info.is_allow_null = is_allow_null;
        field_info.is_key = is_key;
        field_info.precision = 1000;
        field_info.frac = 10000;
        field_info.unique_id = 0;
        field_info.is_bf_column = false;
    }

    void create_and_save_last_position() {
        ASSERT_EQ(_column_writer->create_row_index_entry(), OLAP_SUCCESS);
    }

    ColumnWriter *_column_writer;

    ColumnReader *_column_reader;

    OutStreamFactory *_stream_factory;

    std::vector<size_t> _offsets;

    std::vector<ByteBuffer*> _present_buffers;

    std::vector<ByteBuffer*> _data_buffers;

    std::vector<ByteBuffer*> _second_buffers;

    std::vector<ByteBuffer*> _dictionary_buffers;

    std::vector<ByteBuffer*> _length_buffers;

    ByteBuffer* _shared_buffer;

    std::map<StreamName, ReadOnlyFileStream *> _map_in_streams;

    FileHandler helper;
};

TEST_F(TestColumn, TinyColumnWithoutPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("TinyColumn"), 
                 OLAP_FIELD_TYPE_TINYINT, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 1, 
                 false,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    char value = 1;
    write_row.read(&value, sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    value = 3;
    write_row.read(&value, sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(&value);
    ASSERT_EQ(value, 1);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(&value);
    ASSERT_EQ(value, 3);   

    ASSERT_NE(_column_reader->next(), OLAP_SUCCESS);
}

TEST_F(TestColumn, SeekTinyColumnWithoutPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("TinyColumn"), 
                 OLAP_FIELD_TYPE_TINYINT, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 1, 
                 false,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    char value = 1;
    write_row.read(&value, sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    value = 2;
    write_row.read(&value, sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    create_and_save_last_position();
    
    value = 3;
    write_row.read(&value, sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);
    
    create_and_save_last_position();

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    PositionEntryReader entry1;
    entry1._positions = _column_writer->index()->mutable_entry(0)->_positions;
    entry1._positions_count = _column_writer->index()->mutable_entry(0)->_positions_count;
    entry1._statistics.init(OLAP_FIELD_TYPE_NONE, false);

    PositionEntryReader entry2;
    entry2._positions = _column_writer->index()->mutable_entry(1)->_positions;
    entry2._positions_count = _column_writer->index()->mutable_entry(1)->_positions_count;
    entry2._statistics.init(OLAP_FIELD_TYPE_NONE, false);

    PositionProvider position0(&entry1);
    PositionProvider position1(&entry2);
    
    ASSERT_EQ(_column_reader->seek(&position0), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(&value);
    ASSERT_EQ(value, 1);   

    ASSERT_EQ(_column_reader->seek(&position1), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(&value);
    ASSERT_EQ(value, 3);   

    ASSERT_NE(_column_reader->next(), OLAP_SUCCESS);
}


TEST_F(TestColumn, SkipTinyColumnWithoutPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("TinyColumn"), 
                 OLAP_FIELD_TYPE_TINYINT, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 1, 
                 false,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    char value = 1;
    write_row.read(&value, sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    value = 2;
    write_row.read(&value, sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    value = 3;
    write_row.read(&value, sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    ASSERT_EQ(_column_reader->skip(2), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(&value);
    ASSERT_EQ(value, 3);    

    ASSERT_NE(_column_reader->next(), OLAP_SUCCESS);
}


TEST_F(TestColumn, TinyColumnWithPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("TinyColumn"), 
                 OLAP_FIELD_TYPE_TINYINT, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 1, 
                 true,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    char value = 1;
    write_row.read(&value, sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    value = 3;
    write_row.read(&value, sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(&value);
    ASSERT_EQ(value, 1);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(&value);
    ASSERT_EQ(value, 3);   
}

TEST_F(TestColumn, TinyColumnIndex) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("TinyColumn"), 
                 OLAP_FIELD_TYPE_TINYINT, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 1, 
                 true,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    char value = 1;
    write_row.read(&value, sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    value = 3;
    write_row.read(&value, sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(&value);
    ASSERT_EQ(value, 1);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(&value);
    ASSERT_EQ(value, 3);   
}

TEST_F(TestColumn, SeekTinyColumnWithPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("TinyColumn"), 
                 OLAP_FIELD_TYPE_TINYINT, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 1, 
                 true,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    char value = 1;
    write_row.read(&value, sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    value = 2;
    write_row.read(&value, sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    create_and_save_last_position();
    
    value = 3;
    write_row.read(&value, sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    create_and_save_last_position();

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    PositionEntryReader entry1;
    entry1._positions = _column_writer->index()->mutable_entry(0)->_positions;
    entry1._positions_count = _column_writer->index()->mutable_entry(0)->_positions_count;
    entry1._statistics.init(OLAP_FIELD_TYPE_NONE, false);

    PositionEntryReader entry2;
    entry2._positions = _column_writer->index()->mutable_entry(1)->_positions;
    entry2._positions_count = _column_writer->index()->mutable_entry(1)->_positions_count;
    entry2._statistics.init(OLAP_FIELD_TYPE_NONE, false);

    PositionProvider position1(&entry1);
    PositionProvider position2(&entry2);
    
    ASSERT_EQ(_column_reader->seek(&position1), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(&value);
    ASSERT_EQ(value, 1);   

    ASSERT_EQ(_column_reader->seek(&position2), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(&value);
    ASSERT_EQ(value, 3);   

    ASSERT_NE(_column_reader->next(), OLAP_SUCCESS);
}

TEST_F(TestColumn, SkipTinyColumnWithPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("TinyColumn"), 
                 OLAP_FIELD_TYPE_TINYINT, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 1, 
                 true,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    char value = 1;
    write_row.read(&value, sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    value = 2;
    write_row.read(&value, sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    value = 3;
    write_row.read(&value, sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    ASSERT_EQ(_column_reader->skip(2), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(&value);
    ASSERT_EQ(value, 3);    

    ASSERT_NE(_column_reader->next(), OLAP_SUCCESS);
}

TEST_F(TestColumn, ShortColumnWithoutPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("ShortColumn"), 
                 OLAP_FIELD_TYPE_SMALLINT, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 2, 
                 false,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    int16_t value = 1;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    value = 3;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_EQ(value, 1);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_EQ(value, 3);     
}

TEST_F(TestColumn, SeekShortColumnWithoutPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("ShortColumn"), 
                 OLAP_FIELD_TYPE_SMALLINT, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 2, 
                 false,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    int16_t value = 1;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    value = 2;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    create_and_save_last_position();
    
    value = 3;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    create_and_save_last_position();

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    PositionEntryReader entry1;
    entry1._positions = _column_writer->index()->mutable_entry(0)->_positions;
    entry1._positions_count = _column_writer->index()->mutable_entry(0)->_positions_count;
    entry1._statistics.init(OLAP_FIELD_TYPE_NONE, false);

    PositionEntryReader entry2;
    entry2._positions = _column_writer->index()->mutable_entry(1)->_positions;
    entry2._positions_count = _column_writer->index()->mutable_entry(1)->_positions_count;
    entry2._statistics.init(OLAP_FIELD_TYPE_NONE, false);

    PositionProvider position0(&entry1);
    PositionProvider position1(&entry2);
    
    ASSERT_EQ(_column_reader->seek(&position0), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_EQ(value, 1);   

    ASSERT_EQ(_column_reader->seek(&position1), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_EQ(value, 3);   
}

TEST_F(TestColumn, SkipShortColumnWithoutPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("ShortColumn"), 
                 OLAP_FIELD_TYPE_SMALLINT, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 2, 
                 false,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    int16_t value = 1;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    value = 2;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    value = 3;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    ASSERT_EQ(_column_reader->skip(2), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_EQ(value, 3);   
}

TEST_F(TestColumn, ShortColumnWithPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("ShortColumn"), 
                 OLAP_FIELD_TYPE_SMALLINT, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 2, 
                 true,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    int16_t value = 1;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    value = 3;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_EQ(value, 1);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_EQ(value, 3);       
}

TEST_F(TestColumn, SeekShortColumnWithPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("ShortColumn"), 
                 OLAP_FIELD_TYPE_SMALLINT, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 2, 
                 true,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    int16_t value = 1;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    value = 2;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    create_and_save_last_position();
    
    value = 3;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    create_and_save_last_position();

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    PositionEntryReader entry1;
    entry1._positions = _column_writer->index()->mutable_entry(0)->_positions;
    entry1._positions_count = _column_writer->index()->mutable_entry(0)->_positions_count;
    entry1._statistics.init(OLAP_FIELD_TYPE_NONE, false);

    PositionEntryReader entry2;
    entry2._positions = _column_writer->index()->mutable_entry(1)->_positions;
    entry2._positions_count = _column_writer->index()->mutable_entry(1)->_positions_count;
    entry2._statistics.init(OLAP_FIELD_TYPE_NONE, false);

    PositionProvider position0(&entry1);
    PositionProvider position1(&entry2);
    
    ASSERT_EQ(_column_reader->seek(&position0), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_EQ(value, 1);   

    ASSERT_EQ(_column_reader->seek(&position1), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_EQ(value, 3);   
}

TEST_F(TestColumn, SkipShortColumnWithPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("ShortColumn"), 
                 OLAP_FIELD_TYPE_SMALLINT, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 2, 
                 true,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    int16_t value = 1;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    value = 2;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    value = 3;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    ASSERT_EQ(_column_reader->skip(2), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_EQ(value, 3);   
}


TEST_F(TestColumn, UnsignedShortColumnWithoutPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("UnsignedShortColumn"), 
                 OLAP_FIELD_TYPE_UNSIGNED_SMALLINT,
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 2, 
                 false,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    uint16_t value = 1;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    value = 3;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_EQ(value, 1);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_EQ(value, 3);     
}

TEST_F(TestColumn, UnsignedShortColumnWithPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("UnsignedShortColumn"), 
                 OLAP_FIELD_TYPE_UNSIGNED_SMALLINT, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 2, 
                 true,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    uint16_t value = 1;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    value = 3;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_EQ(value, 1);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_EQ(value, 3);     
}

TEST_F(TestColumn, IntColumnWithoutPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("UnsignedShortColumn"), 
                 OLAP_FIELD_TYPE_INT, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 4, 
                 false,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    int32_t value = 1;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    value = 3;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_EQ(value, 1);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_EQ(value, 3);     
}


TEST_F(TestColumn, IntColumnMassWithoutPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("UnsignedShortColumn"), 
                 OLAP_FIELD_TYPE_INT, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 4, 
                 false,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    
    for (int32_t i = 0; i < 10000; i++) {
        write_row.read(reinterpret_cast<char *>(&i), sizeof(i));
        ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);
    }

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    for (int32_t i = 0; i < 10000; i++) {
        int32_t value;
        ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
        ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
        read_row.write(reinterpret_cast<char *>(&value));
        ASSERT_EQ(value, i);
    }
}


TEST_F(TestColumn, IntColumnWithPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("UnsignedShortColumn"), 
                 OLAP_FIELD_TYPE_INT, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 4, 
                 true,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    int32_t value = -1;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    value = 3;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_EQ(value, -1);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_EQ(value, 3);     
}

TEST_F(TestColumn, UnsignedIntColumnWithoutPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("UnsignedShortColumn"), 
                 OLAP_FIELD_TYPE_UNSIGNED_INT, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 4, 
                 false,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    uint32_t value = 1;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    value = 3;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_EQ(value, 1);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_EQ(value, 3);     
}

TEST_F(TestColumn, UnsignedIntColumnWithPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("UnsignedShortColumn"), 
                 OLAP_FIELD_TYPE_UNSIGNED_INT, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 4, 
                 true,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    uint32_t value = 1;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    value = 3;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_EQ(value, 1);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_EQ(value, 3);     
}

TEST_F(TestColumn, LongColumnWithoutPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("LongColumnWithoutPresent"), 
                 OLAP_FIELD_TYPE_BIGINT, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 8, 
                 false,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    int64_t value = 1;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    value = 3;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_EQ(value, 1);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_EQ(value, 3);     
}

TEST_F(TestColumn, LongColumnWithPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("LongColumnWithPresent"), 
                 OLAP_FIELD_TYPE_BIGINT, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 8, 
                 true,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    int64_t value = 1;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    value = 3;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_EQ(value, 1);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_EQ(value, 3);     
}

TEST_F(TestColumn, UnsignedLongColumnWithoutPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("UnsignedLongColumnWithoutPresent"), 
                 OLAP_FIELD_TYPE_UNSIGNED_BIGINT, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 8, 
                 false,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    uint64_t value = 1;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    value = 3;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_EQ(value, 1);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_EQ(value, 3);     
}

TEST_F(TestColumn, UnsignedLongColumnWithPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("UnsignedLongColumnWithoutPresent"), 
                 OLAP_FIELD_TYPE_UNSIGNED_BIGINT, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 8, 
                 true,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    uint64_t value = 1;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    value = 3;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_EQ(value, 1);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_EQ(value, 3);     
}

TEST_F(TestColumn, FloatColumnWithoutPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("UnsignedLongColumnWithoutPresent"), 
                 OLAP_FIELD_TYPE_FLOAT, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 4, 
                 false,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    float value = 1.234;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    value = 3.234;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_FLOAT_EQ(value, 1.234);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_FLOAT_EQ(value, 3.234);  
 
}

TEST_F(TestColumn, FloatColumnWithPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("FloatColumnWithPresent"), 
                 OLAP_FIELD_TYPE_FLOAT, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 4, 
                 true,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    float value = 1.234;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    value = 3.234;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_FLOAT_EQ(value, 1.234);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_FLOAT_EQ(value, 3.234);  
  
}

TEST_F(TestColumn, SeekFloatColumnWithPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("FloatColumnWithPresent"), 
                 OLAP_FIELD_TYPE_FLOAT, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 4, 
                 true,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    float value = 1.234;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    create_and_save_last_position();
    
    value = 3.234;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    create_and_save_last_position();

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    PositionEntryReader entry1;
    entry1._positions = _column_writer->index()->mutable_entry(0)->_positions;
    entry1._positions_count = _column_writer->index()->mutable_entry(0)->_positions_count;
    entry1._statistics.init(OLAP_FIELD_TYPE_NONE, false);

    PositionEntryReader entry2;
    entry2._positions = _column_writer->index()->mutable_entry(1)->_positions;
    entry2._positions_count = _column_writer->index()->mutable_entry(1)->_positions_count;
    entry2._statistics.init(OLAP_FIELD_TYPE_NONE, false);

    PositionProvider position0(&entry1);
    PositionProvider position1(&entry2);
    
    ASSERT_EQ(_column_reader->seek(&position0), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_FLOAT_EQ(value, 1.234);

    ASSERT_EQ(_column_reader->seek(&position1), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_FLOAT_EQ(value, 3.234);    
}

TEST_F(TestColumn, SkipFloatColumnWithPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("FloatColumnWithPresent"), 
                 OLAP_FIELD_TYPE_FLOAT, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 4, 
                 true,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    float value = 1.234;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    value = 3.234;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    ASSERT_EQ(_column_reader->skip(1), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_FLOAT_EQ(value, 3.234);    
}

TEST_F(TestColumn, DoubleColumnWithoutPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("DoubleColumnWithoutPresent"), 
                 OLAP_FIELD_TYPE_DOUBLE, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 8, 
                 false,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    double value = 1.23456789;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    value = 3.23456789;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_DOUBLE_EQ(value, 1.23456789);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_DOUBLE_EQ(value, 3.23456789);  
 
}

TEST_F(TestColumn, DoubleColumnWithPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("DoubleColumnWithPresent"), 
                 OLAP_FIELD_TYPE_DOUBLE, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 8, 
                 true,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    double value = 1.23456789;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    value = 3.23456789;
    write_row.read(reinterpret_cast<char *>(&value), sizeof(value));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_DOUBLE_EQ(value, 1.23456789);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    read_row.write(reinterpret_cast<char *>(&value));
    ASSERT_DOUBLE_EQ(value, 3.23456789);  
 
}

TEST_F(TestColumn, DiscreteDoubleColumnWithoutPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("DoubleColumnWithoutPresent"), 
                 OLAP_FIELD_TYPE_DISCRETE_DOUBLE, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 8, 
                 false,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    
    std::vector<string> val_string_array;
    val_string_array.push_back("1234.5678");
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    char read_value[20];
    memset(read_value, 0, 20);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), "0&1234.5678", strlen("0&1234.5678")) == 0);
 
}

TEST_F(TestColumn, DiscreteDoubleColumnWithPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("DiscreteDoubleColumnWithPresent"), 
                 OLAP_FIELD_TYPE_DISCRETE_DOUBLE, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 8, 
                 true,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    
    std::vector<string> val_string_array;
    val_string_array.push_back("1234.5678");
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    val_string_array.clear();
    val_string_array.push_back("5678.1234");
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    char read_value[20];
    memset(read_value, 0, 20);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), "0&1234.5678", strlen("0&1234.5678")) == 0);

    memset(read_value, 0, 20);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), "0&5678.1234", strlen("0&5678.1234")) == 0);
 
}

TEST_F(TestColumn, SeekDiscreteDoubleColumnWithPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("DiscreteDoubleColumnWithPresent"), 
                 OLAP_FIELD_TYPE_DISCRETE_DOUBLE, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 8, 
                 true,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    
    std::vector<string> val_string_array;
    val_string_array.push_back("1234.5678");
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    create_and_save_last_position();
    
    val_string_array.clear();
    val_string_array.push_back("5678.1234");
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    create_and_save_last_position();

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    PositionEntryReader entry1;
    entry1._positions = _column_writer->index()->mutable_entry(0)->_positions;
    entry1._positions_count = _column_writer->index()->mutable_entry(0)->_positions_count;
    entry1._statistics.init(OLAP_FIELD_TYPE_NONE, false);

    PositionEntryReader entry2;
    entry2._positions = _column_writer->index()->mutable_entry(1)->_positions;
    entry2._positions_count = _column_writer->index()->mutable_entry(1)->_positions_count;
    entry2._statistics.init(OLAP_FIELD_TYPE_NONE, false);

    PositionProvider position0(&entry1);
    PositionProvider position1(&entry2);

    ASSERT_EQ(_column_reader->seek(&position0), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), "0&1234.5678", strlen("0&1234.5678")) == 0);

    ASSERT_EQ(_column_reader->seek(&position1), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), "0&5678.1234", strlen("0&5678.1234")) == 0);
}

TEST_F(TestColumn, SkipDiscreteDoubleColumnWithPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("DiscreteDoubleColumnWithPresent"), 
                 OLAP_FIELD_TYPE_DISCRETE_DOUBLE, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 8, 
                 true,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    
    std::vector<string> val_string_array;
    val_string_array.push_back("1234.5678");
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    val_string_array.clear();
    val_string_array.push_back("5678.1234");
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    PositionEntryReader entry2;
    entry2._positions = _column_writer->index_entry()->_positions;
    entry2._positions_count = _column_writer->index_entry()->_positions_count;
    entry2._statistics.init(OLAP_FIELD_TYPE_NONE, false);

    UniqueIdPositionProviderMap positions_map;
    positions_map[0] = PositionProvider(&entry2);
    char read_value[20];

    memset(read_value, 0, 20);
    ASSERT_EQ(_column_reader->skip(1), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), "0&5678.1234", strlen("0&5678.1234")) == 0);
}

TEST_F(TestColumn, DatetimeColumnWithoutPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("DatetimeColumnWithoutPresent"), 
                 OLAP_FIELD_TYPE_DATETIME, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 8, 
                 false,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    
    std::vector<string> val_string_array;
    val_string_array.push_back("2000-10-10 10:10:10");
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    char read_value[20];
    memset(read_value, 0, 20);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), "0&2000-10-10 10:10:10", strlen("0&2000-10-10 10:10:10")) == 0);
 
}

TEST_F(TestColumn, DatetimeColumnWithPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("DatetimeColumnWithoutPresent"), 
                 OLAP_FIELD_TYPE_DATETIME, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 8, 
                 true,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    
    std::vector<string> val_string_array;
    val_string_array.push_back("2000-10-10 10:10:10");
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    char read_value[20];
    memset(read_value, 0, 20);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), "0&2000-10-10 10:10:10", strlen("0&2000-10-10 10:10:10")) == 0);

    ASSERT_NE(_column_reader->next(), OLAP_SUCCESS);
 
}

TEST_F(TestColumn, DateColumnWithoutPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("DateColumnWithoutoutPresent"), 
                 OLAP_FIELD_TYPE_DATE, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 3, 
                 false,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    
    std::vector<string> val_string_array;
    val_string_array.push_back("2000-10-10");
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    char read_value[20];
    memset(read_value, 0, 20);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), "0&2000-10-10", strlen("0&2000-10-10")) == 0);
}

TEST_F(TestColumn, DateColumnWithPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("DateColumnWithoutoutPresent"), 
                 OLAP_FIELD_TYPE_DATE, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 3, 
                 true,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    
    std::vector<string> val_string_array;
    val_string_array.push_back("2000-10-10");
    write_row.from_string(val_string_array);
    for (uint32_t i = 0; i < 100; ++i) {
        ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);
    }
    
    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    char read_value[20];
    for (uint32_t i = 0; i < 100; ++i) {
        memset(read_value, 0, 20);
        ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
        ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
        ASSERT_TRUE(strncmp(read_row.to_string().c_str(), "0&2000-10-10", strlen("0&2000-10-10")) == 0);
    }

    ASSERT_NE(_column_reader->next(), OLAP_SUCCESS);
}

TEST_F(TestColumn, DecimalColumnWithoutPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("DecimalColumnWithoutoutPresent"), 
                 OLAP_FIELD_TYPE_DECIMAL, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 12, 
                 false,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    
    std::vector<string> val_string_array;
    val_string_array.push_back("1234.5678");
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    val_string_array.clear();
    val_string_array.push_back("5678.1234");
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    char read_value[20];
    memset(read_value, 0, 20);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), "0&1234.5678", strlen("0&1234.5678")) == 0);

    memset(read_value, 0, 20);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), "0&5678.1234", strlen("0&5678.1234")) == 0);
}

TEST_F(TestColumn, DecimalColumnWithPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("DecimalColumnWithoutoutPresent"), 
                 OLAP_FIELD_TYPE_DECIMAL, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 12, 
                 true,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    
    std::vector<string> val_string_array;
    val_string_array.push_back("1234.5678");
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    val_string_array.clear();
    val_string_array.push_back("5678.1234");
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    char read_value[20];
    memset(read_value, 0, 20);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), "0&1234.5678", strlen("0&1234.5678")) == 0);

    memset(read_value, 0, 20);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), "0&5678.1234", strlen("0&5678.1234")) == 0);
}

TEST_F(TestColumn, SkipDecimalColumnWithPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("DecimalColumnWithPresent"), 
                 OLAP_FIELD_TYPE_DECIMAL, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 12, 
                 true,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    
    std::vector<string> val_string_array;
    val_string_array.push_back("1234.5678");
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    val_string_array.clear();
    val_string_array.push_back("5678.1234");
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    char read_value[20];
    memset(read_value, 0, 20);
    ASSERT_EQ(_column_reader->skip(1), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), "0&5678.1234", strlen("0&5678.1234")) == 0);
}

TEST_F(TestColumn, SeekDecimalColumnWithPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("DecimalColumnWithPresent"), 
                 OLAP_FIELD_TYPE_DECIMAL, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 12, 
                 true,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    
    std::vector<string> val_string_array;
    val_string_array.push_back("1234.5678");
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    create_and_save_last_position();
    
    val_string_array.clear();
    val_string_array.push_back("5678.1234");
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    create_and_save_last_position();

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);
    PositionEntryReader entry1;
    entry1._positions = _column_writer->index()->mutable_entry(0)->_positions;
    entry1._positions_count = _column_writer->index()->mutable_entry(0)->_positions_count;
    entry1._statistics.init(OLAP_FIELD_TYPE_NONE, false);

    PositionEntryReader entry2;
    entry2._positions = _column_writer->index()->mutable_entry(1)->_positions;
    entry2._positions_count = _column_writer->index()->mutable_entry(1)->_positions_count;
    entry2._statistics.init(OLAP_FIELD_TYPE_NONE, false);

    PositionProvider position0(&entry1);
    PositionProvider position1(&entry2);

    char read_value[20];
    memset(read_value, 0, 20);
    ASSERT_EQ(_column_reader->seek(&position0), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), "0&1234.5678", strlen("0&1234.5678")) == 0);
    
    memset(read_value, 0, 20);
    ASSERT_EQ(_column_reader->seek(&position1), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), "0&5678.1234", strlen("0&5678.1234")) == 0);
}

TEST_F(TestColumn, LargeIntColumnWithoutPresent) {
    // init table schema
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("LargeIntColumnWithoutoutPresent"), 
                 OLAP_FIELD_TYPE_LARGEINT, 
                 OLAP_FIELD_AGGREGATION_SUM, 
                 16, 
                 false,
                 true);
    tablet_schema.push_back(field_info);

    // test data
    string value1 = "100000000000000000000000000000000000000";
    string value2 = "-170141183460469231731687303715884105728";

    // write data
    CreateColumnWriter(tablet_schema);
    RowCursor write_row;
    write_row.init(tablet_schema);

    std::vector<string> val_string_array;
    val_string_array.push_back(value1);
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    val_string_array.clear();
    val_string_array.push_back(value2);
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    RowCursor read_row;
    read_row.init(tablet_schema);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS); 
    value1 = "0&" + value1;
    value2 = "0&" + value2;
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), value1.c_str(), value1.size()) == 0);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), value2.c_str(), value2.size()) == 0);
}

TEST_F(TestColumn, LargeIntColumnWithPresent) {
    // init table schema
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("LargeIntColumnWithoutoutPresent"), 
                 OLAP_FIELD_TYPE_LARGEINT, 
                 OLAP_FIELD_AGGREGATION_SUM, 
                 16, 
                 true,
                 true);
    tablet_schema.push_back(field_info);

    // test data
    string value1 = "100000000000000000000000000000000000000";
    string value2 = "-170141183460469231731687303715884105728";

    // write data
    CreateColumnWriter(tablet_schema);
    RowCursor write_row;
    write_row.init(tablet_schema);
    
    std::vector<string> val_string_array;
    val_string_array.push_back(value1);
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    val_string_array.clear();
    val_string_array.push_back(value2);
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    RowCursor read_row;
    read_row.init(tablet_schema);

    value1 = "0&" + value1;
    value2 = "0&" + value2;
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), value1.c_str(), value1.size()) == 0);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), value2.c_str(), value2.size()) == 0);
}

TEST_F(TestColumn, SkipLargeIntColumnWithPresent) {
    // init table schema
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("LargeIntColumnWithPresent"), 
                 OLAP_FIELD_TYPE_LARGEINT, 
                 OLAP_FIELD_AGGREGATION_SUM, 
                 16, 
                 true,
                 true);
    tablet_schema.push_back(field_info);

    // test data
    string value1 = "100000000000000000000000000000000000000";
    string value2 = "-170141183460469231731687303715884105728";

    // write data
    CreateColumnWriter(tablet_schema);
    RowCursor write_row;
    write_row.init(tablet_schema);
    
    std::vector<string> val_string_array;
    val_string_array.push_back(value1);
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    val_string_array.clear();
    val_string_array.push_back(value2);
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    RowCursor read_row;
    read_row.init(tablet_schema);

    value2 = "0&" + value2; 
    ASSERT_EQ(_column_reader->skip(1), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    ASSERT_TRUE(strncmp(read_row.to_string().c_str(), value2.c_str(), value2.size()) == 0);
}

// TODO(jiangguoqiang): this test has a problem. Need to fix it.
// TEST_F(TestColumn, SeekLargeIntColumnWithPresent) {
    // // init table schema
    // std::vector<FieldInfo> tablet_schema;
    // FieldInfo field_info;
    // SetFieldInfo(field_info,
                 // std::string("LargeIntColumnWithPresent"), 
                 // OLAP_FIELD_TYPE_LARGEINT, 
                 // OLAP_FIELD_AGGREGATION_SUM, 
                 // 16, 
                 // true,
                 // true);
    // tablet_schema.push_back(field_info);

    // // test data
    // string value1 = "100000000000000000000000000000000000000";
    // string value2 = "-170141183460469231731687303715884105728";
    // string value3 = "65535";

    // // write data
    // CreateColumnWriter(tablet_schema);
    // RowCursor write_row;
    // write_row.init(tablet_schema);
    
    // std::vector<string> val_string_array;
    // val_string_array.push_back(value1);
    // write_row.from_string(val_string_array);
    // ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    // val_string_array.clear();
    // val_string_array.push_back(value2);
    // write_row.from_string(val_string_array);
    // ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    // create_and_save_last_position();
    
    // val_string_array.clear();
    // val_string_array.push_back(value3);
    // write_row.from_string(val_string_array);
    // ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    // create_and_save_last_position();

    // ColumnDataHeaderMessage header;
    // ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // // read data
    // CreateColumnReader(tablet_schema);
    // RowCursor read_row;
    // read_row.init(tablet_schema);
    // PositionEntryReader entry0;
    // entry0._positions = _column_writer->index()->mutable_entry(0)->_positions;
    // entry0._positions_count = _column_writer->index()->mutable_entry(0)->_positions_count;
    // entry0._statistics.init(OLAP_FIELD_TYPE_NONE);

    // PositionEntryReader entry1;
    // entry1._positions = _column_writer->index()->mutable_entry(1)->_positions;
    // entry1._positions_count = _column_writer->index()->mutable_entry(1)->_positions_count;
    // entry1._statistics.init(OLAP_FIELD_TYPE_NONE);

    // PositionProvider position0(&entry0);
    // PositionProvider position1(&entry1);

    // ASSERT_EQ(_column_reader->seek(&position0), OLAP_SUCCESS);
    // ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    // ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    // ASSERT_TRUE(strncmp(read_row.to_string().c_str(), value1.c_str(), value1.size()) == 0);
    
    // ASSERT_EQ(_column_reader->seek(&position1), OLAP_SUCCESS);
    // ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    // ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    // ASSERT_TRUE(strncmp(read_row.to_string().c_str(), value3.c_str(), value3.size()) == 0);
// }

TEST_F(TestColumn, DirectVarcharColumnWithoutPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("DirectVarcharColumnWithoutoutPresent"), 
                 OLAP_FIELD_TYPE_VARCHAR, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 10, 
                 false,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    
    std::vector<string> val_string_array;
    val_string_array.push_back("YWJjZGU="); //"abcde" base_64_encode is "YWJjZGU="
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);
    for (uint32_t i = 0; i < 2; i++) {
        ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);
    }
    val_string_array.clear();
    val_string_array.push_back("ZWRjYmE="); //"edcba" base_64_encode is "ZWRjYmE="
    write_row.from_string(val_string_array);
    for (uint32_t i = 0; i < 2; i++) {
        ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);
    }
    

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    char read_value[20];
    memset(read_value, 0, 20);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    ASSERT_TRUE(strcmp(read_row.to_string().c_str(), "0&YWJjZGU=") == 0);
    for (uint32_t i = 0; i < 2; i++) {
        ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
        ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
        ASSERT_TRUE(strcmp(read_row.to_string().c_str(), "0&YWJjZGU=") == 0);
    }
    for (uint32_t i = 0; i < 2; i++) {
        ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
        ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
        ASSERT_TRUE(strcmp(read_row.to_string().c_str(), "0&ZWRjYmE=") == 0);
    }
    ASSERT_NE(_column_reader->next(), OLAP_SUCCESS);
}

TEST_F(TestColumn, DirectVarcharColumnWithPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("DirectVarcharColumnWithoutoutPresent"), 
                 OLAP_FIELD_TYPE_VARCHAR, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 10, 
                 true,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    
    std::vector<string> val_string_array;
    val_string_array.push_back("YWJjZGU="); //"abcde" base_64_encode is "YWJjZGU="
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    char read_value[20];
    memset(read_value, 0, 20);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    ASSERT_TRUE(strcmp(read_row.to_string().c_str(), "0&YWJjZGU=") == 0);
}

TEST_F(TestColumn, SkipDirectVarcharColumnWithPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("DirectVarcharColumnWithPresent"), 
                 OLAP_FIELD_TYPE_VARCHAR, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 10, 
                 true,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    
    std::vector<string> val_string_array;
    val_string_array.push_back("YWJjZGU="); //"abcde" base_64_encode is "YWJjZGU="
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    val_string_array.clear();
    val_string_array.push_back("YWFhYWE="); //"aaaaa" base_64_encode is "YWJjZGU="
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    char read_value[20];
    memset(read_value, 0, 20);
    ASSERT_EQ(_column_reader->skip(1), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    ASSERT_TRUE(strcmp(read_row.to_string().c_str(), "0&YWFhYWE=") == 0);
}

TEST_F(TestColumn, SeekDirectVarcharColumnWithoutPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("DirectVarcharColumnWithPresent"), 
                 OLAP_FIELD_TYPE_VARCHAR, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 10, 
                 false,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    
    std::vector<string> val_string_array;
    val_string_array.push_back("YWJjZGU="); //"abcde" base_64_encode is "YWJjZGU="
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    _column_writer->create_row_index_entry();
    
    val_string_array.clear();
    val_string_array.push_back("YWFhYWE="); //"aaaaa" base_64_encode is "YWJjZGU="
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);
    
    _column_writer->create_row_index_entry();

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);


    PositionEntryReader entry1;
    entry1._positions = _column_writer->index()->mutable_entry(0)->_positions;
    entry1._positions_count = _column_writer->index()->mutable_entry(0)->_positions_count;
    entry1._statistics.init(OLAP_FIELD_TYPE_NONE, false);

    PositionEntryReader entry2;
    entry2._positions = _column_writer->index()->mutable_entry(1)->_positions;
    entry2._positions_count = _column_writer->index()->mutable_entry(1)->_positions_count;
    entry2._statistics.init(OLAP_FIELD_TYPE_NONE, false);

    PositionProvider position0(&entry1);
    PositionProvider position1(&entry2);

    ASSERT_EQ(_column_reader->seek(&position0), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    ASSERT_TRUE(strcmp(read_row.to_string().c_str(), "0&YWJjZGU=") == 0);

    ASSERT_EQ(_column_reader->seek(&position1), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    ASSERT_TRUE(strcmp(read_row.to_string().c_str(), "0&YWFhYWE=") == 0);

    ASSERT_NE(_column_reader->next(), OLAP_SUCCESS);
}

TEST_F(TestColumn, SeekDirectVarcharColumnWithPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("DirectVarcharColumnWithPresent"), 
                 OLAP_FIELD_TYPE_VARCHAR, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 10, 
                 true,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    
    std::vector<string> val_string_array;
    val_string_array.push_back("YWJjZGU="); //"abcde" base_64_encode is "YWJjZGU="
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    _column_writer->create_row_index_entry();
    
    val_string_array.clear();
    val_string_array.push_back("YWFhYWE="); //"aaaaa" base_64_encode is "YWJjZGU="
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);
    
    _column_writer->create_row_index_entry();

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    PositionEntryReader entry1;
    entry1._positions = _column_writer->index()->mutable_entry(0)->_positions;
    entry1._positions_count = _column_writer->index()->mutable_entry(0)->_positions_count;
    entry1._statistics.init(OLAP_FIELD_TYPE_NONE, false);

    PositionEntryReader entry2;
    entry2._positions = _column_writer->index()->mutable_entry(1)->_positions;
    entry2._positions_count = _column_writer->index()->mutable_entry(1)->_positions_count;
    entry2._statistics.init(OLAP_FIELD_TYPE_NONE, false);

    PositionProvider position0(&entry1);
    PositionProvider position1(&entry2);

    ASSERT_EQ(_column_reader->seek(&position0), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    ASSERT_TRUE(strcmp(read_row.to_string().c_str(), "0&YWJjZGU=") == 0);


    ASSERT_EQ(_column_reader->seek(&position1), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    ASSERT_TRUE(strcmp(read_row.to_string().c_str(), "0&YWFhYWE=") == 0);

    ASSERT_NE(_column_reader->next(), OLAP_SUCCESS);
}

TEST_F(TestColumn, StringColumnWithoutPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("VarcharColumnWithoutoutPresent"), 
                 OLAP_FIELD_TYPE_CHAR, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 strlen("abcde"), 
                 false,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    
    std::vector<string> val_string_array;
    val_string_array.push_back("abcde"); //"abcde" base_64_encode is "YWJjZGU="
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);
    for (uint32_t i = 0; i < 2; i++) {
        ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);
    }
    val_string_array.clear();
    val_string_array.push_back("edcba"); //"edcba" base_64_encode is "ZWRjYmE="
    write_row.from_string(val_string_array);
    for (uint32_t i = 0; i < 2; i++) {
        ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);
    }

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    char read_value[20];
    memset(read_value, 0, 20);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    ASSERT_TRUE(strcmp(read_row.to_string().c_str(), "0&abcde") == 0);
    for (uint32_t i = 0; i < 2; i++) {
        ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
        ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
        ASSERT_TRUE(strcmp(read_row.to_string().c_str(), "0&abcde") == 0);  
    }
    for (uint32_t i = 0; i < 2; i++) {
        ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
        ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
        ASSERT_TRUE(strcmp(read_row.to_string().c_str(), "0&edcba") == 0);  
    }
    ASSERT_NE(_column_reader->next(), OLAP_SUCCESS);
}

TEST_F(TestColumn, StringColumnWithPresent) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("VarcharColumnWithoutoutPresent"), 
                 OLAP_FIELD_TYPE_CHAR, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 strlen("abcde"), 
                 true,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    
    std::vector<string> val_string_array;
    val_string_array.push_back("abcde"); //"abcde" base_64_encode is "YWJjZGU="
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    char read_value[20];
    memset(read_value, 0, 20);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    ASSERT_TRUE(strcmp(read_row.to_string().c_str(), "0&abcde") == 0);
}

TEST_F(TestColumn, StringColumnWithoutoutPresent2) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("VarcharColumnWithoutoutPresent"), 
                 OLAP_FIELD_TYPE_CHAR, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 20, 
                 false,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);
    
    std::vector<string> val_string_array;
    val_string_array.push_back("abcde"); //"abcde" base_64_encode is "YWJjZGU="
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);
    val_string_array.clear();
    val_string_array.push_back("aaaaa"); //"abcde" base_64_encode is "YWJjZGU="
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);
    val_string_array.clear();
    val_string_array.push_back("bbbbb"); //"abcde" base_64_encode is "YWJjZGU="
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);
    val_string_array.clear();
    val_string_array.push_back("ccccc"); //"abcde" base_64_encode is "YWJjZGU="
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);
    val_string_array.clear();
    val_string_array.push_back("ddddd"); //"abcde" base_64_encode is "YWJjZGU="
    write_row.from_string(val_string_array);
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    char read_value[20];
    memset(read_value, 0, 20);
    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    ASSERT_STREQ(read_row._field_array[0]->_buf, "abcde");

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    ASSERT_STREQ(read_row._field_array[0]->_buf, "aaaaa");

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    ASSERT_STREQ(read_row._field_array[0]->_buf, "bbbbb");

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    ASSERT_STREQ(read_row._field_array[0]->_buf, "ccccc");

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
    ASSERT_STREQ(read_row._field_array[0]->_buf, "ddddd");    
}

TEST_F(TestColumn, DirectVarcharColumnWith65533) {
    // write data
    std::vector<FieldInfo> tablet_schema;
    FieldInfo field_info;
    SetFieldInfo(field_info,
                 std::string("DirectVarcharColumnWithoutoutPresent"), 
                 OLAP_FIELD_TYPE_VARCHAR, 
                 OLAP_FIELD_AGGREGATION_REPLACE, 
                 65535, 
                 false,
                 true);
    tablet_schema.push_back(field_info);

    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);

    std::vector<string> val_string_array;
    val_string_array.push_back(std::string(65533, 'a')); 
    ASSERT_EQ(OLAP_SUCCESS, write_row.from_string(val_string_array));
    ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);

    val_string_array.clear();
    val_string_array.push_back("edcba"); //"edcba" base_64_encode is "ZWRjYmE="
    write_row.from_string(val_string_array);
    for (uint32_t i = 0; i < 2; i++) {
        ASSERT_EQ(_column_writer->write(&write_row), OLAP_SUCCESS);
    }   

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
    ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);

    for (uint32_t i = 0; i < 65533 + 2; i++) {
	if (0 == i) {
	    ASSERT_EQ(read_row.to_string().c_str()[i], '0');		
	} else if (1 == i) {
	    ASSERT_EQ(read_row.to_string().c_str()[i], '&');
	} else {
            ASSERT_EQ(read_row.to_string().c_str()[i], 'a');
	}
    }

    for (uint32_t i = 0; i < 2; i++) {
        ASSERT_EQ(_column_reader->next(), OLAP_SUCCESS);
        ASSERT_EQ(_column_reader->attach(&read_row), OLAP_SUCCESS);
        ASSERT_TRUE(strcmp(read_row.to_string().c_str(), "0&edcba") == 0);
    }   
}


}
}

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("PALO_HOME")) + "/conf/be.conf";
    if (!palo::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    palo::init_glog("be-test");
    int ret = palo::OLAP_SUCCESS;
    testing::InitGoogleTest(&argc, argv);
    ret = RUN_ALL_TESTS();
    google::protobuf::ShutdownProtobufLibrary();
    return ret;
}

