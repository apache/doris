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
#include "olap/stream_name.h"
#include "olap/rowset/column_reader.h"
#include "olap/rowset/column_writer.h"
#include "olap/field.h"
#include "olap/olap_define.h"
#include "olap/olap_common.h"
#include "olap/row_cursor.h"
#include "olap/row_block.h"
#include "runtime/mem_pool.h"
#include "runtime/string_value.hpp"
#include "runtime/vectorized_row_batch.h"
#include "util/logging.h"

using std::string;

namespace doris {

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
        _stream_factory = 
                new(std::nothrow) OutStreamFactory(COMPRESS_LZ4,
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

    void CreateColumnWriter(const TabletSchema& tablet_schema) {
        _column_writer = ColumnWriter::create(
                0, tablet_schema, _stream_factory, 1024, BLOOM_FILTER_DEFAULT_FPP);        
        ASSERT_TRUE(_column_writer != NULL);
        ASSERT_EQ(_column_writer->init(), OLAP_SUCCESS);
    }

    void CreateColumnReader(const TabletSchema& tablet_schema) {
        UniqueIdEncodingMap encodings;
        encodings[0] = ColumnEncodingMessage();
        encodings[0].set_kind(ColumnEncodingMessage::DIRECT);
        encodings[0].set_dictionary_size(1);
        CreateColumnReader(tablet_schema, encodings);
    }

    void CreateColumnReader(
            const TabletSchema& tablet_schema,
            UniqueIdEncodingMap &encodings) {
        UniqueIdToColumnIdMap included;
        included[0] = 0;
        UniqueIdToColumnIdMap segment_included;
        segment_included[0] = 0;

        _column_reader = ColumnReader::create(0,
                                     tablet_schema,
                                     included,
                                     segment_included,
                                     encodings);
        
        ASSERT_TRUE(_column_reader != NULL);

        system("mkdir -p ./ut_dir");
        system("rm ./ut_dir/tmp_file");

        ASSERT_EQ(OLAP_SUCCESS, 
                  helper.open_with_mode("./ut_dir/tmp_file", 
                                        O_CREAT | O_EXCL | O_WRONLY, 
                                        S_IRUSR | S_IWUSR));
        std::vector<int> off;
        std::vector<int> length;
        std::vector<int> buffer_size;
        std::vector<StreamName> name;

        std::map<StreamName, OutStream*>::const_iterator it 
            = _stream_factory->streams().begin();
        for (; it != _stream_factory->streams().end(); ++it) {
            StreamName stream_name = it->first;
            OutStream *out_stream = it->second;
            std::vector<StorageByteBuffer*> *buffers;

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

        ASSERT_EQ(OLAP_SUCCESS, helper.open_with_mode("./ut_dir/tmp_file", 
                O_RDONLY, S_IRUSR | S_IWUSR)); 

        _shared_buffer = StorageByteBuffer::create(
                OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE + sizeof(StreamHead));
        ASSERT_TRUE(_shared_buffer != NULL);

        for (int i = 0; i < off.size(); ++i) {
            ReadOnlyFileStream* in_stream = new (std::nothrow) ReadOnlyFileStream(
                    &helper, 
                    &_shared_buffer,
                    off[i], 
                    length[i], 
                    lz4_decompress, 
                    buffer_size[i],
                    &_stats);
            ASSERT_EQ(OLAP_SUCCESS, in_stream->init());

            _map_in_streams[name[i]] = in_stream;
        }

        ASSERT_EQ(_column_reader->init(
                   &_map_in_streams,
                   1024,
                   _mem_pool.get(),
                   &_stats), OLAP_SUCCESS);
    }

    void AddColumn(std::string name,
                 std::string type,
                 std::string aggregation,
                 uint32_t length,
                 bool is_allow_null,
                 bool is_key) {
        ColumnPB* column = tablet_schema_pb.add_column();
        column->set_unique_id(0);
        column->set_name(name);
        column->set_type(type);
        column->set_is_key(is_key);
        column->set_is_nullable(is_allow_null);
        column->set_length(length);
        column->set_aggregation(aggregation);
        column->set_precision(1000);
        column->set_frac(1000);
        column->set_is_bf_column(false);
    }

    void InitTablet(TabletSchema* tablet_schema) {
        tablet_schema->init_from_pb(tablet_schema_pb);
    }

    void create_and_save_last_position() {
        ASSERT_EQ(_column_writer->create_row_index_entry(), OLAP_SUCCESS);
    }

    ColumnWriter *_column_writer;

    ColumnReader *_column_reader;
    std::unique_ptr<MemTracker> _mem_tracker;
    std::unique_ptr<MemPool> _mem_pool;
    std::unique_ptr<ColumnVector> _col_vector;

    OutStreamFactory *_stream_factory;

    TabletSchemaPB tablet_schema_pb;

    std::vector<size_t> _offsets;
    std::vector<StorageByteBuffer*> _present_buffers;
    std::vector<StorageByteBuffer*> _data_buffers;
    std::vector<StorageByteBuffer*> _second_buffers;
    std::vector<StorageByteBuffer*> _dictionary_buffers;
    std::vector<StorageByteBuffer*> _length_buffers;
    StorageByteBuffer* _shared_buffer;
    std::map<StreamName, ReadOnlyFileStream *> _map_in_streams;
    FileHandler helper;
    OlapReaderStatistics _stats;
};


TEST_F(TestColumn, ConvertFloatToDouble) {
    // write data
    AddColumn(
            "FloatColumn", 
            "FLOAT", 
            "REPLACE", 
            4, 
            false,
            true);
    AddColumn(
            "DoubleColumn", 
            "DOUBLE", 
            "REPLACE", 
            4, 
            false,
            false);
    
    TabletSchema tablet_schema;
    InitTablet(&tablet_schema);
    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    float value = 1.234;
    write_row.set_field_content(0, reinterpret_cast<char *>(&value), _mem_pool.get());
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    value = 3.234;
    write_row.set_field_content(0, reinterpret_cast<char *>(&value), _mem_pool.get());
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);
    
    RowCursor read_row;
    read_row.init(tablet_schema);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(
        _col_vector.get(), 2, _mem_pool.get()), OLAP_SUCCESS);
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    read_row.set_field_content(0, data, _mem_pool.get());
    read_row.convert_from(1, data, read_row.column_schema(0)->type_info(), _mem_pool.get());    
    //float val1 = *reinterpret_cast<float*>( read_row.cell_ptr(0));
    double val2 = *reinterpret_cast<double*>( read_row.cell_ptr(1));
    
    char buf[64];
    memset(buf,0,sizeof(buf));
    sprintf(buf,"%f",val2);
    char* tg;
    double v2 = strtod(buf,&tg);    
    ASSERT_TRUE( v2 == 1.234 );
    
    //test not support type
    TypeInfo* tp = get_type_info(OLAP_FIELD_TYPE_HLL);
    OLAPStatus st = read_row.convert_from(1, data, tp, _mem_pool.get());
    ASSERT_TRUE( st == OLAP_ERR_INVALID_SCHEMA);
}

TEST_F(TestColumn, ConvertDatetimeToDate) {
    // write data
    AddColumn(
            "DatetimeColumn", 
            "DATETIME", 
            "REPLACE", 
            8, 
            false,
            true);
    AddColumn(
            "DateColumn", 
            "DATE", 
            "REPLACE", 
            3, 
            false,
            false);
    
    TabletSchema tablet_schema;
    InitTablet( &tablet_schema );
    CreateColumnWriter(tablet_schema);
    
    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    std::vector<string> val_string_array;
    val_string_array.push_back("2019-11-25 19:07:00");
    val_string_array.push_back("2019-11-24");
    OlapTuple tuple(val_string_array);
    write_row.from_tuple(tuple);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(
        _col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    read_row.set_field_content(0, data, _mem_pool.get());
    char* src = read_row.cell_ptr(0);
    const Field* src_field = read_row.column_schema(0);
    read_row.convert_from(1,src, src_field->type_info(), _mem_pool.get());
    read_row.cell_ptr(1);
    std::string dest_string = read_row.column_schema(1)->to_string(read_row.cell_ptr(1));
    ASSERT_TRUE(strncmp(dest_string.c_str(), "2019-11-25", strlen("2019-11-25")) == 0);
    
    //test not support type
    TypeInfo* tp = get_type_info(OLAP_FIELD_TYPE_HLL);
    OLAPStatus st = read_row.convert_from(1, src, tp, _mem_pool.get());
    ASSERT_TRUE( st == OLAP_ERR_INVALID_SCHEMA);
}

TEST_F(TestColumn, ConvertDateToDatetime) {
    AddColumn(
            "DateColumn",
            "DATE",
            "REPLACE",
            3,
            false,
            true);
    AddColumn(
            "DateTimeColumn",
            "DATETIME",
            "REPLACE",
            8,
            false,
            false);

    TabletSchema tablet_schema;
    InitTablet(&tablet_schema);
    CreateColumnWriter(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    std::vector<std::string> val_string_array;
    std::string origin_val = "2019-12-04";
    std::string convert_val = "2019-12-04 00:00:00";
    val_string_array.emplace_back(origin_val);
    val_string_array.emplace_back(convert_val);
    OlapTuple tuple(val_string_array);
    write_row.from_tuple(tuple);
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header_message;
    ASSERT_EQ(_column_writer->finalize(&header_message), OLAP_SUCCESS);

    CreateColumnReader(tablet_schema);
    RowCursor read_row;
    read_row.init(tablet_schema);
    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(
            _col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    read_row.set_field_content(0, data, _mem_pool.get());
    char* src = read_row.cell_ptr(0);
    const Field* src_field = read_row.column_schema(0);
    read_row.convert_from(1, src, src_field->type_info(), _mem_pool.get());
    read_row.cell_ptr(1);
    std::string dest_string = read_row.column_schema(1)->to_string(read_row.cell_ptr(1));
    ASSERT_TRUE(dest_string.compare(convert_val) == 0);

    //test not support type
    TypeInfo* tp = get_type_info(OLAP_FIELD_TYPE_HLL);
    OLAPStatus st = read_row.convert_from(1, src, tp, _mem_pool.get());
    ASSERT_TRUE( st == OLAP_ERR_INVALID_SCHEMA);
}

TEST_F(TestColumn, ConvertIntToDate) {
    AddColumn(
            "IntColumn",
            "INT",
            "REPLACE",
            4,
            false,
            true);

    AddColumn(
            "DateColumn",
            "DATE",
            "REPLACE",
            3,
            false,
            false);


    TabletSchema tablet_schema;
    InitTablet(&tablet_schema);
    CreateColumnWriter(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    int time_val = 20191205;
    write_row.set_field_content(0, reinterpret_cast<char *>(&time_val), _mem_pool.get());
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    // read data
    CreateColumnReader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(
            _col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    read_row.set_field_content(0, data, _mem_pool.get());
    const Field* src_field = read_row.column_schema(0);
    read_row.convert_from(1, read_row.cell_ptr(0), src_field->type_info(), _mem_pool.get());
    std::string dest_string = read_row.column_schema(1)->to_string(read_row.cell_ptr(1));
    ASSERT_TRUE(strncmp(dest_string.c_str(), "2019-12-05", strlen("2019-12-05")) == 0);

    //test not support type
    TypeInfo* tp = get_type_info(OLAP_FIELD_TYPE_HLL);
    OLAPStatus st = read_row.convert_from(1, read_row.cell_ptr(0), tp, _mem_pool.get());
    ASSERT_TRUE( st == OLAP_ERR_INVALID_SCHEMA);
}

TEST_F(TestColumn, ConvertVarcharToInt) {
    AddColumn(
            "VarcharColumn",
            "VARCHAR",
            "REPLACE",
            255,
            false,
            true);

    AddColumn(
            "IntColumn",
            "INT",
            "REPLACE",
            4,
            false,
            false);

    TabletSchema tablet_schema;
    InitTablet(&tablet_schema);
    CreateColumnWriter(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    // test max int range
    std::string src_str = "2147483647";
    write_row.set_field_content(0, src_str.data(), _mem_pool.get());
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    ColumnDataHeaderMessage header;
    ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

    CreateColumnReader(tablet_schema);

    RowCursor read_row;
    read_row.init(tablet_schema);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    read_row.set_field_content(0, data, _mem_pool.get());
    const Field* src_field = read_row.column_schema(0);
    read_row.convert_from(1, read_row.cell_ptr(0), src_field->type_info(), _mem_pool.get());
    std::string dst_str = read_row.column_schema(1)->to_string(read_row.cell_ptr(1));
    ASSERT_EQ(src_str, dst_str);

    // test invalid schema change
    src_str = "invalid";
    write_row.set_field_content(0, src_str.data(), _mem_pool.get());
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
    data = reinterpret_cast<char*>(_col_vector->col_data());
    read_row.set_field_content(0, data, _mem_pool.get());
    const Field* src_field2 = read_row.column_schema(0);
    ASSERT_EQ(read_row.convert_from(1, read_row.cell_ptr(0), src_field2->type_info(), _mem_pool.get()), OLAP_ERR_INVALID_SCHEMA);

    // test overflow
    src_str = "2147483648";
    write_row.set_field_content(0, src_str.data(), _mem_pool.get());
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
    data = reinterpret_cast<char*>(_col_vector->col_data());
    read_row.set_field_content(0, data, _mem_pool.get());
    const Field* src_field3 = read_row.column_schema(0);
    ASSERT_EQ(read_row.convert_from(1, read_row.cell_ptr(0), src_field3->type_info(), _mem_pool.get()), OLAP_ERR_INVALID_SCHEMA);

    //test not support type
    TypeInfo* tp = get_type_info(OLAP_FIELD_TYPE_HLL);
    OLAPStatus st = read_row.convert_from(1, read_row.cell_ptr(0), tp, _mem_pool.get());
    ASSERT_EQ(st, OLAP_ERR_INVALID_SCHEMA);
}

TEST_F(TestColumn, ConvertVarcharToDate) {
    AddColumn(
            "VarcharColumn",
            "VARCHAR",
            "REPLACE",
            255,
            false,
            true);

    AddColumn(
            "DateColumn",
            "DATE",
            "REPLACE",
            3,
            false,
            false);


    TabletSchema tablet_schema;
    InitTablet(&tablet_schema);
    CreateColumnWriter(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    // test valid format convert
    std::vector<std::string> valid_src_strs = {
        "2019-12-17",
        "19-12-17",
        "20191217",
        "191217",
        "2019/12/17",
        "19/12/17",
    };
    for (const auto& src_str : valid_src_strs) {
        write_row.set_field_content(0, src_str.data(), _mem_pool.get());
        block.set_row(0, write_row);
        block.finalize(1);
        ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

        ColumnDataHeaderMessage header;
        ASSERT_EQ(_column_writer->finalize(&header), OLAP_SUCCESS);

        CreateColumnReader(tablet_schema);

        RowCursor read_row;
        read_row.init(tablet_schema);

        _col_vector.reset(new ColumnVector());
        ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
        char *data = reinterpret_cast<char *>(_col_vector->col_data());
        read_row.set_field_content(0, data, _mem_pool.get());
        const Field *src_field = read_row.column_schema(0);
        read_row.convert_from(1, read_row.cell_ptr(0), src_field->type_info(), _mem_pool.get());
        std::string dst_str = read_row.column_schema(1)->to_string(read_row.cell_ptr(1));
        ASSERT_EQ(src_str, dst_str);
    }

    // test invalid schema change
    std::string src_str = "invalid";
    write_row.set_field_content(0, src_str.data(), _mem_pool.get());
    block.set_row(0, write_row);
    block.finalize(1);
    ASSERT_EQ(_column_writer->write_batch(&block, &write_row), OLAP_SUCCESS);

    _col_vector.reset(new ColumnVector());
    ASSERT_EQ(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), OLAP_SUCCESS);
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    RowCursor read_row;
    read_row.init(tablet_schema);
    read_row.set_field_content(0, data, _mem_pool.get());
    const Field* src_field2 = read_row.column_schema(0);
    ASSERT_EQ(read_row.convert_from(1, read_row.cell_ptr(0), src_field2->type_info(), _mem_pool.get()), OLAP_ERR_INVALID_SCHEMA);

    //test not support type
    TypeInfo* tp = get_type_info(OLAP_FIELD_TYPE_HLL);
    OLAPStatus st = read_row.convert_from(1, read_row.cell_ptr(0), tp, _mem_pool.get());
    ASSERT_EQ(st, OLAP_ERR_INVALID_SCHEMA);
}
  
}

int main(int argc, char** argv) {
    std::string conf_file = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conf_file.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    doris::init_glog("be-test");
    int ret = doris::OLAP_SUCCESS;
    testing::InitGoogleTest(&argc, argv);
    ret = RUN_ALL_TESTS();
    return ret;
}
