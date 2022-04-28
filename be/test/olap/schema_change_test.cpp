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
#include "olap/schema_change.h"

#include <gtest/gtest.h>

#include "olap/byte_buffer.h"
#include "olap/field.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/row_block.h"
#include "olap/row_cursor.h"
#include "olap/rowset/column_reader.h"
#include "olap/rowset/column_writer.h"
#include "olap/stream_name.h"
#include "runtime/mem_pool.h"
#include "runtime/vectorized_row_batch.h"
#include "util/logging.h"

using std::string;

namespace doris {

class TestColumn : public testing::Test {
public:
    TestColumn() : _column_writer(nullptr), _column_reader(nullptr), _stream_factory(nullptr) {
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
        _stream_factory = new (std::nothrow)
                OutStreamFactory(COMPRESS_LZ4, OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE);
        EXPECT_TRUE(_stream_factory != nullptr);
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
        EXPECT_TRUE(_column_writer != nullptr);
        EXPECT_EQ(_column_writer->init(), Status::OK());
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

        SAFE_DELETE(_column_reader);
        _column_reader =
                ColumnReader::create(0, tablet_schema, included, segment_included, encodings);

        EXPECT_TRUE(_column_reader != nullptr);

        EXPECT_EQ(system("mkdir -p ./ut_dir"), 0);
        EXPECT_EQ(system("rm -f ./ut_dir/tmp_file"), 0);

        EXPECT_EQ(Status::OK(),
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
                EXPECT_TRUE(false);
            }

            EXPECT_TRUE(buffers != nullptr);
            off.push_back(helper.tell());
            out_stream->write_to_file(&helper, 0);
            length.push_back(out_stream->get_stream_length());
            buffer_size.push_back(out_stream->get_total_buffer_size());
            name.push_back(stream_name);
        }
        helper.close();

        EXPECT_EQ(Status::OK(),
                  helper.open_with_mode("./ut_dir/tmp_file", O_RDONLY, S_IRUSR | S_IWUSR));

        SAFE_DELETE(_shared_buffer);
        _shared_buffer = StorageByteBuffer::create(OLAP_DEFAULT_COLUMN_STREAM_BUFFER_SIZE +
                                                   sizeof(StreamHead));
        EXPECT_TRUE(_shared_buffer != nullptr);

        for (auto in_stream : _map_in_streams) {
            delete in_stream.second;
        }
        _map_in_streams.clear();

        for (int i = 0; i < off.size(); ++i) {
            ReadOnlyFileStream* in_stream = new (std::nothrow)
                    ReadOnlyFileStream(&helper, &_shared_buffer, off[i], length[i], lz4_decompress,
                                       buffer_size[i], &_stats);
            EXPECT_EQ(Status::OK(), in_stream->init());
            _map_in_streams[name[i]] = in_stream;
        }

        EXPECT_EQ(_column_reader->init(&_map_in_streams, 1024, _mem_pool.get(), &_stats),
                  Status::OK());
    }

    void set_tablet_schema(const std::string& name, const std::string& type,
                           const std::string& aggregation, uint32_t length, bool is_allow_null,
                           bool is_key, TabletSchema* tablet_schema) {
        TabletSchemaPB tablet_schema_pb;
        ColumnPB* column = tablet_schema_pb.add_column();
        column->set_unique_id(0);
        column->set_name(name);
        column->set_type(type);
        column->set_is_key(is_key);
        column->set_is_nullable(is_allow_null);
        column->set_length(length);
        column->set_aggregation(aggregation);
        tablet_schema->init_from_pb(tablet_schema_pb);
    }

    void create_and_save_last_position() {
        EXPECT_EQ(_column_writer->create_row_index_entry(), Status::OK());
    }

    template <typename T>
    void test_convert_to_varchar(const std::string& type_name, int type_size, T val,
                                 const std::string& expected_val, Status expected_st,
                                 int varchar_len = 255) {
        TabletSchema src_tablet_schema;
        set_tablet_schema("ConvertColumn", type_name, "REPLACE", type_size, false, false,
                          &src_tablet_schema);
        create_column_writer(src_tablet_schema);

        RowCursor write_row;
        write_row.init(src_tablet_schema);
        RowBlock block(&src_tablet_schema);
        RowBlockInfo block_info;
        block_info.row_num = 10000;
        block.init(block_info);
        write_row.set_field_content(0, reinterpret_cast<char*>(&val), _mem_pool.get());
        block.set_row(0, write_row);
        block.finalize(1);
        EXPECT_EQ(_column_writer->write_batch(&block, &write_row), Status::OK());
        ColumnDataHeaderMessage header;
        EXPECT_EQ(_column_writer->finalize(&header), Status::OK());
        helper.close();

        TabletSchema dst_tablet_schema;
        set_tablet_schema("VarcharColumn", "VARCHAR", "REPLACE", varchar_len, false, false,
                          &dst_tablet_schema);
        create_column_reader(src_tablet_schema);
        RowCursor read_row;
        read_row.init(dst_tablet_schema);

        _col_vector.reset(new ColumnVector());
        EXPECT_EQ(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), Status::OK());
        char* data = reinterpret_cast<char*>(_col_vector->col_data());
        auto st = read_row.convert_from(0, data, write_row.column_schema(0)->type_info(),
                                        _mem_pool.get());
        EXPECT_EQ(st, expected_st);
        if (st == Status::OK()) {
            std::string dst_str = read_row.column_schema(0)->to_string(read_row.cell_ptr(0));
            EXPECT_TRUE(dst_str.compare(0, expected_val.size(), expected_val) == 0);
        }

        const auto* tp = get_scalar_type_info<OLAP_FIELD_TYPE_HLL>();
        st = read_row.convert_from(0, read_row.cell_ptr(0), tp, _mem_pool.get());
        EXPECT_EQ(st, Status::OLAPInternalError(OLAP_ERR_INVALID_SCHEMA));
    }

    void test_convert_from_varchar(const std::string& type_name, int type_size,
                                   const std::string& value, Status expected_st) {
        TabletSchema tablet_schema;
        set_tablet_schema("VarcharColumn", "VARCHAR", "REPLACE", 255, false, false, &tablet_schema);
        create_column_writer(tablet_schema);

        RowCursor write_row;
        write_row.init(tablet_schema);
        RowBlock block(&tablet_schema);
        RowBlockInfo block_info;
        block_info.row_num = 10000;
        block.init(block_info);
        Slice normal_str(value);
        write_row.set_field_content(0, reinterpret_cast<char*>(&normal_str), _mem_pool.get());
        block.set_row(0, write_row);
        block.finalize(1);
        EXPECT_EQ(_column_writer->write_batch(&block, &write_row), Status::OK());
        ColumnDataHeaderMessage header;
        EXPECT_EQ(_column_writer->finalize(&header), Status::OK());
        helper.close();

        TabletSchema converted_tablet_schema;
        set_tablet_schema("ConvertColumn", type_name, "REPLACE", type_size, false, false,
                          &converted_tablet_schema);
        create_column_reader(tablet_schema);
        RowCursor read_row;
        read_row.init(converted_tablet_schema);

        _col_vector.reset(new ColumnVector());
        EXPECT_EQ(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), Status::OK());
        char* data = reinterpret_cast<char*>(_col_vector->col_data());
        auto st = read_row.convert_from(0, data, write_row.column_schema(0)->type_info(),
                                        _mem_pool.get());
        EXPECT_EQ(st, expected_st);
        if (st == Status::OK()) {
            std::string dst_str = read_row.column_schema(0)->to_string(read_row.cell_ptr(0));
            EXPECT_TRUE(dst_str.compare(0, value.size(), value) == 0);
        }

        const auto* tp = get_scalar_type_info<OLAP_FIELD_TYPE_HLL>();
        st = read_row.convert_from(0, read_row.cell_ptr(0), tp, _mem_pool.get());
        EXPECT_EQ(st, Status::OLAPInternalError(OLAP_ERR_INVALID_SCHEMA));
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
    StorageByteBuffer* _shared_buffer = nullptr;
    std::map<StreamName, ReadOnlyFileStream*> _map_in_streams;
    FileHandler helper;
    OlapReaderStatistics _stats;
};

TEST_F(TestColumn, ConvertFloatToDouble) {
    TabletSchema tablet_schema;
    set_tablet_schema("FloatColumn", "FLOAT", "REPLACE", 4, false, false, &tablet_schema);
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
    EXPECT_EQ(_column_writer->write_batch(&block, &write_row), Status::OK());

    value = 3.234;
    write_row.set_field_content(0, reinterpret_cast<char*>(&value), _mem_pool.get());
    block.set_row(0, write_row);
    block.finalize(1);
    EXPECT_EQ(_column_writer->write_batch(&block, &write_row), Status::OK());

    ColumnDataHeaderMessage header;
    EXPECT_EQ(_column_writer->finalize(&header), Status::OK());

    // read data
    TabletSchema convert_tablet_schema;
    set_tablet_schema("DoubleColumn", "DOUBLE", "REPLACE", 4, false, false, &convert_tablet_schema);
    create_column_reader(tablet_schema);
    RowCursor read_row;
    read_row.init(convert_tablet_schema);
    _col_vector.reset(new ColumnVector());
    EXPECT_EQ(_column_reader->next_vector(_col_vector.get(), 2, _mem_pool.get()), Status::OK());
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    read_row.convert_from(0, data, write_row.column_schema(0)->type_info(), _mem_pool.get());
    //float val1 = *reinterpret_cast<float*>(read_row.cell_ptr(0));
    double val2 = *reinterpret_cast<double*>(read_row.cell_ptr(0));

    char buf[64];
    memset(buf, 0, sizeof(buf));
    sprintf(buf, "%f", val2);
    char* tg;
    double v2 = strtod(buf, &tg);
    EXPECT_EQ(v2, 1.234);

    //test not support type
    const auto* tp = get_scalar_type_info<OLAP_FIELD_TYPE_HLL>();
    Status st = read_row.convert_from(0, data, tp, _mem_pool.get());
    EXPECT_TRUE(st == Status::OLAPInternalError(OLAP_ERR_INVALID_SCHEMA));
}

TEST_F(TestColumn, ConvertDatetimeToDate) {
    TabletSchema tablet_schema;
    set_tablet_schema("DatetimeColumn", "DATETIME", "REPLACE", 8, false, false, &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);
    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    std::vector<std::string> val_string_array;
    std::string origin_val = "2019-11-25 19:07:00";
    val_string_array.emplace_back(origin_val);
    OlapTuple tuple(val_string_array);
    write_row.from_tuple(tuple);
    block.set_row(0, write_row);
    block.finalize(1);
    EXPECT_EQ(_column_writer->write_batch(&block, &write_row), Status::OK());

    ColumnDataHeaderMessage header;
    EXPECT_EQ(_column_writer->finalize(&header), Status::OK());

    // read data
    TabletSchema convert_tablet_schema;
    set_tablet_schema("DateColumn", "DATE", "REPLACE", 3, false, false, &convert_tablet_schema);
    create_column_reader(tablet_schema);
    RowCursor read_row;
    read_row.init(convert_tablet_schema);

    _col_vector.reset(new ColumnVector());
    EXPECT_EQ(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), Status::OK());
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    read_row.convert_from(0, data, write_row.column_schema(0)->type_info(), _mem_pool.get());
    std::string dest_string = read_row.column_schema(0)->to_string(read_row.cell_ptr(0));
    EXPECT_TRUE(strncmp(dest_string.c_str(), "2019-11-25", strlen("2019-11-25")) == 0);

    //test not support type
    const auto* tp = get_scalar_type_info<OLAP_FIELD_TYPE_HLL>();
    Status st = read_row.convert_from(0, data, tp, _mem_pool.get());
    EXPECT_TRUE(st == Status::OLAPInternalError(OLAP_ERR_INVALID_SCHEMA));
}

TEST_F(TestColumn, ConvertDateToDatetime) {
    TabletSchema tablet_schema;
    set_tablet_schema("DateColumn", "DATE", "REPLACE", 3, false, false, &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    std::vector<std::string> val_string_array;
    std::string origin_val = "2019-12-04";
    val_string_array.emplace_back(origin_val);
    OlapTuple tuple(val_string_array);
    write_row.from_tuple(tuple);
    block.set_row(0, write_row);
    block.finalize(1);
    EXPECT_EQ(_column_writer->write_batch(&block, &write_row), Status::OK());

    ColumnDataHeaderMessage header_message;
    EXPECT_EQ(_column_writer->finalize(&header_message), Status::OK());

    TabletSchema convert_tablet_schema;
    set_tablet_schema("DateTimeColumn", "DATETIME", "REPLACE", 8, false, false,
                      &convert_tablet_schema);
    create_column_reader(tablet_schema);
    RowCursor read_row;
    read_row.init(convert_tablet_schema);
    _col_vector.reset(new ColumnVector());
    EXPECT_EQ(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), Status::OK());
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    read_row.set_field_content(0, data, _mem_pool.get());
    read_row.convert_from(0, data, write_row.column_schema(0)->type_info(), _mem_pool.get());
    std::string dest_string = read_row.column_schema(0)->to_string(read_row.cell_ptr(0));
    EXPECT_TRUE(dest_string.compare("2019-12-04 00:00:00") == 0);

    //test not support type
    const auto* tp = get_scalar_type_info<OLAP_FIELD_TYPE_HLL>();
    Status st = read_row.convert_from(0, data, tp, _mem_pool.get());
    EXPECT_TRUE(st == Status::OLAPInternalError(OLAP_ERR_INVALID_SCHEMA));
}

TEST_F(TestColumn, ConvertIntToDate) {
    TabletSchema tablet_schema;
    set_tablet_schema("IntColumn", "INT", "REPLACE", 4, false, false, &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    int time_val = 20191205;
    write_row.set_field_content(0, reinterpret_cast<char*>(&time_val), _mem_pool.get());
    block.set_row(0, write_row);
    block.finalize(1);
    EXPECT_EQ(_column_writer->write_batch(&block, &write_row), Status::OK());

    ColumnDataHeaderMessage header;
    EXPECT_EQ(_column_writer->finalize(&header), Status::OK());

    TabletSchema convert_tablet_schema;
    set_tablet_schema("DateColumn", "DATE", "REPLACE", 3, false, false, &convert_tablet_schema);
    create_column_reader(tablet_schema);

    RowCursor read_row;
    read_row.init(convert_tablet_schema);

    _col_vector.reset(new ColumnVector());
    EXPECT_EQ(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), Status::OK());
    char* data = reinterpret_cast<char*>(_col_vector->col_data());
    read_row.convert_from(0, data, write_row.column_schema(0)->type_info(), _mem_pool.get());
    std::string dest_string = read_row.column_schema(0)->to_string(read_row.cell_ptr(0));
    EXPECT_TRUE(strncmp(dest_string.c_str(), "2019-12-05", strlen("2019-12-05")) == 0);

    //test not support type
    const auto* tp = get_scalar_type_info<OLAP_FIELD_TYPE_HLL>();
    Status st = read_row.convert_from(0, read_row.cell_ptr(0), tp, _mem_pool.get());
    EXPECT_TRUE(st == Status::OLAPInternalError(OLAP_ERR_INVALID_SCHEMA));
}

TEST_F(TestColumn, ConvertVarcharToDate) {
    TabletSchema tablet_schema;
    set_tablet_schema("VarcharColumn", "VARCHAR", "REPLACE", 255, false, false, &tablet_schema);
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    // test valid format convert
    std::vector<Slice> valid_src_strs = {
            "2019-12-17", "19-12-17", "20191217", "191217", "2019/12/17", "19/12/17",
    };
    std::string expected_val("2019-12-17");
    for (auto src_str : valid_src_strs) {
        write_row.set_field_content(0, reinterpret_cast<char*>(&src_str), _mem_pool.get());
        block.set_row(0, write_row);
        block.finalize(1);
        EXPECT_EQ(_column_writer->write_batch(&block, &write_row), Status::OK());

        ColumnDataHeaderMessage header;
        EXPECT_EQ(_column_writer->finalize(&header), Status::OK());

        // because file_helper is reused in this case, we should close it.
        helper.close();
        TabletSchema convert_tablet_schema;
        set_tablet_schema("DateColumn", "DATE", "REPLACE", 3, false, false, &convert_tablet_schema);
        create_column_reader(tablet_schema);
        RowCursor read_row;
        read_row.init(convert_tablet_schema);

        _col_vector.reset(new ColumnVector());
        EXPECT_EQ(_column_reader->next_vector(_col_vector.get(), 1, _mem_pool.get()), Status::OK());
        char* data = reinterpret_cast<char*>(_col_vector->col_data());
        read_row.convert_from(0, data, write_row.column_schema(0)->type_info(), _mem_pool.get());
        std::string dst_str = read_row.column_schema(0)->to_string(read_row.cell_ptr(0));
        EXPECT_EQ(expected_val, dst_str);
    }
    helper.close();
    TabletSchema convert_tablet_schema;
    set_tablet_schema("DateColumn", "DATE", "REPLACE", 3, false, false, &convert_tablet_schema);
    create_column_reader(tablet_schema);
    RowCursor read_row;
    read_row.init(convert_tablet_schema);

    //test not support type
    const auto* tp = get_scalar_type_info<OLAP_FIELD_TYPE_HLL>();
    Status st = read_row.convert_from(0, read_row.cell_ptr(0), tp, _mem_pool.get());
    EXPECT_EQ(st, Status::OLAPInternalError(OLAP_ERR_INVALID_SCHEMA));
}

TEST_F(TestColumn, ConvertVarcharToTinyInt1) {
    test_convert_from_varchar("TINYINT", 1, "127", Status::OK());
}

TEST_F(TestColumn, ConvertVarcharToTinyInt2) {
    test_convert_from_varchar("TINYINT", 1, "128",
                              Status::OLAPInternalError(OLAP_ERR_INVALID_SCHEMA));
}

TEST_F(TestColumn, ConvertVarcharToSmallInt1) {
    test_convert_from_varchar("SMALLINT", 2, "32767", Status::OK());
}

TEST_F(TestColumn, ConvertVarcharToSmallInt2) {
    test_convert_from_varchar("SMALLINT", 2, "32768",
                              Status::OLAPInternalError(OLAP_ERR_INVALID_SCHEMA));
}

TEST_F(TestColumn, ConvertVarcharToInt1) {
    test_convert_from_varchar("INT", 4, "2147483647", Status::OK());
}

TEST_F(TestColumn, ConvertVarcharToInt2) {
    test_convert_from_varchar("INT", 4, "2147483648",
                              Status::OLAPInternalError(OLAP_ERR_INVALID_SCHEMA));
}

TEST_F(TestColumn, ConvertVarcharToBigInt1) {
    test_convert_from_varchar("BIGINT", 8, "9223372036854775807", Status::OK());
}

TEST_F(TestColumn, ConvertVarcharToBigInt2) {
    test_convert_from_varchar("BIGINT", 8, "9223372036854775808",
                              Status::OLAPInternalError(OLAP_ERR_INVALID_SCHEMA));
}

TEST_F(TestColumn, ConvertVarcharToLargeInt1) {
    test_convert_from_varchar("LARGEINT", 16, "170141183460469000000000000000000000000",
                              Status::OK());
}

TEST_F(TestColumn, ConvertVarcharToLargeInt2) {
    test_convert_from_varchar("LARGEINT", 16, "1701411834604690000000000000000000000000",
                              Status::OLAPInternalError(OLAP_ERR_INVALID_SCHEMA));
}

TEST_F(TestColumn, ConvertVarcharToFloat1) {
    test_convert_from_varchar("FLOAT", 4, "3.40282e+38", Status::OK());
}

TEST_F(TestColumn, ConvertVarcharToFloat2) {
    test_convert_from_varchar(
            "FLOAT", 4,
            "17976900000000000632303049213894264349303303643368533621541098328912643414890628994061"
            "52996321966094455338163203127744334848599000464911410516510916727344709727599413825823"
            "04802812882753059262973637182942535982636884444611376868582636745405553206881859340916"
            "3400929532301499014067384276511218551077374242324480.999",
            Status::OLAPInternalError(OLAP_ERR_INVALID_SCHEMA));
}

TEST_F(TestColumn, ConvertVarcharToDouble1) {
    test_convert_from_varchar("DOUBLE", 8, "123.456", Status::OK());
}

TEST_F(TestColumn, ConvertVarcharToDouble2) {
    test_convert_from_varchar(
            "DOUBLE", 8,
            "17976900000000000632303049213894264349303303643368533621541098328912643414890628994061"
            "52996321966094455338163203127744334848599000464911410516510916727344709727599413825823"
            "04802812882753059262973637182942535982636884444611376868582636745405553206881859340916"
            "3400929532301499014067384276511218551077374242324480.0000000000",
            Status::OLAPInternalError(OLAP_ERR_INVALID_SCHEMA));
}

TEST_F(TestColumn, ConvertTinyIntToVarchar3) {
    test_convert_to_varchar<int8_t>("TINYINT", 1, 127, "",
                                    Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR), 3);
}

TEST_F(TestColumn, ConvertTinyIntToVarchar5) {
    test_convert_to_varchar<int8_t>("TINYINT", 1, 127, "127", Status::OK(), 3 + 2);
}

TEST_F(TestColumn, ConvertTinyIntToVarchar4) {
    test_convert_to_varchar<int8_t>("TINYINT", 1, -127, "",
                                    Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR), 4);
}

TEST_F(TestColumn, ConvertTinyIntToVarchar6) {
    // 4: tinyint digit count + minus symbol, +2 for var len bytes
    test_convert_to_varchar<int8_t>("TINYINT", 1, -127, "-127", Status::OK(), 4 + 2);
}

TEST_F(TestColumn, ConvertSmallIntToVarchar5) {
    test_convert_to_varchar<int16_t>("SMALLINT", 2, 32767, "",
                                     Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR), 5);
}

TEST_F(TestColumn, ConvertSmallIntToVarchar7) {
    test_convert_to_varchar<int16_t>("SMALLINT", 2, 32767, "32767", Status::OK(), 7);
}

TEST_F(TestColumn, ConvertSmallIntToVarchar6) {
    test_convert_to_varchar<int16_t>("SMALLINT", 2, -32767, "",
                                     Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR), 6);
}

TEST_F(TestColumn, ConvertSmallIntToVarchar8) {
    test_convert_to_varchar<int16_t>("SMALLINT", 2, -32767, "-32767", Status::OK(), 8);
}

TEST_F(TestColumn, ConvertIntToVarchar10) {
    test_convert_to_varchar<int32_t>("INT", 4, 2147483647, "",
                                     Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR), 10);
}

TEST_F(TestColumn, ConvertIntToVarchar12) {
    test_convert_to_varchar<int32_t>("INT", 4, 2147483647, "2147483647", Status::OK(), 12);
}

TEST_F(TestColumn, ConvertIntToVarchar11) {
    test_convert_to_varchar<int32_t>("INT", 4, -2147483647, "",
                                     Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR), 11);
}

TEST_F(TestColumn, ConvertIntToVarchar13) {
    test_convert_to_varchar<int32_t>("INT", 4, -2147483647, "-2147483647", Status::OK(), 13);
}

TEST_F(TestColumn, ConvertBigIntToVarchar19) {
    test_convert_to_varchar<int64_t>("BIGINT", 8, 9223372036854775807, "",
                                     Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR), 19);
}

TEST_F(TestColumn, ConvertBigIntToVarchar21) {
    test_convert_to_varchar<int64_t>("BIGINT", 8, 9223372036854775807, "9223372036854775807",
                                     Status::OK(), 21);
}

TEST_F(TestColumn, ConvertBigIntToVarchar20) {
    test_convert_to_varchar<int64_t>("BIGINT", 8, -9223372036854775807, "",
                                     Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR), 20);
}

TEST_F(TestColumn, ConvertBigIntToVarchar22) {
    test_convert_to_varchar<int64_t>("BIGINT", 8, -9223372036854775807, "-9223372036854775807",
                                     Status::OK(), 22);
}

TEST_F(TestColumn, ConvertLargeIntToVarchar39) {
    std::string str_val("170141183460469231731687303715884105727");
    StringParser::ParseResult result;
    int128_t int128_val =
            StringParser::string_to_int<int128_t>(str_val.c_str(), str_val.length(), &result);
    DCHECK(result == StringParser::PARSE_SUCCESS);
    test_convert_to_varchar<int128_t>("LARGEINT", 16, int128_val, "",
                                      Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR),
                                      39);
}

TEST_F(TestColumn, ConvertLargeIntToVarchar41) {
    std::string str_val("170141183460469231731687303715884105727");
    StringParser::ParseResult result;
    int128_t int128_val =
            StringParser::string_to_int<int128_t>(str_val.c_str(), str_val.length(), &result);
    DCHECK(result == StringParser::PARSE_SUCCESS);
    test_convert_to_varchar<int128_t>("LARGEINT", 16, int128_val, str_val, Status::OK(), 41);
}

TEST_F(TestColumn, ConvertLargeIntToVarchar40) {
    std::string str_val = "-170141183460469231731687303715884105727";
    StringParser::ParseResult result;
    int128_t int128_val =
            StringParser::string_to_int<int128_t>(str_val.c_str(), str_val.length(), &result);
    DCHECK(result == StringParser::PARSE_SUCCESS);
    test_convert_to_varchar<int128_t>("LARGEINT", 16, int128_val, "",
                                      Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR),
                                      40);
}

TEST_F(TestColumn, ConvertLargeIntToVarchar46) {
    std::string str_val = "-170141183460469231731687303715884105727";
    StringParser::ParseResult result;
    int128_t int128_val =
            StringParser::string_to_int<int128_t>(str_val.c_str(), str_val.length(), &result);
    DCHECK(result == StringParser::PARSE_SUCCESS);
    test_convert_to_varchar<int128_t>("LARGEINT", 16, int128_val, str_val, Status::OK(), 42);
}

TEST_F(TestColumn, ConvertFloatToVarchar11) {
    test_convert_to_varchar<float>("FLOAT", 4, 3.40282e+38, "3.40282e+38",
                                   Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR), 11);
}

TEST_F(TestColumn, ConvertFloatToVarchar13) {
    test_convert_to_varchar<float>("FLOAT", 4, 3.40282e+38, "3.40282e+38", Status::OK(), 13);
}

TEST_F(TestColumn, ConvertFloatToVarchar13_2) {
    test_convert_to_varchar<float>("FLOAT", 4, 3402820000000000000.0, "3.40282e+18", Status::OK(),
                                   13);
}

TEST_F(TestColumn, ConvertFloatToVarchar12) {
    test_convert_to_varchar<float>("FLOAT", 4, -3.40282e+38, "-3.40282e+38",
                                   Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR), 12);
}

TEST_F(TestColumn, ConvertFloatToVarchar14) {
    test_convert_to_varchar<float>("FLOAT", 4, -3.40282e+38, "-3.40282e+38", Status::OK(), 14);
}

TEST_F(TestColumn, ConvertFloatToVarchar14_2) {
    test_convert_to_varchar<float>("FLOAT", 4, -3402820000000000000.0, "-3.40282e+18", Status::OK(),
                                   14);
}

TEST_F(TestColumn, ConvertFloatToVarchar13_3) {
    test_convert_to_varchar<float>("FLOAT", 4, 1.17549435082228750796873653722224568e-38F,
                                   "1.1754944e-38",
                                   Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR), 13);
}

TEST_F(TestColumn, ConvertFloatToVarchar15) {
    test_convert_to_varchar<float>("FLOAT", 4, 1.17549435082228750796873653722224568e-38F,
                                   "1.1754944e-38", Status::OK(), 15);
}

TEST_F(TestColumn, ConvertFloatToVarchar14_3) {
    test_convert_to_varchar<float>("FLOAT", 4, -1.17549435082228750796873653722224568e-38F,
                                   "-1.1754944e-38",
                                   Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR), 14);
}

TEST_F(TestColumn, ConvertFloatToVarchar16) {
    test_convert_to_varchar<float>("FLOAT", 4, -1.17549435082228750796873653722224568e-38F,
                                   "-1.1754944e-38", Status::OK(), 16);
}

TEST_F(TestColumn, ConvertDoubleToVarchar7) {
    test_convert_to_varchar<double>("DOUBLE", 8, 123.456, "123.456",
                                    Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR), 7);
}

TEST_F(TestColumn, ConvertDoubleToVarchar9) {
    test_convert_to_varchar<double>("DOUBLE", 8, 123.456, "123.456", Status::OK(), 9);
}

TEST_F(TestColumn, ConvertDoubleToVarchar23) {
    test_convert_to_varchar<double>("DOUBLE", 8, 1.79769313486231570814527423731704357e+308,
                                    "1.7976931348623157e+308",
                                    Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR), 23);
}

TEST_F(TestColumn, ConvertDoubleToVarchar25) {
    test_convert_to_varchar<double>("DOUBLE", 8, 1.79769313486231570814527423731704357e+308,
                                    "1.7976931348623157e+308", Status::OK(), 25);
}

TEST_F(TestColumn, ConvertDoubleToVarchar22) {
    test_convert_to_varchar<double>("DOUBLE", 8, 1797693134862315708.0, "1.7976931348623158e+18",
                                    Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR), 22);
}

TEST_F(TestColumn, ConvertDoubleToVarchar24) {
    test_convert_to_varchar<double>("DOUBLE", 8, 1797693134862315708.0, "1.7976931348623158e+18",
                                    Status::OK(), 24);
}

TEST_F(TestColumn, ConvertDoubleToVarchar23_2) {
    test_convert_to_varchar<double>("DOUBLE", 8, -1797693134862315708.0, "-1.7976931348623158e+18",
                                    Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR), 23);
}

TEST_F(TestColumn, ConvertDoubleToVarchar25_2) {
    test_convert_to_varchar<double>("DOUBLE", 8, -1797693134862315708.0, "-1.7976931348623158e+18",
                                    Status::OK(), 25);
}

TEST_F(TestColumn, ConvertDoubleToVarchar23_3) {
    test_convert_to_varchar<double>("DOUBLE", 8, 2.22507385850720138309023271733240406e-308,
                                    "2.2250738585072014e-308",
                                    Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR), 23);
}

TEST_F(TestColumn, ConvertDoubleToVarchar25_3) {
    test_convert_to_varchar<double>("DOUBLE", 8, 2.22507385850720138309023271733240406e-308,
                                    "2.2250738585072014e-308", Status::OK(), 25);
}

TEST_F(TestColumn, ConvertDoubleToVarchar24_2) {
    test_convert_to_varchar<double>("DOUBLE", 8, -2.22507385850720138309023271733240406e-308,
                                    "-2.2250738585072014e-308",
                                    Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR), 24);
}

TEST_F(TestColumn, ConvertDoubleToVarchar26) {
    test_convert_to_varchar<double>("DOUBLE", 8, -2.22507385850720138309023271733240406e-308,
                                    "-2.2250738585072014e-308", Status::OK(), 26);
}

TEST_F(TestColumn, ConvertDecimalToVarchar13) {
    decimal12_t val = {456, 789000000};
    test_convert_to_varchar<decimal12_t>("Decimal", 12, val, "456.789000000",
                                         Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR),
                                         13);
}

TEST_F(TestColumn, ConvertDecimalToVarchar15) {
    decimal12_t val = {456, 789000000};
    test_convert_to_varchar<decimal12_t>("Decimal", 12, val, "456.789000000", Status::OK(), 15);
}

TEST_F(TestColumn, ConvertDecimalToVarchar28) {
    decimal12_t val = {999999999999999999, 999999999};
    test_convert_to_varchar<decimal12_t>(
            "Decimal", 12, val, "", Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR), 28);
}

TEST_F(TestColumn, ConvertDecimalToVarchar30) {
    decimal12_t val = {999999999999999999, 999999999};
    test_convert_to_varchar<decimal12_t>("Decimal", 12, val, "999999999999999999.999999999",
                                         Status::OK(), 30);
}

TEST_F(TestColumn, ConvertDecimalToVarchar29) {
    decimal12_t val = {-999999999999999999, 999999999};
    test_convert_to_varchar<decimal12_t>(
            "Decimal", 12, val, "", Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR), 29);
}

TEST_F(TestColumn, ConvertDecimalToVarchar31) {
    decimal12_t val = {-999999999999999999, 999999999};
    test_convert_to_varchar<decimal12_t>("Decimal", 12, val, "-999999999999999999.999999999",
                                         Status::OK(), 31);
}

void CreateTabletSchema(TabletSchema& tablet_schema) {
    TabletSchemaPB tablet_schema_pb;
    tablet_schema_pb.set_keys_type(KeysType::AGG_KEYS);
    tablet_schema_pb.set_num_short_key_columns(2);
    tablet_schema_pb.set_num_rows_per_row_block(1024);
    tablet_schema_pb.set_compress_kind(COMPRESS_NONE);
    tablet_schema_pb.set_next_column_unique_id(4);

    ColumnPB* column_1 = tablet_schema_pb.add_column();
    column_1->set_unique_id(1);
    column_1->set_name("k1");
    column_1->set_type("INT");
    column_1->set_is_key(true);
    column_1->set_length(4);
    column_1->set_index_length(4);
    column_1->set_is_nullable(false);
    column_1->set_is_bf_column(false);

    ColumnPB* column_2 = tablet_schema_pb.add_column();
    column_2->set_unique_id(2);
    column_2->set_name("k2");
    column_2->set_type("VARCHAR");
    column_2->set_length(20);
    column_2->set_index_length(20);
    column_2->set_is_key(true);
    column_2->set_is_nullable(false);
    column_2->set_is_bf_column(false);

    ColumnPB* column_3 = tablet_schema_pb.add_column();
    column_3->set_unique_id(3);
    column_3->set_name("k3");
    column_3->set_type("INT");
    column_3->set_is_key(true);
    column_3->set_length(4);
    column_3->set_index_length(4);
    column_3->set_is_nullable(false);
    column_3->set_is_bf_column(false);

    ColumnPB* column_4 = tablet_schema_pb.add_column();
    column_4->set_unique_id(4);
    column_4->set_name("v1");
    column_4->set_type("INT");
    column_4->set_length(4);
    column_4->set_is_key(false);
    column_4->set_is_nullable(false);
    column_4->set_is_bf_column(false);
    column_4->set_aggregation("SUM");

    tablet_schema.init_from_pb(tablet_schema_pb);
}

TEST_F(TestColumn, ConvertIntToBitmap) {
    //Base Tablet
    TabletSchema tablet_schema;
    CreateTabletSchema(tablet_schema);
    //Base row block
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);
    write_row.allocate_memory_for_string_type(tablet_schema);
    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    std::vector<std::string> val_string_array;
    val_string_array.emplace_back("5");
    val_string_array.emplace_back("4");
    val_string_array.emplace_back("2");
    val_string_array.emplace_back("3");
    OlapTuple tuple(val_string_array);
    write_row.from_tuple(tuple);
    block.set_row(0, write_row);
    block.finalize(1);
    EXPECT_EQ(_column_writer->write_batch(&block, &write_row), Status::OK());
    ColumnDataHeaderMessage header;
    EXPECT_EQ(_column_writer->finalize(&header), Status::OK());

    //Materialized View tablet schema
    TabletSchemaPB mv_tablet_schema_pb;
    mv_tablet_schema_pb.set_keys_type(KeysType::AGG_KEYS);
    mv_tablet_schema_pb.set_num_short_key_columns(2);
    mv_tablet_schema_pb.set_num_rows_per_row_block(1024);
    mv_tablet_schema_pb.set_compress_kind(COMPRESS_NONE);
    mv_tablet_schema_pb.set_next_column_unique_id(3);

    ColumnPB* mv_column_1 = mv_tablet_schema_pb.add_column();
    mv_column_1->set_unique_id(1);
    mv_column_1->set_name("k1");
    mv_column_1->set_type("INT");
    mv_column_1->set_is_key(true);
    mv_column_1->set_length(4);
    mv_column_1->set_index_length(4);
    mv_column_1->set_is_nullable(false);
    mv_column_1->set_is_bf_column(false);

    ColumnPB* mv_column_2 = mv_tablet_schema_pb.add_column();
    mv_column_2->set_unique_id(2);
    mv_column_2->set_name("v1");
    mv_column_2->set_type("OBJECT");
    mv_column_2->set_length(8);
    mv_column_2->set_is_key(false);
    mv_column_2->set_is_nullable(false);
    mv_column_2->set_is_bf_column(false);
    mv_column_2->set_aggregation("BITMAP_UNION");

    TabletSchema mv_tablet_schema;
    mv_tablet_schema.init_from_pb(mv_tablet_schema_pb);

    RowBlockChanger row_block_changer(mv_tablet_schema);
    ColumnMapping* column_mapping = row_block_changer.get_mutable_column_mapping(0);
    column_mapping->ref_column = 0;
    column_mapping = row_block_changer.get_mutable_column_mapping(1);
    column_mapping->ref_column = 2;
    column_mapping->materialized_function = "to_bitmap";

    RowBlock mutable_block(&mv_tablet_schema);
    mutable_block.init(block_info);
    uint64_t filtered_rows = 0;
    row_block_changer.change_row_block(&block, 0, &mutable_block, &filtered_rows);

    RowCursor mv_row_cursor;
    mv_row_cursor.init(mv_tablet_schema);
    mutable_block.get_row(0, &mv_row_cursor);

    auto dst_slice = reinterpret_cast<Slice*>(mv_row_cursor.cell_ptr(1));
    BitmapValue bitmapValue(dst_slice->data);
    EXPECT_EQ(bitmapValue.cardinality(), 1);
}

TEST_F(TestColumn, ConvertCharToHLL) {
    //Base Tablet
    TabletSchema tablet_schema;
    CreateTabletSchema(tablet_schema);

    //Base row block
    create_column_writer(tablet_schema);

    RowCursor write_row;
    write_row.init(tablet_schema);
    write_row.allocate_memory_for_string_type(tablet_schema);
    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    std::vector<std::string> val_string_array;
    //std::string origin_val = "2019-11-25 19:07:00";
    //val_string_array.emplace_back(origin_val);
    val_string_array.emplace_back("1");
    val_string_array.emplace_back("1");
    val_string_array.emplace_back("2");
    val_string_array.emplace_back("3");
    OlapTuple tuple(val_string_array);
    write_row.from_tuple(tuple);
    block.set_row(0, write_row);
    block.finalize(1);
    EXPECT_EQ(_column_writer->write_batch(&block, &write_row), Status::OK());
    ColumnDataHeaderMessage header;
    EXPECT_EQ(_column_writer->finalize(&header), Status::OK());

    //Materialized View tablet schema
    TabletSchemaPB mv_tablet_schema_pb;
    mv_tablet_schema_pb.set_keys_type(KeysType::AGG_KEYS);
    mv_tablet_schema_pb.set_num_short_key_columns(2);
    mv_tablet_schema_pb.set_num_rows_per_row_block(1024);
    mv_tablet_schema_pb.set_compress_kind(COMPRESS_NONE);
    mv_tablet_schema_pb.set_next_column_unique_id(3);

    ColumnPB* mv_column_1 = mv_tablet_schema_pb.add_column();
    mv_column_1->set_unique_id(1);
    mv_column_1->set_name("k1");
    mv_column_1->set_type("INT");
    mv_column_1->set_is_key(true);
    mv_column_1->set_length(4);
    mv_column_1->set_index_length(4);
    mv_column_1->set_is_nullable(false);
    mv_column_1->set_is_bf_column(false);

    ColumnPB* mv_column_2 = mv_tablet_schema_pb.add_column();
    mv_column_2->set_unique_id(2);
    mv_column_2->set_name("v1");
    mv_column_2->set_type("HLL");
    mv_column_2->set_length(4);
    mv_column_2->set_is_key(false);
    mv_column_2->set_is_nullable(false);
    mv_column_2->set_is_bf_column(false);
    mv_column_2->set_aggregation("HLL_UNION");

    TabletSchema mv_tablet_schema;
    mv_tablet_schema.init_from_pb(mv_tablet_schema_pb);

    RowBlockChanger row_block_changer(mv_tablet_schema);
    ColumnMapping* column_mapping = row_block_changer.get_mutable_column_mapping(0);
    column_mapping->ref_column = 0;
    column_mapping = row_block_changer.get_mutable_column_mapping(1);
    column_mapping->ref_column = 1;
    column_mapping->materialized_function = "hll_hash";

    RowBlock mutable_block(&mv_tablet_schema);
    mutable_block.init(block_info);
    uint64_t filtered_rows = 0;
    row_block_changer.change_row_block(&block, 0, &mutable_block, &filtered_rows);

    RowCursor mv_row_cursor;
    mv_row_cursor.init(mv_tablet_schema);
    mutable_block.get_row(0, &mv_row_cursor);

    auto dst_slice = reinterpret_cast<Slice*>(mv_row_cursor.cell_ptr(1));
    HyperLogLog hll(*dst_slice);
    EXPECT_EQ(hll.estimate_cardinality(), 1);
}

TEST_F(TestColumn, ConvertCharToCount) {
    //Base Tablet
    TabletSchema tablet_schema;
    CreateTabletSchema(tablet_schema);

    //Base row block
    create_column_writer(tablet_schema);

    RowBlock block(&tablet_schema);
    RowBlockInfo block_info;
    block_info.row_num = 10000;
    block.init(block_info);

    RowCursor write_row;
    write_row.init(tablet_schema);
    write_row.allocate_memory_for_string_type(tablet_schema);
    std::vector<std::string> val_string_array;
    val_string_array.emplace_back("1");
    val_string_array.emplace_back("1");
    val_string_array.emplace_back("2");
    val_string_array.emplace_back("3");
    OlapTuple tuple(val_string_array);
    write_row.from_tuple(tuple);
    block.set_row(0, write_row);

    block.finalize(1);
    ColumnDataHeaderMessage header;
    EXPECT_EQ(_column_writer->finalize(&header), Status::OK());

    //Materialized View tablet schema
    TabletSchemaPB mv_tablet_schema_pb;
    mv_tablet_schema_pb.set_keys_type(KeysType::AGG_KEYS);
    mv_tablet_schema_pb.set_num_short_key_columns(2);
    mv_tablet_schema_pb.set_num_rows_per_row_block(1024);
    mv_tablet_schema_pb.set_compress_kind(COMPRESS_NONE);
    mv_tablet_schema_pb.set_next_column_unique_id(3);

    ColumnPB* mv_column_1 = mv_tablet_schema_pb.add_column();
    mv_column_1->set_unique_id(1);
    mv_column_1->set_name("k1");
    mv_column_1->set_type("INT");
    mv_column_1->set_is_key(true);
    mv_column_1->set_length(4);
    mv_column_1->set_index_length(4);
    mv_column_1->set_is_nullable(false);
    mv_column_1->set_is_bf_column(false);

    ColumnPB* mv_column_2 = mv_tablet_schema_pb.add_column();
    mv_column_2->set_unique_id(2);
    mv_column_2->set_name("v1");
    mv_column_2->set_type("BIGINT");
    mv_column_2->set_length(4);
    mv_column_2->set_is_key(false);
    mv_column_2->set_is_nullable(false);
    mv_column_2->set_is_bf_column(false);
    mv_column_2->set_aggregation("SUM");

    TabletSchema mv_tablet_schema;
    mv_tablet_schema.init_from_pb(mv_tablet_schema_pb);

    RowBlockChanger row_block_changer(mv_tablet_schema);
    ColumnMapping* column_mapping = row_block_changer.get_mutable_column_mapping(0);
    column_mapping->ref_column = 0;
    column_mapping = row_block_changer.get_mutable_column_mapping(1);
    column_mapping->ref_column = 1;
    column_mapping->materialized_function = "count_field";

    RowBlock mutable_block(&mv_tablet_schema);
    mutable_block.init(block_info);
    uint64_t filtered_rows = 0;
    row_block_changer.change_row_block(&block, 0, &mutable_block, &filtered_rows);

    RowCursor mv_row_cursor;
    mv_row_cursor.init(mv_tablet_schema);
    mutable_block.get_row(0, &mv_row_cursor);

    auto dst = mv_row_cursor.cell_ptr(1);
    EXPECT_EQ(*(int64_t*)dst, 1);
}
} // namespace doris
