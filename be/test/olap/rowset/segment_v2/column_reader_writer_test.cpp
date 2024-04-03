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

#include <iostream>

#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "olap/column_block.h"
#include "olap/decimal12.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/rowset/segment_v2/column_writer.h"
#include "olap/tablet_schema_helper.h"
#include "olap/types.h"
#include "testutil/test_util.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nothing.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

using std::string;

namespace doris {
namespace segment_v2 {

static const std::string TEST_DIR = "./ut_dir/column_reader_writer_test";

class ColumnReaderWriterTest : public testing::Test {
public:
    ColumnReaderWriterTest() : _pool() {}
    ~ColumnReaderWriterTest() override = default;

protected:
    void SetUp() override {
        config::disable_storage_page_cache = true;
        auto st = io::global_local_filesystem()->delete_directory(TEST_DIR);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(TEST_DIR);
        ASSERT_TRUE(st.ok()) << st;
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(TEST_DIR).ok());
    }

private:
    vectorized::Arena _pool;
};

template <FieldType type, EncodingTypePB encoding>
void test_nullable_data(uint8_t* src_data, uint8_t* src_is_null, int num_rows,
                        std::string test_name) {
    using Type = typename TypeTraits<type>::CppType;
    Type* src = (Type*)src_data;

    ColumnMetaPB meta;

    // write data
    std::string fname = TEST_DIR + "/" + test_name;
    auto fs = io::global_local_filesystem();
    {
        io::FileWriterPtr file_writer;
        Status st = fs->create_file(fname, &file_writer);
        EXPECT_TRUE(st.ok()) << st;

        ColumnWriterOptions writer_opts;
        writer_opts.meta = &meta;
        writer_opts.meta->set_column_id(0);
        writer_opts.meta->set_unique_id(0);
        writer_opts.meta->set_type(type);
        if (type == FieldType::OLAP_FIELD_TYPE_CHAR || type == FieldType::OLAP_FIELD_TYPE_VARCHAR) {
            writer_opts.meta->set_length(10);
        } else {
            writer_opts.meta->set_length(0);
        }
        writer_opts.meta->set_encoding(encoding);
        writer_opts.meta->set_compression(segment_v2::CompressionTypePB::LZ4F);
        writer_opts.meta->set_is_nullable(true);
        writer_opts.need_zone_map = true;

        TabletColumn column(OLAP_FIELD_AGGREGATION_NONE, type);
        if (type == FieldType::OLAP_FIELD_TYPE_VARCHAR) {
            column = create_varchar_key(1);
        } else if (type == FieldType::OLAP_FIELD_TYPE_CHAR) {
            column = create_char_key(1);
        }
        std::unique_ptr<ColumnWriter> writer;
        ColumnWriter::create(writer_opts, &column, file_writer.get(), &writer);
        st = writer->init();
        EXPECT_TRUE(st.ok()) << st.to_string();

        for (int i = 0; i < num_rows; ++i) {
            st = writer->append(BitmapTest(src_is_null, i), src + i);
            EXPECT_TRUE(st.ok());
        }

        EXPECT_TRUE(writer->finish().ok());
        EXPECT_TRUE(writer->write_data().ok());
        EXPECT_TRUE(writer->write_ordinal_index().ok());
        EXPECT_TRUE(writer->write_zone_map().ok());

        // close the file
        EXPECT_TRUE(file_writer->close().ok());
    }
    auto type_info = get_scalar_type_info(type);
    io::FileReaderSPtr file_reader;
    ASSERT_EQ(fs->open_file(fname, &file_reader), Status::OK());
    // read and check
    {
        // sequence read
        {
            ColumnReaderOptions reader_opts;
            std::unique_ptr<ColumnReader> reader;
            auto st = ColumnReader::create(reader_opts, meta, num_rows, file_reader, &reader);
            EXPECT_TRUE(st.ok());

            ColumnIterator* iter = nullptr;
            st = reader->new_iterator(&iter);
            EXPECT_TRUE(st.ok());

            ColumnIteratorOptions iter_opts;
            OlapReaderStatistics stats;
            iter_opts.stats = &stats;
            iter_opts.file_reader = file_reader.get();
            st = iter->init(iter_opts);
            EXPECT_TRUE(st.ok());

            st = iter->seek_to_first();
            EXPECT_TRUE(st.ok()) << st.to_string();

            vectorized::Arena pool;
            std::unique_ptr<ColumnVectorBatch> cvb;
            ColumnVectorBatch::create(0, true, type_info, nullptr, &cvb);
            cvb->resize(1024);
            ColumnBlock col(cvb.get(), &pool);

            int idx = 0;
            while (true) {
                size_t rows_read = 1024;
                ColumnBlockView dst(&col);
                st = iter->next_batch(&rows_read, &dst);
                EXPECT_TRUE(st.ok());
                for (int j = 0; j < rows_read; ++j) {
                    EXPECT_EQ(BitmapTest(src_is_null, idx), col.is_null(j));
                    if (!col.is_null(j)) {
                        if (type == FieldType::OLAP_FIELD_TYPE_VARCHAR ||
                            type == FieldType::OLAP_FIELD_TYPE_CHAR) {
                            Slice* src_slice = (Slice*)src_data;
                            EXPECT_EQ(src_slice[idx].to_string(),
                                      reinterpret_cast<const Slice*>(col.cell_ptr(j))->to_string())
                                    << "j:" << j;
                        } else {
                            EXPECT_EQ(src[idx], *reinterpret_cast<const Type*>(col.cell_ptr(j)));
                        }
                    }
                    idx++;
                }
                if (rows_read < 1024) {
                    break;
                }
            }
            delete iter;
        }

        {
            ColumnReaderOptions reader_opts;
            std::unique_ptr<ColumnReader> reader;
            auto st = ColumnReader::create(reader_opts, meta, num_rows, file_reader, &reader);
            EXPECT_TRUE(st.ok());

            ColumnIterator* iter = nullptr;
            st = reader->new_iterator(&iter);
            EXPECT_TRUE(st.ok());

            EXPECT_TRUE(st.ok());
            ColumnIteratorOptions iter_opts;
            OlapReaderStatistics stats;
            iter_opts.stats = &stats;
            iter_opts.file_reader = file_reader.get();
            st = iter->init(iter_opts);
            EXPECT_TRUE(st.ok());

            vectorized::Arena pool;
            std::unique_ptr<ColumnVectorBatch> cvb;
            ColumnVectorBatch::create(0, true, type_info, nullptr, &cvb);
            cvb->resize(1024);
            ColumnBlock col(cvb.get(), &pool);

            for (int rowid = 0; rowid < num_rows; rowid += 4025) {
                st = iter->seek_to_ordinal(rowid);
                EXPECT_TRUE(st.ok());

                int idx = rowid;
                size_t rows_read = 1024;
                ColumnBlockView dst(&col);

                st = iter->next_batch(&rows_read, &dst);
                EXPECT_TRUE(st.ok());
                for (int j = 0; j < rows_read; ++j) {
                    EXPECT_EQ(BitmapTest(src_is_null, idx), col.is_null(j));
                    if (!col.is_null(j)) {
                        if (type == FieldType::OLAP_FIELD_TYPE_VARCHAR ||
                            type == FieldType::OLAP_FIELD_TYPE_CHAR) {
                            Slice* src_slice = (Slice*)src_data;
                            EXPECT_EQ(src_slice[idx].to_string(),
                                      reinterpret_cast<const Slice*>(col.cell_ptr(j))->to_string());
                        } else {
                            EXPECT_EQ(src[idx], *reinterpret_cast<const Type*>(col.cell_ptr(j)));
                        }
                    }
                    idx++;
                }
            }
            delete iter;
        }
    }
}

template <FieldType item_type, EncodingTypePB item_encoding, EncodingTypePB array_encoding>
void test_array_nullable_data(CollectionValue* src_data, uint8_t* src_is_null, int num_rows,
                              std::string test_name) {
    CollectionValue* src = src_data;
    ColumnMetaPB meta;
    TabletColumn list_column(OLAP_FIELD_AGGREGATION_NONE, FieldType::OLAP_FIELD_TYPE_ARRAY);
    int32 item_length = 0;
    if (item_type == FieldType::OLAP_FIELD_TYPE_CHAR ||
        item_type == FieldType::OLAP_FIELD_TYPE_VARCHAR) {
        item_length = 10;
    }
    TabletColumn item_column(OLAP_FIELD_AGGREGATION_NONE, item_type, true, 0, item_length);
    list_column.add_sub_column(item_column);
    Field* field = FieldFactory::create(list_column);

    // write data
    std::string fname = TEST_DIR + "/" + test_name;
    auto fs = io::global_local_filesystem();
    {
        io::FileWriterPtr file_writer;
        Status st = fs->create_file(fname, &file_writer);
        EXPECT_TRUE(st.ok()) << st;

        ColumnWriterOptions writer_opts;
        writer_opts.meta = &meta;
        writer_opts.meta->set_column_id(0);
        writer_opts.meta->set_unique_id(0);
        writer_opts.meta->set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
        writer_opts.meta->set_length(0);
        writer_opts.meta->set_encoding(array_encoding);
        writer_opts.meta->set_compression(segment_v2::CompressionTypePB::LZ4F);
        writer_opts.meta->set_is_nullable(true);
        writer_opts.data_page_size = 5 * 8;

        ColumnMetaPB* child_meta = meta.add_children_columns();

        child_meta->set_column_id(1);
        child_meta->set_unique_id(1);
        child_meta->set_type(item_type);
        child_meta->set_length(item_length);
        child_meta->set_encoding(item_encoding);
        child_meta->set_compression(segment_v2::CompressionTypePB::LZ4F);
        child_meta->set_is_nullable(true);

        std::unique_ptr<ColumnWriter> writer;
        ColumnWriter::create(writer_opts, &list_column, file_writer.get(), &writer);
        st = writer->init();
        EXPECT_TRUE(st.ok()) << st.to_string();

        for (int i = 0; i < num_rows; ++i) {
            st = writer->append(BitmapTest(src_is_null, i), src + i);
            EXPECT_TRUE(st.ok());
        }

        st = writer->finish();
        EXPECT_TRUE(st.ok());

        st = writer->write_data();
        EXPECT_TRUE(st.ok());
        st = writer->write_ordinal_index();
        EXPECT_TRUE(st.ok());

        // close the file
        EXPECT_TRUE(file_writer->close().ok());
    }
    auto type_info = get_type_info(&meta);
    io::FileReaderSPtr file_reader;
    ASSERT_EQ(fs->open_file(fname, &file_reader), Status::OK());
    // read and check
    {
        ColumnReaderOptions reader_opts;
        std::unique_ptr<ColumnReader> reader;
        auto st = ColumnReader::create(reader_opts, meta, num_rows, file_reader, &reader);
        EXPECT_TRUE(st.ok());

        ColumnIterator* iter = nullptr;
        st = reader->new_iterator(&iter);
        EXPECT_TRUE(st.ok());

        ColumnIteratorOptions iter_opts;
        OlapReaderStatistics stats;
        iter_opts.stats = &stats;
        iter_opts.file_reader = file_reader.get();
        st = iter->init(iter_opts);
        EXPECT_TRUE(st.ok());
        // sequence read
        {
            st = iter->seek_to_first();
            EXPECT_TRUE(st.ok()) << st.to_string();

            vectorized::Arena pool;
            std::unique_ptr<ColumnVectorBatch> cvb;
            ColumnVectorBatch::create(0, true, type_info.get(), field, &cvb);
            cvb->resize(1024);
            ColumnBlock col(cvb.get(), &pool);

            int idx = 0;
            while (true) {
                size_t rows_read = 1024;
                ColumnBlockView dst(&col);
                st = iter->next_batch(&rows_read, &dst);
                EXPECT_TRUE(st.ok());
                for (int j = 0; j < rows_read; ++j) {
                    EXPECT_EQ(BitmapTest(src_is_null, idx), col.is_null(j));
                    if (!col.is_null(j)) {
                        EXPECT_TRUE(type_info->equal(&src[idx], col.cell_ptr(j)));
                    }
                    ++idx;
                }
                if (rows_read < 1024) {
                    break;
                }
            }
        }
        // seek read
        {
            vectorized::Arena pool;
            std::unique_ptr<ColumnVectorBatch> cvb;
            ColumnVectorBatch::create(0, true, type_info.get(), field, &cvb);
            cvb->resize(1024);
            ColumnBlock col(cvb.get(), &pool);

            for (int rowid = 0; rowid < num_rows; rowid += 4025) {
                st = iter->seek_to_ordinal(rowid);
                EXPECT_TRUE(st.ok());

                int idx = rowid;
                size_t rows_read = 1024;
                ColumnBlockView dst(&col);

                st = iter->next_batch(&rows_read, &dst);
                EXPECT_TRUE(st.ok());
                for (int j = 0; j < rows_read; ++j) {
                    EXPECT_EQ(BitmapTest(src_is_null, idx), col.is_null(j));
                    if (!col.is_null(j)) {
                        EXPECT_TRUE(type_info->equal(&src[idx], col.cell_ptr(j)));
                    }
                    ++idx;
                }
            }
        }
        delete iter;
    }
    delete field;
}

TEST_F(ColumnReaderWriterTest, test_array_type) {
    size_t num_array = LOOP_LESS_OR_MORE(1024, 24 * 1024);
    size_t num_item = num_array * 3;

    uint8_t* array_is_null = new uint8_t[BitmapSize(num_array)];
    CollectionValue* array_val = new CollectionValue[num_array];
    bool* item_is_null = new bool[num_item];
    uint8_t* item_val = new uint8_t[num_item];
    for (int i = 0; i < num_item; ++i) {
        item_val[i] = i;
        item_is_null[i] = (i % 4) == 0;
        if (i % 3 == 0) {
            size_t array_index = i / 3;
            bool is_null = (array_index % 4) == 1;
            BitmapChange(array_is_null, array_index, is_null);
            if (is_null) {
                continue;
            }
            array_val[array_index].set_data(&item_val[i]);
            array_val[array_index].set_null_signs(&item_is_null[i]);
            array_val[array_index].set_length(3);
        }
    }
    test_array_nullable_data<FieldType::OLAP_FIELD_TYPE_TINYINT, BIT_SHUFFLE, BIT_SHUFFLE>(
            array_val, array_is_null, num_array, "null_array_bs");

    delete[] array_val;
    delete[] item_val;
    delete[] item_is_null;

    array_val = new CollectionValue[num_array];
    Slice* varchar_vals = new Slice[3];
    item_is_null = new bool[3];
    for (int i = 0; i < 3; ++i) {
        item_is_null[i] = i == 1;
        if (i != 1) {
            set_column_value_by_type(FieldType::OLAP_FIELD_TYPE_VARCHAR, i, (char*)&varchar_vals[i],
                                     &_pool);
        }
    }
    for (int i = 0; i < num_array; ++i) {
        bool is_null = (i % 4) == 1;
        BitmapChange(array_is_null, i, is_null);
        if (is_null) {
            continue;
        }
        array_val[i].set_data(varchar_vals);
        array_val[i].set_null_signs(item_is_null);
        array_val[i].set_length(3);
    }
    test_array_nullable_data<FieldType::OLAP_FIELD_TYPE_VARCHAR, DICT_ENCODING, BIT_SHUFFLE>(
            array_val, array_is_null, num_array, "null_array_chars");

    delete[] array_val;
    delete[] varchar_vals;
    delete[] item_is_null;

    delete[] array_is_null;
}

template <FieldType type>
void test_read_default_value(string value, void* result) {
    using Type = typename TypeTraits<type>::CppType;
    const auto* scalar_type_info = get_scalar_type_info<type>();
    // read and check
    {
        TabletColumn tablet_column = create_with_default_value<type>(value);
        DefaultValueColumnIterator iter(tablet_column.has_default_value(),
                                        tablet_column.default_value(), tablet_column.is_nullable(),
                                        create_static_type_info_ptr(scalar_type_info),
                                        tablet_column.precision(), tablet_column.frac());
        ColumnIteratorOptions iter_opts;
        auto st = iter.init(iter_opts);
        EXPECT_TRUE(st.ok());
        // sequence read
        {
            st = iter.seek_to_first();
            EXPECT_TRUE(st.ok()) << st.to_string();

            vectorized::Arena pool;
            std::unique_ptr<ColumnVectorBatch> cvb;
            ColumnVectorBatch::create(0, true, scalar_type_info, nullptr, &cvb);
            cvb->resize(1024);
            ColumnBlock col(cvb.get(), &pool);

            size_t rows_read = 1024;
            ColumnBlockView dst(&col);
            bool has_null;
            st = iter.next_batch(&rows_read, &dst, &has_null);
            EXPECT_TRUE(st.ok());
            for (int j = 0; j < rows_read; ++j) {
                if (type == FieldType::OLAP_FIELD_TYPE_CHAR) {
                    EXPECT_EQ(*(string*)result,
                              reinterpret_cast<const Slice*>(col.cell_ptr(j))->to_string())
                            << "j:" << j;
                } else if (type == FieldType::OLAP_FIELD_TYPE_VARCHAR ||
                           type == FieldType::OLAP_FIELD_TYPE_HLL ||
                           type == FieldType::OLAP_FIELD_TYPE_OBJECT) {
                    EXPECT_EQ(value, reinterpret_cast<const Slice*>(col.cell_ptr(j))->to_string())
                            << "j:" << j;
                } else {
                    ;
                    EXPECT_EQ(*(Type*)result, *(reinterpret_cast<const Type*>(col.cell_ptr(j))));
                }
            }
        }

        {
            vectorized::Arena pool;
            std::unique_ptr<ColumnVectorBatch> cvb;
            ColumnVectorBatch::create(0, true, scalar_type_info, nullptr, &cvb);
            cvb->resize(1024);
            ColumnBlock col(cvb.get(), &pool);

            for (int rowid = 0; rowid < 2048; rowid += 128) {
                st = iter.seek_to_ordinal(rowid);
                EXPECT_TRUE(st.ok());

                size_t rows_read = 1024;
                ColumnBlockView dst(&col);
                bool has_null;
                st = iter.next_batch(&rows_read, &dst, &has_null);
                EXPECT_TRUE(st.ok());
                for (int j = 0; j < rows_read; ++j) {
                    if (type == FieldType::OLAP_FIELD_TYPE_CHAR) {
                        EXPECT_EQ(*(string*)result,
                                  reinterpret_cast<const Slice*>(col.cell_ptr(j))->to_string())
                                << "j:" << j;
                    } else if (type == FieldType::OLAP_FIELD_TYPE_VARCHAR ||
                               type == FieldType::OLAP_FIELD_TYPE_HLL ||
                               type == FieldType::OLAP_FIELD_TYPE_OBJECT) {
                        EXPECT_EQ(value,
                                  reinterpret_cast<const Slice*>(col.cell_ptr(j))->to_string());
                    } else {
                        EXPECT_EQ(*(Type*)result,
                                  *(reinterpret_cast<const Type*>(col.cell_ptr(j))));
                    }
                }
            }
        }
    }
}

static vectorized::MutableColumnPtr create_vectorized_column_ptr(FieldType type) {
    if (type == FieldType::OLAP_FIELD_TYPE_INT) {
        return vectorized::DataTypeInt32().create_column();
    } else if (type == FieldType::OLAP_FIELD_TYPE_SMALLINT) {
        return vectorized::DataTypeInt16().create_column();
    } else if (type == FieldType::OLAP_FIELD_TYPE_BIGINT) {
        return vectorized::DataTypeInt64().create_column();
    } else if (type == FieldType::OLAP_FIELD_TYPE_LARGEINT) {
        return vectorized::DataTypeInt128().create_column();
    } else if (type == FieldType::OLAP_FIELD_TYPE_FLOAT) {
        return vectorized::DataTypeFloat32().create_column();
    } else if (type == FieldType::OLAP_FIELD_TYPE_DOUBLE) {
        return vectorized::DataTypeFloat64().create_column();
    } else if (type == FieldType::OLAP_FIELD_TYPE_CHAR) {
        return vectorized::DataTypeString().create_column();
    } else if (type == FieldType::OLAP_FIELD_TYPE_DATE) {
        return vectorized::DataTypeDate().create_column();
    } else if (type == FieldType::OLAP_FIELD_TYPE_DATETIME) {
        return vectorized::DataTypeDateTime().create_column();
    } else if (type == FieldType::OLAP_FIELD_TYPE_DECIMAL) {
        return vectorized::DataTypeDecimal<vectorized::Decimal128V2>(27, 9).create_column();
    }
    return vectorized::DataTypeNothing().create_column();
}

template <FieldType type>
void test_v_read_default_value(string value, void* result) {
    using Type = typename TypeTraits<type>::CppType;
    const auto* scalar_type_info = get_scalar_type_info<type>();
    // read and check
    {
        TabletColumn tablet_column = create_with_default_value<type>(value);
        DefaultValueColumnIterator iter(tablet_column.has_default_value(),
                                        tablet_column.default_value(), tablet_column.is_nullable(),
                                        create_static_type_info_ptr(scalar_type_info),
                                        tablet_column.precision(), tablet_column.frac());
        ColumnIteratorOptions iter_opts;
        auto st = iter.init(iter_opts);
        EXPECT_TRUE(st.ok());

        // sequence read
        {
            st = iter.seek_to_first();
            EXPECT_TRUE(st.ok()) << st.to_string();

            vectorized::MutableColumnPtr mcp = create_vectorized_column_ptr(type);

            size_t rows_read = 16;
            bool has_null;
            st = iter.next_batch(&rows_read, mcp, &has_null);

            EXPECT_TRUE(st.ok());
            for (int j = 0; j < rows_read; ++j) {
                if (type == FieldType::OLAP_FIELD_TYPE_CHAR) {
                } else if (type == FieldType::OLAP_FIELD_TYPE_VARCHAR ||
                           type == FieldType::OLAP_FIELD_TYPE_HLL ||
                           type == FieldType::OLAP_FIELD_TYPE_OBJECT) {
                } else if (type == FieldType::OLAP_FIELD_TYPE_DATE ||
                           type == FieldType::OLAP_FIELD_TYPE_DATETIME) {
                    StringRef sr = mcp->get_data_at(j);
                    EXPECT_EQ(sr.size, sizeof(vectorized::Int64));

                    auto x = unaligned_load<vectorized::Int64>(sr.data);
                    auto value = binary_cast<vectorized::Int64, VecDateTimeValue>(x);
                    char buf[64] = {};
                    value.to_string(buf);
                    int ret = strcmp(buf, (char*)result);
                    EXPECT_EQ(ret, 0);
                } else if (type == FieldType::OLAP_FIELD_TYPE_DECIMAL) {
                    StringRef sr = mcp->get_data_at(j);
                    EXPECT_EQ(sr.size, sizeof(vectorized::Int128));

                    DecimalV2Value v1(unaligned_load<vectorized::Int128>(sr.data));
                    decimal12_t* v2 = (decimal12_t*)result;

                    EXPECT_EQ(v2->integer, v1.int_value());
                    EXPECT_EQ(v2->fraction, v1.frac_value());
                } else {
                    StringRef sr = mcp->get_data_at(j);
                    EXPECT_EQ(sr.size, sizeof(Type));
                    int ret = memcmp(sr.data, result, sr.size);
                    EXPECT_EQ(ret, 0);
                }
            }
        }
    }
}

TEST_F(ColumnReaderWriterTest, test_nullable) {
    size_t num_uint8_rows = LOOP_LESS_OR_MORE(1024, 1024 * 1024);
    uint8_t* is_null = new uint8_t[num_uint8_rows];
    uint8_t* val = new uint8_t[num_uint8_rows];
    for (int i = 0; i < num_uint8_rows; ++i) {
        val[i] = i;
        BitmapChange(is_null, i, (i % 4) == 0);
    }

    test_nullable_data<FieldType::OLAP_FIELD_TYPE_TINYINT, BIT_SHUFFLE>(
            val, is_null, num_uint8_rows, "null_tiny_bs");
    test_nullable_data<FieldType::OLAP_FIELD_TYPE_SMALLINT, BIT_SHUFFLE>(
            val, is_null, num_uint8_rows / 2, "null_smallint_bs");
    test_nullable_data<FieldType::OLAP_FIELD_TYPE_INT, BIT_SHUFFLE>(
            val, is_null, num_uint8_rows / 4, "null_int_bs");
    test_nullable_data<FieldType::OLAP_FIELD_TYPE_BIGINT, BIT_SHUFFLE>(
            val, is_null, num_uint8_rows / 8, "null_bigint_bs");
    test_nullable_data<FieldType::OLAP_FIELD_TYPE_LARGEINT, BIT_SHUFFLE>(
            val, is_null, num_uint8_rows / 16, "null_largeint_bs");

    // test for the case where most values are not null
    uint8_t* is_null_sparse = new uint8_t[num_uint8_rows];
    for (int i = 0; i < num_uint8_rows; ++i) {
        bool v = false;
        // in order to make some data pages not null, set the first half of values not null.
        // for the second half, only 1/1024 of values are null
        if (i >= (num_uint8_rows / 2)) {
            v = (i % 1024) == 10;
        }
        BitmapChange(is_null_sparse, i, v);
    }
    test_nullable_data<FieldType::OLAP_FIELD_TYPE_TINYINT, BIT_SHUFFLE>(
            val, is_null_sparse, num_uint8_rows, "sparse_null_tiny_bs");

    float* float_vals = new float[num_uint8_rows];
    for (int i = 0; i < num_uint8_rows; ++i) {
        float_vals[i] = i;
        is_null[i] = ((i % 16) == 0);
    }
    test_nullable_data<FieldType::OLAP_FIELD_TYPE_FLOAT, BIT_SHUFFLE>(
            (uint8_t*)float_vals, is_null, num_uint8_rows, "null_float_bs");

    double* double_vals = new double[num_uint8_rows];
    for (int i = 0; i < num_uint8_rows; ++i) {
        double_vals[i] = i;
        is_null[i] = ((i % 16) == 0);
    }
    test_nullable_data<FieldType::OLAP_FIELD_TYPE_DOUBLE, BIT_SHUFFLE>(
            (uint8_t*)double_vals, is_null, num_uint8_rows, "null_double_bs");
    // test_nullable_data<FieldType::OLAP_FIELD_TYPE_FLOAT, BIT_SHUFFLE>(val, is_null, num_uint8_rows / 4, "null_float_bs");
    // test_nullable_data<FieldType::OLAP_FIELD_TYPE_DOUBLE, BIT_SHUFFLE>(val, is_null, num_uint8_rows / 8, "null_double_bs");
    delete[] val;
    delete[] is_null;
    delete[] is_null_sparse;
    delete[] float_vals;
    delete[] double_vals;
}

TEST_F(ColumnReaderWriterTest, test_types) {
    size_t num_uint8_rows = LOOP_LESS_OR_MORE(1024, 1024 * 1024);
    uint8_t* is_null = new uint8_t[num_uint8_rows];

    bool* bool_vals = new bool[num_uint8_rows];
    uint24_t* date_vals = new uint24_t[num_uint8_rows];
    uint64_t* datetime_vals = new uint64_t[num_uint8_rows];
    decimal12_t* decimal_vals = new decimal12_t[num_uint8_rows];
    Slice* varchar_vals = new Slice[num_uint8_rows];
    Slice* char_vals = new Slice[num_uint8_rows];
    for (int i = 0; i < num_uint8_rows; ++i) {
        bool_vals[i] = i % 2;
        date_vals[i] = i + 33;
        datetime_vals[i] = i + 33;
        decimal_vals[i] = {i, i}; // 1.000000001

        set_column_value_by_type(FieldType::OLAP_FIELD_TYPE_VARCHAR, i, (char*)&varchar_vals[i],
                                 &_pool);
        set_column_value_by_type(FieldType::OLAP_FIELD_TYPE_CHAR, i, (char*)&char_vals[i], &_pool,
                                 8);

        BitmapChange(is_null, i, (i % 4) == 0);
    }
    test_nullable_data<FieldType::OLAP_FIELD_TYPE_CHAR, DICT_ENCODING>(
            (uint8_t*)char_vals, is_null, num_uint8_rows, "null_char_bs");
    test_nullable_data<FieldType::OLAP_FIELD_TYPE_VARCHAR, DICT_ENCODING>(
            (uint8_t*)varchar_vals, is_null, num_uint8_rows, "null_varchar_bs");
    test_nullable_data<FieldType::OLAP_FIELD_TYPE_BOOL, BIT_SHUFFLE>(
            (uint8_t*)bool_vals, is_null, num_uint8_rows, "null_bool_bs");
    test_nullable_data<FieldType::OLAP_FIELD_TYPE_DATE, BIT_SHUFFLE>(
            (uint8_t*)date_vals, is_null, num_uint8_rows / 3, "null_date_bs");

    for (int i = 0; i < num_uint8_rows; ++i) {
        BitmapChange(is_null, i, (i % 16) == 0);
    }
    test_nullable_data<FieldType::OLAP_FIELD_TYPE_DATETIME, BIT_SHUFFLE>(
            (uint8_t*)datetime_vals, is_null, num_uint8_rows / 8, "null_datetime_bs");

    for (int i = 0; i < num_uint8_rows; ++i) {
        BitmapChange(is_null, i, (i % 24) == 0);
    }
    test_nullable_data<FieldType::OLAP_FIELD_TYPE_DECIMAL, BIT_SHUFFLE>(
            (uint8_t*)decimal_vals, is_null, num_uint8_rows / 12, "null_decimal_bs");

    delete[] char_vals;
    delete[] varchar_vals;
    delete[] is_null;
    delete[] bool_vals;
    delete[] date_vals;
    delete[] datetime_vals;
    delete[] decimal_vals;
}

TEST_F(ColumnReaderWriterTest, test_default_value) {
    std::string v_int("1");
    int32_t result = 1;
    test_read_default_value<FieldType::OLAP_FIELD_TYPE_TINYINT>(v_int, &result);
    test_read_default_value<FieldType::OLAP_FIELD_TYPE_SMALLINT>(v_int, &result);
    test_read_default_value<FieldType::OLAP_FIELD_TYPE_INT>(v_int, &result);

    std::string v_bigint("9223372036854775807");
    int64_t result_bigint = std::numeric_limits<int64_t>::max();
    test_read_default_value<FieldType::OLAP_FIELD_TYPE_BIGINT>(v_bigint, &result_bigint);
    int128_t result_largeint = std::numeric_limits<int64_t>::max();
    test_read_default_value<FieldType::OLAP_FIELD_TYPE_LARGEINT>(v_bigint, &result_largeint);

    std::string v_float("1.00");
    float result2 = 1.00;
    test_read_default_value<FieldType::OLAP_FIELD_TYPE_FLOAT>(v_float, &result2);

    std::string v_double("1.00");
    double result3 = 1.00;
    test_read_default_value<FieldType::OLAP_FIELD_TYPE_DOUBLE>(v_double, &result3);

    std::string v_varchar("varchar");
    test_read_default_value<FieldType::OLAP_FIELD_TYPE_VARCHAR>(v_varchar, &v_varchar);

    std::string v_char("char");
    test_read_default_value<FieldType::OLAP_FIELD_TYPE_CHAR>(v_char, &v_char);

    char* c = (char*)malloc(1);
    c[0] = 0;
    std::string v_object(c, 1);
    test_read_default_value<FieldType::OLAP_FIELD_TYPE_HLL>(v_object, &v_object);
    test_read_default_value<FieldType::OLAP_FIELD_TYPE_OBJECT>(v_object, &v_object);
    free(c);

    std::string v_date("2019-11-12");
    uint24_t result_date(1034092);
    test_read_default_value<FieldType::OLAP_FIELD_TYPE_DATE>(v_date, &result_date);

    std::string v_datetime("2019-11-12 12:01:08");
    int64_t result_datetime = 20191112120108;
    test_read_default_value<FieldType::OLAP_FIELD_TYPE_DATETIME>(v_datetime, &result_datetime);

    std::string v_decimal("102418.000000002");
    decimal12_t decimal = {102418, 2};
    test_read_default_value<FieldType::OLAP_FIELD_TYPE_DECIMAL>(v_decimal, &decimal);
}

TEST_F(ColumnReaderWriterTest, test_v_default_value) {
    std::string v_int("1");
    int32_t result = 1;
    test_v_read_default_value<FieldType::OLAP_FIELD_TYPE_INT>(v_int, &result);

    std::string v_bigint("9223372036854775807");
    int64_t result_bigint = std::numeric_limits<int64_t>::max();
    test_v_read_default_value<FieldType::OLAP_FIELD_TYPE_BIGINT>(v_bigint, &result_bigint);

    int128_t result_largeint = std::numeric_limits<int64_t>::max();
    test_v_read_default_value<FieldType::OLAP_FIELD_TYPE_LARGEINT>(v_bigint, &result_largeint);

    std::string v_float("1.00");
    float result_float = 1.00;
    test_v_read_default_value<FieldType::OLAP_FIELD_TYPE_FLOAT>(v_float, &result_float);

    std::string v_double("1.99");
    double result_double = 1.99;
    test_v_read_default_value<FieldType::OLAP_FIELD_TYPE_DOUBLE>(v_double, &result_double);

    std::string v_date("2019-11-12");
    char result_date[] = "2019-11-12";
    test_v_read_default_value<FieldType::OLAP_FIELD_TYPE_DATE>(v_date, result_date);

    std::string v_datetime("2019-11-12 12:01:08");
    char result_datetime[] = "2019-11-12 12:01:08";
    test_v_read_default_value<FieldType::OLAP_FIELD_TYPE_DATETIME>(v_datetime, &result_datetime);

    std::string v_decimal("102418.000000002");
    decimal12_t decimal = {102418, 2};
    test_v_read_default_value<FieldType::OLAP_FIELD_TYPE_DECIMAL>(v_decimal, &decimal);
}

TEST_F(ColumnReaderWriterTest, test_single_empty_array) {
    size_t num_array = 1;
    std::unique_ptr<uint8_t[]> array_is_null(new uint8_t[BitmapSize(num_array)]());
    CollectionValue array(0);
    test_array_nullable_data<FieldType::OLAP_FIELD_TYPE_TINYINT, BIT_SHUFFLE, BIT_SHUFFLE>(
            &array, array_is_null.get(), num_array, "test_single_empty_array");
}

TEST_F(ColumnReaderWriterTest, test_mixed_empty_arrays) {
    size_t num_array = 3;
    std::unique_ptr<uint8_t[]> array_is_null(new uint8_t[BitmapSize(num_array)]());
    std::unique_ptr<CollectionValue[]> collection_values(new CollectionValue[num_array]);
    int data[] = {1, 2, 3};
    for (int i = 0; i < num_array; ++i) {
        if (i % 2 == 1) {
            new (&collection_values[i]) CollectionValue(0);
        } else {
            new (&collection_values[i]) CollectionValue(&data, 3, false, nullptr);
        }
    }
    test_array_nullable_data<FieldType::OLAP_FIELD_TYPE_INT, BIT_SHUFFLE, BIT_SHUFFLE>(
            collection_values.get(), array_is_null.get(), num_array, "test_mixed_empty_arrays");
}

} // namespace segment_v2
} // namespace doris
