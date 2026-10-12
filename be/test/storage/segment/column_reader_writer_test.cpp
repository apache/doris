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

#include "core/column/column_complex.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_date.h"
#include "core/data_type/data_type_date_time.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_hll.h"
#include "core/data_type/data_type_nothing.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/decimal12.h"
#include "core/types.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "storage/olap_common.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/column_writer.h"
#include "storage/tablet/tablet_schema_helper.h"
#include "storage/types.h"
#include "testutil/test_util.h"

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
        _old_disable_storage_page_cache = config::disable_storage_page_cache;
        config::disable_storage_page_cache = true;
        auto st = io::global_local_filesystem()->delete_directory(TEST_DIR);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(TEST_DIR);
        ASSERT_TRUE(st.ok()) << st;
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(TEST_DIR).ok());
        config::disable_storage_page_cache = _old_disable_storage_page_cache;
    }

private:
    Arena _pool;
    bool _old_disable_storage_page_cache = false;
};

static MutableColumnPtr create_vectorized_column_ptr(FieldType type);

// Read one row back in its storage format. DATE, DATETIME and DECIMAL change format
// when they are read, so turn them back here.
template <FieldType type>
typename TypeTraits<type>::CppType storage_value(const IColumn& column, size_t row) {
    using Type = typename TypeTraits<type>::CppType;
    StringRef data = column.get_data_at(row);
    if constexpr (type == FieldType::OLAP_FIELD_TYPE_DATE) {
        auto value = binary_cast<Int64, VecDateTimeValue>(unaligned_load<Int64>(data.data));
        return Type(static_cast<uint32_t>(value.to_olap_date()));
    } else if constexpr (type == FieldType::OLAP_FIELD_TYPE_DATETIME) {
        auto value = binary_cast<Int64, VecDateTimeValue>(unaligned_load<Int64>(data.data));
        return static_cast<Type>(value.to_olap_datetime());
    } else if constexpr (type == FieldType::OLAP_FIELD_TYPE_DECIMAL) {
        DecimalV2Value value(unaligned_load<Int128>(data.data));
        return {value.int_value(), value.frac_value()};
    } else {
        return unaligned_load<Type>(data.data);
    }
}

// Check `num_rows` rows of a nullable column against the source, starting at `first_row`.
template <FieldType type>
void check_nullable_rows(const IColumn& column, size_t num_rows, const uint8_t* src_data,
                         const uint8_t* src_is_null, size_t first_row) {
    using Type = typename TypeTraits<type>::CppType;
    const auto& nullable = assert_cast<const ColumnNullable&>(column);
    ASSERT_EQ(num_rows, nullable.size());
    for (size_t j = 0; j < num_rows; ++j) {
        size_t idx = first_row + j;
        EXPECT_EQ(BitmapTest(src_is_null, idx), nullable.is_null_at(j)) << "idx:" << idx;
        if (nullable.is_null_at(j)) {
            continue;
        }
        const IColumn& values = nullable.get_nested_column();
        if constexpr (type == FieldType::OLAP_FIELD_TYPE_CHAR ||
                      type == FieldType::OLAP_FIELD_TYPE_VARCHAR) {
            const Slice& src = reinterpret_cast<const Slice*>(src_data)[idx];
            // CHAR is read back without its trailing '\0' padding.
            size_t size = type == FieldType::OLAP_FIELD_TYPE_CHAR ? strnlen(src.data, src.size)
                                                                  : src.size;
            EXPECT_EQ(std::string(src.data, size), values.get_data_at(j).to_string())
                    << "idx:" << idx;
        } else {
            EXPECT_EQ(reinterpret_cast<const Type*>(src_data)[idx], storage_value<type>(values, j))
                    << "idx:" << idx;
        }
    }
}

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
        writer_opts.meta->set_type(static_cast<int32_t>(type));
        if (type == FieldType::OLAP_FIELD_TYPE_CHAR || type == FieldType::OLAP_FIELD_TYPE_VARCHAR) {
            writer_opts.meta->set_length(10);
        } else {
            writer_opts.meta->set_length(0);
        }
        if (type == FieldType::OLAP_FIELD_TYPE_DECIMAL) {
            // The writer builds a DECIMALV2 data type from the meta, which needs a precision.
            writer_opts.meta->set_precision(27);
            writer_opts.meta->set_frac(9);
        }
        writer_opts.meta->set_encoding(encoding);
        writer_opts.meta->set_compression(segment_v2::CompressionTypePB::LZ4F);
        writer_opts.meta->set_is_nullable(true);
        writer_opts.need_zone_map = true;

        TabletColumn column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE, type);
        if (type == FieldType::OLAP_FIELD_TYPE_VARCHAR) {
            column = *create_varchar_key(1);
        } else if (type == FieldType::OLAP_FIELD_TYPE_CHAR) {
            column = *create_char_key(1);
        }
        std::unique_ptr<ColumnWriter> writer;
        st = ColumnWriter::create(writer_opts, &column, file_writer.get(), &writer);
        EXPECT_TRUE(st.ok()) << st.to_string();
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
    io::FileReaderSPtr file_reader;
    ASSERT_EQ(fs->open_file(fname, &file_reader), Status::OK());
    // read and check
    {
        // sequence read
        {
            ColumnReaderOptions reader_opts;
            std::shared_ptr<ColumnReader> reader;
            auto st = ColumnReader::create(reader_opts, meta, num_rows, file_reader, &reader);
            EXPECT_TRUE(st.ok());

            ColumnIteratorUPtr iter;
            st = reader->new_iterator(&iter, nullptr);
            EXPECT_TRUE(st.ok());

            ColumnIteratorOptions iter_opts;
            OlapReaderStatistics stats;
            iter_opts.stats = &stats;
            iter_opts.file_reader = file_reader.get();
            st = iter->init(iter_opts);
            EXPECT_TRUE(st.ok());

            st = iter->seek_to_ordinal(0);
            EXPECT_TRUE(st.ok()) << st.to_string();

            int idx = 0;
            while (true) {
                size_t rows_read = 1024;
                MutableColumnPtr dst = ColumnNullable::create(create_vectorized_column_ptr(type),
                                                              ColumnUInt8::create());
                bool has_null = false;
                st = iter->next_batch(&rows_read, dst, &has_null);
                EXPECT_TRUE(st.ok());
                check_nullable_rows<type>(*dst, rows_read, src_data, src_is_null, idx);
                idx += rows_read;
                if (rows_read < 1024) {
                    break;
                }
            }
        }

        {
            ColumnReaderOptions reader_opts;
            std::shared_ptr<ColumnReader> reader;
            auto st = ColumnReader::create(reader_opts, meta, num_rows, file_reader, &reader);
            EXPECT_TRUE(st.ok());

            ColumnIteratorUPtr iter;
            st = reader->new_iterator(&iter, nullptr);
            EXPECT_TRUE(st.ok());

            EXPECT_TRUE(st.ok());
            ColumnIteratorOptions iter_opts;
            OlapReaderStatistics stats;
            iter_opts.stats = &stats;
            iter_opts.file_reader = file_reader.get();
            st = iter->init(iter_opts);
            EXPECT_TRUE(st.ok());

            for (int rowid = 0; rowid < num_rows; rowid += 4025) {
                st = iter->seek_to_ordinal(rowid);
                EXPECT_TRUE(st.ok());

                size_t rows_read = 1024;
                MutableColumnPtr dst = ColumnNullable::create(create_vectorized_column_ptr(type),
                                                              ColumnUInt8::create());
                bool has_null = false;
                st = iter->next_batch(&rows_read, dst, &has_null);
                EXPECT_TRUE(st.ok());
                check_nullable_rows<type>(*dst, rows_read, src_data, src_is_null, rowid);
            }
        }
    }
}

TEST_F(ColumnReaderWriterTest, test_array_append_nulls) {
    ColumnMetaPB meta;
    TabletColumn list_column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                             FieldType::OLAP_FIELD_TYPE_ARRAY);
    TabletColumn item_column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                             FieldType::OLAP_FIELD_TYPE_TINYINT, true);
    list_column.add_sub_column(item_column);

    std::string fname = TEST_DIR + "/array_append_nulls";
    auto fs = io::global_local_filesystem();
    io::FileWriterPtr file_writer;
    Status st = fs->create_file(fname, &file_writer);
    ASSERT_TRUE(st.ok()) << st;

    ColumnWriterOptions writer_opts;
    writer_opts.meta = &meta;
    writer_opts.meta->set_column_id(0);
    writer_opts.meta->set_unique_id(0);
    writer_opts.meta->set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_ARRAY));
    writer_opts.meta->set_length(0);
    writer_opts.meta->set_encoding(BIT_SHUFFLE);
    writer_opts.meta->set_compression(segment_v2::CompressionTypePB::LZ4F);
    writer_opts.meta->set_is_nullable(true);

    ColumnMetaPB* child_meta = meta.add_children_columns();
    child_meta->set_column_id(1);
    child_meta->set_unique_id(1);
    child_meta->set_type(static_cast<int32_t>(FieldType::OLAP_FIELD_TYPE_TINYINT));
    child_meta->set_length(0);
    child_meta->set_encoding(BIT_SHUFFLE);
    child_meta->set_compression(segment_v2::CompressionTypePB::LZ4F);
    child_meta->set_is_nullable(true);

    std::unique_ptr<ColumnWriter> writer;
    st = ColumnWriter::create(writer_opts, &list_column, file_writer.get(), &writer);
    ASSERT_TRUE(st.ok()) << st;
    st = writer->init();
    ASSERT_TRUE(st.ok()) << st;

    st = writer->append_nulls(1);
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_TRUE(writer->finish().ok());
    ASSERT_TRUE(writer->write_data().ok());
    ASSERT_TRUE(writer->write_ordinal_index().ok());
    ASSERT_TRUE(file_writer->close().ok());
}

// Check that every row of `column` holds the default value.
template <FieldType type>
void check_default_rows(const IColumn& column, size_t num_rows, const string& value, void* result) {
    using Type = typename TypeTraits<type>::CppType;
    ASSERT_EQ(num_rows, column.size());
    for (size_t j = 0; j < num_rows; ++j) {
        if constexpr (type == FieldType::OLAP_FIELD_TYPE_CHAR) {
            EXPECT_EQ(*(string*)result, column.get_data_at(j).to_string()) << "j:" << j;
        } else if constexpr (type == FieldType::OLAP_FIELD_TYPE_VARCHAR) {
            EXPECT_EQ(value, column.get_data_at(j).to_string()) << "j:" << j;
        } else if constexpr (type == FieldType::OLAP_FIELD_TYPE_HLL) {
            // The default value is a serialized HLL, so serialize it back to compare.
            const auto& hll = assert_cast<const ColumnHLL&>(column).get_element(j);
            std::string bytes(hll.max_serialized_size(), '\0');
            bytes.resize(hll.serialize(reinterpret_cast<uint8_t*>(bytes.data())));
            EXPECT_EQ(value, bytes) << "j:" << j;
        } else {
            EXPECT_EQ(*(Type*)result, storage_value<type>(column, j)) << "j:" << j;
        }
    }
}

template <FieldType type>
void test_read_default_value(string value, void* result) {
    // read and check
    {
        auto tablet_column = create_with_default_value<type>(value);
        DefaultValueColumnIterator iter(
                tablet_column->has_default_value(), tablet_column->default_value(),
                tablet_column->is_nullable(), type, tablet_column->precision(),
                tablet_column->frac(), tablet_column->length());
        ColumnIteratorOptions iter_opts;
        auto st = iter.init(iter_opts);
        EXPECT_TRUE(st.ok());
        // sequence read
        {
            MutableColumnPtr column = create_vectorized_column_ptr(type);
            size_t rows_read = 1024;
            bool has_null;
            st = iter.next_batch(&rows_read, column, &has_null);
            EXPECT_TRUE(st.ok());
            check_default_rows<type>(*column, rows_read, value, result);
        }

        {
            for (int rowid = 0; rowid < 2048; rowid += 128) {
                st = iter.seek_to_ordinal(rowid);
                EXPECT_TRUE(st.ok());

                MutableColumnPtr column = create_vectorized_column_ptr(type);
                size_t rows_read = 1024;
                bool has_null;
                st = iter.next_batch(&rows_read, column, &has_null);
                EXPECT_TRUE(st.ok());
                check_default_rows<type>(*column, rows_read, value, result);
            }
        }
    }
}

static MutableColumnPtr create_vectorized_column_ptr(FieldType type) {
    if (type == FieldType::OLAP_FIELD_TYPE_BOOL) {
        return DataTypeUInt8().create_column();
    } else if (type == FieldType::OLAP_FIELD_TYPE_TINYINT) {
        return DataTypeInt8().create_column();
    } else if (type == FieldType::OLAP_FIELD_TYPE_INT) {
        return DataTypeInt32().create_column();
    } else if (type == FieldType::OLAP_FIELD_TYPE_SMALLINT) {
        return DataTypeInt16().create_column();
    } else if (type == FieldType::OLAP_FIELD_TYPE_BIGINT) {
        return DataTypeInt64().create_column();
    } else if (type == FieldType::OLAP_FIELD_TYPE_LARGEINT) {
        return DataTypeInt128().create_column();
    } else if (type == FieldType::OLAP_FIELD_TYPE_FLOAT) {
        return DataTypeFloat32().create_column();
    } else if (type == FieldType::OLAP_FIELD_TYPE_DOUBLE) {
        return DataTypeFloat64().create_column();
    } else if (type == FieldType::OLAP_FIELD_TYPE_CHAR ||
               type == FieldType::OLAP_FIELD_TYPE_VARCHAR) {
        return DataTypeString().create_column();
    } else if (type == FieldType::OLAP_FIELD_TYPE_HLL) {
        return DataTypeHLL().create_column();
    } else if (type == FieldType::OLAP_FIELD_TYPE_DATE) {
        return DataTypeDate().create_column();
    } else if (type == FieldType::OLAP_FIELD_TYPE_DATETIME) {
        return DataTypeDateTime().create_column();
    } else if (type == FieldType::OLAP_FIELD_TYPE_DECIMAL) {
        return DataTypeDecimalV2(27, 9).create_column();
    }
    return DataTypeNothing().create_column();
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
        // DATETIME is checked when it is read back, so use real times here.
        datetime_vals[i] = 20191112000000 + (i / 3600 % 24) * 10000 + (i / 60 % 60) * 100 + i % 60;
        decimal_vals[i] = {i, i}; // 1.000000001

        set_column_value_by_type(FieldType::OLAP_FIELD_TYPE_VARCHAR, i, (char*)&varchar_vals[i],
                                 _pool);
        set_column_value_by_type(FieldType::OLAP_FIELD_TYPE_CHAR, i, (char*)&char_vals[i], _pool,
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

} // namespace segment_v2
} // namespace doris
