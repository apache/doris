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

#include "olap/rowset/segment_v2/zone_map_index.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <cstdint>
#include <limits>
#include <memory>
#include <string>

#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "olap/comparison_predicate.h"
#include "olap/decimal12.h"
#include "olap/field.h"
#include "olap/olap_common.h"
#include "olap/tablet_schema.h"
#include "olap/tablet_schema_helper.h"
#include "olap/uint24.h"
#include "runtime/decimalv2_value.h"
#include "runtime/define_primitive_type.h"
#include "runtime/type_limit.h"
#include "util/slice.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/functions/cast/cast_to_string.h"
#include "vec/runtime/timestamptz_value.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {
namespace segment_v2 {

class ColumnZoneMapTest : public testing::Test {
public:
    const std::string kTestDir = "./ut_dir/zone_map_index_test";

    void SetUp() override {
        _fs = io::global_local_filesystem();
        auto st = _fs->delete_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        st = _fs->create_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
    }
    void TearDown() override { EXPECT_TRUE(_fs->delete_directory(kTestDir).ok()); }

    void test_string(std::string testname, Field* field, vectorized::DataTypePtr data_type_ptr) {
        std::string filename = kTestDir + "/" + testname;
        auto fs = io::global_local_filesystem();

        std::unique_ptr<ZoneMapIndexWriter> builder(nullptr);
        static_cast<void>(ZoneMapIndexWriter::create(data_type_ptr, field, builder));
        std::vector<std::string> values1 = {"aaaa", "bbbb", "cccc", "dddd", "eeee", "ffff"};
        for (auto& value : values1) {
            Slice slice(value);
            builder->add_values((const uint8_t*)&slice, 1);
        }
        static_cast<void>(builder->flush());
        std::vector<std::string> values2 = {"aaaaa", "bbbbb", "ccccc", "ddddd", "eeeee", "fffff"};
        for (auto& value : values2) {
            Slice slice(value);
            builder->add_values((const uint8_t*)&slice, 1);
        }
        builder->add_nulls(1);
        static_cast<void>(builder->flush());
        for (int i = 0; i < 6; ++i) {
            builder->add_nulls(1);
        }
        static_cast<void>(builder->flush());
        // write out zone map index
        ColumnIndexMetaPB index_meta;
        {
            io::FileWriterPtr file_writer;
            EXPECT_TRUE(fs->create_file(filename, &file_writer).ok());
            EXPECT_TRUE(builder->finish(file_writer.get(), &index_meta).ok());
            EXPECT_EQ(ZONE_MAP_INDEX, index_meta.type());
            EXPECT_TRUE(file_writer->close().ok());
        }

        io::FileReaderSPtr file_reader;
        EXPECT_TRUE(fs->open_file(filename, &file_reader).ok());
        ZoneMapIndexReader column_zone_map(file_reader,
                                           index_meta.zone_map_index().page_zone_maps());
        Status status = column_zone_map.load(true, false);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(3, column_zone_map.num_pages());
        const std::vector<ZoneMapPB>& zone_maps = column_zone_map.page_zone_maps();
        EXPECT_EQ(3, zone_maps.size());
        EXPECT_EQ("aaaa", zone_maps[0].min());
        EXPECT_EQ("ffff", zone_maps[0].max());
        EXPECT_EQ(false, zone_maps[0].has_null());
        EXPECT_EQ(true, zone_maps[0].has_not_null());

        EXPECT_EQ("aaaaa", zone_maps[1].min());
        EXPECT_EQ("fffff", zone_maps[1].max());
        EXPECT_EQ(true, zone_maps[1].has_null());
        EXPECT_EQ(true, zone_maps[1].has_not_null());

        EXPECT_EQ(true, zone_maps[2].has_null());
        EXPECT_EQ(false, zone_maps[2].has_not_null());
    }

    template <PrimitiveType PType>
    void test_string_cut(bool pass_all, int32_t length) {
        static_assert(PType == TYPE_CHAR || PType == TYPE_VARCHAR || PType == TYPE_STRING,
                      "only varchar is supported");
        auto data_type =
                vectorized::DataTypeFactory::instance().create_data_type(PType, true, 0, 0, length);
        TabletColumnPtr tab_col;
        if constexpr (PType == TYPE_CHAR) {
            tab_col = create_char_key(0, true, length);
        } else if constexpr (PType == TYPE_VARCHAR) {
            tab_col = create_varchar_key(0, true, length);
        } else {
            tab_col = create_string_key(0);
        }
        auto field = std::unique_ptr<Field>(FieldFactory::create(*tab_col));

        std::unique_ptr<ZoneMapIndexWriter> writer;
        ASSERT_TRUE(ZoneMapIndexWriter::create(data_type, field.get(), writer).ok());

        // Create a string longer than MAX_ZONE_MAP_INDEX_SIZE (512)
        std::string short_string = "mmmm";

        std::string small_long_string1(MAX_ZONE_MAP_INDEX_SIZE + 100, 'a');
        std::string small_long_string1_expect =
                small_long_string1.substr(0, MAX_ZONE_MAP_INDEX_SIZE);

        std::string big_long_string1(MAX_ZONE_MAP_INDEX_SIZE + 100, 'x');
        std::string big_long_string1_expect = big_long_string1.substr(0, MAX_ZONE_MAP_INDEX_SIZE);
        big_long_string1_expect[MAX_ZONE_MAP_INDEX_SIZE - 1] =
                'y'; // last byte should be incremented

        {
            Slice slices[] = {Slice(short_string), Slice(big_long_string1),
                              Slice(small_long_string1)};
            writer->add_values(&slices, 3);
            if (pass_all) {
                writer->invalid_page_zone_map();
            }
            ASSERT_TRUE(writer->flush().ok());
        }

        std::string big_long_string2(MAX_ZONE_MAP_INDEX_SIZE + 100, 'y');
        std::string big_long_string2_expect = big_long_string2.substr(0, MAX_ZONE_MAP_INDEX_SIZE);
        big_long_string2_expect[MAX_ZONE_MAP_INDEX_SIZE - 1] =
                'z'; // last byte should be incremented
        {
            Slice slices[] = {Slice(short_string), Slice(big_long_string2),
                              Slice(small_long_string1)};
            writer->add_values(&slices, 3);
            if (pass_all) {
                writer->invalid_page_zone_map();
            }
            ASSERT_TRUE(writer->flush().ok());
        }

        std::string file_path = kTestDir + "/varchar_zone_map_truncate";
        io::FileWriterPtr file_writer;
        ASSERT_TRUE(_fs->create_file(file_path, &file_writer).ok());

        ColumnIndexMetaPB index_meta;
        ASSERT_TRUE(writer->finish(file_writer.get(), &index_meta).ok());
        ASSERT_TRUE(file_writer->close().ok());

        const auto& seg_zm = index_meta.zone_map_index().segment_zone_map();
        // Min/Max should be truncated to MAX_ZONE_MAP_INDEX_SIZE and last byte of Max is incremented
        EXPECT_EQ(seg_zm.min().size(), MAX_ZONE_MAP_INDEX_SIZE);
        EXPECT_EQ(seg_zm.min(), small_long_string1_expect);
        EXPECT_EQ(seg_zm.max().size(), MAX_ZONE_MAP_INDEX_SIZE);
        EXPECT_EQ(seg_zm.max(), big_long_string2_expect);

        // Verify ZoneMap::from_proto can correctly parse the truncated zone map
        ZoneMap seg_zone_map;
        ASSERT_TRUE(ZoneMap::from_proto(seg_zm, data_type, seg_zone_map).ok());
        EXPECT_EQ(seg_zone_map.has_null, false);
        EXPECT_EQ(seg_zone_map.has_not_null, true);
        EXPECT_EQ(seg_zone_map.pass_all, false);
        EXPECT_EQ(seg_zone_map.has_positive_inf, false);
        EXPECT_EQ(seg_zone_map.has_negative_inf, false);
        EXPECT_EQ(seg_zone_map.has_nan, false);
        EXPECT_EQ(seg_zone_map.min_value.get<PType>().size(), MAX_ZONE_MAP_INDEX_SIZE);
        EXPECT_EQ(seg_zone_map.min_value.get<PType>(), small_long_string1_expect);
        EXPECT_EQ(seg_zone_map.max_value.get<PType>().size(), MAX_ZONE_MAP_INDEX_SIZE);
        EXPECT_EQ(seg_zone_map.max_value.get<PType>(), big_long_string2_expect);

        io::FileReaderSPtr file_reader;
        EXPECT_TRUE(_fs->open_file(file_path, &file_reader).ok());

        ZoneMapIndexReader column_zone_map(file_reader,
                                           index_meta.zone_map_index().page_zone_maps());
        Status status = column_zone_map.load(true, false);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(2, column_zone_map.num_pages());
        const std::vector<ZoneMapPB>& zone_maps = column_zone_map.page_zone_maps();
        EXPECT_EQ(2, zone_maps.size());

        {
            ZoneMap page_zone_map;
            ASSERT_TRUE(ZoneMap::from_proto(zone_maps[0], data_type, page_zone_map).ok());
            EXPECT_EQ(page_zone_map.has_null, false);
            EXPECT_EQ(page_zone_map.has_not_null, true);
            EXPECT_EQ(page_zone_map.pass_all, pass_all);
            EXPECT_EQ(page_zone_map.has_positive_inf, false);
            EXPECT_EQ(page_zone_map.has_negative_inf, false);
            EXPECT_EQ(page_zone_map.has_nan, false);
            if (!pass_all) {
                EXPECT_EQ(page_zone_map.min_value.get<PType>().size(), MAX_ZONE_MAP_INDEX_SIZE);
                EXPECT_EQ(page_zone_map.min_value.get<PType>(), small_long_string1_expect);
                EXPECT_EQ(page_zone_map.max_value.get<PType>().size(), MAX_ZONE_MAP_INDEX_SIZE);
                EXPECT_EQ(page_zone_map.max_value.get<PType>(), big_long_string1_expect);
            }
        }
        {
            ZoneMap page_zone_map;
            ASSERT_TRUE(ZoneMap::from_proto(zone_maps[1], data_type, page_zone_map).ok());
            EXPECT_EQ(page_zone_map.has_null, false);
            EXPECT_EQ(page_zone_map.has_not_null, true);
            EXPECT_EQ(page_zone_map.pass_all, pass_all);
            EXPECT_EQ(page_zone_map.has_positive_inf, false);
            EXPECT_EQ(page_zone_map.has_negative_inf, false);
            EXPECT_EQ(page_zone_map.has_nan, false);
            if (!pass_all) {
                EXPECT_EQ(page_zone_map.min_value.get<PType>().size(), MAX_ZONE_MAP_INDEX_SIZE);
                EXPECT_EQ(page_zone_map.min_value.get<PType>(), small_long_string1_expect);
                EXPECT_EQ(page_zone_map.max_value.get<PType>().size(), MAX_ZONE_MAP_INDEX_SIZE);
                EXPECT_EQ(page_zone_map.max_value.get<PType>(), big_long_string2_expect);
            }
        }
    }

    void test_char_padding(bool pass_all) {
        int32_t length = 10;
        auto data_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_CHAR, true,
                                                                                  0, 0, length);
        auto tab_col = create_char_key(0, true, length);
        auto field = std::unique_ptr<Field>(FieldFactory::create(*tab_col));
        std::string s_less_than_schema_length1(length - 1, 'a');
        std::string s_less_than_schema_length1_expect(length, 'a');
        s_less_than_schema_length1_expect[length - 1] = '\0';
        std::string s_less_than_schema_length2(length - 2, 'b');
        std::string s_less_than_schema_length2_expect(length, 'b');
        s_less_than_schema_length2_expect[length - 1] = '\0';
        s_less_than_schema_length2_expect[length - 2] = '\0';
        std::unique_ptr<ZoneMapIndexWriter> writer;
        ASSERT_TRUE(ZoneMapIndexWriter::create(data_type, field.get(), writer).ok());
        Slice slices[] = {Slice(s_less_than_schema_length1), Slice(s_less_than_schema_length2)};
        writer->add_values(&slices, 2);
        if (pass_all) {
            writer->invalid_page_zone_map();
        }
        ASSERT_TRUE(writer->flush().ok());

        std::string file_path = kTestDir + "/char_zone_map_padding";
        io::FileWriterPtr file_writer;
        ASSERT_TRUE(_fs->create_file(file_path, &file_writer).ok());

        ColumnIndexMetaPB index_meta;
        ASSERT_TRUE(writer->finish(file_writer.get(), &index_meta).ok());
        ASSERT_TRUE(file_writer->close().ok());

        const auto& seg_zm = index_meta.zone_map_index().segment_zone_map();
        // Min/Max should be truncated to MAX_ZONE_MAP_INDEX_SIZE and last byte of Max is incremented
        EXPECT_EQ(seg_zm.min().size(), s_less_than_schema_length1.size());
        EXPECT_EQ(seg_zm.min(), s_less_than_schema_length1);
        EXPECT_EQ(seg_zm.max().size(), s_less_than_schema_length2.size());
        EXPECT_EQ(seg_zm.max(), s_less_than_schema_length2);

        // Verify ZoneMap::from_proto can correctly parse the truncated zone map
        ZoneMap seg_zone_map;
        ASSERT_TRUE(ZoneMap::from_proto(seg_zm, data_type, seg_zone_map).ok());
        EXPECT_EQ(seg_zone_map.has_null, false);
        EXPECT_EQ(seg_zone_map.has_not_null, true);
        EXPECT_EQ(seg_zone_map.pass_all, false);
        EXPECT_EQ(seg_zone_map.has_positive_inf, false);
        EXPECT_EQ(seg_zone_map.has_negative_inf, false);
        EXPECT_EQ(seg_zone_map.has_nan, false);
        EXPECT_EQ(seg_zone_map.min_value.get<TYPE_CHAR>().size(), length);
        EXPECT_EQ(seg_zone_map.min_value.get<TYPE_CHAR>(), s_less_than_schema_length1_expect);
        EXPECT_EQ(seg_zone_map.max_value.get<TYPE_CHAR>().size(), length);
        EXPECT_EQ(seg_zone_map.max_value.get<TYPE_CHAR>(), s_less_than_schema_length2_expect);

        io::FileReaderSPtr file_reader;
        EXPECT_TRUE(_fs->open_file(file_path, &file_reader).ok());

        ZoneMapIndexReader column_zone_map(file_reader,
                                           index_meta.zone_map_index().page_zone_maps());
        Status status = column_zone_map.load(true, false);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(1, column_zone_map.num_pages());
        const std::vector<ZoneMapPB>& zone_maps = column_zone_map.page_zone_maps();
        EXPECT_EQ(1, zone_maps.size());

        {
            ZoneMap page_zone_map;
            ASSERT_TRUE(ZoneMap::from_proto(zone_maps[0], data_type, page_zone_map).ok());
            EXPECT_EQ(page_zone_map.has_null, false);
            EXPECT_EQ(page_zone_map.has_not_null, true);
            EXPECT_EQ(page_zone_map.pass_all, pass_all);
            EXPECT_EQ(page_zone_map.has_positive_inf, false);
            EXPECT_EQ(page_zone_map.has_negative_inf, false);
            EXPECT_EQ(page_zone_map.has_nan, false);
            if (!pass_all) {
                EXPECT_EQ(page_zone_map.min_value.get<TYPE_CHAR>().size(), length);
                EXPECT_EQ(page_zone_map.min_value.get<TYPE_CHAR>(),
                          s_less_than_schema_length1_expect);
                EXPECT_EQ(page_zone_map.max_value.get<TYPE_CHAR>().size(), length);
                EXPECT_EQ(page_zone_map.max_value.get<TYPE_CHAR>(),
                          s_less_than_schema_length2_expect);
            }
        }
    }
    TabletColumnPtr create_decimalv2_key(int32_t id, bool is_nullable) {
        auto column = std::make_shared<TabletColumn>();
        column->_unique_id = id;
        column->_col_name = std::to_string(id);
        column->_type = FieldType::OLAP_FIELD_TYPE_DECIMAL;
        column->_is_key = true;
        column->_is_nullable = is_nullable;
        column->_length = 8;
        column->_index_length = 4;
        return column;
    }
    void test_decimalv2(bool pass_all) {
        int precision = 18;
        int scale = 7;
        auto data_type = vectorized::DataTypeFactory::instance().create_data_type(
                TYPE_DECIMALV2, true, precision, scale);
        TabletColumnPtr tab_col;
        tab_col = create_decimalv2_key(0, true);
        auto field = std::unique_ptr<Field>(FieldFactory::create(*tab_col));

        std::unique_ptr<ZoneMapIndexWriter> writer;
        ASSERT_TRUE(ZoneMapIndexWriter::create(data_type, field.get(), writer).ok());

        decimal12_t decimal1 {.integer = 123, .fraction = 456};
        decimal12_t decimal2 {.integer = 223, .fraction = 4567};
        decimal12_t decimal3 {.integer = 323, .fraction = 45678};

        {
            decimal12_t values[] = {decimal1, decimal2, decimal3};
            writer->add_values(&values, 3);
            if (pass_all) {
                writer->invalid_page_zone_map();
            }
            ASSERT_TRUE(writer->flush().ok());
        }

        std::string file_path = kTestDir + "/decimalv2_zonemap";
        io::FileWriterPtr file_writer;
        ASSERT_TRUE(_fs->create_file(file_path, &file_writer).ok());

        ColumnIndexMetaPB index_meta;
        ASSERT_TRUE(writer->finish(file_writer.get(), &index_meta).ok());
        ASSERT_TRUE(file_writer->close().ok());

        const auto& seg_zm = index_meta.zone_map_index().segment_zone_map();
        EXPECT_EQ(seg_zm.min(), DecimalV2Value(decimal1.integer, decimal1.fraction));
        EXPECT_EQ(seg_zm.max(), DecimalV2Value(decimal3.integer, decimal3.fraction));

        // Verify ZoneMap::from_proto can correctly parse the truncated zone map
        ZoneMap seg_zone_map;
        ASSERT_TRUE(ZoneMap::from_proto(seg_zm, data_type, seg_zone_map).ok());
        EXPECT_EQ(seg_zone_map.has_null, false);
        EXPECT_EQ(seg_zone_map.has_not_null, true);
        EXPECT_EQ(seg_zone_map.pass_all, false);
        EXPECT_EQ(seg_zone_map.has_positive_inf, false);
        EXPECT_EQ(seg_zone_map.has_negative_inf, false);
        EXPECT_EQ(seg_zone_map.has_nan, false);
        EXPECT_EQ(seg_zone_map.min_value.get<TYPE_DECIMALV2>(),
                  DecimalV2Value(decimal1.integer, decimal1.fraction));
        EXPECT_EQ(seg_zone_map.max_value.get<TYPE_DECIMALV2>(),
                  DecimalV2Value(decimal3.integer, decimal3.fraction));

        io::FileReaderSPtr file_reader;
        EXPECT_TRUE(_fs->open_file(file_path, &file_reader).ok());

        ZoneMapIndexReader column_zone_map(file_reader,
                                           index_meta.zone_map_index().page_zone_maps());
        Status status = column_zone_map.load(true, false);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(1, column_zone_map.num_pages());
        const std::vector<ZoneMapPB>& zone_maps = column_zone_map.page_zone_maps();
        EXPECT_EQ(1, zone_maps.size());

        {
            ZoneMap page_zone_map;
            ASSERT_TRUE(ZoneMap::from_proto(zone_maps[0], data_type, page_zone_map).ok());
            EXPECT_EQ(page_zone_map.has_null, false);
            EXPECT_EQ(page_zone_map.has_not_null, true);
            EXPECT_EQ(page_zone_map.pass_all, pass_all);
            EXPECT_EQ(page_zone_map.has_positive_inf, false);
            EXPECT_EQ(page_zone_map.has_negative_inf, false);
            EXPECT_EQ(page_zone_map.has_nan, false);
            if (!pass_all) {
                EXPECT_EQ(page_zone_map.min_value.get<TYPE_DECIMALV2>(),
                          DecimalV2Value(decimal1.integer, decimal1.fraction));
                EXPECT_EQ(page_zone_map.max_value.get<TYPE_DECIMALV2>(),
                          DecimalV2Value(decimal3.integer, decimal3.fraction));
            }
        }
    }

    TabletColumnPtr create_datev1_key(int32_t id, bool is_nullable) {
        auto column = std::make_shared<TabletColumn>();
        column->_unique_id = id;
        column->_col_name = std::to_string(id);
        column->_type = FieldType::OLAP_FIELD_TYPE_DATE;
        column->_is_key = true;
        column->_is_nullable = is_nullable;
        column->_length = 4;
        column->_index_length = 4;
        return column;
    }
    void test_datev1(bool pass_all) {
        auto data_type = vectorized::DataTypeFactory::instance().create_data_type(TYPE_DATE, true);
        TabletColumnPtr tab_col;
        tab_col = create_datev1_key(0, true);
        auto field = std::unique_ptr<Field>(FieldFactory::create(*tab_col));

        std::unique_ptr<ZoneMapIndexWriter> writer;
        ASSERT_TRUE(ZoneMapIndexWriter::create(data_type, field.get(), writer).ok());

        VecDateTimeValue value1(false, TIME_DATE, 0, 0, 0, 2026, 2, 1);
        VecDateTimeValue value2(false, TIME_DATE, 0, 0, 0, 2026, 2, 2);
        VecDateTimeValue value3(false, TIME_DATE, 0, 0, 0, 2026, 2, 28);
        uint24_t values[] = {value1.to_olap_date(), value2.to_olap_date(), value3.to_olap_date()};
        {
            writer->add_values(&values, 3);
            if (pass_all) {
                writer->invalid_page_zone_map();
            }
            ASSERT_TRUE(writer->flush().ok());
        }

        std::string file_path = kTestDir + "/datev1_zonemap";
        io::FileWriterPtr file_writer;
        ASSERT_TRUE(_fs->create_file(file_path, &file_writer).ok());

        ColumnIndexMetaPB index_meta;
        ASSERT_TRUE(writer->finish(file_writer.get(), &index_meta).ok());
        ASSERT_TRUE(file_writer->close().ok());

        const auto& seg_zm = index_meta.zone_map_index().segment_zone_map();
        // EXPECT_EQ(seg_zm.min(), value1);
        // EXPECT_EQ(seg_zm.max(), value3);

        ZoneMap seg_zone_map;
        ASSERT_TRUE(ZoneMap::from_proto(seg_zm, data_type, seg_zone_map).ok());
        EXPECT_EQ(seg_zone_map.has_null, false);
        EXPECT_EQ(seg_zone_map.has_not_null, true);
        EXPECT_EQ(seg_zone_map.pass_all, false);
        EXPECT_EQ(seg_zone_map.has_positive_inf, false);
        EXPECT_EQ(seg_zone_map.has_negative_inf, false);
        EXPECT_EQ(seg_zone_map.has_nan, false);
        EXPECT_EQ(seg_zone_map.min_value.get<TYPE_DATE>(), value1);
        EXPECT_EQ(seg_zone_map.max_value.get<TYPE_DATE>(), value3);

        io::FileReaderSPtr file_reader;
        EXPECT_TRUE(_fs->open_file(file_path, &file_reader).ok());

        ZoneMapIndexReader column_zone_map(file_reader,
                                           index_meta.zone_map_index().page_zone_maps());
        Status status = column_zone_map.load(true, false);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(1, column_zone_map.num_pages());
        const std::vector<ZoneMapPB>& zone_maps = column_zone_map.page_zone_maps();
        EXPECT_EQ(1, zone_maps.size());

        {
            ZoneMap page_zone_map;
            ASSERT_TRUE(ZoneMap::from_proto(zone_maps[0], data_type, page_zone_map).ok());
            EXPECT_EQ(page_zone_map.has_null, false);
            EXPECT_EQ(page_zone_map.has_not_null, true);
            EXPECT_EQ(page_zone_map.pass_all, pass_all);
            EXPECT_EQ(page_zone_map.has_positive_inf, false);
            EXPECT_EQ(page_zone_map.has_negative_inf, false);
            EXPECT_EQ(page_zone_map.has_nan, false);
            if (!pass_all) {
                EXPECT_EQ(page_zone_map.min_value.get<TYPE_DATE>(), value1);
                EXPECT_EQ(page_zone_map.max_value.get<TYPE_DATE>(), value3);
            }
        }
    }
    TabletColumnPtr create_datetimev1_key(int32_t id, bool is_nullable) {
        auto column = std::make_shared<TabletColumn>();
        column->_unique_id = id;
        column->_col_name = std::to_string(id);
        column->_type = FieldType::OLAP_FIELD_TYPE_DATETIME;
        column->_is_key = true;
        column->_is_nullable = is_nullable;
        column->_length = 8;
        column->_index_length = 4;
        return column;
    }
    void test_datetimev1(bool pass_all) {
        auto data_type =
                vectorized::DataTypeFactory::instance().create_data_type(TYPE_DATETIME, true);
        TabletColumnPtr tab_col;
        tab_col = create_datetimev1_key(0, true);
        auto field = std::unique_ptr<Field>(FieldFactory::create(*tab_col));

        std::unique_ptr<ZoneMapIndexWriter> writer;
        ASSERT_TRUE(ZoneMapIndexWriter::create(data_type, field.get(), writer).ok());

        VecDateTimeValue value1(false, TIME_DATETIME, 18, 12, 10, 2026, 2, 1);
        VecDateTimeValue value2(false, TIME_DATETIME, 18, 13, 0, 2026, 2, 2);
        VecDateTimeValue value3(false, TIME_DATETIME, 18, 20, 0, 2026, 2, 28);
        uint64_t values[] = {value1.to_olap_datetime(), value2.to_olap_datetime(),
                             value3.to_olap_datetime()};
        {
            writer->add_values(&values, 3);
            if (pass_all) {
                writer->invalid_page_zone_map();
            }
            ASSERT_TRUE(writer->flush().ok());
        }

        std::string file_path = kTestDir + "/datetimev1_zonemap";
        io::FileWriterPtr file_writer;
        ASSERT_TRUE(_fs->create_file(file_path, &file_writer).ok());

        ColumnIndexMetaPB index_meta;
        ASSERT_TRUE(writer->finish(file_writer.get(), &index_meta).ok());
        ASSERT_TRUE(file_writer->close().ok());

        const auto& seg_zm = index_meta.zone_map_index().segment_zone_map();

        ZoneMap seg_zone_map;
        ASSERT_TRUE(ZoneMap::from_proto(seg_zm, data_type, seg_zone_map).ok());
        EXPECT_EQ(seg_zone_map.has_null, false);
        EXPECT_EQ(seg_zone_map.has_not_null, true);
        EXPECT_EQ(seg_zone_map.pass_all, false);
        EXPECT_EQ(seg_zone_map.has_positive_inf, false);
        EXPECT_EQ(seg_zone_map.has_negative_inf, false);
        EXPECT_EQ(seg_zone_map.has_nan, false);
        EXPECT_EQ(seg_zone_map.min_value.get<TYPE_DATETIME>(), value1);
        EXPECT_EQ(seg_zone_map.max_value.get<TYPE_DATETIME>(), value3);

        io::FileReaderSPtr file_reader;
        EXPECT_TRUE(_fs->open_file(file_path, &file_reader).ok());

        ZoneMapIndexReader column_zone_map(file_reader,
                                           index_meta.zone_map_index().page_zone_maps());
        Status status = column_zone_map.load(true, false);
        EXPECT_TRUE(status.ok());
        EXPECT_EQ(1, column_zone_map.num_pages());
        const std::vector<ZoneMapPB>& zone_maps = column_zone_map.page_zone_maps();
        EXPECT_EQ(1, zone_maps.size());

        {
            ZoneMap page_zone_map;
            ASSERT_TRUE(ZoneMap::from_proto(zone_maps[0], data_type, page_zone_map).ok());
            EXPECT_EQ(page_zone_map.has_null, false);
            EXPECT_EQ(page_zone_map.has_not_null, true);
            EXPECT_EQ(page_zone_map.pass_all, pass_all);
            EXPECT_EQ(page_zone_map.has_positive_inf, false);
            EXPECT_EQ(page_zone_map.has_negative_inf, false);
            EXPECT_EQ(page_zone_map.has_nan, false);
            if (!pass_all) {
                EXPECT_EQ(page_zone_map.min_value.get<TYPE_DATETIME>(), value1);
                EXPECT_EQ(page_zone_map.max_value.get<TYPE_DATETIME>(), value3);
            }
        }
    }
    io::FileSystemSPtr _fs;
};

// Test for int
TEST_F(ColumnZoneMapTest, NormalTestIntPage) {
    std::string filename = kTestDir + "/NormalTestIntPage";
    auto fs = io::global_local_filesystem();

    TabletColumnPtr int_column = create_int_key(0);
    Field* field = FieldFactory::create(*int_column);
    auto data_type_ptr = vectorized::DataTypeFactory::instance().create_data_type(TYPE_INT, false);

    std::unique_ptr<ZoneMapIndexWriter> builder(nullptr);
    static_cast<void>(ZoneMapIndexWriter::create(data_type_ptr, field, builder));
    std::vector<int> values1 = {1, 10, 11, 20, 21, 22};
    for (auto value : values1) {
        builder->add_values((const uint8_t*)&value, 1);
    }
    static_cast<void>(builder->flush());
    std::vector<int> values2 = {2, 12, 31, 23, 21, 22};
    for (auto value : values2) {
        builder->add_values((const uint8_t*)&value, 1);
    }
    builder->add_nulls(1);
    static_cast<void>(builder->flush());
    builder->add_nulls(6);
    static_cast<void>(builder->flush());
    // write out zone map index
    ColumnIndexMetaPB index_meta;
    {
        io::FileWriterPtr file_writer;
        EXPECT_TRUE(fs->create_file(filename, &file_writer).ok());
        EXPECT_TRUE(builder->finish(file_writer.get(), &index_meta).ok());
        EXPECT_EQ(ZONE_MAP_INDEX, index_meta.type());
        EXPECT_TRUE(file_writer->close().ok());
    }

    io::FileReaderSPtr file_reader;
    EXPECT_TRUE(fs->open_file(filename, &file_reader).ok());
    ZoneMapIndexReader column_zone_map(file_reader, index_meta.zone_map_index().page_zone_maps());
    Status status = column_zone_map.load(true, false);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(3, column_zone_map.num_pages());
    const std::vector<ZoneMapPB>& zone_maps = column_zone_map.page_zone_maps();
    EXPECT_EQ(3, zone_maps.size());

    EXPECT_EQ(std::to_string(1), zone_maps[0].min());
    EXPECT_EQ(std::to_string(22), zone_maps[0].max());
    EXPECT_EQ(false, zone_maps[0].has_null());
    EXPECT_EQ(true, zone_maps[0].has_not_null());

    EXPECT_EQ(std::to_string(2), zone_maps[1].min());
    EXPECT_EQ(std::to_string(31), zone_maps[1].max());
    EXPECT_EQ(true, zone_maps[1].has_null());
    EXPECT_EQ(true, zone_maps[1].has_not_null());

    EXPECT_EQ(true, zone_maps[2].has_null());
    EXPECT_EQ(false, zone_maps[2].has_not_null());
    delete field;
}

// Test for string
TEST_F(ColumnZoneMapTest, NormalTestVarcharPage) {
    TabletColumnPtr varchar_column = create_varchar_key(0);
    Field* field = FieldFactory::create(*varchar_column);
    auto str_data_type_ptr =
            vectorized::DataTypeFactory::instance().create_data_type(TYPE_VARCHAR, false);
    test_string("NormalTestVarcharPage", field, str_data_type_ptr);
    delete field;
}

// Test for string
TEST_F(ColumnZoneMapTest, NormalTestCharPage) {
    TabletColumnPtr char_column = create_char_key(0);
    Field* field = FieldFactory::create(*char_column);
    auto char_data_type_ptr =
            vectorized::DataTypeFactory::instance().create_data_type(TYPE_CHAR, false);
    test_string("NormalTestCharPage", field, char_data_type_ptr);
    delete field;
}

// Test for zone map limit
TEST_F(ColumnZoneMapTest, ZoneMapCut) {
    TabletColumnPtr varchar_column = create_varchar_key(0);
    varchar_column->set_index_length(1024);
    Field* field = FieldFactory::create(*varchar_column);
    auto data_type_ptr =
            vectorized::DataTypeFactory::instance().create_data_type(TYPE_VARCHAR, false);
    test_string("ZoneMapCut", field, data_type_ptr);
    delete field;
}

TEST_F(ColumnZoneMapTest, StringColumnTruncation) {
    test_string_cut<TYPE_CHAR>(false, 200);
    test_string_cut<TYPE_CHAR>(true, 200);

    test_string_cut<TYPE_VARCHAR>(false, -1);
    test_string_cut<TYPE_VARCHAR>(true, -1);

    test_string_cut<TYPE_STRING>(false, -1);
    test_string_cut<TYPE_STRING>(true, -1);
}

TEST_F(ColumnZoneMapTest, CharColumnPadding) {
    test_char_padding(false);
    test_char_padding(true);
}

TEST_F(ColumnZoneMapTest, DecimalV2) {
    test_decimalv2(false);
    test_decimalv2(true);
}

TEST_F(ColumnZoneMapTest, DateV1) {
    test_datev1(false);
    test_datev1(true);
}

TEST_F(ColumnZoneMapTest, DateTimeV1) {
    test_datetimev1(false);
    test_datetimev1(true);
}

// Test for float/double
template <FieldType T>
TabletColumnPtr create_float_column(int32_t id, bool is_nullable) {
    auto column = std::make_shared<TabletColumn>();
    column->_unique_id = id;
    column->_col_name = std::to_string(id);
    column->_type = T;
    column->_is_key = false;
    column->_is_nullable = is_nullable;
    column->_length = (T == FieldType::OLAP_FIELD_TYPE_DOUBLE ? 8 : 4);
    column->_index_length = (T == FieldType::OLAP_FIELD_TYPE_DOUBLE ? 8 : 4);
    column->_is_bf_column = false;
    return column;
}
TEST_F(ColumnZoneMapTest, NormalTestFloatPage) {
    std::string filename = kTestDir + "/NormalTestFloatPage";
    auto fs = io::global_local_filesystem();

    auto column = create_float_column<FieldType::OLAP_FIELD_TYPE_FLOAT>(0, true);
    Field* field = FieldFactory::create(*column);
    auto data_type_ptr =
            vectorized::DataTypeFactory::instance().create_data_type(TYPE_FLOAT, false);

    std::unique_ptr<ZoneMapIndexWriter> builder(nullptr);
    static_cast<void>(ZoneMapIndexWriter::create(data_type_ptr, field, builder));
    std::vector<float> values1 = {
            -std::numeric_limits<float>::infinity(),
            std::numeric_limits<float>::lowest(),
            std::numeric_limits<float>::max(),
            std::numeric_limits<float>::quiet_NaN(),
            std::numeric_limits<float>::infinity(),
            -1234.56F,
            -1.23456F,
            0,
            1.23456F,
            1234.56F,
    };
    // page 1
    for (auto value : values1) {
        builder->add_values((const uint8_t*)&value, 1);
    }
    static_cast<void>(builder->flush());

    // page 2
    std::vector<float> values2 = {
            -1234.56F, -1.23456F, 0, 1.23456F, 1234.56F,
    };
    for (auto value : values2) {
        builder->add_values((const uint8_t*)&value, 1);
    }
    builder->add_nulls(1);
    static_cast<void>(builder->flush());

    // page 3
    builder->add_nulls(6);
    static_cast<void>(builder->flush());

    // write out zone map index
    ColumnIndexMetaPB index_meta;
    {
        io::FileWriterPtr file_writer;
        EXPECT_TRUE(fs->create_file(filename, &file_writer).ok());
        EXPECT_TRUE(builder->finish(file_writer.get(), &index_meta).ok());
        EXPECT_EQ(ZONE_MAP_INDEX, index_meta.type());
        EXPECT_TRUE(file_writer->close().ok());
    }

    io::FileReaderSPtr file_reader;
    EXPECT_TRUE(fs->open_file(filename, &file_reader).ok());

    auto segment_zone_map = index_meta.zone_map_index().segment_zone_map();
    EXPECT_EQ(vectorized::CastToString::from_number(std::numeric_limits<float>::lowest()),
              segment_zone_map.min());
    EXPECT_EQ(vectorized::CastToString::from_number(std::numeric_limits<float>::max()),
              segment_zone_map.max());
    EXPECT_EQ(true, segment_zone_map.has_null());
    EXPECT_EQ(true, segment_zone_map.has_not_null());
    EXPECT_EQ(true, segment_zone_map.has_positive_inf());
    EXPECT_EQ(true, segment_zone_map.has_negative_inf());
    EXPECT_EQ(true, segment_zone_map.has_nan());

    ZoneMapIndexReader column_zone_map(file_reader, index_meta.zone_map_index().page_zone_maps());
    Status status = column_zone_map.load(true, false);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(3, column_zone_map.num_pages());
    const std::vector<ZoneMapPB>& zone_maps = column_zone_map.page_zone_maps();
    EXPECT_EQ(3, zone_maps.size());

    EXPECT_EQ(vectorized::CastToString::from_number(std::numeric_limits<float>::lowest()),
              zone_maps[0].min());
    EXPECT_EQ(vectorized::CastToString::from_number(std::numeric_limits<float>::max()),
              zone_maps[0].max());
    EXPECT_EQ(false, zone_maps[0].has_null());
    EXPECT_EQ(true, zone_maps[0].has_not_null());
    EXPECT_EQ(true, zone_maps[0].has_positive_inf());
    EXPECT_EQ(true, zone_maps[0].has_negative_inf());
    EXPECT_EQ(true, zone_maps[0].has_nan());

    EXPECT_EQ(vectorized::CastToString::from_number(-1234.56F), zone_maps[1].min());
    EXPECT_EQ(vectorized::CastToString::from_number(1234.56F), zone_maps[1].max());
    EXPECT_EQ(true, zone_maps[1].has_null());
    EXPECT_EQ(true, zone_maps[1].has_not_null());

    EXPECT_EQ(true, zone_maps[2].has_null());
    EXPECT_EQ(false, zone_maps[2].has_not_null());
    delete field;
}

TEST_F(ColumnZoneMapTest, NormalTestDoublePage) {
    std::string filename = kTestDir + "/NormalTestDoublePage";
    auto fs = io::global_local_filesystem();

    auto column = create_float_column<FieldType::OLAP_FIELD_TYPE_DOUBLE>(0, true);
    Field* field = FieldFactory::create(*column);
    auto data_type_ptr =
            vectorized::DataTypeFactory::instance().create_data_type(TYPE_DOUBLE, false);

    std::unique_ptr<ZoneMapIndexWriter> builder(nullptr);
    static_cast<void>(ZoneMapIndexWriter::create(data_type_ptr, field, builder));
    std::vector<double> values1 = {
            -std::numeric_limits<double>::infinity(),
            std::numeric_limits<double>::lowest(),
            std::numeric_limits<double>::max(),
            std::numeric_limits<double>::quiet_NaN(),
            std::numeric_limits<double>::infinity(),
            -1234.56789012345,
            0,
            1234.56789012345,
    };
    // page 1
    for (auto value : values1) {
        builder->add_values((const uint8_t*)&value, 1);
    }
    static_cast<void>(builder->flush());

    // page 2
    std::vector<double> values2 = {
            -1234.56789012345,
            0,
            1234.56789012345,
    };
    for (auto value : values2) {
        builder->add_values((const uint8_t*)&value, 1);
    }
    builder->add_nulls(1);
    static_cast<void>(builder->flush());

    // page 3
    builder->add_nulls(6);
    static_cast<void>(builder->flush());

    // write out zone map index
    ColumnIndexMetaPB index_meta;
    {
        io::FileWriterPtr file_writer;
        EXPECT_TRUE(fs->create_file(filename, &file_writer).ok());
        EXPECT_TRUE(builder->finish(file_writer.get(), &index_meta).ok());
        EXPECT_EQ(ZONE_MAP_INDEX, index_meta.type());
        EXPECT_TRUE(file_writer->close().ok());
    }

    io::FileReaderSPtr file_reader;
    EXPECT_TRUE(fs->open_file(filename, &file_reader).ok());

    auto segment_zone_map = index_meta.zone_map_index().segment_zone_map();
    EXPECT_EQ(vectorized::CastToString::from_number(std::numeric_limits<double>::lowest()),
              segment_zone_map.min());
    EXPECT_EQ(vectorized::CastToString::from_number(std::numeric_limits<double>::max()),
              segment_zone_map.max());
    EXPECT_EQ(true, segment_zone_map.has_null());
    EXPECT_EQ(true, segment_zone_map.has_not_null());
    EXPECT_EQ(true, segment_zone_map.has_positive_inf());
    EXPECT_EQ(true, segment_zone_map.has_negative_inf());
    EXPECT_EQ(true, segment_zone_map.has_nan());

    ZoneMapIndexReader column_zone_map(file_reader, index_meta.zone_map_index().page_zone_maps());
    Status status = column_zone_map.load(true, false);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(3, column_zone_map.num_pages());
    const std::vector<ZoneMapPB>& zone_maps = column_zone_map.page_zone_maps();
    EXPECT_EQ(3, zone_maps.size());

    EXPECT_EQ(vectorized::CastToString::from_number(std::numeric_limits<double>::lowest()),
              zone_maps[0].min());
    EXPECT_EQ(vectorized::CastToString::from_number(std::numeric_limits<double>::max()),
              zone_maps[0].max());
    EXPECT_EQ(false, zone_maps[0].has_null());
    EXPECT_EQ(true, zone_maps[0].has_not_null());
    EXPECT_EQ(true, zone_maps[0].has_positive_inf());
    EXPECT_EQ(true, zone_maps[0].has_negative_inf());
    EXPECT_EQ(true, zone_maps[0].has_nan());

    EXPECT_EQ(vectorized::CastToString::from_number(-1234.56789012345), zone_maps[1].min());
    EXPECT_EQ(vectorized::CastToString::from_number(1234.56789012345), zone_maps[1].max());
    EXPECT_EQ(true, zone_maps[1].has_null());
    EXPECT_EQ(true, zone_maps[1].has_not_null());

    EXPECT_EQ(true, zone_maps[2].has_null());
    EXPECT_EQ(false, zone_maps[2].has_not_null());
    delete field;
}

TabletColumnPtr create_timestamptz_column(int32_t id, bool is_nullable) {
    auto column = std::make_shared<TabletColumn>();
    column->_unique_id = id;
    column->_col_name = std::to_string(id);
    column->_type = FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ;
    column->_is_key = false;
    column->_is_nullable = is_nullable;
    column->_length = 8;
    column->_index_length = 8;
    column->_is_bf_column = false;
    return column;
}

TEST_F(ColumnZoneMapTest, TimestamptzPage) {
    std::string filename = kTestDir + "/TimestamptzPage";
    auto fs = io::global_local_filesystem();

    auto column = create_timestamptz_column(0, true);
    Field* field = FieldFactory::create(*column);
    auto data_type_ptr =
            vectorized::DataTypeFactory::instance().create_data_type(TYPE_TIMESTAMPTZ, false);

    std::unique_ptr<ZoneMapIndexWriter> builder(nullptr);
    static_cast<void>(ZoneMapIndexWriter::create(data_type_ptr, field, builder));
    cctz::time_zone time_zone = cctz::fixed_time_zone(std::chrono::hours(0));
    TimezoneUtils::load_offsets_to_cache();
    vectorized::CastParameters params;
    params.is_strict = true;

    // page 1
    // with min and max value
    {
        std::vector<std::string> values = {"0000-01-01 00:00:00", "0000-01-01 00:00:00",
                                           "0000-01-01 03:00:00", "0000-01-01 03:00:00",
                                           "2023-01-01 15:00:00", "2023-01-01 15:00:00",
                                           "9999-12-31 23:59:59", "9999-12-31 23:59:59"};
        for (auto str : values) {
            TimestampTzValue tz {};
            EXPECT_TRUE(tz.from_string(StringRef {str}, &time_zone, params, 0));
            builder->add_values((const uint8_t*)&tz, 1);
        }
        static_cast<void>(builder->flush());
    }

    // page 2
    // with min value
    {
        std::vector<std::string> values = {"0000-01-01 00:00:00", "0000-01-01 00:00:00",
                                           "0000-01-01 03:00:00", "0000-01-01 03:00:00",
                                           "2023-01-01 15:00:00", "2023-01-01 15:00:00",
                                           "8999-12-31 23:59:59", "8999-12-31 23:59:59"};
        for (auto str : values) {
            TimestampTzValue tz {};
            EXPECT_TRUE(tz.from_string(StringRef {str}, &time_zone, params, 0));
            builder->add_values((const uint8_t*)&tz, 1);
        }
        builder->add_nulls(1);
        static_cast<void>(builder->flush());
    }
    // page 3
    // with max value
    {
        std::vector<std::string> values = {"0000-01-01 00:00:59", "0000-01-01 00:00:59",
                                           "0000-01-01 03:00:00", "0000-01-01 03:00:00",
                                           "2023-01-01 15:00:00", "2023-01-01 15:00:00",
                                           "9999-12-31 23:59:59", "9999-12-31 23:59:59"};
        for (auto str : values) {
            TimestampTzValue tz {};
            EXPECT_TRUE(tz.from_string(StringRef {str}, &time_zone, params, 0));
            builder->add_values((const uint8_t*)&tz, 1);
        }
        builder->add_nulls(1);
        static_cast<void>(builder->flush());
    }

    // page 4
    // no min and max value
    {
        std::vector<std::string> values = {"0000-01-01 00:00:59", "0000-01-01 00:00:59",
                                           "0000-01-01 03:00:00", "0000-01-01 03:00:00",
                                           "2023-01-01 15:00:00", "2023-01-01 15:00:00",
                                           "8999-12-31 23:59:59", "8999-12-31 23:59:59"};
        for (auto str : values) {
            TimestampTzValue tz {};
            EXPECT_TRUE(tz.from_string(StringRef {str}, &time_zone, params, 0));
            builder->add_values((const uint8_t*)&tz, 1);
        }
        builder->add_nulls(1);
        static_cast<void>(builder->flush());
    }

    // page 5
    builder->add_nulls(6);
    static_cast<void>(builder->flush());

    // write out zone map index
    ColumnIndexMetaPB index_meta;
    {
        io::FileWriterPtr file_writer;
        EXPECT_TRUE(fs->create_file(filename, &file_writer).ok());
        EXPECT_TRUE(builder->finish(file_writer.get(), &index_meta).ok());
        EXPECT_EQ(ZONE_MAP_INDEX, index_meta.type());
        EXPECT_TRUE(file_writer->close().ok());
    }

    io::FileReaderSPtr file_reader;
    EXPECT_TRUE(fs->open_file(filename, &file_reader).ok());

    vectorized::UInt32 scale = 6;
    auto segment_zone_map = index_meta.zone_map_index().segment_zone_map();
    EXPECT_EQ(
            vectorized::CastToString::from_timestamptz(type_limit<TimestampTzValue>::min(), scale),
            segment_zone_map.min());
    std::string max_str = "9999-12-31 23:59:59";
    TimestampTzValue tz_max {};
    EXPECT_TRUE(tz_max.from_string(StringRef {max_str}, &time_zone, params, 0));
    EXPECT_EQ(vectorized::CastToString::from_timestamptz(tz_max, scale), segment_zone_map.max());
    EXPECT_EQ(true, segment_zone_map.has_null());
    EXPECT_EQ(true, segment_zone_map.has_not_null());

    ZoneMapIndexReader column_zone_map(file_reader, index_meta.zone_map_index().page_zone_maps());
    Status status = column_zone_map.load(true, false);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(5, column_zone_map.num_pages());
    const std::vector<ZoneMapPB>& zone_maps = column_zone_map.page_zone_maps();
    EXPECT_EQ(5, zone_maps.size());

    // page 1
    EXPECT_EQ(
            vectorized::CastToString::from_timestamptz(type_limit<TimestampTzValue>::min(), scale),
            zone_maps[0].min());
    EXPECT_EQ(vectorized::CastToString::from_timestamptz(tz_max, scale), zone_maps[0].max());
    EXPECT_EQ(false, zone_maps[0].has_null());
    EXPECT_EQ(true, zone_maps[0].has_not_null());

    // page 2
    EXPECT_EQ(
            vectorized::CastToString::from_timestamptz(type_limit<TimestampTzValue>::min(), scale),
            zone_maps[1].min());
    {
        TimestampTzValue tz {};
        StringRef str = StringRef {"8999-12-31 23:59:59"};
        EXPECT_TRUE(tz.from_string(str, &time_zone, params, 0));
        EXPECT_EQ(vectorized::CastToString::from_timestamptz(tz, scale), zone_maps[1].max());
        EXPECT_EQ(true, zone_maps[1].has_null());
        EXPECT_EQ(true, zone_maps[1].has_not_null());
    }

    // page 3
    {
        TimestampTzValue tz {};
        StringRef str = StringRef {"0000-01-01 00:00:59"};
        EXPECT_TRUE(tz.from_string(str, &time_zone, params, 0));
        EXPECT_EQ(vectorized::CastToString::from_timestamptz(tz, scale), zone_maps[2].min());
    }
    EXPECT_EQ(vectorized::CastToString::from_timestamptz(tz_max, scale), zone_maps[2].max());
    EXPECT_EQ(true, zone_maps[2].has_null());
    EXPECT_EQ(true, zone_maps[2].has_not_null());

    // page 4
    {
        TimestampTzValue tz {};
        StringRef str = StringRef {"0000-01-01 00:00:59"};
        EXPECT_TRUE(tz.from_string(str, &time_zone, params, 0));
        EXPECT_EQ(vectorized::CastToString::from_timestamptz(tz, scale), zone_maps[3].min());
    }
    {
        TimestampTzValue tz {};
        StringRef str = StringRef {"8999-12-31 23:59:59"};
        EXPECT_TRUE(tz.from_string(str, &time_zone, params, 0));
        EXPECT_EQ(vectorized::CastToString::from_timestamptz(tz, scale), zone_maps[3].max());
    }
    EXPECT_EQ(true, zone_maps[3].has_null());
    EXPECT_EQ(true, zone_maps[3].has_not_null());

    // page 5
    EXPECT_EQ(true, zone_maps[4].has_null());
    EXPECT_EQ(false, zone_maps[4].has_not_null());
    delete field;
}

} // namespace segment_v2
} // namespace doris
