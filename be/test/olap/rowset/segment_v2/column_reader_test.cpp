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
#include "olap/rowset/segment_v2/column_reader.h"

#include <gen_cpp/Descriptors_types.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cmath>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

// Use #define private public to reach ColumnReader::_parse_zone_map, which has no public entry
// point. It must come before any header that pulls in column_reader.h.
#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#endif
#define private public
#include "olap/column_predicate.h"
#include "olap/rowset/segment_v2/column_reader.h"
#undef private
#if defined(__clang__)
#pragma clang diagnostic pop
#endif

#include "agent/be_exec_version_manager.h"
#include "common/config.h"
#include "gen_cpp/olap_file.pb.h"
#include "gen_cpp/segment_v2.pb.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "mock/mock_segment.h"
#include "olap/field.h"
#include "olap/rowset/segment_v2/column_reader_cache.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/rowset/segment_v2/variant/variant_column_reader.h"
#include "olap/rowset/segment_v2/zone_map_index.h"
#include "olap/tablet_schema.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/json/path_in_data.h"

namespace doris::segment_v2 {
class ColumnReaderTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

// Every value written to a FLOAT or DOUBLE zone map is one of seven shapes, and only an ordinary
// finite value moves the recorded bounds -- NaN and infinity go to the flags instead, and
// DBL_MAX/-DBL_MAX happen to be the very values the bounds start from. Walk every non-empty subset
// of the seven and check the one property a zone map has to hold: either it says it is unusable,
// or its bounds cover every value the page holds, so nothing it contains is ever pruned away.
template <PrimitiveType Type>
void test_every_value_combination() {
    using CppType = typename PrimitiveTypeTraits<Type>::CppType;
    constexpr bool is_double = Type == TYPE_DOUBLE;
    const std::vector<std::pair<const char*, CppType>> candidates = {
            {"NaN", std::numeric_limits<CppType>::quiet_NaN()},
            {"+inf", std::numeric_limits<CppType>::infinity()},
            {"-inf", -std::numeric_limits<CppType>::infinity()},
            {"max", std::numeric_limits<CppType>::max()},
            {"lowest", std::numeric_limits<CppType>::lowest()},
            {"1.5", static_cast<CppType>(1.5)},
            {"20.5", static_cast<CppType>(20.5)}};

    // Doris orders NaN above every number, so rank it beyond infinity to compare bounds the way
    // the scan does.
    auto rank = [](CppType v) {
        return std::isnan(v) ? std::numeric_limits<double>::infinity() : static_cast<double>(v);
    };
    auto covers = [&](CppType low, CppType high, CppType v) {
        if (std::isnan(v)) {
            return std::isnan(high);
        }
        return (std::isnan(low) ? false : rank(low) <= rank(v)) &&
               (std::isnan(high) ? true : rank(v) <= rank(high));
    };

    const std::string test_dir = "./ut_dir/column_reader_test";
    auto fs = io::global_local_filesystem();
    ASSERT_TRUE(fs->delete_directory(test_dir).ok());
    ASSERT_TRUE(fs->create_directory(test_dir).ok());

    const FieldType field_type =
            is_double ? FieldType::OLAP_FIELD_TYPE_DOUBLE : FieldType::OLAP_FIELD_TYPE_FLOAT;
    TabletColumn column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE, field_type, true, 0,
                        sizeof(CppType));
    std::unique_ptr<Field> field(FieldFactory::create(column));

    auto reader = std::make_shared<ColumnReader>();
    reader->_meta_type = field_type;
    reader->_data_type = vectorized::DataTypeFactory::instance().create_data_type(Type, false);

    size_t pass_all_count = 0;
    for (uint32_t mask = 1; mask < (1u << candidates.size()); ++mask) {
        std::vector<CppType> values;
        std::string label;
        for (size_t i = 0; i < candidates.size(); ++i) {
            if (mask & (1u << i)) {
                values.push_back(candidates[i].second);
                label += (label.empty() ? "" : ",");
                label += candidates[i].first;
            }
        }

        std::string filename =
                fmt::format("{}/every_value_{}_{}", test_dir, is_double ? "double" : "float", mask);
        std::unique_ptr<ZoneMapIndexWriter> builder;
        ASSERT_TRUE(ZoneMapIndexWriter::create(field.get(), builder).ok()) << label;
        // Stay above zone_map_row_num_threshold so the writer does not invalidate the page for
        // being small, which would hide what is being tested here.
        const size_t rows = config::zone_map_row_num_threshold + 5;
        for (size_t i = 0; i < rows; ++i) {
            CppType value = values[i % values.size()];
            builder->add_values(&value, 1);
        }
        ASSERT_TRUE(builder->flush().ok()) << label;

        ColumnIndexMetaPB index_meta;
        {
            io::FileWriterPtr file_writer;
            ASSERT_TRUE(fs->create_file(filename, &file_writer).ok()) << label;
            ASSERT_TRUE(builder->finish(file_writer.get(), &index_meta).ok()) << label;
            ASSERT_TRUE(file_writer->close().ok()) << label;
        }

        ZoneMapInfo zone_map_info;
        ASSERT_TRUE(reader->_parse_zone_map(index_meta.zone_map_index().segment_zone_map(),
                                            zone_map_info)
                            .ok())
                << label;
        ASSERT_FALSE(zone_map_info.is_all_null) << label;

        if (zone_map_info.pass_all) {
            ++pass_all_count;
            continue;
        }
        auto low = zone_map_info.min_value.get<Type>();
        auto high = zone_map_info.max_value.get<Type>();
        for (auto value : values) {
            EXPECT_TRUE(covers(low, high, value))
                    << "values {" << label << "} left bounds that do not cover " << value;
        }
    }

    // A page reports no bounds exactly when it held no finite value, which is every non-empty
    // subset of NaN, +inf and -inf: seven of the 127.
    EXPECT_EQ(7, pass_all_count) << (is_double ? "double" : "float");
    ASSERT_TRUE(fs->delete_directory(test_dir).ok());
}

TEST_F(ColumnReaderTest, EveryValueCombinationDouble) {
    test_every_value_combination<TYPE_DOUBLE>();
}

TEST_F(ColumnReaderTest, EveryValueCombinationFloat) {
    test_every_value_combination<TYPE_FLOAT>();
}

// A zone map whose bounds come back reversed describes a page that held no finite value: NaN and
// infinity only set the flags, so min stays at DBL_MAX and max at -DBL_MAX.
TEST_F(ColumnReaderTest, ReversedBoundsDegradeToPassAll) {
    auto make_zone_map = [](const std::string& min, const std::string& max) {
        ZoneMapPB pb;
        pb.set_min(min);
        pb.set_max(max);
        pb.set_has_null(false);
        pb.set_has_not_null(true);
        pb.set_pass_all(false);
        return pb;
    };
    const std::string double_lowest = "-1.7976931348623157e+308";
    const std::string double_highest = "1.7976931348623157e+308";
    const std::string float_lowest = "-3.4028235e+38";
    const std::string float_highest = "3.4028235e+38";

    for (bool nullable : {false, true}) {
        for (auto type : {TYPE_DOUBLE, TYPE_FLOAT}) {
            const bool is_double = type == TYPE_DOUBLE;
            auto reader = std::make_shared<ColumnReader>();
            reader->_meta_type = is_double ? FieldType::OLAP_FIELD_TYPE_DOUBLE
                                           : FieldType::OLAP_FIELD_TYPE_FLOAT;
            reader->_data_type =
                    vectorized::DataTypeFactory::instance().create_data_type(type, nullable);
            const auto& lowest = is_double ? double_lowest : float_lowest;
            const auto& highest = is_double ? double_highest : float_highest;

            ZoneMapInfo reversed;
            ASSERT_TRUE(reader->_parse_zone_map(make_zone_map(highest, lowest), reversed).ok());
            EXPECT_TRUE(reversed.pass_all) << "nullable=" << nullable << ", double=" << is_double;

            // The same bounds the right way round stay usable.
            ZoneMapInfo sound;
            ASSERT_TRUE(reader->_parse_zone_map(make_zone_map(lowest, highest), sound).ok());
            EXPECT_FALSE(sound.pass_all) << "nullable=" << nullable << ", double=" << is_double;
        }
    }

    auto reader = std::make_shared<ColumnReader>();
    reader->_meta_type = FieldType::OLAP_FIELD_TYPE_DOUBLE;
    reader->_data_type =
            vectorized::DataTypeFactory::instance().create_data_type(TYPE_DOUBLE, false);

    // A flag says what the page held, but the bounds are still the reversed pair whatever it says.
    for (bool nan : {false, true}) {
        for (bool pos_inf : {false, true}) {
            for (bool neg_inf : {false, true}) {
                auto pb = make_zone_map(double_highest, double_lowest);
                pb.set_has_nan(nan);
                pb.set_has_positive_inf(pos_inf);
                pb.set_has_negative_inf(neg_inf);
                ZoneMapInfo flagged;
                ASSERT_TRUE(reader->_parse_zone_map(pb, flagged).ok());
                EXPECT_TRUE(flagged.pass_all)
                        << "nan=" << nan << ", +inf=" << pos_inf << ", -inf=" << neg_inf;
            }
        }
    }

    // 4.0 wrote bounds with digits10 + 1 digits, so a FLOAT page of only NaN recorded
    // 3.402823e+38 rather than FLT_MAX. It parses back finite and no longer equals the value the
    // writer starts from -- the reversal survives the lossy round trip where the value does not.
    auto float_reader = std::make_shared<ColumnReader>();
    float_reader->_meta_type = FieldType::OLAP_FIELD_TYPE_FLOAT;
    float_reader->_data_type =
            vectorized::DataTypeFactory::instance().create_data_type(TYPE_FLOAT, false);
    auto truncated = make_zone_map("3.402823e+38", "-3.402823e+38");
    truncated.set_has_nan(true);
    ZoneMapInfo from_4_0;
    ASSERT_TRUE(float_reader->_parse_zone_map(truncated, from_4_0).ok());
    EXPECT_TRUE(from_4_0.pass_all);

    // A bound that reads back as infinity is no use either, and must not fail the scan.
    ZoneMapInfo overflowed;
    ASSERT_TRUE(reader->_parse_zone_map(
                              make_zone_map("-1.797693134862316e+308", "1.797693134862316e+308"),
                              overflowed)
                        .ok());
    EXPECT_TRUE(overflowed.pass_all);

    // A flag on top of bounds that do describe finite values leaves the zone map usable.
    auto pb = make_zone_map("1.5", "20.5");
    pb.set_has_nan(true);
    ZoneMapInfo partly_nan;
    ASSERT_TRUE(reader->_parse_zone_map(pb, partly_nan).ok());
    EXPECT_FALSE(partly_nan.pass_all);
    EXPECT_TRUE(std::isnan(partly_nan.max_value.get<TYPE_DOUBLE>()));
    EXPECT_EQ(partly_nan.min_value.get<TYPE_DOUBLE>(), 1.5);
}

TEST_F(ColumnReaderTest, StructAccessPaths) {
    auto create_struct_iterator = []() {
        auto null_reader = std::make_shared<ColumnReader>();
        auto null_iterator = std::make_unique<FileColumnIterator>(null_reader);

        std::vector<ColumnIteratorUPtr> sub_column_iterators;
        auto sub_reader1 = std::make_shared<ColumnReader>();
        auto sub_iterator1 = std::make_unique<FileColumnIterator>(sub_reader1);
        sub_iterator1->set_column_name("sub_col_1");
        auto sub_reader2 = std::make_shared<ColumnReader>();
        auto sub_iterator2 = std::make_unique<FileColumnIterator>(sub_reader2);
        sub_iterator2->set_column_name("sub_col_2");

        sub_column_iterators.emplace_back(std::move(sub_iterator1));
        sub_column_iterators.emplace_back(std::move(sub_iterator2));
        auto iterator = std::make_unique<StructFileColumnIterator>(std::make_shared<ColumnReader>(),
                                                                   std::move(null_iterator),
                                                                   std::move(sub_column_iterators));
        return iterator;
    };

    auto iterator = create_struct_iterator();
    auto st = iterator->set_access_paths(TColumnAccessPaths {}, TColumnAccessPaths {});

    ASSERT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();
    ASSERT_EQ(iterator->_reading_flag, ColumnIterator::ReadingFlag::NORMAL_READING);

    TColumnAccessPaths all_access_paths;
    all_access_paths.emplace_back();

    TColumnAccessPaths predicate_access_paths;
    predicate_access_paths.emplace_back();

    st = iterator->set_access_paths(all_access_paths, predicate_access_paths);
    // empty paths leads to error
    ASSERT_FALSE(st.ok());

    // Only reading sub_col_1
    // sub_col_2 should be set to SKIP_READING
    all_access_paths[0].data_access_path.path = {"self", "sub_col_1"};

    predicate_access_paths[0].data_access_path.path = {"self", "sub_col_1"};

    st = iterator->set_access_paths(all_access_paths, predicate_access_paths);
    // invalid name leads to error
    ASSERT_FALSE(st.ok());

    iterator->set_column_name("self");
    // now column name is "self", should be ok
    st = iterator->set_access_paths(all_access_paths, predicate_access_paths);
    ASSERT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();
    ASSERT_EQ(iterator->_reading_flag, ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);

    ASSERT_EQ(iterator->_sub_column_iterators[0]->_reading_flag,
              ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);
    ASSERT_EQ(iterator->_sub_column_iterators[1]->_reading_flag,
              ColumnIterator::ReadingFlag::SKIP_READING);

    // Reading all sub columns
    all_access_paths[0].data_access_path.path = {"self"};
    iterator = create_struct_iterator();
    iterator->set_column_name("self");
    st = iterator->set_access_paths(all_access_paths, predicate_access_paths);

    ASSERT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();
    ASSERT_EQ(iterator->_reading_flag, ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);

    ASSERT_EQ(iterator->_sub_column_iterators[0]->_reading_flag,
              ColumnIterator::ReadingFlag::READING_FOR_PREDICATE);
    ASSERT_EQ(iterator->_sub_column_iterators[1]->_reading_flag,
              ColumnIterator::ReadingFlag::NEED_TO_READ);
}

TEST_F(ColumnReaderTest, MultiAccessPaths) {
    auto create_struct_iterator = []() {
        auto null_reader = std::make_shared<ColumnReader>();
        auto null_iterator = std::make_unique<FileColumnIterator>(null_reader);

        std::vector<ColumnIteratorUPtr> sub_column_iterators;
        auto sub_reader1 = std::make_shared<ColumnReader>();
        auto sub_iterator1 = std::make_unique<FileColumnIterator>(sub_reader1);
        sub_iterator1->set_column_name("sub_col_1");
        auto sub_reader2 = std::make_shared<ColumnReader>();
        auto sub_iterator2 = std::make_unique<FileColumnIterator>(sub_reader2);
        sub_iterator2->set_column_name("sub_col_2");

        sub_column_iterators.emplace_back(std::move(sub_iterator1));
        sub_column_iterators.emplace_back(std::move(sub_iterator2));
        auto iterator = std::make_unique<StructFileColumnIterator>(std::make_shared<ColumnReader>(),
                                                                   std::move(null_iterator),
                                                                   std::move(sub_column_iterators));
        return iterator;
    };

    auto create_struct_iterator2 = [](ColumnIteratorUPtr&& nested_iterator) {
        auto null_reader = std::make_shared<ColumnReader>();
        auto null_iterator = std::make_unique<FileColumnIterator>(null_reader);

        std::vector<ColumnIteratorUPtr> sub_column_iterators;
        auto sub_reader1 = std::make_shared<ColumnReader>();
        auto sub_iterator1 = std::make_unique<FileColumnIterator>(sub_reader1);
        sub_iterator1->set_column_name("sub_col_1");

        sub_column_iterators.emplace_back(std::move(sub_iterator1));
        sub_column_iterators.emplace_back(std::move(nested_iterator));
        auto iterator = std::make_unique<StructFileColumnIterator>(std::make_shared<ColumnReader>(),
                                                                   std::move(null_iterator),
                                                                   std::move(sub_column_iterators));
        return iterator;
    };

    auto struct_iterator = create_struct_iterator();
    struct_iterator->set_column_name("struct");

    auto map_iterator = std::make_unique<MapFileColumnIterator>(
            std::make_shared<ColumnReader>(),
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()), // null iterator
            std::make_unique<OffsetFileColumnIterator>(
                    std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>())),
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()),
            std::move(struct_iterator));

    auto array_iterator = std::make_unique<ArrayFileColumnIterator>(
            std::make_shared<ColumnReader>(),
            std::make_unique<OffsetFileColumnIterator>(
                    std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>())),
            std::move(map_iterator),
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));

    // here create:
    // struct<
    //      sub_col_1,
    //      sub_col_2: array<
    //          map<
    //              key,
    //              value: struct<
    //                  sub_col_1,
    //                  sub_col_2
    //              >
    //          >
    //      >
    //  >
    array_iterator->set_column_name("sub_col_2");
    auto iterator = create_struct_iterator2(std::move(array_iterator));
    TColumnAccessPaths all_access_paths;
    all_access_paths.emplace_back();

    // all access paths:
    // self.sub_col_2.*.KEYS
    // predicates paths empty
    all_access_paths[0].data_access_path.path = {"self", "sub_col_2", "*", "KEYS"};

    TColumnAccessPaths predicate_access_paths;

    iterator->set_column_name("self");
    auto st = iterator->set_access_paths(all_access_paths, predicate_access_paths);

    ASSERT_TRUE(st.ok()) << "failed to set access paths: " << st.to_string();
    ASSERT_EQ(iterator->_reading_flag, ColumnIterator::ReadingFlag::NEED_TO_READ);

    ASSERT_EQ(iterator->_sub_column_iterators[0]->_reading_flag,
              ColumnIterator::ReadingFlag::SKIP_READING);
    ASSERT_EQ(iterator->_sub_column_iterators[1]->_reading_flag,
              ColumnIterator::ReadingFlag::NEED_TO_READ);

    auto* array_iter =
            static_cast<ArrayFileColumnIterator*>(iterator->_sub_column_iterators[1].get());
    ASSERT_EQ(array_iter->_item_iterator->_reading_flag, ColumnIterator::ReadingFlag::NEED_TO_READ);

    auto* map_iter = static_cast<MapFileColumnIterator*>(array_iter->_item_iterator.get());
    ASSERT_EQ(map_iter->_key_iterator->_reading_flag, ColumnIterator::ReadingFlag::NEED_TO_READ);
    ASSERT_EQ(map_iter->_val_iterator->_reading_flag, ColumnIterator::ReadingFlag::SKIP_READING);
}

TEST_F(ColumnReaderTest, OffsetPeekUsesPageSentinelWhenNoRemaining) {
    // create a bare FileColumnIterator with a dummy ColumnReader
    auto reader = std::make_shared<ColumnReader>();
    auto file_iter = std::make_unique<FileColumnIterator>(reader);
    auto* page = file_iter->get_current_page();

    // simulate a page that has no remaining offsets in decoder but has a valid
    // next_array_item_ordinal recorded in footer
    page->num_rows = 0;
    page->offset_in_page = 0;
    page->next_array_item_ordinal = 12345;

    OffsetFileColumnIterator offset_iter(std::move(file_iter));
    ordinal_t offset = 0;
    auto st = offset_iter._peek_one_offset(&offset);

    ASSERT_TRUE(st.ok()) << "peek one offset failed: " << st.to_string();
    ASSERT_EQ(static_cast<ordinal_t>(12345), offset);
}

TEST_F(ColumnReaderTest, OffsetCalculateOffsetsUsesPageSentinelForLastOffset) {
    // create offset iterator with a page whose sentinel offset is set in footer
    auto reader = std::make_shared<ColumnReader>();
    auto file_iter = std::make_unique<FileColumnIterator>(reader);
    auto* page = file_iter->get_current_page();

    // simulate page with no remaining values, but a valid next_array_item_ordinal
    page->num_rows = 0;
    page->offset_in_page = 0;
    page->next_array_item_ordinal = 15;

    OffsetFileColumnIterator offset_iter(std::move(file_iter));

    // prepare in-memory column offsets:
    // offsets_data = [first_column_offset, first_storage_offset, next_storage_offset_placeholder]
    // first_column_offset = 100
    // first_storage_offset = 10
    // placeholder real next_storage_offset will be fetched from page sentinel (15)
    vectorized::ColumnArray::ColumnOffsets column_offsets;
    auto& data = column_offsets.get_data();
    data.push_back(100); // index 0: first_column_offset
    data.push_back(10);  // index 1: first_storage_offset
    data.push_back(12);  // index 2: placeholder storage offset for middle element

    auto st = offset_iter._calculate_offsets(1, column_offsets);
    ASSERT_TRUE(st.ok()) << "calculate offsets failed: " << st.to_string();

    // after calculation:
    // data[1] = 100 + (12 - 10) = 102
    // data[2] = 100 + (15 - 10) = 105 (using page sentinel as next_storage_offset)
    ASSERT_EQ(static_cast<ordinal_t>(100), data[0]);
    ASSERT_EQ(static_cast<ordinal_t>(102), data[1]);
    ASSERT_EQ(static_cast<ordinal_t>(105), data[2]);
}

TEST_F(ColumnReaderTest, MapReadByRowidsSkipReadingResizesDestination) {
    // create a basic map iterator with dummy readers/iterators
    auto map_reader = std::make_shared<ColumnReader>();
    auto null_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto offsets_iter = std::make_unique<OffsetFileColumnIterator>(
            std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>()));
    auto key_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());
    auto val_iter = std::make_unique<FileColumnIterator>(std::make_shared<ColumnReader>());

    MapFileColumnIterator map_iter(map_reader, std::move(null_iter), std::move(offsets_iter),
                                   std::move(key_iter), std::move(val_iter));
    map_iter.set_column_name("map_col");
    map_iter.set_reading_flag(ColumnIterator::ReadingFlag::SKIP_READING);

    // prepare an empty ColumnMap as destination
    auto keys = vectorized::ColumnInt32::create();
    auto values = vectorized::ColumnInt32::create();
    auto offsets = vectorized::ColumnArray::ColumnOffsets::create();
    offsets->get_data().push_back(0);
    auto column_map =
            vectorized::ColumnMap::create(std::move(keys), std::move(values), std::move(offsets));
    vectorized::MutableColumnPtr dst = std::move(column_map);

    const rowid_t rowids[] = {1, 5, 7};
    size_t count = sizeof(rowids) / sizeof(rowids[0]);
    auto st = map_iter.read_by_rowids(rowids, count, dst);

    ASSERT_TRUE(st.ok()) << "read_by_rowids failed: " << st.to_string();
    ASSERT_EQ(count, dst->size());
}
} // namespace doris::segment_v2