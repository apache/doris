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

#include <gen_cpp/segment_v2.pb.h>
#include <gtest/gtest.h>

#include "olap/comparison_predicate.h"
#include "olap/in_list_predicate.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/beta_rowset_writer.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/segment_v2/bloom_filter_index_reader.h"
#include "olap/storage_engine.h"
#include "runtime/define_primitive_type.h"
#include "util/date_func.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {

using namespace doris::vectorized;

constexpr static uint32_t MAX_PATH_LEN = 1024;
constexpr static std::string_view dest_dir = "./ut_dir/date_bloom_filter";
static int64_t inc_id = 1000;

class DateBloomFilterTest : public ::testing::Test {
protected:
    void SetUp() override {
        // absolute dir
        char buffer[MAX_PATH_LEN];
        EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        _curreent_dir = std::string(buffer);
        _absolute_dir = _curreent_dir + std::string(dest_dir);
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(_absolute_dir).ok());

        // storage engine
        doris::EngineOptions options;
        auto engine = std::make_unique<StorageEngine>(options);
        _engine_ref = engine.get();
        _data_dir = std::make_unique<DataDir>(*_engine_ref, _absolute_dir);
        static_cast<void>(_data_dir->update_capacity());
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));

        // tablet_schema
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(KeysType::DUP_KEYS);

        construct_column(schema_pb.add_column(), 0, "DATE", "date_column");
        construct_column(schema_pb.add_column(), 1, "DATETIME", "datetime_column");
        schema_pb.set_bf_fpp(0.05);
        _tablet_schema.reset(new TabletSchema);
        _tablet_schema->init_from_pb(schema_pb);

        // tablet
        TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));

        _tablet.reset(new Tablet(*_engine_ref, tablet_meta, _data_dir.get()));
        EXPECT_TRUE(_tablet->init().ok());
    }
    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        _engine_ref = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

    void construct_column(ColumnPB* column_pb, int32_t col_unique_id,
                          const std::string& column_type, const std::string& column_name) {
        column_pb->set_unique_id(col_unique_id);
        column_pb->set_name(column_name);
        column_pb->set_type(column_type);
        column_pb->set_is_key(true);
        column_pb->set_is_nullable(true);
        column_pb->set_is_bf_column(true);
    }

    RowsetWriterContext rowset_writer_context() {
        RowsetWriterContext context;
        RowsetId rowset_id;
        rowset_id.init(inc_id);
        context.rowset_id = rowset_id;
        context.rowset_type = BETA_ROWSET;
        context.data_dir = _data_dir.get();
        context.rowset_state = VISIBLE;
        context.tablet_schema = _tablet_schema;
        context.tablet_path = _tablet->tablet_path();
        context.version = Version(inc_id, inc_id);
        context.max_rows_per_segment = 200;
        inc_id++;
        return context;
    }

    DateBloomFilterTest() = default;
    ~DateBloomFilterTest() override = default;

private:
    TabletSchemaSPtr _tablet_schema = nullptr;
    StorageEngine* _engine_ref = nullptr;
    std::unique_ptr<DataDir> _data_dir = nullptr;
    TabletSharedPtr _tablet = nullptr;
    std::string _absolute_dir;
    std::string _curreent_dir;
};

TEST_F(DateBloomFilterTest, query_index_test) {
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    RowsetSharedPtr rowset;
    const auto& res =
            RowsetFactory::create_rowset_writer(*_engine_ref, rowset_writer_context(), false);
    EXPECT_TRUE(res.has_value()) << res.error();
    const auto& rowset_writer = res.value();

    Block block = _tablet_schema->create_block();
    auto columns = block.mutate_columns();

    auto date = timestamp_from_date("2024-11-08");
    auto datetime = timestamp_from_datetime("2024-11-08 09:00:00");
    uint24_t olap_date_value(date.to_olap_date());
    uint64_t olap_datetime_value(datetime.to_olap_datetime());
    columns[0]->insert_many_fix_len_data(reinterpret_cast<const char*>(&olap_date_value), 1);
    columns[1]->insert_many_fix_len_data(reinterpret_cast<const char*>(&olap_datetime_value), 1);

    date = timestamp_from_date("2024-11-09");
    datetime = timestamp_from_datetime("2024-11-09 09:00:00");
    olap_date_value = date.to_olap_date();
    olap_datetime_value = datetime.to_olap_datetime();
    columns[0]->insert_many_fix_len_data(reinterpret_cast<const char*>(&olap_date_value), 1);
    columns[1]->insert_many_fix_len_data(reinterpret_cast<const char*>(&olap_datetime_value), 1);

    EXPECT_TRUE(rowset_writer->add_block(&block).ok());
    EXPECT_TRUE(rowset_writer->flush().ok());
    EXPECT_TRUE(rowset_writer->build(rowset).ok());
    EXPECT_TRUE(_tablet->add_rowset(rowset).ok());

    segment_v2::SegmentSharedPtr segment;
    EXPECT_TRUE(((BetaRowset*)rowset.get())->load_segment(0, nullptr, &segment).ok());
    std::shared_ptr<SegmentFooterPB> footer_pb_shared;
    auto st = segment->_get_segment_footer(footer_pb_shared, nullptr);
    EXPECT_TRUE(st.ok());
    st = segment->_create_column_meta(*footer_pb_shared);
    EXPECT_TRUE(st.ok());

    // date
    {
        std::shared_ptr<ColumnReader> reader;
        OlapReaderStatistics stats;
        st = segment->get_column_reader(_tablet_schema->column_by_uid(0), &reader, &stats);
        EXPECT_TRUE(st.ok());
        std::unique_ptr<BloomFilterIndexIterator> bf_iter;
        EXPECT_TRUE(reader->_bloom_filter_index->load(true, true, nullptr).ok());
        EXPECT_TRUE(reader->_bloom_filter_index->new_iterator(&bf_iter, nullptr).ok());
        std::unique_ptr<BloomFilter> bf;
        EXPECT_TRUE(bf_iter->read_bloom_filter(0, &bf).ok());
        auto test = [&](const std::string& query_string, bool result) {
            auto date = timestamp_from_date(query_string);
            std::unique_ptr<ComparisonPredicateBase<TYPE_DATE, PredicateType::EQ>> date_pred(
                    new ComparisonPredicateBase<TYPE_DATE, PredicateType::EQ>(0, date));
            EXPECT_EQ(date_pred->evaluate_and(bf.get()), result);
        };
        test("2024-11-08", true);
        test("2024-11-09", true);
        test("2024-11-20", false);
    }

    // datetime
    {
        std::shared_ptr<ColumnReader> reader;
        OlapReaderStatistics stats;
        st = segment->get_column_reader(_tablet_schema->column_by_uid(1), &reader, &stats);
        EXPECT_TRUE(st.ok());
        std::unique_ptr<BloomFilterIndexIterator> bf_iter;
        EXPECT_TRUE(reader->_bloom_filter_index->load(true, true, nullptr).ok());
        EXPECT_TRUE(reader->_bloom_filter_index->new_iterator(&bf_iter, nullptr).ok());
        std::unique_ptr<BloomFilter> bf;
        EXPECT_TRUE(bf_iter->read_bloom_filter(0, &bf).ok());
        auto test = [&](const std::string& query_string, bool result) {
            auto datetime = timestamp_from_datetime(query_string);
            std::unique_ptr<ComparisonPredicateBase<TYPE_DATETIME, PredicateType::EQ>> date_pred(
                    new ComparisonPredicateBase<TYPE_DATETIME, PredicateType::EQ>(0, datetime));
            EXPECT_EQ(date_pred->evaluate_and(bf.get()), result);
        };
        test("2024-11-08 09:00:00", true);
        test("2024-11-09 09:00:00", true);
        test("2024-11-20 09:00:00", false);
    }
}

TEST_F(DateBloomFilterTest, in_list_predicate_test) {
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());

    RowsetSharedPtr rowset;
    const auto& res =
            RowsetFactory::create_rowset_writer(*_engine_ref, rowset_writer_context(), false);
    EXPECT_TRUE(res.has_value()) << res.error();
    const auto& rowset_writer = res.value();

    Block block = _tablet_schema->create_block();
    auto columns = block.mutate_columns();

    // Insert test data
    auto date = timestamp_from_date("2024-11-08");
    auto datetime = timestamp_from_datetime("2024-11-08 09:00:00");
    uint24_t olap_date_value(date.to_olap_date());
    uint64_t olap_datetime_value(datetime.to_olap_datetime());
    columns[0]->insert_many_fix_len_data(reinterpret_cast<const char*>(&olap_date_value), 1);
    columns[1]->insert_many_fix_len_data(reinterpret_cast<const char*>(&olap_datetime_value), 1);

    date = timestamp_from_date("2024-11-09");
    datetime = timestamp_from_datetime("2024-11-09 09:00:00");
    olap_date_value = date.to_olap_date();
    olap_datetime_value = datetime.to_olap_datetime();
    columns[0]->insert_many_fix_len_data(reinterpret_cast<const char*>(&olap_date_value), 1);
    columns[1]->insert_many_fix_len_data(reinterpret_cast<const char*>(&olap_datetime_value), 1);

    EXPECT_TRUE(rowset_writer->add_block(&block).ok());
    EXPECT_TRUE(rowset_writer->flush().ok());
    EXPECT_TRUE(rowset_writer->build(rowset).ok());
    EXPECT_TRUE(_tablet->add_rowset(rowset).ok());

    segment_v2::SegmentSharedPtr segment;
    EXPECT_TRUE(((BetaRowset*)rowset.get())->load_segment(0, nullptr, &segment).ok());
    std::shared_ptr<SegmentFooterPB> footer_pb_shared;
    auto st = segment->_get_segment_footer(footer_pb_shared, nullptr);
    EXPECT_TRUE(st.ok());
    st = segment->_create_column_meta(*(footer_pb_shared));
    EXPECT_TRUE(st.ok());

    // Test DATE column with IN predicate
    {
        std::shared_ptr<ColumnReader> reader;
        OlapReaderStatistics stats;
        st = segment->get_column_reader(_tablet_schema->column_by_uid(0), &reader, &stats);
        EXPECT_TRUE(st.ok());
        std::unique_ptr<BloomFilterIndexIterator> bf_iter;
        EXPECT_TRUE(reader->_bloom_filter_index->load(true, true, nullptr).ok());
        EXPECT_TRUE(reader->_bloom_filter_index->new_iterator(&bf_iter, nullptr).ok());
        std::unique_ptr<BloomFilter> bf;
        EXPECT_TRUE(bf_iter->read_bloom_filter(0, &bf).ok());

        // Test positive cases
        auto test_positive = [&](const std::vector<std::string>& values, bool result) {
            auto hybrid_set = std::make_shared<HybridSet<PrimitiveType::TYPE_DATE>>(false);
            for (const auto& value : values) {
                auto v = timestamp_from_date(value);
                hybrid_set->insert(&v);
            }
            std::unique_ptr<InListPredicateBase<TYPE_DATE, PredicateType::IN_LIST,
                                                HybridSet<PrimitiveType::TYPE_DATE>>>
                    date_pred(new InListPredicateBase<TYPE_DATE, PredicateType::IN_LIST,
                                                      HybridSet<PrimitiveType::TYPE_DATE>>(
                            0, hybrid_set));
            EXPECT_EQ(date_pred->evaluate_and(bf.get()), result);
        };

        test_positive({"2024-11-08", "2024-11-09"}, true);
        test_positive({"2024-11-08"}, true);
        test_positive({"2024-11-09"}, true);

        auto test_negative = [&](const std::vector<std::string>& values, bool result) {
            auto hybrid_set = std::make_shared<HybridSet<PrimitiveType::TYPE_DATE>>(false);

            for (const auto& value : values) {
                auto v = timestamp_from_date(value);
                hybrid_set->insert(&v);
            }

            std::unique_ptr<InListPredicateBase<TYPE_DATE, PredicateType::IN_LIST,
                                                HybridSet<PrimitiveType::TYPE_DATE>>>
                    date_pred(new InListPredicateBase<TYPE_DATE, PredicateType::IN_LIST,
                                                      HybridSet<PrimitiveType::TYPE_DATE>>(
                            0, hybrid_set));

            EXPECT_EQ(date_pred->evaluate_and(bf.get()), result);
        };

        test_negative({"2024-11-20"}, false);
        test_negative({"2024-11-08", "2024-11-20"}, true);
        test_negative({"2024-11-20", "2024-11-21"}, false);
    }

    // Test DATETIME column with IN predicate
    {
        std::shared_ptr<ColumnReader> reader;
        OlapReaderStatistics stats;
        st = segment->get_column_reader(_tablet_schema->column_by_uid(1), &reader, &stats);
        EXPECT_TRUE(st.ok());
        std::unique_ptr<BloomFilterIndexIterator> bf_iter;
        EXPECT_TRUE(reader->_bloom_filter_index->load(true, true, nullptr).ok());
        EXPECT_TRUE(reader->_bloom_filter_index->new_iterator(&bf_iter, nullptr).ok());
        std::unique_ptr<BloomFilter> bf;
        EXPECT_TRUE(bf_iter->read_bloom_filter(0, &bf).ok());

        // Test positive cases
        auto test_positive = [&](const std::vector<std::string>& values, bool result) {
            auto hybrid_set = std::make_shared<HybridSet<PrimitiveType::TYPE_DATETIME>>(false);
            for (const auto& value : values) {
                auto v = timestamp_from_datetime(value);
                hybrid_set->insert(&v);
            }
            std::unique_ptr<InListPredicateBase<TYPE_DATETIME, PredicateType::IN_LIST,
                                                HybridSet<PrimitiveType::TYPE_DATETIME>>>
                    datetime_pred(new InListPredicateBase<TYPE_DATETIME, PredicateType::IN_LIST,
                                                          HybridSet<PrimitiveType::TYPE_DATETIME>>(
                            0, hybrid_set));
            EXPECT_EQ(datetime_pred->evaluate_and(bf.get()), result);
        };

        test_positive({"2024-11-08 09:00:00", "2024-11-09 09:00:00"}, true);
        test_positive({"2024-11-08 09:00:00"}, true);
        test_positive({"2024-11-09 09:00:00"}, true);

        // Test negative cases
        auto test_negative = [&](const std::vector<std::string>& values, bool result) {
            auto hybrid_set = std::make_shared<HybridSet<PrimitiveType::TYPE_DATETIME>>(false);
            for (const auto& value : values) {
                auto v = timestamp_from_datetime(value);
                hybrid_set->insert(&v);
            }
            std::unique_ptr<InListPredicateBase<TYPE_DATETIME, PredicateType::IN_LIST,
                                                HybridSet<PrimitiveType::TYPE_DATETIME>>>
                    datetime_pred(new InListPredicateBase<TYPE_DATETIME, PredicateType::IN_LIST,
                                                          HybridSet<PrimitiveType::TYPE_DATETIME>>(
                            0, hybrid_set));
            EXPECT_EQ(datetime_pred->evaluate_and(bf.get()), result);
        };

        test_negative({"2024-11-20 09:00:00"}, false);
        test_negative({"2024-11-08 09:00:00", "2024-11-20 09:00:00"}, true);
        test_negative({"2024-11-20 09:00:00", "2024-11-21 09:00:00"}, false);
    }
}

} // namespace doris
