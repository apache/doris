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

// SNII bigram-defer PRODUCTION-WIRING tests, one layer above
// snii_bigram_defer_writer_test.cpp: these drive the real ColumnWriter stack
// (ColumnWriter::create -> ScalarColumnWriter / ArrayColumnWriter ::init ->
// IndexColumnWriter::create -> set_direct_load forwarding) so that the
// `opts.is_direct_load` -> `set_direct_load(true)` hand-off in
// column_writer.cpp is what decides deferral for scalar indexes. ARRAY indexes
// deliberately keep their bigram build to preserve existing SNII full-build
// phrase behavior at array-element boundaries.
// The scenarios below pin both outcomes:
//   1. scalar direct load + config on -> no bigram sentinel;
//   2. ARRAY direct load + config on -> byte-identical to the full-build
//      baseline, with a phrase query excluding a cross-element false match;
//   3. any not-direct load + config on -> byte-identical to the config-off
//      baseline (compaction / schema change shape stays full).
// The segment-writer-level `write_type == TYPE_DIRECT` assignments feeding
// this flag are covered end-to-end in snii_bigram_defer_e2e_test.cpp.

#include <gtest/gtest.h>

#include <climits>
#include <cstdint>
#include <fstream>
#include <memory>
#include <numeric>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_string.h"
#include "core/field.h"
#include "core/types.h"
#include "gen_cpp/segment_v2.pb.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/format/phrase_bigram.h"
#include "storage/index/snii/io/local_file.h"
#include "storage/index/snii/query/phrase_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/iterator/olap_data_convertor.h"
#include "storage/segment/column_writer.h"
#include "storage/segment/variant/variant_column_writer_impl.h" // _init_column_meta
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {
namespace {

constexpr int64_t kIndexId = 3;
constexpr const char* kTestDir = "./ut_dir/snii_bigram_defer_wiring_test";

void assert_ok(const Status& status) {
    ASSERT_TRUE(status.ok()) << status.to_string();
}

// English-parser fulltext phrase index on column unique id 0 -- the shape whose
// hidden bigram build the direct-load deferral targets.
void init_phrase_index_meta(TabletIndex* meta) {
    TabletIndexPB pb;
    pb.set_index_type(IndexType::INVERTED);
    pb.set_index_id(kIndexId);
    pb.set_index_name("defer_wiring_idx");
    pb.add_col_unique_id(0);
    pb.mutable_properties()->insert({"parser", "english"});
    pb.mutable_properties()->insert({"lower_case", "true"});
    pb.mutable_properties()->insert({"support_phrase", "true"});
    meta->init_from_pb(pb);
}

TabletColumn make_string_column() {
    TabletColumn column;
    column.set_unique_id(0);
    column.set_name("c1");
    column.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    column.set_length(INT_MAX);
    column.set_is_nullable(false);
    return column;
}

TabletColumn make_array_string_column() {
    TabletColumn array;
    array.set_unique_id(0);
    array.set_name("arr1");
    array.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
    array.set_length(0);
    array.set_index_length(0);
    array.set_is_nullable(false);
    array.set_is_bf_column(false);
    TabletColumn item;
    item.set_unique_id(1);
    item.set_name("arr_sub_string");
    item.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    item.set_length(INT_MAX);
    array.add_sub_column(item);
    return array;
}

// Runs one column through the production ColumnWriter stack exactly as a
// segment writer would: create() (whose init() creates the index writer and
// forwards opts.is_direct_load via set_direct_load), append converted olap
// data, then the finish/write sequence and the index file close. The block
// column at position 0 must match `column`'s type.
void write_column_segment(const std::string& name, const TabletColumn& column,
                          const TabletIndex& index_meta, bool is_direct_load, const Block& block) {
    const std::string dat_path = std::string(kTestDir) + "/" + name + ".dat";
    const std::string idx_path = std::string(kTestDir) + "/" + name + ".idx";
    io::FileWriterPtr dat_writer;
    assert_ok(io::global_local_filesystem()->create_file(dat_path, &dat_writer));
    io::FileWriterPtr idx_file;
    assert_ok(io::global_local_filesystem()->create_file(idx_path, &idx_file));
    IndexFileWriter index_file_writer(io::global_local_filesystem(), idx_path, "test_rowset",
                                      /*seg_id=*/0, InvertedIndexStorageFormatPB::SNII,
                                      std::move(idx_file), /*can_use_ram_dir=*/true,
                                      /*tablet_id=*/300);

    ColumnMetaPB meta;
    ColumnWriterOptions opts;
    opts.meta = &meta;
    opts.compression_type = CompressionTypePB::LZ4;
    opts.storage_format = TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V2;
    opts.need_inverted_index = true;
    opts.inverted_indexes = {&index_meta};
    opts.index_file_writer = &index_file_writer;
    opts.is_direct_load = is_direct_load; // the wire under test
    _init_column_meta(&meta, /*column_id=*/0, column, opts);

    std::unique_ptr<ColumnWriter> writer;
    assert_ok(ColumnWriter::create(opts, &column, dat_writer.get(), &writer));
    assert_ok(writer->init());

    const size_t num_rows = block.rows();
    OlapBlockDataConvertor converter;
    converter.add_column_data_convertor(column);
    converter.set_source_content(&block, 0, num_rows);
    auto [convert_status, accessor] = converter.convert_column_data(0);
    assert_ok(convert_status);
    assert_ok(writer->append(accessor->get_nullmap(), accessor->get_data(), num_rows));

    assert_ok(writer->finish());
    assert_ok(writer->write_data());
    assert_ok(writer->write_ordinal_index());
    assert_ok(writer->write_inverted_index());
    assert_ok(index_file_writer.begin_close());
    assert_ok(index_file_writer.finish_close());
    assert_ok(dat_writer->close());
}

// 80 rows with the adjacent (hello, world) pair: df 80 sits inside the default
// prune survival window [64, 128], so a full build provably materializes the
// pair and the deferred segment's pair absence is a real difference.
Block make_string_block() {
    auto column = ColumnString::create();
    for (uint32_t i = 0; i < 80; ++i) {
        const std::string row = "hello world item" + std::to_string(i);
        column->insert_data(row.data(), row.size());
    }
    Block block;
    block.insert({std::move(column), std::make_shared<DataTypeString>(), "c1"});
    return block;
}

// 80 values carry the pair inside one array element, then one row splits the
// same terms across two elements. The pair df remains in the default survival
// window [64, 128], so a full build must exclude the split row from the phrase
// result using its hidden pair posting.
Block make_array_string_block() {
    DataTypePtr array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    MutableColumnPtr column = array_type->create_column();
    for (uint32_t i = 0; i < 80; ++i) {
        Array row;
        row.push_back(
                Field::create_field<TYPE_STRING>(String("hello world item") + std::to_string(i)));
        column->insert(Field::create_field<TYPE_ARRAY>(row));
    }
    Array split_row;
    split_row.push_back(Field::create_field<TYPE_STRING>(String("hello")));
    split_row.push_back(Field::create_field<TYPE_STRING>(String("world")));
    column->insert(Field::create_field<TYPE_ARRAY>(split_row));
    Block block;
    block.insert({std::move(column), array_type, "arr1"});
    return block;
}

bool lookup_found(const ::doris::snii::reader::LogicalIndexReader& idx, const std::string& term) {
    bool found = false;
    ::doris::snii::format::DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    EXPECT_TRUE(idx.lookup(term, &found, &entry, &frq_base, &prx_base).ok());
    return found;
}

std::vector<uint32_t> run_phrase(const ::doris::snii::reader::LogicalIndexReader& idx,
                                 const std::vector<std::string>& terms) {
    std::vector<uint32_t> docids;
    EXPECT_TRUE(::doris::snii::query::phrase_query(idx, terms, &docids).ok());
    return docids;
}

std::vector<uint32_t> phrase_docids(size_t count) {
    std::vector<uint32_t> docids(count);
    std::iota(docids.begin(), docids.end(), 0);
    return docids;
}

std::string read_file_bytes(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    EXPECT_TRUE(in.good()) << path;
    std::ostringstream out;
    out << in.rdbuf();
    return out.str();
}

void expect_file_bytes_equal(const std::string& path, const std::string& expected,
                             const std::string& shape) {
    EXPECT_EQ(read_file_bytes(path), expected) << shape;
}

void expect_deferred_bigram_layout(const ::doris::snii::reader::LogicalIndexReader& idx,
                                   const std::string& shape) {
    EXPECT_FALSE(lookup_found(idx, ::doris::snii::format::make_phrase_bigram_sentinel_term()))
            << shape;
    EXPECT_FALSE(
            lookup_found(idx, ::doris::snii::format::make_phrase_bigram_term("hello", "world")))
            << shape;
    EXPECT_TRUE(lookup_found(idx, "hello")) << shape;
}

void expect_full_bigram_layout(const ::doris::snii::reader::LogicalIndexReader& idx,
                               const std::string& shape) {
    EXPECT_TRUE(lookup_found(idx, ::doris::snii::format::make_phrase_bigram_sentinel_term()))
            << shape;
    EXPECT_TRUE(lookup_found(idx, ::doris::snii::format::make_phrase_bigram_term("hello", "world")))
            << shape;
    EXPECT_TRUE(lookup_found(idx, "hello")) << shape;
}

void expect_phrase_result(const ::doris::snii::reader::LogicalIndexReader& idx,
                          const std::vector<uint32_t>& expected, const std::string& shape) {
    EXPECT_EQ(run_phrase(idx, {"hello", "world"}), expected) << shape;
}

class SniiBigramDeferWiringTest : public testing::Test {
protected:
    void SetUp() override {
        assert_ok(io::global_local_filesystem()->delete_directory(kTestDir));
        assert_ok(io::global_local_filesystem()->create_directory(kTestDir));
        _saved_defer = config::snii_bigram_defer_build_to_compaction;
        _saved_prune_min_df = config::snii_bigram_prune_min_df;
        _saved_prune_max_ratio = config::snii_bigram_prune_max_df_ratio;
        init_phrase_index_meta(&_meta);
    }

    void TearDown() override {
        config::snii_bigram_defer_build_to_compaction = _saved_defer;
        config::snii_bigram_prune_min_df = _saved_prune_min_df;
        config::snii_bigram_prune_max_df_ratio = _saved_prune_max_ratio;
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    // A baseline (config off), then with the config ON one direct-load segment
    // and one explicitly-not-direct segment through the same ColumnWriter
    // stack. ARRAY indexes preserve the full-build baseline even when direct.
    void run_wiring_scenario(const std::string& shape, const TabletColumn& column,
                             const Block& block, bool expect_direct_defer,
                             const std::vector<uint32_t>& expected_phrase) {
        config::snii_bigram_defer_build_to_compaction = false;
        write_column_segment(shape + "_baseline", column, _meta, /*is_direct_load=*/false, block);

        config::snii_bigram_defer_build_to_compaction = true;
        write_column_segment(shape + "_direct", column, _meta, /*is_direct_load=*/true, block);
        write_column_segment(shape + "_not_direct", column, _meta, /*is_direct_load=*/false, block);

        const std::string baseline_path = std::string(kTestDir) + "/" + shape + "_baseline.idx";
        const std::string baseline_bytes = read_file_bytes(baseline_path);
        ASSERT_FALSE(baseline_bytes.empty());

        // Direct load + config on: scalars defer, while arrays retain the full
        // build to preserve existing SNII full-build behavior at element
        // boundaries.
        ::doris::snii::io::LocalFileReader direct_file;
        ::doris::snii::reader::SniiSegmentReader direct_segment;
        ::doris::snii::reader::LogicalIndexReader direct_idx;
        assert_ok(direct_file.open(std::string(kTestDir) + "/" + shape + "_direct.idx"));
        assert_ok(::doris::snii::reader::SniiSegmentReader::open(&direct_file, &direct_segment));
        assert_ok(direct_segment.open_index(static_cast<uint64_t>(kIndexId), /*index_suffix=*/"",
                                            &direct_idx));
        if (expect_direct_defer) {
            expect_deferred_bigram_layout(direct_idx, shape);
        } else {
            expect_file_bytes_equal(std::string(kTestDir) + "/" + shape + "_direct.idx",
                                    baseline_bytes, shape);
            expect_full_bigram_layout(direct_idx, shape);
        }
        expect_phrase_result(direct_idx, expected_phrase, shape);

        // Not a direct load, config on: full build, byte-identical to the
        // config-off baseline. An inverted forward (deferring compaction /
        // schema change instead of loads) fails here.
        expect_file_bytes_equal(std::string(kTestDir) + "/" + shape + "_not_direct.idx",
                                baseline_bytes, shape);
        ::doris::snii::io::LocalFileReader full_file;
        ::doris::snii::reader::SniiSegmentReader full_segment;
        ::doris::snii::reader::LogicalIndexReader full_idx;
        assert_ok(full_file.open(std::string(kTestDir) + "/" + shape + "_not_direct.idx"));
        assert_ok(::doris::snii::reader::SniiSegmentReader::open(&full_file, &full_segment));
        assert_ok(full_segment.open_index(static_cast<uint64_t>(kIndexId), /*index_suffix=*/"",
                                          &full_idx));
        expect_full_bigram_layout(full_idx, shape);
        expect_phrase_result(full_idx, expected_phrase, shape);
    }

    TabletIndex _meta;

private:
    bool _saved_defer = false;
    int32_t _saved_prune_min_df = 0;
    double _saved_prune_max_ratio = 0.0;
};

// ScalarColumnWriter::init forwards opts.is_direct_load to the SNII index
// writer (the scalar leg of the column_writer.cpp wiring).
TEST_F(SniiBigramDeferWiringTest, ScalarColumnWriterForwardsDirectLoadHint) {
    const auto expected_phrase = phrase_docids(80);
    run_wiring_scenario("scalar", make_string_column(), make_string_block(),
                        /*expect_direct_defer=*/true, expected_phrase);
}

// ArrayColumnWriter passes single_field=false to SNII. Direct array loads keep
// the full bigram layout, including its legacy element-boundary behavior.
TEST_F(SniiBigramDeferWiringTest, ArrayColumnWriterKeepsElementBoundaries) {
    // Force the legacy full layout so a missing pair has the existing
    // unambiguous "no adjacency" meaning. This makes the boundary regression
    // independent of the ambient df-prune configuration.
    config::snii_bigram_prune_min_df = 0;
    config::snii_bigram_prune_max_df_ratio = 0.0;
    const auto expected_phrase = phrase_docids(80);
    run_wiring_scenario("array", make_array_string_column(), make_array_string_block(),
                        /*expect_direct_defer=*/false, expected_phrase);
}

} // namespace
} // namespace doris::segment_v2
