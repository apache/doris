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

// A segment can be handed to VerticalSegmentWriter one column group at a time --
// what compaction does -- or as one whole-schema block through write_block(),
// which is what a load flush does. The two produce the same rows and the same
// key bounds; they do not produce the same bytes, because a group writes its own
// index sections right after its data, so the column group segment interleaves
// the two.
//
// These tests drive both shapes over identical rows, read both segments back and
// check the rows and the primary key index match. They also cover the row store
// column, whose values are generated from whole rows rather than read out of the
// block: write_block() pumps it from its generator in bounded batches.

#include <gtest/gtest.h>

#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/consts.h"
#include "core/block/block.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/field.h"
#include "io/fs/local_file_system.h"
#include "storage/index/indexed_column_reader.h"
#include "storage/index/primary_key_index.h"
#include "storage/index/short_key_index.h"
#include "storage/iterators.h"
#include "storage/merger.h"
#include "storage/olap_define.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/schema.h"
#include "storage/segment/segment.h"
#include "storage/segment/vertical_segment_writer.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/transform/block_transform.h"
#include "storage/utils.h"

namespace doris::segment_v2 {

static const std::string kSegmentDir = "./ut_dir/segment_writer_write_paths_test";
// Small enough that kNumRows fills several primary key index data pages.
static constexpr int32_t kPrimaryKeyDataPageSize = 1024;
static constexpr size_t kNumRows = 512;
// A short key every 64 rows, so a segment holds several and the later ones come from
// the rows of appends past the first.
static constexpr uint32_t kRowsPerShortKey = 64;
static constexpr int64_t kTabletId = 10000;

namespace {

TabletColumnPtr create_int_key(int32_t id) {
    auto column = std::make_shared<TabletColumn>();
    column->_unique_id = id;
    column->_col_name = std::to_string(id);
    column->_type = FieldType::OLAP_FIELD_TYPE_INT;
    column->_is_key = true;
    column->_is_nullable = false;
    column->_length = 4;
    column->_index_length = 4;
    return column;
}

TabletColumnPtr create_int_value(int32_t id) {
    auto column = std::make_shared<TabletColumn>();
    column->_unique_id = id;
    column->_col_name = std::to_string(id);
    column->_type = FieldType::OLAP_FIELD_TYPE_INT;
    column->_is_key = false;
    column->_is_nullable = true;
    column->_length = 4;
    column->_index_length = 4;
    return column;
}

TabletColumnPtr create_seq_column(int32_t id) {
    auto column = create_int_value(id);
    column->_col_name = SEQUENCE_COL;
    return column;
}

TabletColumn create_child(int32_t id, FieldType type, int32_t length) {
    TabletColumn column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE, type,
                        /*is_nullable=*/true);
    column.set_unique_id(id);
    column.set_name(std::to_string(id));
    column.set_length(length);
    column.set_index_length(length);
    return column;
}

TabletColumnPtr create_complex_value(int32_t id, FieldType type, int32_t length,
                                     std::vector<TabletColumn> children) {
    auto column = create_int_value(id);
    column->_type = type;
    column->_length = length;
    column->_index_length = length;
    for (auto& child : children) {
        column->add_sub_column(child);
    }
    return column;
}

bool is_complex(const TabletColumn& column) {
    return column.type() == FieldType::OLAP_FIELD_TYPE_ARRAY ||
           column.type() == FieldType::OLAP_FIELD_TYPE_MAP ||
           column.type() == FieldType::OLAP_FIELD_TYPE_STRUCT;
}

// One key column and two value columns, so a column group split has something
// to split. A sequence column goes last, so the key group it joins holds it at
// another position than the schema does. The complex columns are an ARRAY<INT>,
// a MAP<INT, INT>, a STRUCT<CHAR(4), INT> and an ARRAY<CHAR(4)>.
TabletSchemaSPtr create_schema(KeysType keys_type, bool with_cluster_key,
                               bool with_key_column = true, bool with_seq = false,
                               bool with_complex = false) {
    TabletSchemaSPtr schema = std::make_shared<TabletSchema>();
    if (with_key_column) {
        schema->append_column(*create_int_key(0));
    } else {
        schema->append_column(*create_int_value(0));
    }
    schema->append_column(*create_int_value(1));
    schema->append_column(*create_int_value(2));
    if (with_seq) {
        schema->append_column(*create_seq_column(3));
    }
    if (with_complex) {
        constexpr auto INT = FieldType::OLAP_FIELD_TYPE_INT;
        constexpr auto CHAR = FieldType::OLAP_FIELD_TYPE_CHAR;
        schema->append_column(*create_complex_value(4, FieldType::OLAP_FIELD_TYPE_ARRAY,
                                                    OLAP_ARRAY_MAX_LENGTH,
                                                    {create_child(41, INT, 4)}));
        schema->append_column(
                *create_complex_value(5, FieldType::OLAP_FIELD_TYPE_MAP, OLAP_MAP_MAX_LENGTH,
                                      {create_child(51, INT, 4), create_child(52, INT, 4)}));
        schema->append_column(
                *create_complex_value(6, FieldType::OLAP_FIELD_TYPE_STRUCT, OLAP_STRUCT_MAX_LENGTH,
                                      {create_child(61, CHAR, 4), create_child(62, INT, 4)}));
        schema->append_column(*create_complex_value(7, FieldType::OLAP_FIELD_TYPE_ARRAY,
                                                    OLAP_ARRAY_MAX_LENGTH,
                                                    {create_child(71, CHAR, 4)}));
    }
    schema->_keys_type = keys_type;
    schema->_num_short_key_columns = with_key_column ? 1 : 0;
    if (with_cluster_key) {
        // sort the segment by the first value column instead of the key column
        schema->_cluster_key_uids = {1};
    }
    return schema;
}

// Every fifth row has a NULL sequence value.
bool seq_is_null(size_t row) {
    return row % 5 == 0;
}

// Row `row` of a complex column: NULL every seventh row, otherwise up to two elements, some of
// them NULL, so the offsets, the items and both levels of null bits change from row to row.
Field complex_value(const TabletColumn& column, size_t row) {
    if (row % 7 == 0) {
        return {};
    }
    const auto r = static_cast<int32_t>(row);
    const auto int_item = [](int32_t value) {
        return value % 5 == 0 ? Field() : Field::create_field<TYPE_INT>(value);
    };
    const auto char_item = [](int32_t value) {
        return value % 4 == 0 ? Field()
                              : Field::create_field<TYPE_STRING>(std::to_string(value % 1000));
    };
    switch (column.type()) {
    case FieldType::OLAP_FIELD_TYPE_ARRAY: {
        Array items;
        for (int32_t i = 0; i < r % 3; ++i) {
            items.push_back(column.get_sub_column(0).type() == FieldType::OLAP_FIELD_TYPE_CHAR
                                    ? char_item(r + i)
                                    : int_item(r * 10 + i));
        }
        return Field::create_field<TYPE_ARRAY>(items);
    }
    case FieldType::OLAP_FIELD_TYPE_MAP: {
        Array keys;
        Array values;
        for (int32_t i = 0; i < r % 3; ++i) {
            keys.push_back(Field::create_field<TYPE_INT>(r * 10 + i));
            values.push_back(int_item(r * 100 + i));
        }
        return Field::create_field<TYPE_MAP>(Map {Field::create_field<TYPE_ARRAY>(keys),
                                                  Field::create_field<TYPE_ARRAY>(values)});
    }
    default: {
        Struct fields;
        fields.push_back(char_item(r));
        fields.push_back(int_item(r));
        return Field::create_field<TYPE_STRUCT>(fields);
    }
    }
}

// Rows in the order the segment stores them: the key column descends and the
// first value column ascends, so both are unique and the block is already sorted
// by whichever of them the table sorts by.
Block create_block(const TabletSchemaSPtr& schema, bool sorted_by_value) {
    Block block = schema->create_storage_block();
    auto key_column = block.get_by_position(0).column->assert_mutable();
    auto sort_column = block.get_by_position(1).column->assert_mutable();
    auto plain_column = block.get_by_position(2).column->assert_mutable();
    for (size_t row = 0; row < kNumRows; ++row) {
        auto ascending = static_cast<int32_t>(row);
        auto descending = static_cast<int32_t>(kNumRows - row);
        int32_t key = sorted_by_value ? descending : ascending;
        int32_t sort_value = sorted_by_value ? ascending : descending;
        key_column->insert_data(reinterpret_cast<const char*>(&key), sizeof(int32_t));
        sort_column->insert_data(reinterpret_cast<const char*>(&sort_value), sizeof(int32_t));
        plain_column->insert_data(reinterpret_cast<const char*>(&ascending), sizeof(int32_t));
    }
    block.replace_by_position(0, std::move(key_column));
    block.replace_by_position(1, std::move(sort_column));
    block.replace_by_position(2, std::move(plain_column));
    if (schema->has_sequence_col()) {
        const auto seq_idx = static_cast<size_t>(schema->sequence_col_idx());
        auto seq_column = block.get_by_position(seq_idx).column->assert_mutable();
        for (size_t row = 0; row < kNumRows; ++row) {
            auto seq = static_cast<int32_t>(row);
            if (seq_is_null(row)) {
                seq_column->insert_default();
            } else {
                seq_column->insert_data(reinterpret_cast<const char*>(&seq), sizeof(int32_t));
            }
        }
        block.replace_by_position(seq_idx, std::move(seq_column));
    }
    for (size_t cid = 0; cid < schema->num_columns(); ++cid) {
        if (!is_complex(schema->column(cid))) {
            continue;
        }
        auto values = block.get_by_position(cid).column->assert_mutable();
        for (size_t row = 0; row < kNumRows; ++row) {
            values->insert(complex_value(schema->column(cid), row));
        }
        block.replace_by_position(cid, std::move(values));
    }
    return block;
}

std::string read_file(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    EXPECT_TRUE(in.good()) << "cannot read " << path;
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

// Fills the derived column with one default value per row, in batches, the way
// the row store generator does, and counts the rows it was asked for.
class CountingGenerator : public DerivedColumnGenerator {
public:
    size_t generate(const Block& block, size_t row_pos, size_t max_rows, size_t max_bytes,
                    IColumn* dst) const override {
        EXPECT_GT(max_rows, 0);
        EXPECT_LE(row_pos + max_rows, block.rows());
        for (size_t i = 0; i < max_rows; ++i) {
            dst->insert_default();
        }
        rows_generated += max_rows;
        return max_rows;
    }

    mutable size_t rows_generated = 0;
};

} // namespace

class VerticalSegmentWriterWritePathsTest : public testing::Test {
public:
    void SetUp() override {
        auto fs = io::global_local_filesystem();
        auto st = fs->delete_directory(kSegmentDir);
        ASSERT_TRUE(st.ok() || st.is<ErrorCode::NOT_FOUND>()) << st;
        ASSERT_TRUE(fs->create_directory(kSegmentDir).ok());
        _saved_primary_key_data_page_size = config::primary_key_data_page_size;
    }

    void TearDown() override {
        config::primary_key_data_page_size = _saved_primary_key_data_page_size;
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kSegmentDir).ok());
    }

protected:
    // Makes the primary key index fill several data pages at kNumRows, so the
    // tests cover a segment whose index really is written page by page.
    static void shrink_primary_key_index_pages() {
        config::primary_key_data_page_size = kPrimaryKeyDataPageSize;
    }

    struct WrittenSegment {
        std::string bytes;
        uint32_t row_count = 0;
        std::string min_key;
        std::string max_key;
        uint64_t index_size = 0;
        // what a reader gets back: every row, and every primary key index entry
        // in index order, with the sequence and row id suffixes still attached
        std::string rows;
        std::vector<std::string> primary_key_entries;
        // the short key index entries of a table without a primary key index
        std::vector<std::string> short_key_entries;
        // the primary key index fits in one data page
        bool primary_key_index_single_page = true;
    };

    // The writer keeps a pointer into rowset_ctx, so the holder lives on the
    // heap and never moves.
    struct WriterHolder {
        RowsetWriterContext rowset_ctx;
        VerticalSegmentWriterOptions opts;
        io::FileWriterPtr file_writer;
        std::unique_ptr<VerticalSegmentWriter> writer;
        std::string path;
    };

    std::unique_ptr<WriterHolder> make_writer(const TabletSchemaSPtr& schema, bool mow,
                                              const std::string& name) {
        auto holder = std::make_unique<WriterHolder>();
        holder->path = fmt::format("{}/{}.dat", kSegmentDir, name);
        EXPECT_TRUE(io::global_local_filesystem()
                            ->create_file(holder->path, &holder->file_writer)
                            .ok());
        holder->rowset_ctx.tablet_id = kTabletId;
        holder->rowset_ctx.tablet_schema = schema;
        holder->opts.enable_unique_key_merge_on_write = mow;
        holder->opts.rowset_ctx = &holder->rowset_ctx;
        holder->opts.num_rows_per_block = kRowsPerShortKey;
        holder->writer = std::make_unique<VerticalSegmentWriter>(holder->file_writer.get(),
                                                                 /*segment_id=*/0, schema, nullptr,
                                                                 nullptr, holder->opts, nullptr);
        return holder;
    }

    // Opens the segment the way a reader does and pulls every row out of it,
    // plus the primary key index when the table has one.
    static void read_back(const std::string& path, const TabletSchemaSPtr& schema, bool mow,
                          WrittenSegment* out) {
        RowsetId rowset_id;
        rowset_id.init(10002);
        std::shared_ptr<Segment> segment;
        ASSERT_TRUE(Segment::open(io::global_local_filesystem(), path, kTabletId,
                                  /*segment_id=*/0, rowset_id, schema, io::FileReaderOptions {},
                                  &segment)
                            .ok());
        auto read_schema = std::make_shared<ReadSchema>(schema->columns());
        OlapReaderStatistics stats;
        StorageReadOptions read_options(stats);
        std::unique_ptr<RowwiseIterator> iterator;
        ASSERT_TRUE(segment->new_iterator(read_schema, read_options, &iterator).ok());
        MutableBlock contents(schema->create_storage_block());
        while (true) {
            Block batch = schema->create_storage_block();
            auto st = iterator->next_batch(&batch);
            if (st.is<ErrorCode::END_OF_FILE>()) {
                break;
            }
            ASSERT_TRUE(st.ok()) << st;
            ASSERT_TRUE(contents.add_rows(&batch, 0, batch.rows()).ok());
        }
        Block rows = contents.to_block();
        ASSERT_EQ(rows.rows(), segment->num_rows());
        out->rows = rows.dump_data(0, rows.rows());
        if (mow) {
            ASSERT_TRUE(segment->load_pk_index_and_bf(&stats).ok());
            const auto* pk_index = segment->get_primary_key_index();
            ASSERT_NE(pk_index, nullptr);
            ASSERT_EQ(pk_index->num_rows(), segment->num_rows());
            std::unique_ptr<IndexedColumnIterator> entries;
            ASSERT_TRUE(pk_index->new_iterator(&entries, nullptr).ok());
            auto entry_type = DataTypeFactory::instance().create_data_type(pk_index->type(), 1, 0);
            auto entry_column = entry_type->create_column();
            ASSERT_TRUE(entries->seek_to_ordinal(0).ok());
            size_t num_read = segment->num_rows();
            ASSERT_TRUE(entries->next_batch(&num_read, entry_column).ok());
            ASSERT_EQ(num_read, segment->num_rows());
            for (size_t i = 0; i < num_read; ++i) {
                out->primary_key_entries.push_back(entry_column->get_data_at(i).to_string());
            }
            out->primary_key_index_single_page = segment->_pk_index_meta->primary_key_index()
                                                         .ordinal_index_meta()
                                                         .is_root_data_page();
        } else {
            ASSERT_TRUE(segment->load_index(&stats).ok());
            const auto* short_keys = segment->get_short_key_index();
            ASSERT_NE(short_keys, nullptr);
            for (uint32_t i = 0; i < short_keys->num_items(); ++i) {
                out->short_key_entries.push_back(short_keys->key(i).to_string());
            }
        }
    }

    // The row id a cluster key table appends to every primary key index entry:
    // a marker byte, then the row id big-endian.
    static uint32_t rowid_of(const std::string& entry) {
        EXPECT_GE(entry.size(), PrimaryKeyIndexReader::ROW_ID_LENGTH);
        const auto* bytes = reinterpret_cast<const uint8_t*>(entry.data()) + entry.size() - 4;
        return (uint32_t(bytes[0]) << 24) | (uint32_t(bytes[1]) << 16) | (uint32_t(bytes[2]) << 8) |
               uint32_t(bytes[3]);
    }

    static void collect(WriterHolder& holder, const TabletSchemaSPtr& schema, bool mow,
                        WrittenSegment* out) {
        out->row_count = holder.writer->row_count();
        out->min_key = holder.writer->min_encoded_key().to_string();
        out->max_key = holder.writer->max_encoded_key().to_string();
        out->bytes = read_file(holder.path);
        ASSERT_NO_FATAL_FAILURE(read_back(holder.path, schema, mow, out));
    }

    // One group per value column, key columns together in the first group --
    // what compaction hands the writer. Each group's rows arrive in
    // `append_batches` append_block calls.
    void write_by_column_groups(const TabletSchemaSPtr& schema, const Block& block, bool mow,
                                const std::string& name, size_t append_batches,
                                WrittenSegment* out) {
        auto holder = make_writer(schema, mow, name);
        std::vector<std::vector<uint32_t>> column_groups;
        std::vector<uint32_t> key_group_cluster_key_idxes;
        Merger::vertical_split_columns(*schema, &column_groups, &key_group_cluster_key_idxes,
                                       /*num_columns_per_group=*/1);
        for (size_t group = 0; group < column_groups.size(); ++group) {
            const auto& column_ids = column_groups[group];
            ASSERT_TRUE(holder->writer->init(column_ids, /*has_key=*/group == 0).ok());
            Block group_block;
            for (auto cid : column_ids) {
                group_block.insert(block.get_by_position(cid));
            }
            const size_t rows = group_block.rows();
            const size_t batch_rows = (rows + append_batches - 1) / append_batches;
            for (size_t row_pos = 0; row_pos < rows; row_pos += batch_rows) {
                size_t num_rows = std::min(batch_rows, rows - row_pos);
                ASSERT_TRUE(holder->writer->append_block(&group_block, row_pos, num_rows).ok());
            }
            uint64_t group_index_size = 0;
            ASSERT_TRUE(holder->writer->finalize_columns(&group_index_size).ok());
        }
        auto result_row_count = holder->writer->row_count();
        uint64_t segment_file_size = 0;
        ASSERT_TRUE(holder->writer->finalize_footer(&segment_file_size).ok());
        collect(*holder, schema, mow, out);
        ASSERT_EQ(out->row_count, result_row_count);
        ASSERT_EQ(out->bytes.size(), segment_file_size);
    }

    // The whole block in one write_block call: the load flush shape.
    void write_whole_block(const TabletSchemaSPtr& schema, const Block& block, bool mow,
                           const std::string& name, WrittenSegment* out) {
        auto holder = make_writer(schema, mow, name);
        ASSERT_TRUE(holder->writer->write_block(&block, 0, block.rows()).ok());
        uint64_t segment_file_size = 0;
        uint64_t index_size = 0;
        ASSERT_TRUE(holder->writer->finalize_columns(&index_size).ok());
        ASSERT_TRUE(holder->writer->finalize_footer(&segment_file_size).ok());
        collect(*holder, schema, mow, out);
        out->index_size = index_size;
        ASSERT_EQ(out->bytes.size(), segment_file_size);
    }

    void expect_both_shapes_agree(KeysType keys_type, bool mow, bool with_cluster_key,
                                  const std::string& name, size_t append_batches = 1,
                                  bool with_seq = false, bool with_complex = false) {
        auto schema = create_schema(keys_type, with_cluster_key, /*with_key_column=*/true, with_seq,
                                    with_complex);
        Block block = create_block(schema, with_cluster_key);
        WrittenSegment grouped;
        WrittenSegment whole;
        ASSERT_NO_FATAL_FAILURE(write_by_column_groups(schema, block, mow, name + "_groups",
                                                       append_batches, &grouped));
        ASSERT_NO_FATAL_FAILURE(write_whole_block(schema, block, mow, name + "_whole", &whole));
        EXPECT_EQ(grouped.row_count, kNumRows) << name;
        EXPECT_EQ(whole.row_count, kNumRows) << name;
        EXPECT_EQ(grouped.min_key, whole.min_key) << name;
        EXPECT_EQ(grouped.max_key, whole.max_key) << name;
        EXPECT_FALSE(grouped.bytes.empty()) << name;
        // both segments hand a reader back exactly the rows that went in
        EXPECT_EQ(grouped.rows, block.dump_data(0, block.rows())) << name;
        EXPECT_EQ(whole.rows, block.dump_data(0, block.rows())) << name;
        EXPECT_EQ(grouped.primary_key_entries, whole.primary_key_entries) << name;
        EXPECT_EQ(grouped.short_key_entries, whole.short_key_entries) << name;
        if (!mow) {
            EXPECT_EQ(whole.short_key_entries.size(), kNumRows / kRowsPerShortKey) << name;
        }
        if (with_seq) {
            // Entry i holds the i-th smallest key, and its sequence suffix
            // starts after the key marker and the four key bytes.
            ASSERT_EQ(grouped.primary_key_entries.size(), kNumRows) << name;
            for (size_t i = 0; i < kNumRows; ++i) {
                const size_t row = with_cluster_key ? kNumRows - 1 - i : i;
                EXPECT_EQ(static_cast<uint8_t>(grouped.primary_key_entries[i][5]),
                          seq_is_null(row) ? KeyConsts::KEY_NULL_FIRST_MARKER
                                           : KeyConsts::KEY_NORMAL_MARKER)
                        << name << " entry " << i;
            }
        }
        if (with_cluster_key) {
            // The keys run kNumRows..1 down the block, so the i-th smallest key
            // sits in row kNumRows-1-i: every entry must point back at its row.
            ASSERT_EQ(grouped.primary_key_entries.size(), kNumRows) << name;
            for (size_t i = 0; i < kNumRows; ++i) {
                EXPECT_EQ(rowid_of(grouped.primary_key_entries[i]), kNumRows - 1 - i)
                        << name << " entry " << i;
            }
        }
    }

private:
    int32_t _saved_primary_key_data_page_size = 0;
};

TEST_F(VerticalSegmentWriterWritePathsTest, DupKeysColumnGroupsMatchWholeSchema) {
    expect_both_shapes_agree(DUP_KEYS, /*mow=*/false, /*with_cluster_key=*/false, "dup");
}

// Every append after the first hands the writer a row_pos past zero: the short keys
// must come from the rows from row_pos on, not from the start of the block.
TEST_F(VerticalSegmentWriterWritePathsTest, DupKeysSeveralAppendsMatchWholeSchema) {
    expect_both_shapes_agree(DUP_KEYS, /*mow=*/false, /*with_cluster_key=*/false, "dup_appends",
                             /*append_batches=*/3);
}

// From the second append on, each ARRAY, MAP and STRUCT writer gets a row_pos past zero and
// child writers already holding the earlier rows' elements: the offsets, the elements, the
// padded CHAR items and the null bits must carry on from them.
TEST_F(VerticalSegmentWriterWritePathsTest, ComplexColumnsSeveralAppendsMatchWholeSchema) {
    expect_both_shapes_agree(DUP_KEYS, /*mow=*/false, /*with_cluster_key=*/false, "complex_appends",
                             /*append_batches=*/3, /*with_seq=*/false,
                             /*with_complex=*/true);
}

TEST_F(VerticalSegmentWriterWritePathsTest, AggKeysSeveralAppendsMatchWholeSchema) {
    expect_both_shapes_agree(AGG_KEYS, /*mow=*/false, /*with_cluster_key=*/false, "agg_appends",
                             /*append_batches=*/3);
}

TEST_F(VerticalSegmentWriterWritePathsTest, MowColumnGroupsMatchWholeSchema) {
    expect_both_shapes_agree(UNIQUE_KEYS, /*mow=*/true, /*with_cluster_key=*/false, "mow");
}

// The cluster key case buffers its primary keys and sorts them when the key
// group finalizes, so the index pages land wherever that sort runs; the page
// size is turned down so that really is several pages.
TEST_F(VerticalSegmentWriterWritePathsTest, MowWithClusterKeyColumnGroupsMatchWholeSchema) {
    shrink_primary_key_index_pages();
    expect_both_shapes_agree(UNIQUE_KEYS, /*mow=*/true, /*with_cluster_key=*/true,
                             "mow_cluster_key");
}

// Compaction feeds a group one block at a time. Every primary key carries the
// row id of its row, which must keep counting across those calls.
TEST_F(VerticalSegmentWriterWritePathsTest, MowWithClusterKeySeveralAppendsMatchWholeSchema) {
    shrink_primary_key_index_pages();
    expect_both_shapes_agree(UNIQUE_KEYS, /*mow=*/true, /*with_cluster_key=*/true,
                             "mow_cluster_key_appends", /*append_batches=*/3);
}

// Every append after the first hands the writer a row_pos past zero: the keys
// must come from the rows from row_pos on, not from the start of the block.
TEST_F(VerticalSegmentWriterWritePathsTest, MowSeveralAppendsMatchWholeSchema) {
    expect_both_shapes_agree(UNIQUE_KEYS, /*mow=*/true, /*with_cluster_key=*/false, "mow_appends",
                             /*append_batches=*/3);
}

// The key group holds the sequence column right after the key column, not where
// the schema has it; every primary key index entry carries its value as the
// suffix, a NULL one included.
TEST_F(VerticalSegmentWriterWritePathsTest, MowWithSequenceColumnGroupsMatchWholeSchema) {
    expect_both_shapes_agree(UNIQUE_KEYS, /*mow=*/true, /*with_cluster_key=*/false, "mow_seq",
                             /*append_batches=*/3, /*with_seq=*/true);
}

// With cluster keys each primary key index entry carries the sequence value and
// then the row id.
TEST_F(VerticalSegmentWriterWritePathsTest,
       MowWithClusterKeyAndSequenceColumnGroupsMatchWholeSchema) {
    shrink_primary_key_index_pages();
    expect_both_shapes_agree(UNIQUE_KEYS, /*mow=*/true, /*with_cluster_key=*/true,
                             "mow_cluster_key_seq", /*append_batches=*/3, /*with_seq=*/true);
}

// The primary key index really does span several data pages at this row count,
// so the tests above cover a segment whose index is written page by page.
TEST_F(VerticalSegmentWriterWritePathsTest, ClusterKeyPrimaryKeyIndexSpansSeveralDataPages) {
    shrink_primary_key_index_pages();
    auto schema = create_schema(UNIQUE_KEYS, /*with_cluster_key=*/true);
    Block block = create_block(schema, /*sorted_by_value=*/true);
    WrittenSegment whole;
    ASSERT_NO_FATAL_FAILURE(write_whole_block(schema, block, /*mow=*/true, "page_span", &whole));
    EXPECT_GT(whole.index_size, static_cast<uint64_t>(kPrimaryKeyDataPageSize));
    EXPECT_FALSE(whole.primary_key_index_single_page);
}

// The row store column is generated from whole rows, so write_block pumps it
// from its generator in bounded batches instead of reading it from the block.
TEST_F(VerticalSegmentWriterWritePathsTest, GeneratedColumnIsPumpedByWriteBlock) {
    auto schema = create_schema(DUP_KEYS, /*with_cluster_key=*/false);
    Block block = create_block(schema, /*sorted_by_value=*/false);
    auto holder = make_writer(schema, /*mow=*/false, "generated");
    const uint32_t derived_cid = 2;
    auto generator = std::make_shared<CountingGenerator>();
    holder->writer->set_derived_column({derived_cid, generator});
    ASSERT_TRUE(holder->writer->write_block(&block, 0, block.rows()).ok());
    uint64_t segment_file_size = 0;
    uint64_t index_size = 0;
    ASSERT_TRUE(holder->writer->finalize_columns(&index_size).ok());
    ASSERT_TRUE(holder->writer->finalize_footer(&segment_file_size).ok());
    EXPECT_EQ(holder->writer->row_count(), kNumRows);
    EXPECT_EQ(generator->rows_generated, kNumRows);
    EXPECT_GT(segment_file_size, 0);
}

// A duplicate table without key columns gets no key group from
// vertical_split_columns, so the first value group carries has_key=true --
// matching what the whole-schema init() always did. Both shapes must agree.
TEST_F(VerticalSegmentWriterWritePathsTest, KeylessDupColumnGroupsMatchWholeSchema) {
    auto schema = create_schema(DUP_KEYS, /*with_cluster_key=*/false, /*with_key_column=*/false);
    Block block = create_block(schema, /*sorted_by_value=*/false);
    WrittenSegment grouped;
    WrittenSegment whole;
    ASSERT_NO_FATAL_FAILURE(
            write_by_column_groups(schema, block, /*mow=*/false, "keyless_groups", 1, &grouped));
    ASSERT_NO_FATAL_FAILURE(
            write_whole_block(schema, block, /*mow=*/false, "keyless_whole", &whole));
    EXPECT_EQ(grouped.row_count, kNumRows);
    EXPECT_EQ(whole.row_count, kNumRows);
    EXPECT_EQ(grouped.min_key, whole.min_key);
    EXPECT_EQ(grouped.max_key, whole.max_key);
    EXPECT_EQ(grouped.rows, block.dump_data(0, block.rows()));
    EXPECT_EQ(whole.rows, block.dump_data(0, block.rows()));
}

} // namespace doris::segment_v2
