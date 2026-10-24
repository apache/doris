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

// End-to-end coverage of BlockReader over real rowsets: every (table model, read mode) pair of
// BlockReader::_init_block_iterator writes its own segments with RowsetWriter, reads them back
// through BlockReader::init + next_block_with_aggregation, and checks both the
// TabletBlockIterator that was picked and the rows it yields. The rowsets overlap on keys and
// have several segments, so the segment merge, the rowset merge and the output Block
// limits all take part; the batch sizes are small so the group and Block boundaries show up.

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#endif
#include "storage/iterator/agg_key_block_iterator.h"
#include "storage/iterator/block_reader.h"
#include "storage/iterator/changelog_block_iterator.h"
#include "storage/iterator/direct_block_iterator.h"
#include "storage/iterator/sequence_map_block_iterator.h"
#include "storage/iterator/unique_key_block_iterator.h"
#if defined(__clang__)
#pragma clang diagnostic pop
#endif

#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "storage/changelog.h"
#include "storage/iterator/changelog_block_reader_utils.h"
#include "storage/olap_common.h"
#include "storage/options.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/schema.h"
#include "storage/storage_engine.h"
#include "storage/tablet/base_tablet.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_reader.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"
#include "util/uid_util.h"

namespace doris {

namespace {

constexpr uint32_t kMaxPathLen = 1024;
constexpr int kVarcharLength = 65533;

// One tablet column of the test schemas. VARCHAR cells are written as the decimal text of the
// row value, so every cell of a row is an int64_t in the tests.
struct ColumnSpec {
    ColumnSpec(std::string name_, std::string type_, bool is_key_ = false,
               std::string aggregation_ = "")
            : name(std::move(name_)),
              type(std::move(type_)),
              is_key(is_key_),
              aggregation(std::move(aggregation_)) {}

    std::string name;
    std::string type; // INT, BIGINT, TINYINT or VARCHAR
    bool is_key;
    std::string aggregation; // AGG_KEYS value columns: SUM, MAX or REPLACE
};

using Row = std::vector<int64_t>;
using SequenceMapSpec = std::vector<std::pair<std::string, std::vector<std::string>>>;

int column_length(const std::string& type) {
    if (type == "INT") {
        return 4;
    }
    if (type == "BIGINT") {
        return 8;
    }
    if (type == "TINYINT") {
        return 1;
    }
    DORIS_CHECK_EQ(type, "VARCHAR");
    return kVarcharLength;
}

TPrimitiveType::type thrift_type(FieldType type) {
    switch (type) {
    case FieldType::OLAP_FIELD_TYPE_INT:
        return TPrimitiveType::INT;
    case FieldType::OLAP_FIELD_TYPE_BIGINT:
        return TPrimitiveType::BIGINT;
    case FieldType::OLAP_FIELD_TYPE_TINYINT:
        return TPrimitiveType::TINYINT;
    case FieldType::OLAP_FIELD_TYPE_VARCHAR:
        return TPrimitiveType::VARCHAR;
    default:
        DORIS_CHECK(false) << "unexpected test column type " << static_cast<int>(type);
        return TPrimitiveType::INVALID_TYPE;
    }
}

TKeysType::type thrift_keys_type(KeysType keys_type) {
    switch (keys_type) {
    case KeysType::DUP_KEYS:
        return TKeysType::DUP_KEYS;
    case KeysType::UNIQUE_KEYS:
        return TKeysType::UNIQUE_KEYS;
    case KeysType::AGG_KEYS:
        return TKeysType::AGG_KEYS;
    default:
        DORIS_CHECK(false) << "unexpected keys type " << static_cast<int>(keys_type);
        return TKeysType::DUP_KEYS;
    }
}

void append_cell(IColumn& column, FieldType type, int64_t value) {
    switch (type) {
    case FieldType::OLAP_FIELD_TYPE_INT: {
        auto v = static_cast<int32_t>(value);
        column.insert_data(reinterpret_cast<const char*>(&v), sizeof(v));
        return;
    }
    case FieldType::OLAP_FIELD_TYPE_BIGINT: {
        column.insert_data(reinterpret_cast<const char*>(&value), sizeof(value));
        return;
    }
    case FieldType::OLAP_FIELD_TYPE_TINYINT: {
        auto v = static_cast<int8_t>(value);
        column.insert_data(reinterpret_cast<const char*>(&v), sizeof(v));
        return;
    }
    case FieldType::OLAP_FIELD_TYPE_VARCHAR: {
        const std::string text = std::to_string(value);
        column.insert_data(text.data(), text.size());
        return;
    }
    default:
        DORIS_CHECK(false) << "unexpected test column type " << static_cast<int>(type);
    }
}

int64_t read_cell(const IColumn& column, size_t row) {
    const IColumn* col = &column;
    if (const auto* nullable = check_and_get_column<ColumnNullable>(*col)) {
        DORIS_CHECK(!nullable->is_null_at(row));
        col = &nullable->get_nested_column();
    }
    if (const auto* string_column = check_and_get_column<ColumnString>(*col)) {
        return std::stoll(string_column->get_data_at(row).to_string());
    }
    return col->get_int(row);
}

template <typename IteratorType>
bool uses_iterator(const BlockReader& reader) {
    return dynamic_cast<const IteratorType*>(reader._block_iter.get()) != nullptr;
}

std::vector<Row> sorted(std::vector<Row> rows) {
    std::ranges::sort(rows);
    return rows;
}

// A BlockReader together with the ReaderParams it was initialized from: TabletReader keeps
// pointers into the params, so they must outlive the read.
struct OpenedReader {
    TabletReader::ReaderParams params;
    BlockReader reader;
    bool initialized = false;

    // Read every Block until eof and return the rows in output order.
    std::vector<Row> drain() {
        std::vector<Row> rows;
        if (!initialized) {
            ADD_FAILURE() << "reader was not initialized";
            return rows;
        }
        bool eof = false;
        int guard = 0;
        while (!eof) {
            Block block = params.read_schema->create_read_block();
            Status st = reader.next_block_with_aggregation(&block, &eof);
            EXPECT_TRUE(st.ok()) << st;
            if (!st.ok()) {
                break;
            }
            for (size_t r = 0; r < block.rows(); ++r) {
                Row row;
                for (size_t c = 0; c < block.columns(); ++c) {
                    row.push_back(read_cell(*block.get_by_position(c).column, r));
                }
                rows.push_back(std::move(row));
            }
            if (++guard >= 1000) {
                ADD_FAILURE() << "drain did not terminate";
                break;
            }
        }
        return rows;
    }
};

// How a test opens its BlockReader; the setters chain, e.g. ReadSpec().direct().batch(2).
struct ReadSpec {
    ReaderType reader_type = ReaderType::READER_QUERY;
    bool direct_mode = false;
    bool read_changelog = false;
    TChangelogScanType::type changelog_scan_type = TChangelogScanType::NONE;
    size_t batch_size = 3;
    DeleteBitmapPtr delete_bitmap;

    ReadSpec& type(ReaderType value) {
        reader_type = value;
        return *this;
    }
    ReadSpec& direct() {
        direct_mode = true;
        return *this;
    }
    ReadSpec& changelog(TChangelogScanType::type scan_type) {
        read_changelog = true;
        changelog_scan_type = scan_type;
        return *this;
    }
    ReadSpec& batch(size_t rows) {
        batch_size = rows;
        return *this;
    }
    ReadSpec& with_delete_bitmap(DeleteBitmapPtr bitmap) {
        delete_bitmap = std::move(bitmap);
        return *this;
    }
};

} // namespace

class BlockReaderTableModelsTest : public testing::Test {
protected:
    void SetUp() override {
        char buffer[kMaxPathLen];
        EXPECT_NE(getcwd(buffer, kMaxPathLen), nullptr);
        _absolute_dir = std::string(buffer) + "/ut_dir/block_reader_table_models_test";
        auto st = io::global_local_filesystem()->delete_directory(_absolute_dir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(_absolute_dir);
        ASSERT_TRUE(st.ok()) << st;
        EXPECT_TRUE(io::global_local_filesystem()
                            ->create_directory(_absolute_dir + "/tablet_path")
                            .ok());
        doris::EngineOptions options;
        auto engine = std::make_unique<StorageEngine>(options);
        _engine = engine.get();
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));

        _saved_adaptive_batch_size = config::enable_adaptive_batch_size;
        _saved_prune_delete_sign = config::enable_prune_delete_sign_when_base_compaction;
        // Row-count limits only, so the Block boundaries of a test are deterministic.
        config::enable_adaptive_batch_size = false;
    }

    void TearDown() override {
        config::enable_adaptive_batch_size = _saved_adaptive_batch_size;
        config::enable_prune_delete_sign_when_base_compaction = _saved_prune_delete_sign;
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        _engine = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

    // Column unique ids are the positions in `columns`; `sequence_map` lists, per sequence
    // column, the value columns it governs.
    TabletSchemaSPtr make_schema(KeysType keys_type, const std::vector<ColumnSpec>& columns,
                                 const SequenceMapSpec& sequence_map = {}) {
        TabletSchemaPB pb;
        pb.set_keys_type(keys_type);
        pb.set_num_short_key_columns(1);
        pb.set_num_rows_per_row_block(1024);
        pb.set_compress_kind(COMPRESS_NONE);
        pb.set_next_column_unique_id(static_cast<int32_t>(columns.size()));
        auto uid_of = [&](const std::string& name) -> uint32_t {
            for (size_t i = 0; i < columns.size(); ++i) {
                if (columns[i].name == name) {
                    return static_cast<uint32_t>(i);
                }
            }
            ADD_FAILURE() << "unknown column " << name;
            return 0;
        };
        for (size_t i = 0; i < columns.size(); ++i) {
            const auto& spec = columns[i];
            ColumnPB* column = pb.add_column();
            column->set_unique_id(static_cast<int32_t>(i));
            column->set_name(spec.name);
            column->set_type(spec.type);
            column->set_is_key(spec.is_key);
            column->set_length(column_length(spec.type));
            column->set_index_length(column_length(spec.type));
            column->set_is_nullable(false);
            if (!spec.aggregation.empty()) {
                column->set_aggregation(spec.aggregation);
            }
        }
        for (const auto& [sequence_column, value_columns] : sequence_map) {
            ColumnGroupPB* group = pb.mutable_seq_map()->add_cg();
            group->set_sequence_column(uid_of(sequence_column));
            for (const auto& value_column : value_columns) {
                group->add_columns_in_group(uid_of(value_column));
            }
        }
        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(pb);
        return schema;
    }

    // Write one rowset with one segment per entry of `segments`; several segments are declared
    // overlapping, so the rowset reader has to merge them. Rows of a segment must be in key
    // order.
    RowsetSharedPtr write_rowset(const TabletSchemaSPtr& schema,
                                 const std::vector<std::vector<Row>>& segments, int64_t version) {
        static int64_t next_rowset_id = 10000;
        RowsetWriterContext context;
        RowsetId rowset_id;
        rowset_id.init(next_rowset_id++);
        context.rowset_id = rowset_id;
        context.rowset_type = BETA_ROWSET;
        context.rowset_state = VISIBLE;
        context.tablet_schema = schema;
        context.tablet_path = _absolute_dir + "/tablet_path";
        context.version = Version(version, version);
        context.segments_overlap = segments.size() > 1 ? OVERLAPPING : NONOVERLAPPING;
        context.max_rows_per_segment = UINT32_MAX;
        auto res = RowsetFactory::create_rowset_writer(*_engine, context, false);
        EXPECT_TRUE(res.has_value()) << res.error();
        auto writer = std::move(res).value();

        for (const auto& segment : segments) {
            Block block = schema->create_storage_block();
            {
                auto columns_guard = block.mutate_columns_scoped();
                auto& columns = columns_guard.mutable_columns();
                for (const auto& row : segment) {
                    EXPECT_EQ(row.size(), columns.size());
                    for (size_t c = 0; c < columns.size(); ++c) {
                        append_cell(*columns[c], schema->column(c).type(), row[c]);
                    }
                }
            }
            EXPECT_TRUE(writer->add_block(&block).ok());
            EXPECT_TRUE(writer->flush().ok());
        }

        RowsetSharedPtr rowset;
        EXPECT_TRUE(writer->build(rowset).ok());
        EXPECT_EQ(segments.size(), rowset->num_segments());
        return rowset;
    }

    TabletSharedPtr create_tablet(const TabletSchema& schema, bool enable_unique_key_merge_on_write,
                                  bool changelog_role = false) {
        std::vector<TColumn> columns;
        std::unordered_map<uint32_t, uint32_t> col_ordinal_to_unique_id;
        for (size_t i = 0; i < schema.num_columns(); ++i) {
            const TabletColumn& column = schema.column(i);
            TColumn t_column;
            t_column.column_type.type = thrift_type(column.type());
            if (column.type() == FieldType::OLAP_FIELD_TYPE_VARCHAR) {
                t_column.column_type.__set_len(kVarcharLength);
            }
            t_column.__set_column_name(column.name());
            t_column.__set_is_key(column.is_key());
            columns.push_back(t_column);
            col_ordinal_to_unique_id[i] = column.unique_id();
        }

        TTabletSchema t_schema;
        t_schema.__set_short_key_column_count(schema.num_short_key_columns());
        t_schema.__set_schema_hash(3333);
        t_schema.__set_keys_type(thrift_keys_type(schema.keys_type()));
        t_schema.__set_storage_type(TStorageType::COLUMN);
        t_schema.__set_columns(columns);
        TabletMetaSharedPtr tablet_meta(
                new TabletMeta(1, 1, 1, 1, 1, 1, t_schema, 1, col_ordinal_to_unique_id,
                               UniqueId(1, 2), TTabletType::TABLET_TYPE_DISK,
                               TCompressionType::LZ4F, 0, enable_unique_key_merge_on_write));
        if (changelog_role) {
            tablet_meta->set_tablet_role(TabletRolePB::TABLET_ROLE_CHANGELOG);
        }

        TabletSharedPtr tablet(new Tablet(*_engine, tablet_meta, nullptr));
        static_cast<void>(tablet->init());
        return tablet;
    }

    // Open a BlockReader over `rowsets` with the full tablet schema as read schema; the caller
    // checks the picked iterator and drains the rows.
    std::unique_ptr<OpenedReader> open_reader(const TabletSharedPtr& tablet,
                                              const TabletSchemaSPtr& schema,
                                              const std::vector<RowsetSharedPtr>& rowsets,
                                              const ReadSpec& spec) {
        auto opened = std::make_unique<OpenedReader>();
        auto& params = opened->params;
        params.tablet = tablet;
        params.tablet_schema = schema;
        params.reader_type = spec.reader_type;
        params.direct_mode = spec.direct_mode;
        params.read_changelog = spec.read_changelog;
        params.changelog_scan_type = spec.changelog_scan_type;
        params.version = Version(0, rowsets.back()->end_version());

        TabletReadSource read_source;
        for (const auto& rowset : rowsets) {
            RowsetReaderSharedPtr rs_reader;
            EXPECT_TRUE(rowset->create_reader(&rs_reader).ok());
            read_source.rs_splits.emplace_back(std::move(rs_reader));
        }
        read_source.fill_delete_predicates();
        params.set_read_source(std::move(read_source));
        params.delete_bitmap = spec.delete_bitmap;
        params.read_schema = std::make_shared<ReadSchema>(schema->columns());

        opened->reader.set_batch_size(static_cast<int>(spec.batch_size));
        Status st = opened->reader.init(params);
        EXPECT_TRUE(st.ok()) << st;
        opened->initialized = st.ok();
        return opened;
    }

    StorageEngine* _engine = nullptr;
    std::string _absolute_dir;
    bool _saved_adaptive_batch_size = false;
    bool _saved_prune_delete_sign = false;
};

// DUP_KEYS: every stored row comes back untouched, duplicate keys included.
TEST_F(BlockReaderTableModelsTest, DupKeysQueryUsesDirectIterator) {
    auto schema = make_schema(KeysType::DUP_KEYS, {{"k", "INT", true}, {"v", "INT"}});
    auto tablet = create_tablet(*schema, /*enable_unique_key_merge_on_write=*/false);
    std::vector<RowsetSharedPtr> rowsets = {
            write_rowset(schema, {{{1, 10}, {3, 30}}, {{2, 20}, {3, 31}}}, /*version=*/2),
            write_rowset(schema, {{{1, 11}, {4, 40}}}, /*version=*/3),
    };

    auto opened = open_reader(tablet, schema, rowsets, ReadSpec().batch(2));
    EXPECT_TRUE(uses_iterator<DirectBlockIterator>(opened->reader));

    EXPECT_EQ(sorted(opened->drain()),
              (std::vector<Row> {{1, 10}, {1, 11}, {2, 20}, {3, 30}, {3, 31}, {4, 40}}));
    EXPECT_EQ(opened->reader.merged_rows(), 0);
}

// AGG_KEYS: the two overlapping segments of the first rowset and the second rowset all hold
// the same keys; every key group is folded into one row with its SUM, MAX and the REPLACE value
// of the newest rowset. Shared by the query and the compaction tests.
class BlockReaderAggKeysTest : public BlockReaderTableModelsTest {
protected:
    void SetUp() override {
        BlockReaderTableModelsTest::SetUp();
        _schema = make_schema(KeysType::AGG_KEYS, {{"k", "INT", true},
                                                   {"s", "BIGINT", false, "SUM"},
                                                   {"m", "INT", false, "MAX"},
                                                   {"r", "VARCHAR", false, "REPLACE"}});
        _tablet = create_tablet(*_schema, /*enable_unique_key_merge_on_write=*/false);
        _rowsets = {
                write_rowset(_schema,
                             {{{1, 1, 5, 100}, {2, 2, 7, 200}}, {{1, 10, 3, 100}, {3, 3, 9, 300}}},
                             /*version=*/2),
                write_rowset(_schema, {{{2, 20, 1, 201}, {3, 30, 11, 301}}}, /*version=*/3),
        };
    }

    const std::vector<Row> _raw_rows = {{1, 1, 5, 100},  {1, 10, 3, 100}, {2, 2, 7, 200},
                                        {2, 20, 1, 201}, {3, 3, 9, 300},  {3, 30, 11, 301}};
    const std::vector<Row> _folded_rows = {{1, 11, 5, 100}, {2, 22, 7, 201}, {3, 33, 11, 301}};

    TabletSchemaSPtr _schema;
    TabletSharedPtr _tablet;
    std::vector<RowsetSharedPtr> _rowsets;
};

TEST_F(BlockReaderAggKeysTest, QueryFoldsEveryKeyGroup) {
    for (size_t batch_size : {1, 2, 100}) {
        SCOPED_TRACE("batch_size=" + std::to_string(batch_size));
        auto opened = open_reader(_tablet, _schema, _rowsets, ReadSpec().batch(batch_size));
        EXPECT_TRUE(uses_iterator<AggKeyBlockIterator>(opened->reader));

        EXPECT_EQ(opened->drain(), _folded_rows);
        // Every key had one more row than its result.
        EXPECT_EQ(opened->reader.merged_rows(), 3);
    }
}

TEST_F(BlockReaderAggKeysTest, DirectModeReturnsTheStoredRows) {
    auto opened = open_reader(_tablet, _schema, _rowsets, ReadSpec().direct().batch(2));
    EXPECT_TRUE(uses_iterator<DirectBlockIterator>(opened->reader));

    EXPECT_EQ(sorted(opened->drain()), _raw_rows);
    EXPECT_EQ(opened->reader.merged_rows(), 0);
}

TEST_F(BlockReaderAggKeysTest, CompactionFoldsEveryKeyGroup) {
    auto opened = open_reader(_tablet, _schema, _rowsets,
                              ReadSpec().type(ReaderType::READER_BASE_COMPACTION).batch(2));
    EXPECT_TRUE(uses_iterator<AggKeyBlockIterator>(opened->reader));

    EXPECT_EQ(opened->drain(), _folded_rows);
    EXPECT_EQ(opened->reader.merged_rows(), 3);
}

// UNIQUE_KEYS merge-on-read: the second rowset overwrites keys 2 and 3 of the first one and
// marks key 3 deleted.
class BlockReaderUniqueKeysTest : public BlockReaderTableModelsTest {
protected:
    void SetUp() override {
        BlockReaderTableModelsTest::SetUp();
        _schema = make_schema(KeysType::UNIQUE_KEYS,
                              {{"k", "INT", true}, {"v", "INT"}, {DELETE_SIGN, "TINYINT"}});
    }

    std::vector<RowsetSharedPtr> write_rowsets() {
        return {
                write_rowset(_schema, {{{1, 10, 0}, {2, 20, 0}, {3, 30, 0}, {4, 40, 0}}},
                             /*version=*/2),
                write_rowset(_schema, {{{2, 200, 0}, {3, 300, 1}}}, /*version=*/3),
        };
    }

    TabletSchemaSPtr _schema;
};

TEST_F(BlockReaderUniqueKeysTest, QueryKeepsTheNewestVersionOfEveryKey) {
    auto tablet = create_tablet(*_schema, /*enable_unique_key_merge_on_write=*/false);
    auto rowsets = write_rowsets();
    for (size_t batch_size : {1, 3, 100}) {
        SCOPED_TRACE("batch_size=" + std::to_string(batch_size));
        auto opened = open_reader(tablet, _schema, rowsets, ReadSpec().batch(batch_size));
        EXPECT_TRUE(uses_iterator<UniqueKeyBlockIterator>(opened->reader));

        // A query keeps the deleted row; the FE filters it with a predicate.
        EXPECT_EQ(opened->drain(),
                  (std::vector<Row> {{1, 10, 0}, {2, 200, 0}, {3, 300, 1}, {4, 40, 0}}));
        EXPECT_EQ(opened->reader.merged_rows(), 2);
        EXPECT_EQ(opened->reader.stats().rows_del_filtered, 0);
    }
}

TEST_F(BlockReaderUniqueKeysTest, BaseCompactionDropsTheDeletedKeys) {
    config::enable_prune_delete_sign_when_base_compaction = true;
    auto tablet = create_tablet(*_schema, /*enable_unique_key_merge_on_write=*/false);
    auto rowsets = write_rowsets();

    auto opened = open_reader(tablet, _schema, rowsets,
                              ReadSpec().type(ReaderType::READER_BASE_COMPACTION).batch(3));
    EXPECT_TRUE(uses_iterator<UniqueKeyBlockIterator>(opened->reader));

    EXPECT_EQ(opened->drain(), (std::vector<Row> {{1, 10, 0}, {2, 200, 0}, {4, 40, 0}}));
    EXPECT_EQ(opened->reader.merged_rows(), 2);
    EXPECT_EQ(opened->reader.stats().rows_del_filtered, 1);
}

TEST_F(BlockReaderUniqueKeysTest, MergeOnWriteQueryUsesDirectIterator) {
    auto tablet = create_tablet(*_schema, /*enable_unique_key_merge_on_write=*/true);
    auto rowsets = write_rowsets();
    // What publishing the second rowset would have recorded: its keys 2 and 3 shadow rows 1
    // and 2 of the first rowset's only segment.
    auto delete_bitmap = std::make_shared<DeleteBitmap>(tablet->tablet_id());
    delete_bitmap->add({rowsets[0]->rowset_id(), 0, 3}, 1);
    delete_bitmap->add({rowsets[0]->rowset_id(), 0, 3}, 2);

    auto opened = open_reader(tablet, _schema, rowsets,
                              ReadSpec().batch(3).with_delete_bitmap(delete_bitmap));
    EXPECT_TRUE(uses_iterator<DirectBlockIterator>(opened->reader));

    EXPECT_EQ(sorted(opened->drain()),
              (std::vector<Row> {{1, 10, 0}, {2, 200, 0}, {3, 300, 1}, {4, 40, 0}}));
    EXPECT_EQ(opened->reader.merged_rows(), 0);
}

// UNIQUE_KEYS with a sequence map {s1 -> v1, s2 -> v2}: the newest rowset is the base row of
// key 1, but the older rowset has the larger s1, so v1 and s1 come from the older row while v2
// and s2 stay from the newest one.
TEST_F(BlockReaderTableModelsTest, SequenceMapQueryMergesEverySequenceGroup) {
    auto schema = make_schema(KeysType::UNIQUE_KEYS,
                              {{"k", "INT", true},
                               {"v1", "INT"},
                               {"s1", "BIGINT"},
                               {"v2", "INT"},
                               {"s2", "BIGINT"},
                               {DELETE_SIGN, "TINYINT"}},
                              {{"s1", {"v1"}}, {"s2", {"v2"}}});
    ASSERT_TRUE(schema->has_seq_map());
    auto tablet = create_tablet(*schema, /*enable_unique_key_merge_on_write=*/false);
    std::vector<RowsetSharedPtr> rowsets = {
            write_rowset(schema, {{{1, 10, 5, 100, 1, 0}, {2, 20, 1, 200, 1, 0}}}, /*version=*/2),
            write_rowset(schema, {{{1, 11, 3, 101, 7, 0}}}, /*version=*/3),
    };

    auto opened = open_reader(tablet, schema, rowsets, ReadSpec().batch(1));
    EXPECT_TRUE(uses_iterator<SequenceMapBlockIterator>(opened->reader));

    EXPECT_EQ(opened->drain(), (std::vector<Row> {{1, 10, 5, 101, 7, 0}, {2, 20, 1, 200, 1, 0}}));
    EXPECT_EQ(opened->reader.merged_rows(), 1);
}

// Changelog tablet with two rowsets holding the changes of keys 1 and 2 at TSOs 1..4; the
// changelog scans read the merged (key, TSO) stream.
class BlockReaderChangelogTest : public BlockReaderTableModelsTest {
protected:
    void SetUp() override {
        BlockReaderTableModelsTest::SetUp();
        _schema = make_schema(KeysType::DUP_KEYS,
                              {{"k", "BIGINT", true},
                               {"v", "BIGINT"},
                               {changelog::build_before_column_name("v"), "BIGINT"},
                               {CHANGELOG_TSO_COL, "BIGINT"},
                               {CHANGELOG_LSN_COL, "BIGINT"},
                               {CHANGELOG_OP_COL, "BIGINT"}});
        _tablet = create_tablet(*_schema, /*enable_unique_key_merge_on_write=*/false,
                                /*changelog_role=*/true);
        ASSERT_TRUE(_tablet->is_changelog_tablet());
        // Columns: k, v, before(v), tso, lsn, op.
        _rowsets = {
                write_rowset(_schema,
                             {{{1, 10, 0, 1, 1, static_cast<int64_t>(ChangelogOp::APPEND)},
                               {2, 21, 20, 2, 2, static_cast<int64_t>(ChangelogOp::UPDATE)}}},
                             /*version=*/2),
                write_rowset(_schema,
                             {{{1, 15, 10, 3, 3, static_cast<int64_t>(ChangelogOp::UPDATE)},
                               {2, 21, 21, 4, 4, static_cast<int64_t>(ChangelogOp::DELETE)}}},
                             /*version=*/3),
        };
        // A singleton rowset serves its publish TSO for the TSO column instead of the stored
        // placeholder, so the second rowset's changes sort after the first one's.
        _rowsets[0]->rowset_meta()->set_commit_tso(10);
        _rowsets[1]->rowset_meta()->set_commit_tso(20);
    }

    // (k, v, op) of every output row.
    static std::vector<Row> key_value_op(const std::vector<Row>& rows) {
        std::vector<Row> out;
        for (const auto& row : rows) {
            out.push_back({row[0], row[1], row[5]});
        }
        return out;
    }

    TabletSchemaSPtr _schema;
    TabletSharedPtr _tablet;
    std::vector<RowsetSharedPtr> _rowsets;
};

TEST_F(BlockReaderChangelogTest, MinDeltaScanFoldsEveryKey) {
    auto opened = open_reader(_tablet, _schema, _rowsets,
                              ReadSpec().changelog(TChangelogScanType::MIN_DELTA).batch(1));
    EXPECT_TRUE(uses_iterator<ChangelogMinDeltaBlockIterator>(opened->reader));

    // key 1: APPEND + UPDATE -> INSERT of the final value; key 2: UPDATE + DELETE -> DELETE
    // carrying the before image of the first op.
    EXPECT_EQ(
            key_value_op(opened->drain()),
            (std::vector<Row> {{1, 15, static_cast<int64_t>(changelog::StreamChangeOp::INSERT)},
                               {2, 20, static_cast<int64_t>(changelog::StreamChangeOp::DELETE)}}));
}

TEST_F(BlockReaderChangelogTest, DetailScanEmitsEveryChange) {
    // batch_size 2 makes both UPDATE pairs straddle a Block boundary.
    auto opened = open_reader(_tablet, _schema, _rowsets,
                              ReadSpec().changelog(TChangelogScanType::DETAIL).batch(2));
    EXPECT_TRUE(uses_iterator<ChangelogDetailBlockIterator>(opened->reader));

    EXPECT_EQ(key_value_op(opened->drain()),
              (std::vector<Row> {
                      {1, 10, static_cast<int64_t>(changelog::StreamChangeOp::INSERT)},
                      {1, 10, static_cast<int64_t>(changelog::StreamChangeOp::UPDATE_BEFORE)},
                      {1, 15, static_cast<int64_t>(changelog::StreamChangeOp::UPDATE_AFTER)},
                      {2, 20, static_cast<int64_t>(changelog::StreamChangeOp::UPDATE_BEFORE)},
                      {2, 21, static_cast<int64_t>(changelog::StreamChangeOp::UPDATE_AFTER)},
                      {2, 21, static_cast<int64_t>(changelog::StreamChangeOp::DELETE)}}));
}

} // namespace doris
