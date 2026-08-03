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

#include "storage/rowset/beta_rowset_reader.h"

#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "storage/predicate/column_predicate.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/rowset_reader_context.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

// Verifies the forced TSO range pushdown in BetaRowsetReader::get_segment_iterators (introduced
// for binlog/snapshot incremental read). The (start_tso, end_tso] comparison predicates and the
// tso predicate column must be injected directly onto read options, and the tso column must be
// appended to read_columns only when it is not already in return_columns.
//
// The rowset here has zero segments, so get_segment_iterators skips segment iterator creation
// and returns after the injection logic, which is exactly the code path under test.
class BetaRowsetReaderTsoInjectionTest : public testing::Test {
protected:
    // Schema: (k1 int key, __binlog_tso__ bigint). Column index 1 is used as the tso column.
    static constexpr ColumnId kTsoColumnId = 1;

    void SetUp() override {
        _tablet_schema = std::make_shared<TabletSchema>();
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(DUP_KEYS);
        schema_pb.set_num_short_key_columns(1);
        schema_pb.set_num_rows_per_row_block(1024);
        schema_pb.set_next_column_unique_id(3);

        ColumnPB* key = schema_pb.add_column();
        key->set_unique_id(1);
        key->set_name("k1");
        key->set_type("INT");
        key->set_is_key(true);
        key->set_length(4);
        key->set_index_length(4);
        key->set_is_nullable(false);

        ColumnPB* tso = schema_pb.add_column();
        tso->set_unique_id(2);
        tso->set_name("__binlog_tso__");
        tso->set_type("BIGINT");
        tso->set_is_key(false);
        tso->set_length(8);
        tso->set_is_nullable(false);

        _tablet_schema->init_from_pb(schema_pb);

        _rowset_meta = std::make_shared<RowsetMeta>();
        RowsetMetaPB rowset_meta_pb;
        rowset_meta_pb.set_rowset_id(10001);
        rowset_meta_pb.set_tablet_id(1);
        rowset_meta_pb.set_rowset_type(BETA_ROWSET);
        rowset_meta_pb.set_rowset_state(VISIBLE);
        rowset_meta_pb.set_start_version(2);
        rowset_meta_pb.set_end_version(2);
        rowset_meta_pb.set_num_segments(0);
        rowset_meta_pb.set_num_rows(0);
        rowset_meta_pb.set_empty(true);
        _rowset_meta->init_from_pb(rowset_meta_pb);

        _rowset = std::make_shared<BetaRowset>(_tablet_schema, _rowset_meta, "");
        _reader = std::make_shared<BetaRowsetReader>(_rowset);
    }

    RowsetReaderContext make_context() {
        RowsetReaderContext ctx;
        ctx.tablet_schema = _tablet_schema;
        ctx.return_columns = &_return_columns;
        ctx.need_ordered_result = false;
        return ctx;
    }

    // Counts predicates that act on the tso column in the injected read options.
    static int count_tso_predicates(const BetaRowsetReader& reader) {
        int count = 0;
        for (const auto& pred : reader._read_options.column_predicates) {
            if (pred->column_id() == kTsoColumnId) {
                ++count;
            }
        }
        return count;
    }

    TabletSchemaSPtr _tablet_schema;
    RowsetMetaSharedPtr _rowset_meta;
    BetaRowsetSharedPtr _rowset;
    std::shared_ptr<BetaRowsetReader> _reader;
    std::vector<uint32_t> _return_columns = {0};
};

TEST_F(BetaRowsetReaderTsoInjectionTest, InjectBothStartAndEndTso) {
    RowsetReaderContext ctx = make_context();
    ctx.tso_predicate_column_id = kTsoColumnId;
    ctx.start_tso = 100;
    ctx.end_tso = 200;

    std::vector<RowwiseIteratorUPtr> iters;
    ASSERT_TRUE(_reader->get_segment_iterators(&ctx, &iters).ok());

    // Both GT(start) and LE(end) predicates are injected on the tso column.
    EXPECT_EQ(count_tso_predicates(*_reader), 2);
    EXPECT_EQ(_reader->_read_options.col_id_to_predicates.count(kTsoColumnId), 1);
}

TEST_F(BetaRowsetReaderTsoInjectionTest, InjectOnlyStartTso) {
    RowsetReaderContext ctx = make_context();
    ctx.tso_predicate_column_id = kTsoColumnId;
    ctx.start_tso = 100;

    std::vector<RowwiseIteratorUPtr> iters;
    ASSERT_TRUE(_reader->get_segment_iterators(&ctx, &iters).ok());

    EXPECT_EQ(count_tso_predicates(*_reader), 1);
    EXPECT_EQ(_reader->_read_options.col_id_to_predicates.count(kTsoColumnId), 1);
}

TEST_F(BetaRowsetReaderTsoInjectionTest, InjectOnlyEndTso) {
    RowsetReaderContext ctx = make_context();
    ctx.tso_predicate_column_id = kTsoColumnId;
    ctx.end_tso = 200;

    std::vector<RowwiseIteratorUPtr> iters;
    ASSERT_TRUE(_reader->get_segment_iterators(&ctx, &iters).ok());

    EXPECT_EQ(count_tso_predicates(*_reader), 1);
    EXPECT_EQ(_reader->_read_options.col_id_to_predicates.count(kTsoColumnId), 1);
}

TEST_F(BetaRowsetReaderTsoInjectionTest, NoInjectionWhenTsoRangeAbsent) {
    RowsetReaderContext ctx = make_context();
    // Column id present but neither start nor end tso set: nothing should be injected.
    ctx.tso_predicate_column_id = kTsoColumnId;

    std::vector<RowwiseIteratorUPtr> iters;
    ASSERT_TRUE(_reader->get_segment_iterators(&ctx, &iters).ok());

    EXPECT_EQ(count_tso_predicates(*_reader), 0);
}

TEST_F(BetaRowsetReaderTsoInjectionTest, AppendTsoColumnWhenNotInReturnColumns) {
    RowsetReaderContext ctx = make_context();
    // return_columns only has k1 (id 0), tso column (id 1) must be appended to read columns.
    ctx.tso_predicate_column_id = kTsoColumnId;
    ctx.start_tso = 100;

    std::vector<RowwiseIteratorUPtr> iters;
    ASSERT_TRUE(_reader->get_segment_iterators(&ctx, &iters).ok());

    // input schema (read columns) = return_columns + appended tso column = 2 columns.
    EXPECT_EQ(_reader->_input_schema->num_column_ids(), 2);
}

TEST_F(BetaRowsetReaderTsoInjectionTest, NotAppendTsoColumnWhenAlreadyInReturnColumns) {
    RowsetReaderContext ctx = make_context();
    // tso column already selected in return_columns: it must not be appended twice.
    std::vector<uint32_t> return_columns = {0, kTsoColumnId};
    ctx.return_columns = &return_columns;
    ctx.tso_predicate_column_id = kTsoColumnId;
    ctx.start_tso = 100;

    std::vector<RowwiseIteratorUPtr> iters;
    ASSERT_TRUE(_reader->get_segment_iterators(&ctx, &iters).ok());

    EXPECT_EQ(_reader->_input_schema->num_column_ids(), 2);
}

} // namespace doris
