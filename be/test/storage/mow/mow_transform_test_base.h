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

#pragma once

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "common/consts.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "storage/data_dir.h"
#include "storage/iterator/olap_data_convertor.h"
#include "storage/key/row_key_encoder.h"
#include "storage/olap_common.h"
#include "storage/options.h"
#include "storage/partial_update_info.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/rowset/rowset_reader_context.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/segment/historical_row_retriever.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_manager.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"
#include "testutil/creators.h"

namespace doris {

// One input row for a MoW unique-key tablet with layout: k INT (key), v INT (value), [seq INT],
// __DORIS_DELETE_SIGN__ TINYINT. `seq` is only meaningful when the schema was built with has_seq.
struct MowRow {
    int32_t k;
    int32_t v;
    int32_t seq = 0;
    int8_t delete_sign = 0;
};

// Shared fixture for the MoW transform-chain test layers (key probe, historical row fetcher,
// partial update fill). Sets up a real StorageEngine + DataDir, and can write committed MoW rowsets
// whose primary key index BaseTablet:: lookup_row_key (and therefore MowKeyProbe) can query.
class MowTransformTestBase : public testing::Test {
public:
    void SetUp() override {
        char buffer[1024];
        EXPECT_NE(getcwd(buffer, sizeof(buffer)), nullptr);
        _root = std::string(buffer) + "/mow_transform_ut";
        config::storage_root_path = _root;

        auto fs = io::global_local_filesystem();
        ASSERT_TRUE(fs->delete_directory(_root).ok());
        ASSERT_TRUE(fs->create_directory(_root).ok());

        std::vector<StorePath> paths;
        paths.emplace_back(_root, -1);
        EngineOptions options;
        options.store_paths = paths;
        auto engine = std::make_unique<StorageEngine>(options);
        ASSERT_TRUE(engine->open().ok());
        _engine = engine.get();
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));

        _data_dir = std::make_unique<DataDir>(*_engine, _root);
        static_cast<void>(_data_dir->update_capacity());
    }

    void TearDown() override {
        _engine = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        static_cast<void>(io::global_local_filesystem()->delete_directory(_root));
    }

protected:
    // (k INT key, v INT, [seq INT], delete-sign) UNIQUE_KEYS MoW schema.
    TabletSchemaSPtr create_mow_schema(bool has_seq) {
        TabletSchemaPB pb;
        pb.set_keys_type(UNIQUE_KEYS);
        pb.set_num_short_key_columns(1);
        pb.set_num_rows_per_row_block(1024);
        pb.set_compress_kind(COMPRESS_LZ4);
        pb.set_next_column_unique_id(10);

        auto add_col = [&](int uid, const std::string& name, const std::string& type, bool is_key,
                           bool nullable, const std::string& def = "") {
            ColumnPB* c = pb.add_column();
            c->set_unique_id(uid);
            c->set_name(name);
            c->set_type(type);
            c->set_is_key(is_key);
            c->set_length(type == "TINYINT" ? 1 : 4);
            c->set_index_length(type == "TINYINT" ? 1 : 4);
            c->set_is_nullable(nullable);
            c->set_is_bf_column(false);
            c->set_aggregation("NONE");
            if (!def.empty()) {
                c->set_default_value(def);
            }
        };

        add_col(0, "k", "INT", true, false);
        add_col(1, "v", "INT", false, true, std::to_string(0));
        int next_uid = 2;
        int seq_idx = -1;
        if (has_seq) {
            seq_idx = pb.column_size();
            add_col(next_uid++, SEQUENCE_COL, "INT", false, false, std::to_string(0));
        }
        // delete sign column (hidden) -- value 1 means the row is a delete.
        add_col(next_uid++, DELETE_SIGN, "TINYINT", false, false, std::to_string(0));
        if (seq_idx >= 0) {
            pb.set_sequence_col_idx(seq_idx);
        }

        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(pb);
        return schema;
    }

    // (k INT key, v INT NOT NULL without default, delete-sign) UNIQUE_KEYS MoW schema: the shape
    // whose missing column can neither be defaulted nor nulled in strict-mode partial update.
    TabletSchemaSPtr create_mow_schema_required_value() {
        TabletSchemaPB pb;
        pb.set_keys_type(UNIQUE_KEYS);
        pb.set_num_short_key_columns(1);
        pb.set_num_rows_per_row_block(1024);
        pb.set_compress_kind(COMPRESS_LZ4);
        pb.set_next_column_unique_id(10);

        auto add_col = [&](int uid, const std::string& name, const std::string& type, bool is_key,
                           bool nullable, const std::string& def = "") {
            ColumnPB* c = pb.add_column();
            c->set_unique_id(uid);
            c->set_name(name);
            c->set_type(type);
            c->set_is_key(is_key);
            c->set_length(type == "TINYINT" ? 1 : 4);
            c->set_index_length(type == "TINYINT" ? 1 : 4);
            c->set_is_nullable(nullable);
            c->set_aggregation("NONE");
            if (!def.empty()) {
                c->set_default_value(def);
            }
        };
        add_col(0, "k", "INT", true, false);
        add_col(1, "v", "INT", false, /*nullable=*/false); // NOT NULL, no default
        add_col(2, DELETE_SIGN, "TINYINT", false, false, std::to_string(0));
        pb.set_delete_sign_idx(2);

        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(pb);
        return schema;
    }

    // Flushes `blocks` one segment each through the real rowset writer with the
    // partial-update context set, i.e. through the transform chain's fill path.
    // `stats_out`, when given, receives the writer-level partial update counters
    // (the chain's probe counters folded into the flusher totals).
    Status flush_partial_rowset_segments(
            const TabletSchemaSPtr& schema, int64_t rowset_numeric_id, int64_t version,
            const TabletSharedPtr& tablet, const std::shared_ptr<MowContext>& mow_context,
            const std::shared_ptr<PartialUpdateInfo>& partial_update_info,
            const std::vector<Block*>& blocks, RowsetSharedPtr* rowset,
            PartialUpdateStats* stats_out = nullptr, int64_t* writer_num_rows = nullptr) {
        RowsetWriterContext context;
        TabletSharedPtr unused_tablet;
        make_rowset_ctx(schema, rowset_numeric_id, version, &context, &unused_tablet);
        context.tablet = tablet;
        context.data_dir = tablet->data_dir();
        context.mow_context = mow_context;
        context.partial_update_info = partial_update_info;
        context.is_transient_rowset_writer = false;
        context.segments_overlap = NONOVERLAPPING;
        context.max_rows_per_segment = 1024;
        context.enable_segcompaction = false;

        auto writer_result = RowsetFactory::create_rowset_writer(*_engine, context, false);
        if (!writer_result.has_value()) {
            return writer_result.error();
        }
        auto writer = std::move(writer_result).value();
        int32_t segment_id = 0;
        for (Block* block : blocks) {
            RETURN_IF_ERROR(writer->flush_memtable(block, segment_id++, nullptr));
        }
        RETURN_IF_ERROR(writer->flush());
        if (writer_num_rows != nullptr) {
            // the flusher-level input-row counter behind RowsetWriter::num_rows(),
            // which the load-close check compares against the received rows
            *writer_num_rows = writer->num_rows();
        }
        if (stats_out != nullptr) {
            stats_out->num_rows_updated = writer->num_rows_updated();
            stats_out->num_rows_deleted = writer->num_rows_deleted();
            stats_out->num_rows_new_added = writer->num_rows_new_added();
            stats_out->num_rows_filtered = writer->num_rows_filtered();
        }
        return writer->build(*rowset);
    }

    Status flush_partial_rowset(const TabletSchemaSPtr& schema, int64_t rowset_numeric_id,
                                int64_t version, const TabletSharedPtr& tablet,
                                const std::shared_ptr<MowContext>& mow_context,
                                const std::shared_ptr<PartialUpdateInfo>& partial_update_info,
                                Block* block, RowsetSharedPtr* rowset,
                                PartialUpdateStats* stats_out = nullptr,
                                int64_t* writer_num_rows = nullptr) {
        return flush_partial_rowset_segments(schema, rowset_numeric_id, version, tablet,
                                             mow_context, partial_update_info, {block}, rowset,
                                             stats_out, writer_num_rows);
    }

    // Reads every row of `rowset` back into `output` in key order, all columns.
    Status read_rowset(const RowsetSharedPtr& rowset, const TabletSchemaSPtr& schema,
                       Block* output) {
        auto read_schema = std::make_shared<ReadSchema>(schema->columns());
        RowsetReaderContext context;
        context.tablet_schema = schema;
        context.read_schema = read_schema;
        context.need_ordered_result = true;
        OlapReaderStatistics statistics;
        context.stats = &statistics;

        RowsetReaderSharedPtr reader;
        RETURN_IF_ERROR(rowset->create_reader(&reader));
        RETURN_IF_ERROR(reader->init(&context));
        *output = read_schema->create_read_block();
        while (true) {
            Block batch = read_schema->create_read_block();
            auto status = reader->next_batch(&batch);
            if (status.is<ErrorCode::END_OF_FILE>()) {
                return Status::OK();
            }
            RETURN_IF_ERROR(status);
            auto guard = output->mutate_columns_scoped();
            auto& output_columns = guard.mutable_columns();
            for (size_t cid = 0; cid < output_columns.size(); ++cid) {
                output_columns[cid]->insert_range_from(*batch.get_by_position(cid).column, 0,
                                                       batch.rows());
            }
        }
    }

    // (k INT key, v VARIANT, delete-sign) UNIQUE_KEYS MoW schema, for the variant parse stage.
    TabletSchemaSPtr create_variant_schema() {
        TabletSchemaPB pb;
        pb.set_keys_type(UNIQUE_KEYS);
        pb.set_num_short_key_columns(1);
        pb.set_num_rows_per_row_block(1024);
        pb.set_compress_kind(COMPRESS_LZ4);
        pb.set_next_column_unique_id(10);
        {
            ColumnPB* c = pb.add_column();
            c->set_unique_id(0);
            c->set_name("k");
            c->set_type("INT");
            c->set_is_key(true);
            c->set_length(4);
            c->set_index_length(4);
            c->set_is_nullable(false);
            c->set_aggregation("NONE");
        }
        {
            ColumnPB* c = pb.add_column();
            c->set_unique_id(1);
            c->set_name("v");
            c->set_type("VARIANT");
            c->set_is_key(false);
            c->set_length(2147483643);
            c->set_index_length(4);
            c->set_is_nullable(false);
            c->set_aggregation("NONE");
            c->set_variant_max_subcolumns_count(3);
        }
        {
            ColumnPB* c = pb.add_column();
            c->set_unique_id(2);
            c->set_name(DELETE_SIGN);
            c->set_type("TINYINT");
            c->set_is_key(false);
            c->set_length(1);
            c->set_index_length(1);
            c->set_is_nullable(false);
            c->set_aggregation("NONE");
            c->set_default_value(std::to_string(0));
        }
        pb.set_delete_sign_idx(2);
        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(pb);
        return schema;
    }

    // (k INT key, v INT, delete-sign, __DORIS_SKIP_BITMAP_COL__) flexible partial update MoW
    // schema: flexible loads carry a full-width block plus the per-row skip bitmap.
    // Flexible MoW schema. Without seq: k(0) v(1) delete_sign(2) skip_bitmap(3).
    // With seq: k(0) v(1) seq(2) delete_sign(3) skip_bitmap(4).
    TabletSchemaSPtr create_flexible_mow_schema(bool has_seq = false) {
        TabletSchemaPB pb;
        pb.set_keys_type(UNIQUE_KEYS);
        pb.set_num_short_key_columns(1);
        pb.set_num_rows_per_row_block(1024);
        pb.set_compress_kind(COMPRESS_LZ4);
        pb.set_next_column_unique_id(10);

        auto type_length = [](const std::string& type) -> int32_t {
            if (type == "TINYINT") {
                return 1;
            }
            if (type == "BITMAP") {
                return 16;
            }
            return 4;
        };
        auto add_col = [&](int uid, const std::string& name, const std::string& type, bool is_key,
                           bool nullable, const std::string& def = "") {
            ColumnPB* c = pb.add_column();
            c->set_unique_id(uid);
            c->set_name(name);
            c->set_type(type);
            c->set_is_key(is_key);
            c->set_length(type_length(type));
            c->set_index_length(type_length(type));
            c->set_is_nullable(nullable);
            c->set_aggregation("NONE");
            if (!def.empty()) {
                c->set_default_value(def);
            }
        };
        int next = 0;
        add_col(next, "k", "INT", true, false);
        ++next;
        add_col(next, "v", "INT", false, true, std::to_string(0));
        ++next;
        if (has_seq) {
            add_col(next, SEQUENCE_COL, "INT", false, false, std::to_string(0));
            pb.set_sequence_col_idx(next);
            ++next;
        }
        add_col(next, DELETE_SIGN, "TINYINT", false, false, std::to_string(0));
        // init_from_pb reads these hidden-column indices straight from the PB
        // fields (it does not scan by name), so they must be set explicitly.
        pb.set_delete_sign_idx(next);
        ++next;
        add_col(next, SKIP_BITMAP_COL, "BITMAP", false, false);
        pb.set_skip_bitmap_col_idx(next);

        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(pb);
        return schema;
    }

    // (k INT key, v INT, [seq INT], delete-sign, __DORIS_ROW_STORE_COL__ STRING) with the hidden
    // full row-store column enabled -- the only shape for which the write path touches the row
    // cache.
    TabletSchemaSPtr create_row_store_schema(bool has_seq = false) {
        TabletSchemaPB pb;
        pb.set_keys_type(UNIQUE_KEYS);
        pb.set_num_short_key_columns(1);
        pb.set_num_rows_per_row_block(1024);
        pb.set_compress_kind(COMPRESS_LZ4);
        pb.set_next_column_unique_id(10);
        pb.set_store_row_column(true);

        auto add = [&](int uid, const std::string& name, const std::string& type, bool is_key,
                       int len, const std::string& def = "") {
            ColumnPB* c = pb.add_column();
            c->set_unique_id(uid);
            c->set_name(name);
            c->set_type(type);
            c->set_is_key(is_key);
            c->set_length(len);
            c->set_index_length(type == "STRING" ? 4 : len);
            // the hidden row-store column must be non-nullable: its writer static_casts the built
            // column to a plain ColumnString. The delete-sign and sequence columns are also
            // non-nullable (NOT NULL with default 0 in real schemas) -- get_delete_sign_column_data
            // assert_casts the former to a plain ColumnVector<Int8>.
            c->set_is_nullable(!is_key && name != BeConsts::ROW_STORE_COL && name != DELETE_SIGN &&
                               name != SEQUENCE_COL);
            c->set_is_bf_column(false);
            c->set_aggregation("NONE");
            if (!def.empty()) {
                c->set_default_value(def);
            }
        };
        add(0, "k", "INT", true, 4);
        add(1, "v", "INT", false, 4, std::to_string(0));
        int next_uid = 2;
        if (has_seq) {
            pb.set_sequence_col_idx(pb.column_size());
            add(next_uid++, SEQUENCE_COL, "INT", false, 4, std::to_string(0));
        }
        add(next_uid++, DELETE_SIGN, "TINYINT", false, 1, std::to_string(0));
        add(next_uid++, BeConsts::ROW_STORE_COL, "STRING", false, 2147483643);

        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(pb);
        return schema;
    }

    // A MoW tablet over the engine's data dir; no rowsets are registered with it.
    // The two tablets a row-binlog write really uses: the source tablet, and the
    // independent binlog tablet whose own schema is the derived binlog schema
    // (source keys + visible columns, then TSO/LSN/OP). Built through the
    // engine's tablet manager so both are exactly what the write path sees.
    struct BinlogTablets {
        TabletSharedPtr source;
        TabletSharedPtr binlog;
    };

    BinlogTablets create_binlog_tablets(int64_t tablet_id, TKeysType::type keys_type, bool mow) {
        // Three columns (one key, two values) so a narrow partial-update block
        // can sit strictly between num_key_columns and num_columns, which the
        // binlog MoW derive requires.
        auto request =
                testutil::create_tablet_request(tablet_id, /*schema_hash=*/1, /*partition_id=*/10,
                                                /*short_key_column_count=*/1, keys_type,
                                                {{"k1", TPrimitiveType::INT, true},
                                                 {"v1", TPrimitiveType::INT, false},
                                                 {"v2", TPrimitiveType::INT, false}});
        if (mow) {
            request.__set_enable_unique_key_merge_on_write(true);
        }
        testutil::enable_row_binlog(&request);
        RuntimeProfile profile("binlog_ut");
        EXPECT_TRUE(_engine->create_tablet(request, &profile).ok());

        auto binlog_request = request;
        binlog_request.tablet_id = tablet_id + kBinlogTabletIdOffset;
        binlog_request.tablet_schema = testutil::create_row_binlog_tablet_schema(
                request.tablet_schema, request.tablet_schema.schema_hash + 1);
        binlog_request.__set_tablet_role(TTabletRole::TABLET_ROLE_ROW_BINLOG);
        // hidden source columns are dropped from the binlog schema, so the
        // indices pointing at them must not follow along
        binlog_request.tablet_schema.__set_delete_sign_idx(-1);
        binlog_request.tablet_schema.__set_sequence_col_idx(-1);
        // AFTER value columns are nullable in a real binlog schema, even when the
        // source column is NOT NULL. The TSO/LSN/op columns keep the nullability
        // create_row_binlog_tablet_schema gave them.
        for (auto& column : binlog_request.tablet_schema.columns) {
            if (!column.is_key && column.column_name != BINLOG_TSO_COL &&
                column.column_name != BINLOG_LSN_COL && column.column_name != BINLOG_OP_COL) {
                column.__set_is_allow_null(true);
            }
        }
        EXPECT_TRUE(_engine->create_tablet(binlog_request, &profile).ok());

        return {_engine->tablet_manager()->get_tablet(tablet_id),
                _engine->tablet_manager()->get_tablet(binlog_request.tablet_id)};
    }

    // The per-segment LSN range the binlog derive consumes (one LSN per row).
    static std::shared_ptr<std::vector<int64_t>> make_seg_lsn(size_t num_rows,
                                                              int64_t start = 1000) {
        auto lsn_ids = std::make_shared<std::vector<int64_t>>();
        for (size_t i = 0; i < num_rows; ++i) {
            lsn_ids->push_back(start + static_cast<int64_t>(i));
        }
        return lsn_ids;
    }

    // (k1 key, v1, v2, delete-sign) UNIQUE_KEYS MoW source schema for the binlog
    // partial-update path. It has three visible columns so a narrow PU block can
    // sit strictly between num_key_columns and num_columns, and a delete-sign
    // column (with a default) which the fixed-PU fill requires.
    TabletSchemaSPtr create_binlog_pu_source_schema() {
        TabletSchemaPB pb;
        pb.set_keys_type(UNIQUE_KEYS);
        pb.set_num_short_key_columns(1);
        pb.set_num_rows_per_row_block(1024);
        pb.set_compress_kind(COMPRESS_LZ4);
        pb.set_next_column_unique_id(10);

        auto add_col = [&](int uid, const std::string& name, const std::string& type, bool is_key,
                           bool nullable, const std::string& def = "", bool visible = true) {
            ColumnPB* c = pb.add_column();
            c->set_unique_id(uid);
            c->set_name(name);
            c->set_type(type);
            c->set_is_key(is_key);
            c->set_length(type == "TINYINT" ? 1 : 4);
            c->set_index_length(type == "TINYINT" ? 1 : 4);
            c->set_is_nullable(nullable);
            c->set_aggregation("NONE");
            c->set_visible(visible);
            if (!def.empty()) {
                c->set_default_value(def);
            }
        };
        add_col(0, "k1", "INT", true, false);
        add_col(1, "v1", "INT", false, true, std::to_string(0));
        add_col(2, "v2", "INT", false, true, std::to_string(0));
        // the delete-sign column is hidden, so num_visible_columns stays 3 and
        // the binlog schema's num_columns (3 + TSO/LSN/OP) rule holds.
        add_col(3, DELETE_SIGN, "TINYINT", false, false, std::to_string(0), /*visible=*/false);
        pb.set_delete_sign_idx(3);

        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(pb);
        return schema;
    }

    TabletSharedPtr make_tablet(const TabletSchemaSPtr& schema, int64_t tablet_id) {
        TabletMetaSharedPtr tablet_meta = std::make_shared<TabletMeta>();
        tablet_meta->_tablet_id = tablet_id;
        static_cast<void>(tablet_meta->set_partition_id(10));
        tablet_meta->_schema = schema;
        tablet_meta->_enable_unique_key_merge_on_write = true;
        return std::make_shared<Tablet>(*_engine, tablet_meta, _data_dir.get(), "mow_ut");
    }

    void make_rowset_ctx(const TabletSchemaSPtr& schema, int64_t rowset_numeric_id, int64_t version,
                         RowsetWriterContext* ctx, TabletSharedPtr* out_tablet) {
        RowsetId rid;
        rid.init(rowset_numeric_id);
        ctx->rowset_id = rid;
        ctx->tablet_id = kTabletId;
        ctx->tablet_schema_hash = 1;
        ctx->partition_id = 10;
        ctx->rowset_type = BETA_ROWSET;
        ctx->tablet_path = _root;
        ctx->rowset_state = VISIBLE;
        ctx->tablet_schema = schema;
        ctx->version = {version, version};
        ctx->enable_unique_key_merge_on_write = true;
        ctx->write_type = DataWriteType::TYPE_DIRECT;

        auto tablet = make_tablet(schema, kTabletId);
        ctx->tablet = tablet;
        *out_tablet = tablet;
    }

    // Builds and commits a MoW rowset containing `rows` (must be sorted unique by key, like a real
    // flushed segment). Returns the committed rowset.
    RowsetSharedPtr write_rowset(const TabletSchemaSPtr& schema, int64_t rowset_numeric_id,
                                 int64_t version, const std::vector<MowRow>& rows,
                                 TabletSharedPtr* out_tablet) {
        RowsetWriterContext ctx;
        make_rowset_ctx(schema, rowset_numeric_id, version, &ctx, out_tablet);

        auto rw = RowsetFactory::create_rowset_writer(*_engine, ctx, false);
        EXPECT_TRUE(rw.has_value()) << rw.error();
        auto writer = std::move(rw).value();

        Block block = schema->create_storage_block();
        std::vector<IColumn*> mcols;
        for (size_t i = 0; i < block.columns(); ++i) {
            mcols.push_back(block.get_by_position(i).column->assert_mutable().get());
        }
        const bool has_seq = schema->has_sequence_col();
        for (const auto& r : rows) {
            size_t c = 0;
            mcols[c++]->insert_data(reinterpret_cast<const char*>(&r.k), sizeof(int32_t));
            mcols[c++]->insert_data(reinterpret_cast<const char*>(&r.v), sizeof(int32_t));
            if (has_seq) {
                mcols[c++]->insert_data(reinterpret_cast<const char*>(&r.seq), sizeof(int32_t));
            }
            mcols[c++]->insert_data(reinterpret_cast<const char*>(&r.delete_sign), sizeof(int8_t));
        }
        // Fill any trailing columns not described by MowRow (e.g. the flexible skip-bitmap column)
        // with defaults so every column has the same height.
        for (size_t cid = (has_seq ? 4 : 3); cid < mcols.size(); ++cid) {
            mcols[cid]->insert_many_defaults(rows.size());
        }
        EXPECT_TRUE(writer->add_block(&block).ok());
        EXPECT_TRUE(writer->flush().ok());
        RowsetSharedPtr rowset;
        EXPECT_TRUE(writer->build(rowset).ok());
        EXPECT_TRUE(rowset != nullptr);
        return rowset;
    }

    // Like write_rowset, but the caller fills the block itself: for schemas whose
    // column layout MowRow cannot express.
    RowsetSharedPtr write_rowset_block(const TabletSchemaSPtr& schema, int64_t rowset_numeric_id,
                                       int64_t version, const std::function<void(Block&)>& fill,
                                       TabletSharedPtr* out_tablet) {
        RowsetWriterContext ctx;
        make_rowset_ctx(schema, rowset_numeric_id, version, &ctx, out_tablet);
        auto rw = RowsetFactory::create_rowset_writer(*_engine, ctx, false);
        EXPECT_TRUE(rw.has_value()) << rw.error();
        auto writer = std::move(rw).value();

        Block block = schema->create_storage_block();
        fill(block);
        EXPECT_TRUE(writer->add_block(&block).ok());
        EXPECT_TRUE(writer->flush().ok());
        RowsetSharedPtr rowset;
        EXPECT_TRUE(writer->build(rowset).ok());
        EXPECT_TRUE(rowset != nullptr);
        return rowset;
    }

    std::shared_ptr<MowContext> make_mow_context(int64_t version,
                                                 const std::vector<RowsetSharedPtr>& rowsets) {
        auto rsids = std::make_shared<RowsetIdUnorderedSet>();
        for (const auto& rs : rowsets) {
            rsids->insert(rs->rowset_id());
        }
        auto delete_bitmap = std::make_shared<DeleteBitmap>(kTabletId);
        return std::make_shared<MowContext>(version, /*txn_id=*/1, rsids, rowsets, delete_bitmap);
    }

    // Encodes a primary key (single INT key column = `k`) the same way the fill stage does: convert
    // a 1-row block then RowKeyEncoder::full_encode_primary_keys.
    std::string encode_key(const TabletSchemaSPtr& schema, const RowKeyEncoder& encoder,
                           int32_t k) {
        Block block = schema->create_storage_block({0});
        block.get_by_position(0).column->assert_mutable()->insert_data(
                reinterpret_cast<const char*>(&k), sizeof(int32_t));
        OlapBlockDataConvertor convertor;
        convertor.add_column_data_convertor(schema->column(0));
        convertor.set_source_content(&block, 0, 1);
        auto [st, accessor] = convertor.convert_column_data(0);
        EXPECT_TRUE(st.ok()) << st;
        std::vector<IOlapColumnDataAccessor*> key_columns {accessor};
        return encoder.full_encode_primary_keys(key_columns, 0);
    }

    // Encodes a primary key plus its sequence suffix (incoming seq value), the way
    // MowKeyProbe::probe expects when key_has_seq_suffix is true.
    std::string encode_key_with_seq(const TabletSchemaSPtr& schema, const RowKeyEncoder& encoder,
                                    int32_t k, int32_t seq) {
        const auto seq_idx = static_cast<uint32_t>(schema->sequence_col_idx());
        Block block = schema->create_storage_block({0, seq_idx});
        block.get_by_position(0).column->assert_mutable()->insert_data(
                reinterpret_cast<const char*>(&k), sizeof(int32_t));
        block.get_by_position(1).column->assert_mutable()->insert_data(
                reinterpret_cast<const char*>(&seq), sizeof(int32_t));
        OlapBlockDataConvertor convertor;
        convertor.add_column_data_convertor(schema->column(0));
        convertor.add_column_data_convertor(schema->column(seq_idx));
        convertor.set_source_content(&block, 0, 1);
        auto [st0, key_acc] = convertor.convert_column_data(0);
        EXPECT_TRUE(st0.ok()) << st0;
        auto [st1, seq_acc] = convertor.convert_column_data(1);
        EXPECT_TRUE(st1.ok()) << st1;
        std::vector<IOlapColumnDataAccessor*> key_columns {key_acc};
        std::string key = encoder.full_encode_primary_keys(key_columns, 0);
        encoder.append_seq_suffix(&key, seq_acc, 0);
        return key;
    }

    segment_v2::HistoricalRowRetrieverContext make_fetcher_ctx(
            const TabletSchemaSPtr& schema, const TabletSharedPtr& tablet,
            std::shared_ptr<PartialUpdateInfo> pui = nullptr) {
        return segment_v2::HistoricalRowRetrieverContext {.tablet = tablet,
                                                          .tablet_schema = schema,
                                                          .rowset_writer_ctx = nullptr,
                                                          .partial_update_info = std::move(pui),
                                                          .is_transient_rowset_writer = false,
                                                          .write_type = DataWriteType::TYPE_DIRECT};
    }

    // Reads an int32 cell from a (possibly nullable) INT column of `block`.
    static int32_t read_int(const Block& block, size_t col_pos, size_t row) {
        const IColumn* col = block.get_by_position(col_pos).column.get();
        if (col->is_nullable()) {
            col = &assert_cast<const ColumnNullable&>(*col).get_nested_column();
        }
        return assert_cast<const ColumnInt32&>(*col).get_data()[row];
    }

    // Reads an int8 cell from a (possibly nullable) TINYINT column of `block`.
    static int8_t read_tinyint(const Block& block, size_t col_pos, size_t row) {
        const IColumn* col = block.get_by_position(col_pos).column.get();
        if (col->is_nullable()) {
            col = &assert_cast<const ColumnNullable&>(*col).get_nested_column();
        }
        return assert_cast<const ColumnInt8&>(*col).get_data()[row];
    }

    // True iff the cell is SQL NULL. A non-nullable column is never null. Use alongside read_int to
    // distinguish "value X" from "null with X left in the nested column".
    static bool read_is_null(const Block& block, size_t col_pos, size_t row) {
        const IColumn* col = block.get_by_position(col_pos).column.get();
        if (!col->is_nullable()) {
            return false;
        }
        return assert_cast<const ColumnNullable&>(*col).is_null_at(row);
    }

    static constexpr int64_t kTabletId = 90001;
    // keeps a binlog tablet's id clear of its source tablet's
    static constexpr int64_t kBinlogTabletIdOffset = 10000;

    StorageEngine* _engine = nullptr;
    std::unique_ptr<DataDir> _data_dir;
    std::string _root;
};

} // namespace doris
