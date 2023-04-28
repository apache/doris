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

#include "olap/schema_change.h"

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/olap_file.pb.h>

#include <algorithm>
#include <exception>
#include <map>
#include <mutex>
#include <roaring/roaring.hh>
#include <tuple>

#include "common/logging.h"
#include "common/status.h"
#include "gutil/hash/hash.h"
#include "gutil/integral_types.h"
#include "gutil/strings/numbers.h"
#include "io/fs/file_system.h"
#include "io/io_common.h"
#include "olap/data_dir.h"
#include "olap/delete_handler.h"
#include "olap/field.h"
#include "olap/iterators.h"
#include "olap/merger.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_reader_context.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/inverted_index_writer.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/schema.h"
#include "olap/segment_loader.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_schema.h"
#include "olap/types.h"
#include "olap/utils.h"
#include "olap/wrapper_field.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "util/defer_op.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_reader.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/olap/olap_data_convertor.h"

namespace doris {
class CollectionValue;

using namespace ErrorCode;

constexpr int ALTER_TABLE_BATCH_SIZE = 4096;

class MultiBlockMerger {
public:
    MultiBlockMerger(TabletSharedPtr tablet) : _tablet(tablet), _cmp(tablet) {}

    Status merge(const std::vector<std::unique_ptr<vectorized::Block>>& blocks,
                 RowsetWriter* rowset_writer, uint64_t* merged_rows) {
        int rows = 0;
        for (auto& block : blocks) {
            rows += block->rows();
        }
        if (!rows) {
            return Status::OK();
        }

        std::vector<RowRef> row_refs;
        row_refs.reserve(rows);
        for (auto& block : blocks) {
            for (uint16_t i = 0; i < block->rows(); i++) {
                row_refs.emplace_back(block.get(), i);
            }
        }
        // TODO: try to use pdqsort to replace std::sort
        // The block version is incremental.
        std::stable_sort(row_refs.begin(), row_refs.end(), _cmp);

        auto finalized_block = _tablet->tablet_schema()->create_block();
        int columns = finalized_block.columns();
        *merged_rows += rows;

        if (_tablet->keys_type() == KeysType::AGG_KEYS) {
            auto tablet_schema = _tablet->tablet_schema();
            int key_number = _tablet->num_key_columns();

            std::vector<vectorized::AggregateFunctionPtr> agg_functions;
            std::vector<vectorized::AggregateDataPtr> agg_places;

            for (int i = key_number; i < columns; i++) {
                try {
                    vectorized::AggregateFunctionPtr function =
                            tablet_schema->column(i).get_aggregate_function(
                                    {finalized_block.get_data_type(i)},
                                    vectorized::AGG_LOAD_SUFFIX);
                    agg_functions.push_back(function);
                    // create aggregate data
                    vectorized::AggregateDataPtr place = new char[function->size_of_data()];
                    function->create(place);
                    agg_places.push_back(place);
                } catch (...) {
                    for (int j = 0; j < i - key_number; ++j) {
                        agg_functions[j]->destroy(agg_places[j]);
                        delete[] agg_places[j];
                    }
                    throw;
                }
            }

            DEFER({
                for (int i = 0; i < columns - key_number; i++) {
                    agg_functions[i]->destroy(agg_places[i]);
                    delete[] agg_places[i];
                }
            });

            for (int i = 0; i < rows; i++) {
                auto row_ref = row_refs[i];

                for (int j = key_number; j < columns; j++) {
                    auto column_ptr = row_ref.get_column(j).get();
                    agg_functions[j - key_number]->add(
                            agg_places[j - key_number],
                            const_cast<const vectorized::IColumn**>(&column_ptr), row_ref.position,
                            nullptr);
                }

                if (i == rows - 1 || _cmp.compare(row_refs[i], row_refs[i + 1])) {
                    for (int j = 0; j < key_number; j++) {
                        finalized_block.get_by_position(j).column->assume_mutable()->insert_from(
                                *row_ref.get_column(j), row_ref.position);
                    }

                    for (int j = key_number; j < columns; j++) {
                        agg_functions[j - key_number]->insert_result_into(
                                agg_places[j - key_number],
                                finalized_block.get_by_position(j).column->assume_mutable_ref());
                        agg_functions[j - key_number]->reset(agg_places[j - key_number]);
                    }

                    if (i == rows - 1 || finalized_block.rows() == ALTER_TABLE_BATCH_SIZE) {
                        *merged_rows -= finalized_block.rows();
                        rowset_writer->add_block(&finalized_block);
                        finalized_block.clear_column_data();
                    }
                }
            }
        } else {
            std::vector<RowRef> pushed_row_refs;
            if (_tablet->keys_type() == KeysType::DUP_KEYS) {
                std::swap(pushed_row_refs, row_refs);
            } else if (_tablet->keys_type() == KeysType::UNIQUE_KEYS) {
                for (int i = 0; i < rows; i++) {
                    if (i == rows - 1 || _cmp.compare(row_refs[i], row_refs[i + 1])) {
                        pushed_row_refs.push_back(row_refs[i]);
                    }
                }
            }

            // update real inserted row number
            rows = pushed_row_refs.size();
            *merged_rows -= rows;

            for (int i = 0; i < rows; i += ALTER_TABLE_BATCH_SIZE) {
                int limit = std::min(ALTER_TABLE_BATCH_SIZE, rows - i);

                for (int idx = 0; idx < columns; idx++) {
                    auto column = finalized_block.get_by_position(idx).column->assume_mutable();

                    for (int j = 0; j < limit; j++) {
                        auto row_ref = pushed_row_refs[i + j];
                        column->insert_from(*row_ref.get_column(idx), row_ref.position);
                    }
                }
                rowset_writer->add_block(&finalized_block);
                finalized_block.clear_column_data();
            }
        }

        RETURN_IF_ERROR(rowset_writer->flush());
        return Status::OK();
    }

private:
    struct RowRef {
        RowRef(vectorized::Block* block_, uint16_t position_)
                : block(block_), position(position_) {}
        vectorized::ColumnPtr get_column(int index) const {
            return block->get_by_position(index).column;
        }
        const vectorized::Block* block;
        uint16_t position;
    };

    struct RowRefComparator {
        RowRefComparator(TabletSharedPtr tablet) : _num_columns(tablet->num_key_columns()) {}

        int compare(const RowRef& lhs, const RowRef& rhs) const {
            return lhs.block->compare_at(lhs.position, rhs.position, _num_columns, *rhs.block, -1);
        }

        bool operator()(const RowRef& lhs, const RowRef& rhs) const {
            return compare(lhs, rhs) < 0;
        }

        const size_t _num_columns;
    };

    TabletSharedPtr _tablet;
    RowRefComparator _cmp;
};

BlockChanger::BlockChanger(TabletSchemaSPtr tablet_schema, DescriptorTbl desc_tbl)
        : _desc_tbl(desc_tbl) {
    _schema_mapping.resize(tablet_schema->num_columns());
}

BlockChanger::~BlockChanger() {
    for (auto it = _schema_mapping.begin(); it != _schema_mapping.end(); ++it) {
        SAFE_DELETE(it->default_value);
    }
    _schema_mapping.clear();
}

ColumnMapping* BlockChanger::get_mutable_column_mapping(size_t column_index) {
    if (column_index >= _schema_mapping.size()) {
        return nullptr;
    }

    return &(_schema_mapping[column_index]);
}

Status BlockChanger::change_block(vectorized::Block* ref_block,
                                  vectorized::Block* new_block) const {
    ObjectPool pool;
    RuntimeState* state = pool.add(RuntimeState::create_unique().release());
    state->set_desc_tbl(&_desc_tbl);
    state->set_be_exec_version(_fe_compatible_version);
    RowDescriptor row_desc =
            RowDescriptor(_desc_tbl.get_tuple_descriptor(_desc_tbl.get_row_tuples()[0]), false);

    if (_where_expr != nullptr) {
        vectorized::VExprContext* ctx = nullptr;
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(&pool, *_where_expr, &ctx));
        Defer defer {[&]() { ctx->close(state); }};
        RETURN_IF_ERROR(ctx->prepare(state, row_desc));
        RETURN_IF_ERROR(ctx->open(state));

        RETURN_IF_ERROR(
                vectorized::VExprContext::filter_block(ctx, ref_block, ref_block->columns()));
    }

    const int row_size = ref_block->rows();
    const int column_size = new_block->columns();

    // swap ref_block[key] and new_block[value]
    std::map<int, int> swap_idx_map;

    for (int idx = 0; idx < column_size; idx++) {
        int ref_idx = _schema_mapping[idx].ref_column;

        if (ref_idx < 0 && _type != ROLLUP) {
            // new column, write default value
            auto value = _schema_mapping[idx].default_value;
            auto column = new_block->get_by_position(idx).column->assume_mutable();
            if (value->is_null()) {
                DCHECK(column->is_nullable());
                column->insert_many_defaults(row_size);
            } else {
                auto type_info = get_type_info(_schema_mapping[idx].new_column);
                DefaultValueColumnIterator::insert_default_data(type_info.get(), value->size(),
                                                                value->ptr(), column, row_size);
            }
        } else if (_schema_mapping[idx].expr != nullptr) {
            vectorized::VExprContext* ctx = nullptr;
            RETURN_IF_ERROR(
                    vectorized::VExpr::create_expr_tree(&pool, *_schema_mapping[idx].expr, &ctx));
            Defer defer {[&]() { ctx->close(state); }};
            RETURN_IF_ERROR(ctx->prepare(state, row_desc));
            RETURN_IF_ERROR(ctx->open(state));

            int result_column_id = -1;
            RETURN_IF_ERROR(ctx->execute(ref_block, &result_column_id));
            if (ref_block->get_by_position(result_column_id).column->size() != row_size) {
                return Status::Error<ErrorCode::INTERNAL_ERROR>(
                        "{} size invalid, expect={}, real={}", new_block->get_by_position(idx).name,
                        row_size, ref_block->get_by_position(result_column_id).column->size());
            }

            if (_type != ROLLUP) {
                RETURN_IF_ERROR(
                        _check_cast_valid(ref_block->get_by_position(ref_idx).column,
                                          ref_block->get_by_position(result_column_id).column));
            }
            swap_idx_map[result_column_id] = idx;
        } else {
            // same type, just swap column
            swap_idx_map[ref_idx] = idx;
        }
    }

    for (auto it : swap_idx_map) {
        auto& ref_col = ref_block->get_by_position(it.first);
        auto& new_col = new_block->get_by_position(it.second);

        bool ref_col_nullable = ref_col.column->is_nullable();
        bool new_col_nullable = new_col.column->is_nullable();

        if (ref_col_nullable != new_col_nullable) {
            // not nullable to nullable
            if (new_col_nullable) {
                auto* new_nullable_col = assert_cast<vectorized::ColumnNullable*>(
                        new_col.column->assume_mutable().get());

                new_nullable_col->swap_nested_column(ref_col.column);
                new_nullable_col->get_null_map_data().resize_fill(new_nullable_col->size());
            } else {
                // nullable to not nullable:
                // suppose column `c_phone` is originally varchar(16) NOT NULL,
                // then do schema change `alter table test modify column c_phone int not null`,
                // the cast expr of schema change is `CastExpr(CAST String to Nullable(Int32))`,
                // so need to handle nullable to not nullable here
                auto* ref_nullable_col = assert_cast<vectorized::ColumnNullable*>(
                        ref_col.column->assume_mutable().get());

                ref_nullable_col->swap_nested_column(new_col.column);
            }
        } else {
            new_block->get_by_position(it.second).column.swap(
                    ref_block->get_by_position(it.first).column);
        }
    }
    return Status::OK();
}

// This check is to prevent schema-change from causing data loss
Status BlockChanger::_check_cast_valid(vectorized::ColumnPtr ref_column,
                                       vectorized::ColumnPtr new_column) const {
    if (ref_column->is_nullable() != new_column->is_nullable()) {
        if (ref_column->is_nullable()) {
            auto* ref_null_map =
                    vectorized::check_and_get_column<vectorized::ColumnNullable>(ref_column)
                            ->get_null_map_column()
                            .get_data()
                            .data();

            bool is_changed = false;
            for (size_t i = 0; i < ref_column->size(); i++) {
                is_changed |= ref_null_map[i];
            }
            if (is_changed) {
                return Status::DataQualityError("Null data is changed to not nullable");
            }
        } else {
            auto* new_null_map =
                    vectorized::check_and_get_column<vectorized::ColumnNullable>(new_column)
                            ->get_null_map_column()
                            .get_data()
                            .data();

            bool is_changed = false;
            for (size_t i = 0; i < ref_column->size(); i++) {
                is_changed |= new_null_map[i];
            }
            if (is_changed) {
                return Status::DataQualityError("Some data is changed to null");
            }
        }
    }

    if (ref_column->is_nullable() && new_column->is_nullable()) {
        auto* ref_null_map =
                vectorized::check_and_get_column<vectorized::ColumnNullable>(ref_column)
                        ->get_null_map_column()
                        .get_data()
                        .data();
        auto* new_null_map =
                vectorized::check_and_get_column<vectorized::ColumnNullable>(new_column)
                        ->get_null_map_column()
                        .get_data()
                        .data();

        bool is_changed = false;
        for (size_t i = 0; i < ref_column->size(); i++) {
            is_changed |= (ref_null_map[i] != new_null_map[i]);
        }
        if (is_changed) {
            return Status::DataQualityError("is_null of data is changed!");
        }
    }
    return Status::OK();
}

Status LinkedSchemaChange::process(RowsetReaderSharedPtr rowset_reader, RowsetWriter* rowset_writer,
                                   TabletSharedPtr new_tablet, TabletSharedPtr base_tablet,
                                   TabletSchemaSPtr base_tablet_schema) {
    // In some cases, there may be more than one type of rowset in a tablet,
    // in which case the conversion cannot be done directly by linked schema change,
    // but requires direct schema change to rewrite the data.
    if (rowset_reader->type() != rowset_writer->type()) {
        LOG(INFO) << "the type of rowset " << rowset_reader->rowset()->rowset_id()
                  << " in base tablet is not same as type " << rowset_writer->type()
                  << ", use direct schema change.";
        return SchemaChangeHandler::get_sc_procedure(_changer, false, true)
                ->process(rowset_reader, rowset_writer, new_tablet, base_tablet,
                          base_tablet_schema);
    } else {
        Status status = rowset_writer->add_rowset_for_linked_schema_change(rowset_reader->rowset());
        if (!status) {
            LOG(WARNING) << "fail to convert rowset."
                         << ", new_tablet=" << new_tablet->full_name()
                         << ", version=" << rowset_writer->version().first << "-"
                         << rowset_writer->version().second << ", error status " << status;
            return status;
        }
        // copy delete bitmap to new tablet.
        if (new_tablet->keys_type() == UNIQUE_KEYS &&
            new_tablet->enable_unique_key_merge_on_write()) {
            DeleteBitmap origin_delete_bitmap(base_tablet->tablet_id());
            base_tablet->tablet_meta()->delete_bitmap().subset(
                    {rowset_reader->rowset()->rowset_id(), 0, 0},
                    {rowset_reader->rowset()->rowset_id(), UINT32_MAX, INT64_MAX},
                    &origin_delete_bitmap);
            for (auto iter = origin_delete_bitmap.delete_bitmap.begin();
                 iter != origin_delete_bitmap.delete_bitmap.end(); ++iter) {
                int ret = new_tablet->tablet_meta()->delete_bitmap().set(
                        {rowset_writer->rowset_id(), std::get<1>(iter->first),
                         std::get<2>(iter->first)},
                        iter->second);
                DCHECK(ret == 1);
            }
        }
        return Status::OK();
    }
}

Status VSchemaChangeDirectly::_inner_process(RowsetReaderSharedPtr rowset_reader,
                                             RowsetWriter* rowset_writer,
                                             TabletSharedPtr new_tablet,
                                             TabletSchemaSPtr base_tablet_schema) {
    do {
        auto new_block =
                vectorized::Block::create_unique(new_tablet->tablet_schema()->create_block());
        auto ref_block = vectorized::Block::create_unique(base_tablet_schema->create_block());

        rowset_reader->next_block(ref_block.get());
        if (ref_block->rows() == 0) {
            break;
        }

        RETURN_IF_ERROR(_changer.change_block(ref_block.get(), new_block.get()));
        RETURN_IF_ERROR(rowset_writer->add_block(new_block.get()));
    } while (true);

    if (!rowset_writer->flush()) {
        return Status::Error<ALTER_STATUS_ERR>();
    }

    return Status::OK();
}

VSchemaChangeWithSorting::VSchemaChangeWithSorting(const BlockChanger& changer,
                                                   size_t memory_limitation)
        : _changer(changer),
          _memory_limitation(memory_limitation),
          _temp_delta_versions(Version::mock()) {
    _mem_tracker = std::make_unique<MemTracker>(
            fmt::format("VSchemaChangeWithSorting:changer={}", std::to_string(int64(&changer))));
}

Status VSchemaChangeWithSorting::_inner_process(RowsetReaderSharedPtr rowset_reader,
                                                RowsetWriter* rowset_writer,
                                                TabletSharedPtr new_tablet,
                                                TabletSchemaSPtr base_tablet_schema) {
    // for internal sorting
    std::vector<std::unique_ptr<vectorized::Block>> blocks;

    // for external sorting
    // src_rowsets to store the rowset generated by internal sorting
    std::vector<RowsetSharedPtr> src_rowsets;

    Defer defer {[&]() {
        // remove the intermediate rowsets generated by internal sorting
        for (auto& row_set : src_rowsets) {
            StorageEngine::instance()->add_unused_rowset(row_set);
        }
    }};

    RowsetSharedPtr rowset = rowset_reader->rowset();
    SegmentsOverlapPB segments_overlap = rowset->rowset_meta()->segments_overlap();
    int64_t newest_write_timestamp = rowset->newest_write_timestamp();
    _temp_delta_versions.first = _temp_delta_versions.second;

    auto create_rowset = [&]() -> Status {
        if (blocks.empty()) {
            return Status::OK();
        }

        RowsetSharedPtr rowset;
        RETURN_IF_ERROR(_internal_sorting(
                blocks, Version(_temp_delta_versions.second, _temp_delta_versions.second),
                newest_write_timestamp, new_tablet, BETA_ROWSET, segments_overlap, &rowset));
        src_rowsets.push_back(rowset);

        for (auto& block : blocks) {
            _mem_tracker->release(block->allocated_bytes());
        }
        blocks.clear();

        // increase temp version
        _temp_delta_versions.second++;
        return Status::OK();
    };

    auto new_block = vectorized::Block::create_unique(new_tablet->tablet_schema()->create_block());

    do {
        auto ref_block = vectorized::Block::create_unique(base_tablet_schema->create_block());
        rowset_reader->next_block(ref_block.get());
        if (ref_block->rows() == 0) {
            break;
        }

        RETURN_IF_ERROR(_changer.change_block(ref_block.get(), new_block.get()));
        if (_mem_tracker->consumption() + new_block->allocated_bytes() > _memory_limitation) {
            RETURN_IF_ERROR(create_rowset());

            if (_mem_tracker->consumption() + new_block->allocated_bytes() > _memory_limitation) {
                LOG(WARNING) << "Memory limitation is too small for Schema Change."
                             << " _memory_limitation=" << _memory_limitation
                             << ", new_block->allocated_bytes()=" << new_block->allocated_bytes()
                             << ", consumption=" << _mem_tracker->consumption();
                return Status::Error<INVALID_ARGUMENT>();
            }
        }
        _mem_tracker->consume(new_block->allocated_bytes());

        // move unique ptr
        blocks.push_back(
                vectorized::Block::create_unique(new_tablet->tablet_schema()->create_block()));
        swap(blocks.back(), new_block);
    } while (true);

    RETURN_IF_ERROR(create_rowset());

    if (src_rowsets.empty()) {
        RETURN_IF_ERROR(rowset_writer->flush());
    } else {
        RETURN_IF_ERROR(_external_sorting(src_rowsets, rowset_writer, new_tablet));
    }

    return Status::OK();
}

Status VSchemaChangeWithSorting::_internal_sorting(
        const std::vector<std::unique_ptr<vectorized::Block>>& blocks, const Version& version,
        int64_t newest_write_timestamp, TabletSharedPtr new_tablet, RowsetTypePB new_rowset_type,
        SegmentsOverlapPB segments_overlap, RowsetSharedPtr* rowset) {
    uint64_t merged_rows = 0;
    MultiBlockMerger merger(new_tablet);

    std::unique_ptr<RowsetWriter> rowset_writer;
    RowsetWriterContext context;
    context.version = version;
    context.rowset_state = VISIBLE;
    context.segments_overlap = segments_overlap;
    context.tablet_schema = new_tablet->tablet_schema();
    context.newest_write_timestamp = newest_write_timestamp;
    RETURN_IF_ERROR(new_tablet->create_rowset_writer(context, &rowset_writer));

    Defer defer {[&]() {
        new_tablet->data_dir()->remove_pending_ids(ROWSET_ID_PREFIX +
                                                   rowset_writer->rowset_id().to_string());
    }};

    RETURN_IF_ERROR(merger.merge(blocks, rowset_writer.get(), &merged_rows));

    _add_merged_rows(merged_rows);
    *rowset = rowset_writer->build();
    return Status::OK();
}

Status VSchemaChangeWithSorting::_external_sorting(vector<RowsetSharedPtr>& src_rowsets,
                                                   RowsetWriter* rowset_writer,
                                                   TabletSharedPtr new_tablet) {
    std::vector<RowsetReaderSharedPtr> rs_readers;
    for (auto& rowset : src_rowsets) {
        RowsetReaderSharedPtr rs_reader;
        RETURN_IF_ERROR(rowset->create_reader(&rs_reader));
        rs_readers.push_back(rs_reader);
    }

    Merger::Statistics stats;
    RETURN_IF_ERROR(Merger::vmerge_rowsets(new_tablet, READER_ALTER_TABLE,
                                           new_tablet->tablet_schema(), rs_readers, rowset_writer,
                                           &stats));

    _add_merged_rows(stats.merged_rows);
    _add_filtered_rows(stats.filtered_rows);
    return Status::OK();
}

SchemaChangeForInvertedIndex::SchemaChangeForInvertedIndex(
        const std::vector<TOlapTableIndex>& alter_inverted_indexs,
        const TabletSchemaSPtr& tablet_schema)
        : _alter_inverted_indexs(alter_inverted_indexs), _tablet_schema(tablet_schema) {
    _olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();
}

SchemaChangeForInvertedIndex::~SchemaChangeForInvertedIndex() {
    VLOG_NOTICE << "~SchemaChangeForInvertedIndex()";
    _inverted_index_builders.clear();
    _index_metas.clear();
    _olap_data_convertor.reset();
}

Status SchemaChangeForInvertedIndex::process(RowsetReaderSharedPtr rowset_reader,
                                             RowsetWriter* rowset_writer,
                                             TabletSharedPtr new_tablet,
                                             TabletSharedPtr base_tablet,
                                             TabletSchemaSPtr base_tablet_schema) {
    Status res = Status::OK();
    if (rowset_reader->rowset()->empty() || rowset_reader->rowset()->num_rows() == 0) {
        return Status::OK();
    }

    // create inverted index writer
    auto rowset_meta = rowset_reader->rowset()->rowset_meta();
    std::string segment_dir = base_tablet->tablet_path();
    auto fs = rowset_meta->fs();

    // load segments
    SegmentCacheHandle segment_cache_handle;
    RETURN_NOT_OK(SegmentLoader::instance()->load_segments(
            std::static_pointer_cast<BetaRowset>(rowset_reader->rowset()), &segment_cache_handle,
            false));

    for (auto& seg_ptr : segment_cache_handle.get_segments()) {
        std::string segment_filename =
                fmt::format("{}_{}.dat", rowset_meta->rowset_id().to_string(), seg_ptr->id());
        std::vector<ColumnId> return_columns;
        std::vector<std::pair<int64_t, int64_t>> inverted_index_writer_signs;
        _olap_data_convertor->reserve(_alter_inverted_indexs.size());
        // create inverted index writer
        for (auto& inverted_index : _alter_inverted_indexs) {
            DCHECK_EQ(inverted_index.columns.size(), 1);
            auto index_id = inverted_index.index_id;
            auto column_name = inverted_index.columns[0];
            auto column_idx = _tablet_schema->field_index(column_name);
            if (column_idx < 0) {
                LOG(WARNING) << "referenced column was missing. "
                             << "[column=" << column_name << " referenced_column=" << column_idx
                             << "]";
                return Status::Error<CE_CMD_PARAMS_ERROR>();
            }
            auto column = _tablet_schema->column(column_idx);
            return_columns.emplace_back(column_idx);
            _olap_data_convertor->add_column_data_convertor(column);

            std::unique_ptr<Field> field(FieldFactory::create(column));
            _index_metas.emplace_back(new TabletIndex());
            _index_metas.back()->init_from_thrift(inverted_index, *_tablet_schema);
            std::unique_ptr<segment_v2::InvertedIndexColumnWriter> inverted_index_builder;
            try {
                RETURN_IF_ERROR(segment_v2::InvertedIndexColumnWriter::create(
                        field.get(), &inverted_index_builder, segment_filename, segment_dir,
                        _index_metas.back().get(), fs));
            } catch (const std::exception& e) {
                LOG(WARNING) << "CLuceneError occured: " << e.what();
                return Status::Error<IO_ERROR>();
            }

            if (inverted_index_builder) {
                auto writer_sign = std::make_pair(seg_ptr->id(), index_id);
                _inverted_index_builders.insert(
                        std::make_pair(writer_sign, std::move(inverted_index_builder)));
                inverted_index_writer_signs.push_back(writer_sign);
            }
        }

        // create iterator for each segment
        StorageReadOptions read_options;
        OlapReaderStatistics stats;
        read_options.stats = &stats;
        read_options.tablet_schema = _tablet_schema;
        std::unique_ptr<Schema> schema =
                std::make_unique<Schema>(_tablet_schema->columns(), return_columns);
        std::unique_ptr<RowwiseIterator> iter;
        res = seg_ptr->new_iterator(*schema, read_options, &iter);
        if (!res.ok()) {
            LOG(WARNING) << "failed to create iterator[" << seg_ptr->id()
                         << "]: " << res.to_string();
            return Status::Error<ROWSET_READER_INIT>();
        }

        std::shared_ptr<vectorized::Block> block =
                std::make_shared<vectorized::Block>(_tablet_schema->create_block(return_columns));
        while (true) {
            res = iter->next_batch(block.get());
            if (!res.ok()) {
                if (res.is<END_OF_FILE>()) {
                    break;
                }
                RETURN_NOT_OK_LOG(
                        res, "failed to read next block when schema change for inverted index.");
            }

            // write inverted index
            if (_write_inverted_index(iter->data_id(), block.get()) != Status::OK()) {
                res = Status::Error<SCHEMA_CHANGE_INFO_INVALID>();
                LOG(WARNING) << "failed to write block.";
                return res;
            }
            block->clear_column_data();
        }

        // finish write inverted index, flush data to compound file
        for (auto& writer_sign : inverted_index_writer_signs) {
            try {
                if (_inverted_index_builders[writer_sign]) {
                    _inverted_index_builders[writer_sign]->finish();
                }
            } catch (const std::exception& e) {
                LOG(WARNING) << "CLuceneError occured: " << e.what();
                return Status::Error<IO_ERROR>();
            }
        }

        _olap_data_convertor->reset();
    }

    _inverted_index_builders.clear();
    _index_metas.clear();

    LOG(INFO) << "all row nums. source_rows=" << rowset_reader->rowset()->num_rows();
    return res;
}

Status SchemaChangeForInvertedIndex::_add_nullable(
        const std::string& column_name, const std::pair<int64_t, int64_t>& index_writer_sign,
        Field* field, const uint8_t* null_map, const uint8_t** ptr, size_t num_rows) {
    size_t offset = 0;
    auto next_run_step = [&]() {
        size_t step = 1;
        for (auto i = offset + 1; i < num_rows; ++i) {
            if (null_map[offset] == null_map[i]) {
                step++;
            } else {
                break;
            }
        }
        return step;
    };

    try {
        do {
            auto step = next_run_step();
            if (null_map[offset]) {
                RETURN_IF_ERROR(_inverted_index_builders[index_writer_sign]->add_nulls(step));
            } else {
                if (field->type() == FieldType::OLAP_FIELD_TYPE_ARRAY) {
                    DCHECK(field->get_sub_field_count() == 1);
                    const auto* col_cursor = reinterpret_cast<const CollectionValue*>(*ptr);
                    RETURN_IF_ERROR(_inverted_index_builders[index_writer_sign]->add_array_values(
                            field->get_sub_field(0)->size(), col_cursor, step));
                } else {
                    RETURN_IF_ERROR(_inverted_index_builders[index_writer_sign]->add_values(
                            column_name, *ptr, step));
                }
            }
            *ptr += field->size() * step;
            offset += step;
        } while (offset < num_rows);
    } catch (const std::exception& e) {
        LOG(WARNING) << "CLuceneError occured: " << e.what();
        return Status::Error<IO_ERROR>();
    }

    return Status::OK();
}

Status SchemaChangeForInvertedIndex::_add_data(const std::string& column_name,
                                               const std::pair<int64_t, int64_t>& index_writer_sign,
                                               Field* field, const uint8_t** ptr, size_t num_rows) {
    try {
        if (field->type() == FieldType::OLAP_FIELD_TYPE_ARRAY) {
            DCHECK(field->get_sub_field_count() == 1);
            const auto* col_cursor = reinterpret_cast<const CollectionValue*>(*ptr);
            RETURN_IF_ERROR(_inverted_index_builders[index_writer_sign]->add_array_values(
                    field->get_sub_field(0)->size(), col_cursor, num_rows));
        } else {
            RETURN_IF_ERROR(_inverted_index_builders[index_writer_sign]->add_values(
                    column_name, *ptr, num_rows));
        }
    } catch (const std::exception& e) {
        LOG(WARNING) << "CLuceneError occured: " << e.what();
        return Status::Error<IO_ERROR>();
    }

    return Status::OK();
}

Status SchemaChangeForInvertedIndex::_write_inverted_index(int32_t segment_idx,
                                                           vectorized::Block* block) {
    VLOG_DEBUG << "begin to write inverted index";
    // converter block data
    _olap_data_convertor->set_source_content(block, 0, block->rows());
    int idx = 0;
    for (auto& inverted_index : _alter_inverted_indexs) {
        auto column_name = inverted_index.columns[0];
        auto column_idx = _tablet_schema->field_index(column_name);
        if (column_idx < 0) {
            LOG(WARNING) << "referenced column was missing. "
                         << "[column=" << column_name << " referenced_column=" << column_idx << "]";
            return Status::Error<CE_CMD_PARAMS_ERROR>();
        }
        auto column = _tablet_schema->column(column_idx);
        auto index_id = inverted_index.index_id;

        auto converted_result = _olap_data_convertor->convert_column_data(idx++);
        if (converted_result.first != Status::OK()) {
            LOG(WARNING) << "failed to convert block, errcode: " << converted_result.first;
            return converted_result.first;
        }

        auto writer_sign = std::make_pair(segment_idx, index_id);
        std::unique_ptr<Field> field(FieldFactory::create(column));
        const auto* ptr = (const uint8_t*)converted_result.second->get_data();
        if (converted_result.second->get_nullmap()) {
            RETURN_IF_ERROR(_add_nullable(column_name, writer_sign, field.get(),
                                          converted_result.second->get_nullmap(), &ptr,
                                          block->rows()));
        } else {
            RETURN_IF_ERROR(_add_data(column_name, writer_sign, field.get(), &ptr, block->rows()));
        }
    }
    _olap_data_convertor->clear_source_content();

    return Status::OK();
}

Status SchemaChangeHandler::process_alter_tablet_v2(const TAlterTabletReqV2& request) {
    if (!request.__isset.desc_tbl) {
        return Status::Error<INVALID_ARGUMENT>().append(
                "desc_tbl is not set. Maybe the FE version is not equal to the BE "
                "version.");
    }

    LOG(INFO) << "begin to do request alter tablet: base_tablet_id=" << request.base_tablet_id
              << ", new_tablet_id=" << request.new_tablet_id
              << ", alter_version=" << request.alter_version;

    TabletSharedPtr base_tablet =
            StorageEngine::instance()->tablet_manager()->get_tablet(request.base_tablet_id);
    if (base_tablet == nullptr) {
        LOG(WARNING) << "fail to find base tablet. base_tablet=" << request.base_tablet_id;
        return Status::Error<TABLE_NOT_FOUND>();
    }
    // Lock schema_change_lock util schema change info is stored in tablet header
    std::unique_lock<std::mutex> schema_change_lock(base_tablet->get_schema_change_lock(),
                                                    std::try_to_lock);
    if (!schema_change_lock.owns_lock()) {
        LOG(WARNING) << "failed to obtain schema change lock. "
                     << "base_tablet=" << request.base_tablet_id;
        return Status::Error<TRY_LOCK_FAILED>();
    }

    Status res = _do_process_alter_tablet_v2(request);
    LOG(INFO) << "finished alter tablet process, res=" << res;
    return res;
}

Status SchemaChangeHandler::process_alter_inverted_index(const TAlterInvertedIndexReq& request) {
    LOG(INFO) << "begin to do request alter inverted index: tablet_id=" << request.tablet_id
              << ", schema_hash=" << request.schema_hash
              << ", alter_version=" << request.alter_version;
    TabletSharedPtr tablet =
            StorageEngine::instance()->tablet_manager()->get_tablet(request.tablet_id);
    if (tablet == nullptr) {
        LOG(WARNING) << "fail to find tablet. tablet=" << request.tablet_id;
        return Status::Error<TABLE_NOT_FOUND>();
    }

    // // Lock schema_change_lock util schema change info is stored in tablet header
    // std::unique_lock<std::mutex> schema_change_lock(tablet->get_schema_change_lock(),
    //                                                 std::try_to_lock);
    // if (!schema_change_lock.owns_lock()) {
    //     LOG(WARNING) << "failed to obtain schema change lock. "
    //                  << "tablet=" << request.tablet_id;
    //     return Status::Error<TRY_LOCK_FAILED>();
    // }

    Status res = _do_process_alter_inverted_index(tablet, request);
    LOG(INFO) << "finished alter inverted index process, res=" << res;
    return res;
}

std::shared_mutex SchemaChangeHandler::_mutex;
std::unordered_set<int64_t> SchemaChangeHandler::_tablet_ids_in_converting;

// In the past schema change and rollup will create new tablet  and will wait for txns starting before the task to finished
// It will cost a lot of time to wait and the task is very difficult to understand.
// In alter task v2, FE will call BE to create tablet and send an alter task to BE to convert historical data.
// The admin should upgrade all BE and then upgrade FE.
// Should delete the old code after upgrade finished.
Status SchemaChangeHandler::_do_process_alter_tablet_v2(const TAlterTabletReqV2& request) {
    Status res = Status::OK();
    TabletSharedPtr base_tablet =
            StorageEngine::instance()->tablet_manager()->get_tablet(request.base_tablet_id);
    if (base_tablet == nullptr) {
        LOG(WARNING) << "fail to find base tablet. base_tablet=" << request.base_tablet_id;
        return Status::Error<TABLE_NOT_FOUND>();
    }

    // new tablet has to exist
    TabletSharedPtr new_tablet =
            StorageEngine::instance()->tablet_manager()->get_tablet(request.new_tablet_id);
    if (new_tablet == nullptr) {
        LOG(WARNING) << "fail to find new tablet."
                     << " new_tablet=" << request.new_tablet_id;
        return Status::Error<TABLE_NOT_FOUND>();
    }

    // check if tablet's state is not_ready, if it is ready, it means the tablet already finished
    // check whether the tablet's max continuous version == request.version
    if (new_tablet->tablet_state() != TABLET_NOTREADY) {
        res = _validate_alter_result(new_tablet, request);
        LOG(INFO) << "tablet's state=" << new_tablet->tablet_state()
                  << " the convert job already finished, check its version"
                  << " res=" << res;
        return res;
    }

    LOG(INFO) << "finish to validate alter tablet request. begin to convert data from base tablet "
                 "to new tablet"
              << " base_tablet=" << base_tablet->full_name()
              << " new_tablet=" << new_tablet->full_name();

    std::shared_lock base_migration_rlock(base_tablet->get_migration_lock(), std::try_to_lock);
    if (!base_migration_rlock.owns_lock()) {
        return Status::Error<TRY_LOCK_FAILED>();
    }
    std::shared_lock new_migration_rlock(new_tablet->get_migration_lock(), std::try_to_lock);
    if (!new_migration_rlock.owns_lock()) {
        return Status::Error<TRY_LOCK_FAILED>();
    }

    std::vector<Version> versions_to_be_changed;
    int64_t end_version = -1;
    // reader_context is stack variables, it's lifetime should keep the same
    // with rs_readers
    RowsetReaderContext reader_context;
    std::vector<RowsetReaderSharedPtr> rs_readers;
    // delete handlers for new tablet
    DeleteHandler delete_handler;
    std::vector<ColumnId> return_columns;
    // Create a new tablet schema, should merge with dropped columns in light weight schema change
    TabletSchemaSPtr base_tablet_schema = std::make_shared<TabletSchema>();
    base_tablet_schema->copy_from(*base_tablet->tablet_schema());
    if (!request.columns.empty() && request.columns[0].col_unique_id >= 0) {
        base_tablet_schema->clear_columns();
        for (const auto& column : request.columns) {
            base_tablet_schema->append_column(TabletColumn(column));
        }
    }
    // Use tablet schema directly from base tablet, they are the newest schema, not contain
    // dropped column during light weight schema change.
    // But the tablet schema in base tablet maybe not the latest from FE, so that if fe pass through
    // a tablet schema, then use request schema.
    size_t num_cols = request.columns.empty() ? base_tablet->tablet_schema()->num_columns()
                                              : request.columns.size();
    return_columns.resize(num_cols);
    for (int i = 0; i < num_cols; ++i) {
        return_columns[i] = i;
    }

    // begin to find deltas to convert from base tablet to new tablet so that
    // obtain base tablet and new tablet's push lock and header write lock to prevent loading data
    {
        std::lock_guard<std::mutex> base_tablet_lock(base_tablet->get_push_lock());
        std::lock_guard<std::mutex> new_tablet_lock(new_tablet->get_push_lock());
        std::lock_guard<std::shared_mutex> base_tablet_wlock(base_tablet->get_header_lock());
        std::lock_guard<std::shared_mutex> new_tablet_wlock(new_tablet->get_header_lock());

        do {
            RowsetSharedPtr max_rowset;
            // get history data to be converted and it will check if there is hold in base tablet
            if (!_get_versions_to_be_changed(base_tablet, &versions_to_be_changed, &max_rowset)) {
                LOG(WARNING) << "fail to get version to be changed. res=" << res;
                break;
            }

            // should check the max_version >= request.alter_version, if not the convert is useless
            if (max_rowset == nullptr || max_rowset->end_version() < request.alter_version) {
                res = Status::InternalError(
                        "base tablet's max version={} is less than request version={}",
                        (max_rowset == nullptr ? 0 : max_rowset->end_version()),
                        request.alter_version);
                break;
            }
            // before calculating version_to_be_changed,
            // remove all data from new tablet, prevent to rewrite data(those double pushed when wait)
            LOG(INFO) << "begin to remove all data from new tablet to prevent rewrite."
                      << " new_tablet=" << new_tablet->full_name();
            std::vector<RowsetSharedPtr> rowsets_to_delete;
            std::vector<std::pair<Version, RowsetSharedPtr>> version_rowsets;
            new_tablet->acquire_version_and_rowsets(&version_rowsets);
            std::sort(version_rowsets.begin(), version_rowsets.end(),
                      [](const std::pair<Version, RowsetSharedPtr>& l,
                         const std::pair<Version, RowsetSharedPtr>& r) {
                          return l.first.first < r.first.first;
                      });
            for (auto& pair : version_rowsets) {
                if (pair.first.second <= max_rowset->end_version()) {
                    rowsets_to_delete.push_back(pair.second);
                } else if (pair.first.first <= max_rowset->end_version()) {
                    // If max version is [X-10] and new tablet has version [7-9][10-12],
                    // we only can remove [7-9] from new tablet. If we add [X-10] to new tablet, it will has version
                    // cross: [X-10] [10-12].
                    // So, we should return OLAP_ERR_VERSION_ALREADY_MERGED for fast fail.
                    LOG(WARNING) << "New tablet has a version " << pair.first
                                 << " crossing base tablet's max_version="
                                 << max_rowset->end_version();
                    return Status::Error<VERSION_ALREADY_MERGED>();
                }
            }
            std::vector<RowsetSharedPtr> empty_vec;
            new_tablet->modify_rowsets(empty_vec, rowsets_to_delete);
            // inherit cumulative_layer_point from base_tablet
            // check if new_tablet.ce_point > base_tablet.ce_point?
            new_tablet->set_cumulative_layer_point(-1);
            // save tablet meta
            new_tablet->save_meta();
            for (auto& rowset : rowsets_to_delete) {
                // do not call rowset.remove directly, using gc thread to delete it
                StorageEngine::instance()->add_unused_rowset(rowset);
            }

            // init one delete handler
            for (auto& version : versions_to_be_changed) {
                end_version = std::max(end_version, version.second);
            }

            // acquire data sources correspond to history versions
            base_tablet->capture_rs_readers(versions_to_be_changed, &rs_readers);
            if (rs_readers.empty()) {
                LOG(WARNING) << "fail to acquire all data sources. "
                             << "version_num=" << versions_to_be_changed.size()
                             << ", data_source_num=" << rs_readers.size();
                res = Status::Error<ALTER_DELTA_DOES_NOT_EXISTS>();
                break;
            }
            auto& all_del_preds = base_tablet->delete_predicates();
            for (auto& delete_pred : all_del_preds) {
                if (delete_pred->version().first > end_version) {
                    continue;
                }
                base_tablet_schema->merge_dropped_columns(
                        base_tablet->tablet_schema(delete_pred->version()));
            }
            res = delete_handler.init(base_tablet_schema, all_del_preds, end_version);
            if (!res) {
                LOG(WARNING) << "init delete handler failed. base_tablet="
                             << base_tablet->full_name() << ", end_version=" << end_version;
                break;
            }

            reader_context.reader_type = READER_ALTER_TABLE;
            reader_context.tablet_schema = base_tablet_schema;
            reader_context.need_ordered_result = true;
            reader_context.delete_handler = &delete_handler;
            reader_context.return_columns = &return_columns;
            reader_context.sequence_id_idx = reader_context.tablet_schema->sequence_col_idx();
            reader_context.is_unique = base_tablet->keys_type() == UNIQUE_KEYS;
            reader_context.batch_size = ALTER_TABLE_BATCH_SIZE;
            reader_context.delete_bitmap = &base_tablet->tablet_meta()->delete_bitmap();
            reader_context.version = Version(0, end_version);
            for (auto& rs_reader : rs_readers) {
                res = rs_reader->init(&reader_context);
                if (!res) {
                    LOG(WARNING) << "failed to init rowset reader: " << base_tablet->full_name();
                    break;
                }
            }
        } while (false);
    }

    do {
        if (!res) {
            break;
        }
        SchemaChangeParams sc_params;

        DescriptorTbl::create(&sc_params.pool, request.desc_tbl, &sc_params.desc_tbl);
        sc_params.base_tablet = base_tablet;
        sc_params.new_tablet = new_tablet;
        sc_params.ref_rowset_readers = rs_readers;
        sc_params.delete_handler = &delete_handler;
        sc_params.base_tablet_schema = base_tablet_schema;
        sc_params.be_exec_version = request.be_exec_version;
        DCHECK(request.__isset.alter_tablet_type);
        switch (request.alter_tablet_type) {
        case TAlterTabletType::SCHEMA_CHANGE:
            sc_params.alter_tablet_type = AlterTabletType::SCHEMA_CHANGE;
            break;
        case TAlterTabletType::ROLLUP:
            sc_params.alter_tablet_type = AlterTabletType::ROLLUP;
            break;
        case TAlterTabletType::MIGRATION:
            sc_params.alter_tablet_type = AlterTabletType::MIGRATION;
            break;
        }
        if (request.__isset.materialized_view_params) {
            for (auto item : request.materialized_view_params) {
                AlterMaterializedViewParam mv_param;
                mv_param.column_name = item.column_name;
                /*
                 * origin_column_name is always be set now,
                 * but origin_column_name may be not set in some materialized view function. eg:count(1)
                */
                if (item.__isset.origin_column_name) {
                    mv_param.origin_column_name = item.origin_column_name;
                }

                if (item.__isset.mv_expr) {
                    mv_param.expr = std::make_shared<TExpr>(item.mv_expr);
                }
                sc_params.materialized_params_map.insert(
                        std::make_pair(item.column_name, mv_param));
            }
        }
        {
            std::lock_guard<std::shared_mutex> wrlock(_mutex);
            _tablet_ids_in_converting.insert(new_tablet->tablet_id());
        }
        res = _convert_historical_rowsets(sc_params);
        if (new_tablet->keys_type() != UNIQUE_KEYS ||
            !new_tablet->enable_unique_key_merge_on_write() || !res) {
            {
                std::lock_guard<std::shared_mutex> wrlock(_mutex);
                _tablet_ids_in_converting.erase(new_tablet->tablet_id());
            }
        }
        if (!res) {
            break;
        }

        // For unique with merge-on-write table, should process delete bitmap here.
        // 1. During double write, the newly imported rowsets does not calculate
        // delete bitmap and publish successfully.
        // 2. After conversion, calculate delete bitmap for the rowsets imported
        // during double write. During this period, new data can still be imported
        // witout calculating delete bitmap and publish successfully.
        // 3. Block the new publish, calculate the delete bitmap of the
        // incremental rowsets.
        // 4. Switch the tablet status to TABLET_RUNNING. The newly imported
        // data will calculate delete bitmap.
        if (new_tablet->keys_type() == UNIQUE_KEYS &&
            new_tablet->enable_unique_key_merge_on_write()) {
            // step 2
            int64_t max_version = new_tablet->max_version().second;
            std::vector<RowsetSharedPtr> rowsets;
            if (end_version < max_version) {
                LOG(INFO)
                        << "alter table for unique with merge-on-write, calculate delete bitmap of "
                        << "double write rowsets for version: " << end_version + 1 << "-"
                        << max_version;
                RETURN_IF_ERROR(new_tablet->capture_consistent_rowsets(
                        {end_version + 1, max_version}, &rowsets));
            }
            for (auto rowset_ptr : rowsets) {
                if (rowset_ptr->version().second <= end_version) {
                    continue;
                }
                std::lock_guard<std::mutex> rwlock(new_tablet->get_rowset_update_lock());
                std::shared_lock<std::shared_mutex> wrlock(new_tablet->get_header_lock());
                RETURN_IF_ERROR(new_tablet->update_delete_bitmap_without_lock(rowset_ptr));
            }

            // step 3
            std::lock_guard<std::mutex> rwlock(new_tablet->get_rowset_update_lock());
            std::lock_guard<std::shared_mutex> new_wlock(new_tablet->get_header_lock());
            int64_t new_max_version = new_tablet->max_version().second;
            rowsets.clear();
            if (max_version < new_max_version) {
                LOG(INFO)
                        << "alter table for unique with merge-on-write, calculate delete bitmap of "
                        << "incremental rowsets for version: " << max_version + 1 << "-"
                        << new_max_version;
                RETURN_IF_ERROR(new_tablet->capture_consistent_rowsets(
                        {max_version + 1, new_max_version}, &rowsets));
            }
            for (auto rowset_ptr : rowsets) {
                if (rowset_ptr->version().second <= max_version) {
                    continue;
                }
                RETURN_IF_ERROR(new_tablet->update_delete_bitmap_without_lock(rowset_ptr));
            }

            // step 4
            {
                std::lock_guard<std::shared_mutex> wrlock(_mutex);
                _tablet_ids_in_converting.erase(new_tablet->tablet_id());
            }
            res = new_tablet->set_tablet_state(TabletState::TABLET_RUNNING);
            if (!res) {
                break;
            }
            new_tablet->save_meta();
        } else {
            // set state to ready
            std::lock_guard<std::shared_mutex> new_wlock(new_tablet->get_header_lock());
            res = new_tablet->set_tablet_state(TabletState::TABLET_RUNNING);
            if (!res) {
                break;
            }
            new_tablet->save_meta();
        }
    } while (false);

    if (res) {
        // _validate_alter_result should be outside the above while loop.
        // to avoid requiring the header lock twice.
        res = _validate_alter_result(new_tablet, request);
    }

    // if failed convert history data, then just remove the new tablet
    if (!res) {
        LOG(WARNING) << "failed to alter tablet. base_tablet=" << base_tablet->full_name()
                     << ", drop new_tablet=" << new_tablet->full_name();
        // do not drop the new tablet and its data. GC thread will
    }

    return res;
}

Status SchemaChangeHandler::_do_process_alter_inverted_index(
        TabletSharedPtr tablet, const TAlterInvertedIndexReq& request) {
    Status res = Status::OK();
    // TODO(wy): check whether the tablet's max continuous version == request.version
    if (tablet->tablet_state() == TABLET_TOMBSTONED || tablet->tablet_state() == TABLET_STOPPED ||
        tablet->tablet_state() == TABLET_SHUTDOWN) {
        LOG(WARNING) << "tablet's state=" << tablet->tablet_state()
                     << " cannot alter inverted index";
        return Status::Error<ErrorCode::INTERNAL_ERROR>();
    }

    std::shared_lock base_migration_rlock(tablet->get_migration_lock(), std::try_to_lock);
    if (!base_migration_rlock.owns_lock()) {
        return Status::Error<TRY_LOCK_FAILED>();
    }

    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->copy_from(*tablet->tablet_schema());
    if (!request.columns.empty() && request.columns[0].col_unique_id >= 0) {
        tablet_schema->clear_columns();
        for (const auto& column : request.columns) {
            tablet_schema->append_column(TabletColumn(column));
        }
    }

    // get rowset reader
    std::vector<RowsetReaderSharedPtr> rs_readers;
    RETURN_IF_ERROR(_get_rowset_readers(tablet, tablet_schema, request, &rs_readers));
    if (request.__isset.is_drop_op && request.is_drop_op) {
        // drop index
        res = _drop_inverted_index(rs_readers, tablet_schema, tablet, request);
    } else {
        // add index
        res = _add_inverted_index(rs_readers, tablet_schema, tablet, request);
    }

    if (!res.ok()) {
        LOG(WARNING) << "failed to alter tablet. tablet=" << tablet->full_name();
        return res;
    }

    return Status::OK();
}

Status SchemaChangeHandler::_get_rowset_readers(TabletSharedPtr tablet,
                                                const TabletSchemaSPtr& tablet_schema,
                                                const TAlterInvertedIndexReq& request,
                                                std::vector<RowsetReaderSharedPtr>* rs_readers) {
    Status res = Status::OK();
    std::vector<Version> versions_to_be_changed;
    std::vector<ColumnId> return_columns;
    std::vector<TOlapTableIndex> alter_inverted_indexs;

    if (request.__isset.alter_inverted_indexes) {
        alter_inverted_indexs = request.alter_inverted_indexes;
    }

    for (auto& inverted_index : alter_inverted_indexs) {
        DCHECK_EQ(inverted_index.columns.size(), 1);
        auto column_name = inverted_index.columns[0];
        auto idx = tablet_schema->field_index(column_name);
        if (idx < 0) {
            LOG(WARNING) << "referenced column was missing. "
                         << "[column=" << column_name << " referenced_column=" << idx << "]";
            return Status::Error<CE_CMD_PARAMS_ERROR>();
        }
        return_columns.emplace_back(idx);
    }

    // obtain base tablet's push lock and header write lock to prevent loading data
    {
        std::lock_guard<std::mutex> tablet_lock(tablet->get_push_lock());
        std::lock_guard<std::shared_mutex> tablet_wlock(tablet->get_header_lock());

        do {
            RowsetSharedPtr max_rowset;
            // get history data to rebuild inverted index and it will check if there is hold in base tablet
            res = _get_versions_to_be_changed(tablet, &versions_to_be_changed, &max_rowset);
            if (!res.ok()) {
                LOG(WARNING) << "fail to get version to be rebuild inverted index. res=" << res;
                break;
            }

            // should check the max_version >= request.alter_version, if not the rebuild index is useless
            if (max_rowset == nullptr || max_rowset->end_version() < request.alter_version) {
                LOG(WARNING) << "base tablet's max version="
                             << (max_rowset == nullptr ? 0 : max_rowset->end_version())
                             << " is less than request version=" << request.alter_version;
                res = Status::InternalError(
                        "base tablet's max version={} is less than request version={}",
                        (max_rowset == nullptr ? 0 : max_rowset->end_version()),
                        request.alter_version);
                break;
            }

            // init one delete handler
            int64_t end_version = -1;
            for (auto& version : versions_to_be_changed) {
                end_version = std::max(end_version, version.second);
            }

            auto& all_del_preds = tablet->delete_predicates();
            for (auto& delete_pred : all_del_preds) {
                if (delete_pred->version().first > end_version) {
                    continue;
                }
                tablet_schema->merge_dropped_columns(tablet->tablet_schema(delete_pred->version()));
            }
            DeleteHandler delete_handler;
            res = delete_handler.init(tablet_schema, all_del_preds, end_version);
            if (!res) {
                LOG(WARNING) << "init delete handler failed. tablet=" << tablet->full_name()
                             << ", end_version=" << end_version;
                break;
            }

            // acquire data sources correspond to history versions
            tablet->capture_rs_readers(versions_to_be_changed, rs_readers);
            if (rs_readers->size() < 1) {
                LOG(WARNING) << "fail to acquire all data sources. "
                             << "version_num=" << versions_to_be_changed.size()
                             << ", data_source_num=" << rs_readers->size();
                res = Status::Error<ALTER_DELTA_DOES_NOT_EXISTS>();
                break;
            }

            // reader_context is stack variables, it's lifetime should keep the same with rs_readers
            RowsetReaderContext reader_context;
            reader_context.reader_type = READER_ALTER_TABLE;
            reader_context.tablet_schema = tablet_schema;
            reader_context.need_ordered_result = false;
            reader_context.delete_handler = &delete_handler;
            reader_context.return_columns = &return_columns;
            reader_context.sequence_id_idx = reader_context.tablet_schema->sequence_col_idx();
            reader_context.is_unique = tablet->keys_type() == UNIQUE_KEYS;
            reader_context.batch_size = ALTER_TABLE_BATCH_SIZE;
            reader_context.delete_bitmap = &tablet->tablet_meta()->delete_bitmap();
            reader_context.version = Version(0, end_version);

            for (auto& rs_reader : *rs_readers) {
                res = rs_reader->init(&reader_context);
                if (!res.ok()) {
                    LOG(WARNING) << "failed to init rowset reader: " << tablet->full_name();
                    break;
                }
            }
        } while (false);
    }

    return res;
}

Status SchemaChangeHandler::_drop_inverted_index(std::vector<RowsetReaderSharedPtr> rs_readers,
                                                 const TabletSchemaSPtr& tablet_schema,
                                                 TabletSharedPtr tablet,
                                                 const TAlterInvertedIndexReq& request) {
    LOG(INFO) << "begin to drop inverted index";
    Status res = Status::OK();

    std::vector<TOlapTableIndex> alter_inverted_indexs;
    if (request.__isset.alter_inverted_indexes) {
        alter_inverted_indexs = request.alter_inverted_indexes;
    }

    for (auto& rs_reader : rs_readers) {
        auto rowset_meta = rs_reader->rowset()->rowset_meta();
        auto fs = rowset_meta->fs();
        for (auto i = 0; i < rowset_meta->num_segments(); ++i) {
            std::string segment_path = BetaRowset::segment_file_path(tablet->tablet_path(),
                                                                     rowset_meta->rowset_id(), i);
            for (auto& inverted_index : alter_inverted_indexs) {
                auto column_name = inverted_index.columns[0];
                auto column_idx = tablet_schema->field_index(column_name);
                if (column_idx < 0) {
                    LOG(WARNING) << "referenced column was missing. "
                                 << "[column=" << column_name << " referenced_column=" << column_idx
                                 << "]";
                    return Status::Error<CE_CMD_PARAMS_ERROR>();
                }
                auto column = tablet_schema->column(column_idx);
                auto index_id = inverted_index.index_id;

                std::string inverted_index_file =
                        InvertedIndexDescriptor::get_index_file_name(segment_path, index_id);
                bool file_exist = false;
                fs->exists(inverted_index_file, &file_exist);
                if (!file_exist) {
                    return Status::OK();
                }
                LOG(INFO) << "will drop inverted index cid: " << index_id
                          << ", column_name: " << column_name
                          << ", inverted_index_file: " << inverted_index_file;
                res = fs->delete_file(inverted_index_file);
                if (!res.ok()) {
                    LOG(WARNING) << "failed to delete file: " << inverted_index_file
                                 << ", res: " << res.to_string();
                    return res;
                }
            }
        }
    }

    return Status::OK();
}

Status SchemaChangeHandler::_add_inverted_index(std::vector<RowsetReaderSharedPtr> rs_readers,
                                                const TabletSchemaSPtr& tablet_schema,
                                                TabletSharedPtr tablet,
                                                const TAlterInvertedIndexReq& request) {
    LOG(INFO) << "begin to add inverted index, tablet=" << tablet->full_name();
    Status res = Status::OK();
    std::vector<TOlapTableIndex> alter_inverted_indexs;
    if (request.__isset.alter_inverted_indexes) {
        alter_inverted_indexs = request.alter_inverted_indexes;
    }

    {
        std::lock_guard<std::shared_mutex> wrlock(_mutex);
        _tablet_ids_in_converting.insert(tablet->tablet_id());
    }

    res = _rebuild_inverted_index(rs_readers, tablet_schema, tablet, alter_inverted_indexs);

    {
        std::lock_guard<std::shared_mutex> wrlock(_mutex);
        _tablet_ids_in_converting.erase(tablet->tablet_id());
    }

    // if (res.ok()) {
    //     // _validate_alter_result should be outside the above while loop.
    //     // to avoid requiring the header lock twice.
    //     res = _validate_alter_result(tablet, request);
    // }

    if (!res.ok()) {
        LOG(WARNING) << "failed to alter add inverte index. tablet=" << tablet->full_name();
    }
    return res;
}

Status SchemaChangeHandler::_rebuild_inverted_index(
        const std::vector<RowsetReaderSharedPtr>& rs_readers, const TabletSchemaSPtr& tablet_schema,
        TabletSharedPtr tablet, const std::vector<TOlapTableIndex>& alter_inverted_indexs) {
    LOG(INFO) << "begin to rebuild inverted index"
              << ", tablet=" << tablet->full_name();
    Status res = Status::OK();
    auto sc_procedure =
            std::make_unique<SchemaChangeForInvertedIndex>(alter_inverted_indexs, tablet_schema);
    // read tablet data and write inverted index
    for (auto& rs_reader : rs_readers) {
        VLOG_TRACE << "begin to read a history rowset. version=" << rs_reader->version().first
                   << "-" << rs_reader->version().second;
        res = sc_procedure->process(rs_reader, nullptr, nullptr, tablet, nullptr);
        if (!res.ok() && res.code() != ErrorCode::END_OF_FILE) {
            LOG(WARNING) << "failed to process the version."
                         << " version=" << rs_reader->version().first << "-"
                         << rs_reader->version().second;
            return res;
        }

        VLOG_TRACE << "succeed to write inverted index."
                   << " version=" << rs_reader->version().first << "-"
                   << rs_reader->version().second;
    }

    LOG(INFO) << "finish to write inverted index to tablet: " << tablet->full_name();
    return Status::OK();
}

bool SchemaChangeHandler::tablet_in_converting(int64_t tablet_id) {
    std::shared_lock rdlock(_mutex);
    return _tablet_ids_in_converting.find(tablet_id) != _tablet_ids_in_converting.end();
}

Status SchemaChangeHandler::_get_versions_to_be_changed(
        TabletSharedPtr base_tablet, std::vector<Version>* versions_to_be_changed,
        RowsetSharedPtr* max_rowset) {
    RowsetSharedPtr rowset = base_tablet->rowset_with_max_version();
    if (rowset == nullptr) {
        LOG(WARNING) << "Tablet has no version. base_tablet=" << base_tablet->full_name();
        return Status::Error<ALTER_DELTA_DOES_NOT_EXISTS>();
    }
    *max_rowset = rowset;

    RETURN_NOT_OK(base_tablet->capture_consistent_versions(Version(0, rowset->version().second),
                                                           versions_to_be_changed));

    return Status::OK();
}

Status SchemaChangeHandler::_convert_historical_rowsets(const SchemaChangeParams& sc_params) {
    LOG(INFO) << "begin to convert historical rowsets for new_tablet from base_tablet."
              << " base_tablet=" << sc_params.base_tablet->full_name()
              << ", new_tablet=" << sc_params.new_tablet->full_name();

    // find end version
    int32_t end_version = -1;
    for (size_t i = 0; i < sc_params.ref_rowset_readers.size(); ++i) {
        if (sc_params.ref_rowset_readers[i]->version().second > end_version) {
            end_version = sc_params.ref_rowset_readers[i]->version().second;
        }
    }

    // Add filter information in change, and filter column information will be set in _parse_request
    // And filter some data every time the row block changes
    BlockChanger changer(sc_params.new_tablet->tablet_schema(), *sc_params.desc_tbl);

    bool sc_sorting = false;
    bool sc_directly = false;

    // a.Parse the Alter request and convert it into an internal representation
    Status res = _parse_request(sc_params, &changer, &sc_sorting, &sc_directly);
    LOG(INFO) << "schema change type, sc_sorting: " << sc_sorting
              << ", sc_directly: " << sc_directly
              << ", base_tablet=" << sc_params.base_tablet->full_name()
              << ", new_tablet=" << sc_params.new_tablet->full_name();

    auto process_alter_exit = [&]() -> Status {
        {
            // save tablet meta here because rowset meta is not saved during add rowset
            std::lock_guard<std::shared_mutex> new_wlock(sc_params.new_tablet->get_header_lock());
            sc_params.new_tablet->save_meta();
        }
        if (res) {
            Version test_version(0, end_version);
            res = sc_params.new_tablet->check_version_integrity(test_version);
        }

        LOG(INFO) << "finish converting rowsets for new_tablet from base_tablet. "
                  << "base_tablet=" << sc_params.base_tablet->full_name()
                  << ", new_tablet=" << sc_params.new_tablet->full_name();
        return res;
    };

    if (!res) {
        LOG(WARNING) << "failed to parse the request. res=" << res;
        return process_alter_exit();
    }

    if (!sc_sorting && !sc_directly && sc_params.alter_tablet_type == AlterTabletType::ROLLUP) {
        res = Status::Error<SCHEMA_SCHEMA_INVALID>(
                "Don't support to add materialized view by linked schema change");
        return process_alter_exit();
    }

    // b. Generate historical data converter
    auto sc_procedure = get_sc_procedure(changer, sc_sorting, sc_directly);

    // c.Convert historical data
    for (auto& rs_reader : sc_params.ref_rowset_readers) {
        VLOG_TRACE << "begin to convert a history rowset. version=" << rs_reader->version().first
                   << "-" << rs_reader->version().second;

        // set status for monitor
        // As long as there is a new_table as running, ref table is set as running
        // NOTE If the first sub_table fails first, it will continue to go as normal here
        TabletSharedPtr new_tablet = sc_params.new_tablet;
        // When tablet create new rowset writer, it may change rowset type, in this case
        // linked schema change will not be used.
        std::unique_ptr<RowsetWriter> rowset_writer;
        RowsetWriterContext context;
        context.version = rs_reader->version();
        context.rowset_state = VISIBLE;
        context.segments_overlap = rs_reader->rowset()->rowset_meta()->segments_overlap();
        context.tablet_schema = new_tablet->tablet_schema();
        context.newest_write_timestamp = rs_reader->newest_write_timestamp();
        context.fs = rs_reader->rowset()->rowset_meta()->fs();
        Status status = new_tablet->create_rowset_writer(context, &rowset_writer);
        if (!status.ok()) {
            res = Status::Error<ROWSET_BUILDER_INIT>();
            return process_alter_exit();
        }

        if (res = sc_procedure->process(rs_reader, rowset_writer.get(), sc_params.new_tablet,
                                        sc_params.base_tablet, sc_params.base_tablet_schema);
            !res) {
            LOG(WARNING) << "failed to process the version."
                         << " version=" << rs_reader->version().first << "-"
                         << rs_reader->version().second << ", " << res.to_string();
            new_tablet->data_dir()->remove_pending_ids(ROWSET_ID_PREFIX +
                                                       rowset_writer->rowset_id().to_string());
            return process_alter_exit();
        }
        new_tablet->data_dir()->remove_pending_ids(ROWSET_ID_PREFIX +
                                                   rowset_writer->rowset_id().to_string());
        // Add the new version of the data to the header
        // In order to prevent the occurrence of deadlock, we must first lock the old table, and then lock the new table
        std::lock_guard<std::mutex> lock(sc_params.new_tablet->get_push_lock());
        RowsetSharedPtr new_rowset = rowset_writer->build();
        if (new_rowset == nullptr) {
            LOG(WARNING) << "failed to build rowset, exit alter process";
            return process_alter_exit();
        }
        res = sc_params.new_tablet->add_rowset(new_rowset);
        if (res.is<PUSH_VERSION_ALREADY_EXIST>()) {
            LOG(WARNING) << "version already exist, version revert occurred. "
                         << "tablet=" << sc_params.new_tablet->full_name() << ", version='"
                         << rs_reader->version().first << "-" << rs_reader->version().second;
            StorageEngine::instance()->add_unused_rowset(new_rowset);
            res = Status::OK();
        } else if (!res) {
            LOG(WARNING) << "failed to register new version. "
                         << " tablet=" << sc_params.new_tablet->full_name()
                         << ", version=" << rs_reader->version().first << "-"
                         << rs_reader->version().second;
            StorageEngine::instance()->add_unused_rowset(new_rowset);
            return process_alter_exit();
        } else {
            VLOG_NOTICE << "register new version. tablet=" << sc_params.new_tablet->full_name()
                        << ", version=" << rs_reader->version().first << "-"
                        << rs_reader->version().second;
        }

        VLOG_TRACE << "succeed to convert a history version."
                   << " version=" << rs_reader->version().first << "-"
                   << rs_reader->version().second;
    }

    // XXX:The SchemaChange state should not be canceled at this time, because the new Delta has to be converted to the old and new Schema version
    return process_alter_exit();
}

// @static
// Analyze the mapping of the column and the mapping of the filter key
Status SchemaChangeHandler::_parse_request(const SchemaChangeParams& sc_params,
                                           BlockChanger* changer, bool* sc_sorting,
                                           bool* sc_directly) {
    changer->set_type(sc_params.alter_tablet_type);
    changer->set_compatible_version(sc_params.be_exec_version);

    TabletSharedPtr base_tablet = sc_params.base_tablet;
    TabletSharedPtr new_tablet = sc_params.new_tablet;
    TabletSchemaSPtr base_tablet_schema = sc_params.base_tablet_schema;
    const std::unordered_map<std::string, AlterMaterializedViewParam>& materialized_function_map =
            sc_params.materialized_params_map;
    DescriptorTbl desc_tbl = *sc_params.desc_tbl;
    // set column mapping
    for (int i = 0, new_schema_size = new_tablet->tablet_schema()->num_columns();
         i < new_schema_size; ++i) {
        const TabletColumn& new_column = new_tablet->tablet_schema()->column(i);
        const string& column_name = new_column.name();
        ColumnMapping* column_mapping = changer->get_mutable_column_mapping(i);
        column_mapping->new_column = &new_column;

        if (materialized_function_map.find(column_name) != materialized_function_map.end()) {
            auto mvParam = materialized_function_map.find(column_name)->second;
            column_mapping->expr = mvParam.expr;
            int32_t column_index = base_tablet_schema->field_index(mvParam.origin_column_name);
            if (column_index >= 0) {
                column_mapping->ref_column = column_index;
                continue;
            } else if (sc_params.alter_tablet_type != ROLLUP) {
                LOG(WARNING) << fmt::format(
                        "referenced column was missing. [column={} ,origin_column={}]", column_name,
                        mvParam.origin_column_name);
                return Status::Error<CE_CMD_PARAMS_ERROR>();
            }
        }

        int32_t column_index = base_tablet_schema->field_index(column_name);
        if (column_index >= 0) {
            column_mapping->ref_column = column_index;
            continue;
        }

        // Newly added column go here
        column_mapping->ref_column = -1;

        if (i < base_tablet_schema->num_short_key_columns()) {
            *sc_directly = true;
        }
        RETURN_IF_ERROR(
                _init_column_mapping(column_mapping, new_column, new_column.default_value()));

        VLOG_TRACE << "A column with default value will be added after schema changing. "
                   << "column=" << column_name << ", default_value=" << new_column.default_value();
    }

    if (materialized_function_map.count(WHERE_SIGN)) {
        changer->set_where_expr(materialized_function_map.find(WHERE_SIGN)->second.expr);
    }

    // Check if re-aggregation is needed.
    *sc_sorting = false;
    // If the reference sequence of the Key column is out of order, it needs to be reordered
    int num_default_value = 0;

    for (int i = 0, new_schema_size = new_tablet->num_key_columns(); i < new_schema_size; ++i) {
        ColumnMapping* column_mapping = changer->get_mutable_column_mapping(i);

        if (column_mapping->ref_column < 0) {
            num_default_value++;
            continue;
        }

        if (column_mapping->ref_column != i - num_default_value) {
            *sc_sorting = true;
            return Status::OK();
        }
    }

    TabletSchemaSPtr new_tablet_schema = new_tablet->tablet_schema();
    if (base_tablet_schema->keys_type() != new_tablet_schema->keys_type()) {
        // only when base table is dup and mv is agg
        // the rollup job must be reagg.
        *sc_sorting = true;
        return Status::OK();
    }

    // If the sort of key has not been changed but the new keys num is less then base's,
    // the new table should be re agg.
    // So we also need to set sc_sorting = true.
    // A, B, C are keys(sort keys), D is value
    // followings need resort:
    //      old keys:    A   B   C   D
    //      new keys:    A   B
    if (new_tablet_schema->keys_type() != KeysType::DUP_KEYS &&
        new_tablet->num_key_columns() < base_tablet_schema->num_key_columns()) {
        // this is a table with aggregate key type, and num of key columns in new schema
        // is less, which means the data in new tablet should be more aggregated.
        // so we use sorting schema change to sort and merge the data.
        *sc_sorting = true;
        return Status::OK();
    }

    if (base_tablet_schema->num_short_key_columns() != new_tablet->num_short_key_columns()) {
        // the number of short_keys changed, can't do linked schema change
        *sc_directly = true;
        return Status::OK();
    }

    for (size_t i = 0; i < new_tablet->num_columns(); ++i) {
        ColumnMapping* column_mapping = changer->get_mutable_column_mapping(i);
        if (column_mapping->ref_column < 0) {
            continue;
        } else {
            auto column_new = new_tablet_schema->column(i);
            auto column_old = base_tablet_schema->column(column_mapping->ref_column);
            if (column_new.type() != column_old.type() ||
                column_new.precision() != column_old.precision() ||
                column_new.frac() != column_old.frac() ||
                column_new.length() != column_old.length() ||
                column_new.is_bf_column() != column_old.is_bf_column() ||
                column_new.has_bitmap_index() != column_old.has_bitmap_index() ||
                new_tablet_schema->has_inverted_index(column_new.unique_id()) !=
                        base_tablet_schema->has_inverted_index(column_old.unique_id())) {
                *sc_directly = true;
                return Status::OK();
            }
        }
    }

    if (!sc_params.delete_handler->empty()) {
        // there exists delete condition in header, can't do linked schema change
        *sc_directly = true;
    }

    if (base_tablet->tablet_meta()->preferred_rowset_type() !=
        new_tablet->tablet_meta()->preferred_rowset_type()) {
        // If the base_tablet and new_tablet rowset types are different, just use directly type
        *sc_directly = true;
    }

    // if rs_reader has remote files, link schema change is not supported,
    // use directly schema change instead.
    if (!(*sc_directly) && !(*sc_sorting)) {
        // check has remote rowset
        for (auto& rs_reader : sc_params.ref_rowset_readers) {
            if (!rs_reader->rowset()->is_local()) {
                *sc_directly = true;
                break;
            }
        }
    }

    return Status::OK();
}

Status SchemaChangeHandler::_init_column_mapping(ColumnMapping* column_mapping,
                                                 const TabletColumn& column_schema,
                                                 const std::string& value) {
    column_mapping->default_value = WrapperField::create(column_schema);

    if (column_mapping->default_value == nullptr) {
        return Status::Error<MEM_ALLOC_FAILED>();
    }

    if (column_schema.is_nullable() && value.length() == 0) {
        column_mapping->default_value->set_null();
    } else {
        column_mapping->default_value->from_string(value, column_schema.precision(),
                                                   column_schema.frac());
    }

    return Status::OK();
}

Status SchemaChangeHandler::_validate_alter_result(TabletSharedPtr new_tablet,
                                                   const TAlterTabletReqV2& request) {
    Version max_continuous_version = {-1, 0};
    new_tablet->max_continuous_version_from_beginning(&max_continuous_version);
    LOG(INFO) << "find max continuous version of tablet=" << new_tablet->full_name()
              << ", start_version=" << max_continuous_version.first
              << ", end_version=" << max_continuous_version.second;
    if (max_continuous_version.second < request.alter_version) {
        return Status::InternalError("result version={} is less than request version={}",
                                     max_continuous_version.second, request.alter_version);
    }

    std::vector<std::pair<Version, RowsetSharedPtr>> version_rowsets;
    {
        std::shared_lock rdlock(new_tablet->get_header_lock());
        new_tablet->acquire_version_and_rowsets(&version_rowsets);
    }
    for (auto& pair : version_rowsets) {
        RowsetSharedPtr rowset = pair.second;
        if (!rowset->check_file_exist()) {
            return Status::Error<FILE_NOT_EXIST>();
        }
    }
    return Status::OK();
}

} // namespace doris
