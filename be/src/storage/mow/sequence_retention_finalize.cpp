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

#include "storage/mow/sequence_retention_finalize.h"

#include <algorithm>
#include <numeric>

#include "common/cast_set.h"
#include "common/config.h"
#include "core/block/block.h"
#include "core/column/column_vector.h"
#include "io/cache/block_file_cache_factory.h"
#include "storage/delete/calc_delete_bitmap_executor.h"
#include "storage/index/inverted/common/inverted_index_desc.h"
#include "storage/partial_update_info.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_loader.h"
#include "storage/sequence_retention.h"
#include "storage/tablet/base_tablet.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/txn/txn_manager.h"

namespace doris {
namespace {

void mask_segments(const RowsetSharedPtr& rowset,
                   const std::vector<segment_v2::SegmentSharedPtr>& segments, size_t begin,
                   DeleteBitmap* bitmap) {
    for (size_t pos = begin; pos < segments.size(); ++pos) {
        roaring::Roaring rows;
        rows.addRange(0, segments[pos]->num_rows());
        bitmap->merge({rowset->rowset_id(), segments[pos]->id(), DeleteBitmap::TEMP_VERSION_COMMON},
                      rows);
    }
}

Status read_input(const RowsetSharedPtr& rowset, uint32_t segment_id, uint32_t first,
                  uint32_t count, const std::vector<uint32_t>& cids, Block* block) {
    *block = rowset->tablet_schema()->create_storage_block(cids);
    auto column_guard = block->mutate_columns_scoped();
    auto& columns = column_guard.mutable_columns();
    std::vector<uint32_t> rowids(count);
    std::iota(rowids.begin(), rowids.end(), first);
    RETURN_IF_ERROR(BaseTablet::fetch_values_by_rowids(rowset, *rowset->tablet_schema(), segment_id,
                                                       rowids, cids, columns));
    return Status::OK();
}

// Failed attempts can leave an unregistered segment file. It has never been visible or read
// through the page cache. Drop its write-through file-cache block before reusing that next ID.
void clear_unregistered_segment_cache(const RowsetSharedPtr& rowset, int64_t segment_id) {
    if (!rowset->is_local() && config::enable_file_cache) {
        auto remove = [](const auto& key) {
            io::FileCacheFactory::instance()->get_by_path(key)->remove_if_cached(key);
        };
        remove(segment_v2::Segment::file_cache_key(rowset->rowset_id().to_string(),
                                                   cast_set<uint32_t>(segment_id)));
        if (rowset->tablet_schema()->get_inverted_index_storage_format() ==
            InvertedIndexStorageFormatPB::V1) {
            for (const auto& index : rowset->tablet_schema()->inverted_indexes()) {
                remove(io::BlockFileCache::hash(
                        segment_v2::InvertedIndexDescriptor::get_index_file_name_v1(
                                rowset->rowset_id().to_string(), segment_id, index->index_id(),
                                index->get_index_suffix())));
            }
        } else {
            remove(io::BlockFileCache::hash(
                    segment_v2::InvertedIndexDescriptor::get_index_file_name_v2(
                            rowset->rowset_id().to_string(), segment_id)));
        }
    }
}

Status replay_partial_input(const BaseTabletSPtr& tablet, TabletTxnInfo* transaction,
                            const std::shared_ptr<MowContext>& context,
                            const std::vector<segment_v2::SegmentSharedPtr>& original_segments,
                            uint32_t original_count, int64_t txn_expiration) {
    const auto& rowset = transaction->rowset;
    const auto& schema = *rowset->tablet_schema();
    std::vector<uint32_t> cids;
    if (transaction->partial_update_info &&
        transaction->partial_update_info->is_fixed_partial_update()) {
        cids = transaction->partial_update_info->update_cids;
        // Preserve the sink-generated auto-inc value even when it was not in the provided set.
        for (uint32_t cid = 0; cid < schema.num_columns(); ++cid) {
            if (schema.column(cid).is_auto_increment() &&
                std::find(cids.begin(), cids.end(), cid) == cids.end()) {
                cids.push_back(cid);
            }
        }
    } else {
        cids.resize(schema.num_columns());
        std::iota(cids.begin(), cids.end(), 0);
    }
    const auto& changelog = transaction->attach_changelog.rowset;
    // A writer flushes one bounded block into a fresh segment. The next block can then probe
    // earlier completed segments of this transaction, without ever consulting raw candidates.
    constexpr uint32_t batch_size = 4096;
    for (uint32_t pos = 0; pos < original_count; ++pos) {
        const auto& segment = original_segments[pos];
        for (uint32_t first = 0; first < segment->num_rows(); first += batch_size) {
            Block input;
            RETURN_IF_ERROR(read_input(rowset, segment->id(), first,
                                       std::min(batch_size, segment->num_rows() - first), cids,
                                       &input));
            auto writer = DORIS_TRY(tablet->create_transient_rowset_writer(
                    *rowset, transaction->partial_update_info, txn_expiration));
            auto& writer_context = const_cast<RowsetWriterContext&>(writer->context());
            writer_context.tablet = tablet;
            writer_context.mow_context = context;
            writer_context.sequence_retention = context->sequence_retention;
            writer_context.sequence_retention_replay = true;
            const auto writing_segment_id = DORIS_TRY(writer->allocate_segment_id());
            clear_unregistered_segment_cache(rowset, writing_segment_id);
            std::unique_ptr<RowsetWriter> changelog_writer;
            if (changelog) {
                changelog_writer = DORIS_TRY(
                        transaction->attach_changelog.tablet->create_transient_rowset_writer(
                                *changelog, transaction->partial_update_info, txn_expiration));
                auto& history_context =
                        const_cast<RowsetWriterContext&>(changelog_writer->context());
                history_context.sequence_retention = context->sequence_retention;
                history_context.sequence_retention_replay = true;
                history_context.write_changelog_opt().set_need_before(
                        transaction->attach_changelog.tablet->binlog_config()
                                .need_historical_value());
                auto& source =
                        history_context.write_changelog_opt().write_changelog_config().source;
                source.tablet_schema = writer_context.tablet_schema;
                source.partial_update_info = transaction->partial_update_info;
                source.mow_context = context;
                source.source_write_type = DataWriteType::TYPE_DIRECT;
                // Replay must rebuild AFTER from supplied cells even though the output writer
                // is transient. The legacy transient path receives an already-filled block.
                source.is_transient_rowset_writer = false;
                source.base_tablet = tablet;
                const auto original_changelog_segment = changelog->segment(pos).id();
                Block raw_lsn;
                RETURN_IF_ERROR(read_input(
                        changelog, cast_set<uint32_t>(original_changelog_segment), first,
                        cast_set<uint32_t>(input.rows()),
                        {cast_set<uint32_t>(changelog->tablet_schema()->changelog_lsn_col_idx())},
                        &raw_lsn));
                const auto& values =
                        assert_cast<const ColumnInt64&>(*raw_lsn.get_by_position(0).column)
                                .get_data();
                auto lsns =
                        std::make_shared<const std::vector<int64_t>>(values.begin(), values.end());
                history_context.allocated_lsn_map =
                        std::make_shared<segment_v2::SegmentAllocatedLsnMap>();
                const auto history_segment_id = DORIS_TRY(changelog_writer->allocate_segment_id());
                if (history_segment_id != writing_segment_id) {
                    return Status::InternalError(
                            "sequence retention source/changelog physical segments are not "
                            "aligned");
                }
                history_context.insert_segment_allocated_lsns(history_segment_id, lsns);
                clear_unregistered_segment_cache(changelog, history_segment_id);
                // BEFORE must see the predecessor before the data probe mutates the scratch bitmap.
                RETURN_IF_ERROR(changelog_writer->flush_single_block(&input, writing_segment_id));
                RETURN_IF_ERROR(changelog_writer->flush());
            }
            RETURN_IF_ERROR(writer->flush_single_block(&input, writing_segment_id));
            RETURN_IF_ERROR(writer->flush());
            RowsetSharedPtr derived;
            RETURN_IF_ERROR(writer->build(derived));
            RowsetSharedPtr derived_changelog;
            if (changelog_writer) {
                RETURN_IF_ERROR(changelog_writer->build(derived_changelog));
                if (derived->num_segments() != derived_changelog->num_segments() ||
                    derived->num_rows() != derived_changelog->num_rows()) {
                    return Status::InternalError(
                            "sequence retention source/changelog replay is not aligned");
                }
            }
            rowset->merge_rowset_meta(*derived->rowset_meta());
            if (derived_changelog) {
                changelog->merge_rowset_meta(*derived_changelog->rowset_meta());
            }
        }
    }
    return Status::OK();
}

} // namespace

Status finalize_sequence_retention(const BaseTabletSPtr& tablet, TabletTxnInfo* transaction,
                                   int64_t txn_id, int64_t txn_expiration,
                                   DeleteBitmapPtr tablet_delete_bitmap) {
    const auto& rowset = transaction->rowset;
    const auto& input = rowset->rowset_meta()->sequence_retention_input();
    const auto commit_version = rowset->start_version();
    std::shared_ptr<const SequenceRetentionContext> context;
    RowsetIdUnorderedSet current_ids;
    std::vector<RowsetSharedPtr> current_rowsets;
    {
        std::shared_lock lock(tablet->get_header_lock());
        context = std::make_shared<const SequenceRetentionContext>(
                SequenceRetentionContext {.gap = tablet->tablet_meta()->sequence_retention_gap(),
                                          .plan_version = input.plan_version()});
        RETURN_IF_ERROR(context->check_commit(commit_version));
        const auto predecessor = transaction->is_txn_load ? transaction->next_visible_version - 1
                                                          : commit_version - 1;
        RETURN_IF_ERROR(tablet->get_all_rs_id_unlocked(predecessor, &current_ids));
        current_rowsets = tablet->get_rowset_by_ids(&current_ids);
    }
    // Whole-row writes may reuse the early bitmap below after validating its prefix.
    auto bitmap = std::make_shared<DeleteBitmap>(tablet->tablet_id());
    auto mow = std::make_shared<MowContext>(commit_version - 1, txn_id,
                                            std::make_shared<RowsetIdUnorderedSet>(current_ids),
                                            std::move(current_rowsets), bitmap);
    mow->sequence_retention = context;
    mow->sequence_retention_final = true;
    mow->sequence_retention_tablet_bitmap = std::move(tablet_delete_bitmap);
    if (transaction->is_txn_load) {
        for (const auto& previous : transaction->invisible_rowsets) {
            mow->rowset_ptrs.insert(mow->rowset_ptrs.begin(), previous);
            mow->sequence_retention_in_transaction_rowsets.insert(previous->rowset_id());
        }
    }
    std::vector<segment_v2::SegmentSharedPtr> segments;
    RETURN_IF_ERROR(std::static_pointer_cast<BetaRowset>(rowset)->load_segments(&segments));
    if (!input.has_original_segment_count() || input.original_segment_count() > segments.size()) {
        return Status::InternalError(
                "sequence retention rowset lost its original input boundary: {}",
                rowset->rowset_id().to_string());
    }
    const auto original_count = cast_set<uint32_t>(input.original_segment_count());
    const bool partial = transaction->partial_update_info &&
                         transaction->partial_update_info->is_partial_update();
    const bool changelog = transaction->attach_changelog.rowset != nullptr;
    if (partial || changelog) {
        if (changelog) {
            auto& attached = transaction->attach_changelog;
            attached.delete_bitmap = std::make_shared<DeleteBitmap>(attached.tablet->tablet_id());
            std::vector<segment_v2::SegmentSharedPtr> history_segments;
            RETURN_IF_ERROR(std::static_pointer_cast<BetaRowset>(attached.rowset)
                                    ->load_segments(&history_segments));
            mask_segments(attached.rowset, history_segments, 0, attached.delete_bitmap.get());
        }
        // Prior derived outputs are discarded too; retries always start from original columns.
        mask_segments(rowset, segments, 0, bitmap.get());
        mow->rowset_ptrs.insert(mow->rowset_ptrs.begin(), rowset);
        mow->sequence_retention_in_transaction_rowsets.insert(rowset->rowset_id());
        RETURN_IF_ERROR(replay_partial_input(tablet, transaction, mow, segments, original_count,
                                             txn_expiration));
    } else {
        segments.resize(original_count);
        const auto* baseline = transaction->sequence_retention_baseline.get();
        if (baseline != nullptr && baseline->rowset_id != rowset->rowset_id()) {
            baseline = nullptr;
        }
        // After a completed attempt, or after the cache lost the original computation, the
        // full predecessor can be checked again without touching the incoming data files.
        const bool reuse = baseline && !transaction->is_txn_load &&
                           baseline->version >= context->expiry_version() &&
                           baseline->version < commit_version;
        auto options = std::make_shared<SequenceRetentionBitmapOptions>();
        options->retention = context;
        options->same_transaction_rowsets =
                std::move(mow->sequence_retention_in_transaction_rowsets);
        options->invalidate_row_cache = !config::disable_storage_row_cache &&
                                        rowset->tablet_schema()->has_row_store_for_all_columns();
        if (reuse) {
            options->intra_rowset_bitmap = baseline->intra_rowset_bitmap;
            bitmap = std::make_shared<DeleteBitmap>(*transaction->delete_bitmap);
            for (const auto& id : transaction->rowset_ids) {
                if (!current_ids.contains(id)) {
                    bitmap->remove({id, 0, 0}, {id, UINT32_MAX, UINT64_MAX});
                }
            }
            std::erase_if(mow->rowset_ptrs, [&](const auto& current) {
                return transaction->rowset_ids.contains(current->rowset_id());
            });
        } else if (baseline) {
            bitmap->merge(*baseline->intra_rowset_bitmap);
        } else {
            RETURN_IF_ERROR(tablet->calc_delete_bitmap_between_segments(
                    rowset->tablet_schema(), rowset->rowset_id(), segments, bitmap));
        }
        auto token = segments.size() > 1 ? tablet->calc_delete_bitmap_executor()->create_token()
                                         : nullptr;
        RETURN_IF_ERROR(BaseTablet::calc_delete_bitmap(
                tablet, rowset, segments, mow->rowset_ptrs, bitmap, commit_version - 1, token.get(),
                nullptr, mow->sequence_retention_tablet_bitmap, options));
        if (token != nullptr) {
            RETURN_IF_ERROR(token->wait());
        }
    }
    if (config::enable_merge_on_write_correctness_check) {
        tablet->add_sentinel_mark_to_delete_bitmap(bitmap.get(), current_ids);
    }
    rowset->rowset_meta()->mutable_sequence_retention_input()->set_finalized_version(
            commit_version);
    if (changelog) {
        transaction->attach_changelog.rowset->rowset_meta()
                ->mutable_sequence_retention_input()
                ->set_finalized_version(commit_version);
    }
    const auto lock_id = transaction->is_txn_load ? transaction->lock_id : -1;
    const auto next_visible =
            transaction->is_txn_load ? transaction->next_visible_version : commit_version;
    RETURN_IF_ERROR(tablet->save_delete_bitmap(transaction, txn_id, bitmap, nullptr, current_ids,
                                               lock_id, next_visible));
    transaction->delete_bitmap = std::move(bitmap);
    transaction->rowset_ids = std::move(current_ids);
    transaction->sequence_retention_baseline.reset();
    return Status::OK();
}
} // namespace doris
