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

#include "olap/rowset/segment_v2/segment_writer.h"

#include <assert.h>
#include <gen_cpp/segment_v2.pb.h>
#include <parallel_hashmap/phmap.h>

#include <algorithm>
#include <ostream>
#include <unordered_map>
#include <utility>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "cloud/config.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/logging.h" // LOG
#include "common/status.h"
#include "gutil/port.h"
#include "io/fs/file_writer.h"
#include "olap/data_dir.h"
#include "olap/key_coder.h"
#include "olap/olap_common.h"
#include "olap/primary_key_index.h"
#include "olap/row_cursor.h"                      // RowCursor // IWYU pragma: keep
#include "olap/rowset/rowset_writer_context.h"    // RowsetWriterContext
#include "olap/rowset/segment_v2/column_writer.h" // ColumnWriter
#include "olap/rowset/segment_v2/page_io.h"
#include "olap/rowset/segment_v2/page_pointer.h"
#include "olap/segment_loader.h"
#include "olap/short_key_index.h"
#include "olap/storage_engine.h"
#include "olap/tablet_schema.h"
#include "olap/utils.h"
#include "runtime/memory/mem_tracker.h"
#include "service/point_query_executor.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/faststring.h"
#include "util/key_util.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/schema_util.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/io/reader_buffer.h"
#include "vec/jsonb/serialize.h"
#include "vec/olap/olap_data_convertor.h"

namespace doris {
namespace segment_v2 {

using namespace ErrorCode;

const char* k_segment_magic = "D0R1";
const uint32_t k_segment_magic_length = 4;

SegmentWriter::SegmentWriter(io::FileWriter* file_writer, uint32_t segment_id,
                             TabletSchemaSPtr tablet_schema, BaseTabletSPtr tablet,
                             DataDir* data_dir, uint32_t max_row_per_segment,
                             const SegmentWriterOptions& opts,
                             std::shared_ptr<MowContext> mow_context)
        : _segment_id(segment_id),
          _tablet_schema(std::move(tablet_schema)),
          _tablet(std::move(tablet)),
          _data_dir(data_dir),
          _max_row_per_segment(max_row_per_segment),
          _opts(opts),
          _file_writer(file_writer),
          _mem_tracker(std::make_unique<MemTracker>("SegmentWriter:Segment-" +
                                                    std::to_string(segment_id))),
          _mow_context(std::move(mow_context)) {
    CHECK_NOTNULL(file_writer);
    _num_key_columns = _tablet_schema->num_key_columns();
    _num_short_key_columns = _tablet_schema->num_short_key_columns();
    DCHECK(_num_key_columns >= _num_short_key_columns);
    for (size_t cid = 0; cid < _num_key_columns; ++cid) {
        const auto& column = _tablet_schema->column(cid);
        _key_coders.push_back(get_key_coder(column.type()));
        _key_index_size.push_back(column.index_length());
    }
    // encode the sequence id into the primary key index
    if (_tablet_schema->has_sequence_col() && _tablet_schema->keys_type() == UNIQUE_KEYS &&
        _opts.enable_unique_key_merge_on_write) {
        const auto& column = _tablet_schema->column(_tablet_schema->sequence_col_idx());
        _seq_coder = get_key_coder(column.type());
    }
}

SegmentWriter::~SegmentWriter() {
    _mem_tracker->release(_mem_tracker->consumption());
}

void SegmentWriter::init_column_meta(ColumnMetaPB* meta, uint32_t column_id,
                                     const TabletColumn& column, TabletSchemaSPtr tablet_schema) {
    meta->set_column_id(column_id);
    meta->set_unique_id(column.unique_id());
    meta->set_type(int(column.type()));
    meta->set_length(column.length());
    meta->set_encoding(DEFAULT_ENCODING);
    meta->set_compression(_opts.compression_type);
    meta->set_is_nullable(column.is_nullable());
    for (uint32_t i = 0; i < column.get_subtype_count(); ++i) {
        init_column_meta(meta->add_children_columns(), column_id, column.get_sub_column(i),
                         tablet_schema);
    }
}

Status SegmentWriter::init() {
    std::vector<uint32_t> column_ids;
    int column_cnt = _tablet_schema->num_columns();
    for (uint32_t i = 0; i < column_cnt; ++i) {
        column_ids.emplace_back(i);
    }
    return init(column_ids, true);
}

Status SegmentWriter::init(const std::vector<uint32_t>& col_ids, bool has_key) {
    DCHECK(_column_writers.empty());
    DCHECK(_column_ids.empty());
    _has_key = has_key;
    _column_writers.reserve(_tablet_schema->columns().size());
    _column_ids.insert(_column_ids.end(), col_ids.begin(), col_ids.end());
    _olap_data_convertor = std::make_unique<vectorized::OlapBlockDataConvertor>();
    if (_opts.compression_type == UNKNOWN_COMPRESSION) {
        _opts.compression_type = _tablet_schema->compression_type();
    }
    auto create_column_writer = [&](uint32_t cid, const auto& column) -> auto {
        ColumnWriterOptions opts;
        opts.meta = _footer.add_columns();

        init_column_meta(opts.meta, cid, column, _tablet_schema);

        // now we create zone map for key columns in AGG_KEYS or all column in UNIQUE_KEYS or DUP_KEYS
        // and not support zone map for array type and jsonb type.
        opts.need_zone_map =
                (column.is_key() || _tablet_schema->keys_type() != KeysType::AGG_KEYS) &&
                column.type() != FieldType::OLAP_FIELD_TYPE_OBJECT;
        opts.need_bloom_filter = column.is_bf_column();
        auto* tablet_index = _tablet_schema->get_ngram_bf_index(column.unique_id());
        if (tablet_index) {
            opts.need_bloom_filter = true;
            opts.is_ngram_bf_index = true;
            opts.gram_size = tablet_index->get_gram_size();
            opts.gram_bf_size = tablet_index->get_gram_bf_size();
        }

        opts.need_bitmap_index = column.has_bitmap_index();
        bool skip_inverted_index = false;
        if (_opts.rowset_ctx != nullptr) {
            // skip write inverted index for index compaction
            skip_inverted_index =
                    _opts.rowset_ctx->skip_inverted_index.count(column.unique_id()) > 0;
        }
        // skip write inverted index on load if skip_write_index_on_load is true
        if (_opts.write_type == DataWriteType::TYPE_DIRECT &&
            _tablet_schema->skip_write_index_on_load()) {
            skip_inverted_index = true;
        }
        // indexes for this column
        opts.indexes = _tablet_schema->get_indexes_for_column(column.unique_id());
        for (auto index : opts.indexes) {
            if (!skip_inverted_index && index && index->index_type() == IndexType::INVERTED) {
                opts.inverted_index = index;
                // TODO support multiple inverted index
                break;
            }
        }
        if (column.type() == FieldType::OLAP_FIELD_TYPE_STRUCT) {
            opts.need_zone_map = false;
            if (opts.need_bloom_filter) {
                return Status::NotSupported("Do not support bloom filter for struct type");
            }
            if (opts.need_bitmap_index) {
                return Status::NotSupported("Do not support bitmap index for struct type");
            }
        }
        if (column.type() == FieldType::OLAP_FIELD_TYPE_ARRAY) {
            opts.need_zone_map = false;
            if (opts.need_bloom_filter) {
                return Status::NotSupported("Do not support bloom filter for array type");
            }
            if (opts.need_bitmap_index) {
                return Status::NotSupported("Do not support bitmap index for array type");
            }
        }
        if (column.type() == FieldType::OLAP_FIELD_TYPE_JSONB) {
            opts.need_zone_map = false;
            if (opts.need_bloom_filter) {
                return Status::NotSupported("Do not support bloom filter for jsonb type");
            }
            if (opts.need_bitmap_index) {
                return Status::NotSupported("Do not support bitmap index for jsonb type");
            }
        }
        if (column.type() == FieldType::OLAP_FIELD_TYPE_AGG_STATE) {
            opts.need_zone_map = false;
            if (opts.need_bloom_filter) {
                return Status::NotSupported("Do not support bloom filter for agg_state type");
            }
            if (opts.need_bitmap_index) {
                return Status::NotSupported("Do not support bitmap index for agg_state type");
            }
        }
        if (column.type() == FieldType::OLAP_FIELD_TYPE_MAP) {
            opts.need_zone_map = false;
            if (opts.need_bloom_filter) {
                return Status::NotSupported("Do not support bloom filter for map type");
            }
            if (opts.need_bitmap_index) {
                return Status::NotSupported("Do not support bitmap index for map type");
            }
        }

        if (column.is_row_store_column()) {
            // smaller page size for row store column
            opts.data_page_size = config::row_column_page_size;
        }

        std::unique_ptr<ColumnWriter> writer;
        RETURN_IF_ERROR(ColumnWriter::create(opts, &column, _file_writer, &writer));
        RETURN_IF_ERROR(writer->init());
        _column_writers.push_back(std::move(writer));

        _olap_data_convertor->add_column_data_convertor(column);
        return Status::OK();
    };

    RETURN_IF_ERROR(_create_writers(*_tablet_schema, col_ids, create_column_writer));

    // we don't need the short key index for unique key merge on write table.
    if (_has_key) {
        if (_tablet_schema->keys_type() == UNIQUE_KEYS && _opts.enable_unique_key_merge_on_write) {
            size_t seq_col_length = 0;
            if (_tablet_schema->has_sequence_col()) {
                seq_col_length =
                        _tablet_schema->column(_tablet_schema->sequence_col_idx()).length() + 1;
            }
            _primary_key_index_builder.reset(
                    new PrimaryKeyIndexBuilder(_file_writer, seq_col_length));
            RETURN_IF_ERROR(_primary_key_index_builder->init());
        } else {
            _short_key_index_builder.reset(
                    new ShortKeyIndexBuilder(_segment_id, _opts.num_rows_per_block));
        }
    }
    return Status::OK();
}

Status SegmentWriter::_create_writers(
        const TabletSchema& tablet_schema, const std::vector<uint32_t>& col_ids,
        std::function<Status(uint32_t, const TabletColumn&)> create_column_writer) {
    _olap_data_convertor->reserve(col_ids.size());
    for (auto& cid : col_ids) {
        RETURN_IF_ERROR(create_column_writer(cid, tablet_schema.column(cid)));
    }
    return Status::OK();
}

void SegmentWriter::_maybe_invalid_row_cache(const std::string& key) {
    // Just invalid row cache for simplicity, since the rowset is not visible at present.
    // If we update/insert cache, if load failed rowset will not be visible but cached data
    // will be visible, and lead to inconsistency.
    if (!config::disable_storage_row_cache && _tablet_schema->store_row_column() &&
        _opts.write_type == DataWriteType::TYPE_DIRECT) {
        // invalidate cache
        RowCache::instance()->erase({_opts.rowset_ctx->tablet_id, key});
    }
}

void SegmentWriter::_serialize_block_to_row_column(vectorized::Block& block) {
    if (block.rows() == 0) {
        return;
    }
    MonotonicStopWatch watch;
    watch.start();
    // find row column id
    int row_column_id = 0;
    for (int i = 0; i < _tablet_schema->num_columns(); ++i) {
        if (_tablet_schema->column(i).is_row_store_column()) {
            row_column_id = i;
            break;
        }
    }
    if (row_column_id == 0) {
        return;
    }
    vectorized::ColumnString* row_store_column =
            static_cast<vectorized::ColumnString*>(block.get_by_position(row_column_id)
                                                           .column->assume_mutable_ref()
                                                           .assume_mutable()
                                                           .get());
    row_store_column->clear();
    vectorized::DataTypeSerDeSPtrs serdes =
            vectorized::create_data_type_serdes(block.get_data_types());
    vectorized::JsonbSerializeUtil::block_to_jsonb(*_tablet_schema, block, *row_store_column,
                                                   _tablet_schema->num_columns(), serdes);
    VLOG_DEBUG << "serialize , num_rows:" << block.rows() << ", row_column_id:" << row_column_id
               << ", total_byte_size:" << block.allocated_bytes() << ", serialize_cost(us)"
               << watch.elapsed_time() / 1000;
}

// for partial update, we should do following steps to fill content of block:
// 1. set block data to data convertor, and get all key_column's converted slice
// 2. get pk of input block, and read missing columns
//       2.1 first find key location{rowset_id, segment_id, row_id}
//       2.2 build read plan to read by batch
//       2.3 fill block
// 3. set columns to data convertor and then write all columns
Status SegmentWriter::append_block_with_partial_content(const vectorized::Block* block,
                                                        size_t row_pos, size_t num_rows) {
    if (config::cloud_mode) {
        // TODO(plat1ko)
        return Status::NotSupported("append_block_with_partial_content");
    }
    auto tablet = static_cast<Tablet*>(_tablet.get());
    if (block->columns() <= _tablet_schema->num_key_columns() ||
        block->columns() >= _tablet_schema->num_columns()) {
        return Status::InternalError(
                fmt::format("illegal partial update block columns: {}, num key columns: {}, total "
                            "schema columns: {}",
                            block->columns(), _tablet_schema->num_key_columns(),
                            _tablet_schema->num_columns()));
    }
    DCHECK(_tablet_schema->keys_type() == UNIQUE_KEYS && _opts.enable_unique_key_merge_on_write);

    DCHECK(_opts.rowset_ctx->partial_update_info);
    // find missing column cids
    std::vector<uint32_t> missing_cids = _opts.rowset_ctx->partial_update_info->missing_cids;
    std::vector<uint32_t> including_cids = _opts.rowset_ctx->partial_update_info->update_cids;

    // create full block and fill with input columns
    auto full_block = _tablet_schema->create_block();
    size_t input_id = 0;
    for (auto i : including_cids) {
        full_block.replace_by_position(i, block->get_by_position(input_id++).column);
    }
    _olap_data_convertor->set_source_content_with_specifid_columns(&full_block, row_pos, num_rows,
                                                                   including_cids);

    bool have_input_seq_column = false;
    // write including columns
    std::vector<vectorized::IOlapColumnDataAccessor*> key_columns;
    vectorized::IOlapColumnDataAccessor* seq_column = nullptr;
    size_t segment_start_pos;
    for (auto cid : including_cids) {
        // here we get segment column row num before append data.
        segment_start_pos = _column_writers[cid]->get_next_rowid();
        // olap data convertor alway start from id = 0
        auto converted_result = _olap_data_convertor->convert_column_data(cid);
        if (!converted_result.first.ok()) {
            return converted_result.first;
        }
        if (cid < _num_key_columns) {
            key_columns.push_back(converted_result.second);
        } else if (_tablet_schema->has_sequence_col() &&
                   cid == _tablet_schema->sequence_col_idx()) {
            seq_column = converted_result.second;
            have_input_seq_column = true;
        }
        RETURN_IF_ERROR(_column_writers[cid]->append(converted_result.second->get_nullmap(),
                                                     converted_result.second->get_data(),
                                                     num_rows));
    }

    bool has_default_or_nullable = false;
    std::vector<bool> use_default_or_null_flag;
    use_default_or_null_flag.reserve(num_rows);
    const vectorized::Int8* delete_sign_column_data = nullptr;
    if (const vectorized::ColumnWithTypeAndName* delete_sign_column =
                full_block.try_get_by_name(DELETE_SIGN);
        delete_sign_column != nullptr) {
        auto& delete_sign_col =
                reinterpret_cast<const vectorized::ColumnInt8&>(*(delete_sign_column->column));
        if (delete_sign_col.size() >= row_pos + num_rows) {
            delete_sign_column_data = delete_sign_col.get_data().data();
        }
    }

    std::vector<RowsetSharedPtr> specified_rowsets;
    {
        std::shared_lock rlock(tablet->get_header_lock());
        specified_rowsets = tablet->get_rowset_by_ids(&_mow_context->rowset_ids);
    }
    std::vector<std::unique_ptr<SegmentCacheHandle>> segment_caches(specified_rowsets.size());
    // locate rows in base data

    int64_t num_rows_filtered = 0;
    for (size_t block_pos = row_pos; block_pos < row_pos + num_rows; block_pos++) {
        // block   segment
        //   2   ->   0
        //   3   ->   1
        //   4   ->   2
        //   5   ->   3
        // here row_pos = 2, num_rows = 4.
        size_t delta_pos = block_pos - row_pos;
        size_t segment_pos = segment_start_pos + delta_pos;
        std::string key = full_encode_keys(key_columns, delta_pos);
        if (have_input_seq_column) {
            _encode_seq_column(seq_column, delta_pos, &key);
        }
        // If the table have sequence column, and the include-cids don't contain the sequence
        // column, we need to update the primary key index builder at the end of this method.
        // At that time, we have a valid sequence column to encode the key with seq col.
        if (!_tablet_schema->has_sequence_col() || have_input_seq_column) {
            RETURN_IF_ERROR(_primary_key_index_builder->add_item(key));
        }
        _maybe_invalid_row_cache(key);

        RowLocation loc;
        // save rowset shared ptr so this rowset wouldn't delete
        RowsetSharedPtr rowset;
        auto st = tablet->lookup_row_key(key, have_input_seq_column, specified_rowsets, &loc,
                                         _mow_context->max_version, segment_caches, &rowset);
        if (st.is<KEY_NOT_FOUND>()) {
            if (_opts.rowset_ctx->partial_update_info->is_strict_mode) {
                ++num_rows_filtered;
                // delete the invalid newly inserted row
                _mow_context->delete_bitmap->add({_opts.rowset_ctx->rowset_id, _segment_id,
                                                  DeleteBitmap::TEMP_VERSION_COMMON},
                                                 segment_pos);
            }

            if (!_opts.rowset_ctx->partial_update_info->can_insert_new_rows_in_partial_update) {
                return Status::InternalError(
                        "the unmentioned columns should have default value or be nullable for "
                        "newly inserted rows in non-strict mode partial update");
            }
            has_default_or_nullable = true;
            use_default_or_null_flag.emplace_back(true);
            continue;
        }
        if (!st.ok() && !st.is<KEY_ALREADY_EXISTS>()) {
            LOG(WARNING) << "failed to lookup row key, error: " << st;
            return st;
        }

        // if the delete sign is marked, it means that the value columns of the row
        // will not be read. So we don't need to read the missing values from the previous rows.
        // But we still need to mark the previous row on delete bitmap
        if (delete_sign_column_data != nullptr && delete_sign_column_data[block_pos] != 0) {
            has_default_or_nullable = true;
            use_default_or_null_flag.emplace_back(true);
            if (!_tablet_schema->has_sequence_col() && !have_input_seq_column) {
                // we can directly use delete bitmap to mark the rows with delete sign as deleted
                // if sequence column doesn't exist to eliminate reading delete sign columns in later reads
                _mow_context->delete_bitmap->add({_opts.rowset_ctx->rowset_id, _segment_id,
                                                  DeleteBitmap::TEMP_VERSION_FOR_DELETE_SIGN},
                                                 segment_pos);
            }
        } else {
            // partial update should not contain invisible columns
            use_default_or_null_flag.emplace_back(false);
            _rsid_to_rowset.emplace(rowset->rowset_id(), rowset);
            tablet->prepare_to_read(loc, segment_pos, &_rssid_to_rid);
        }

        if (st.is<KEY_ALREADY_EXISTS>()) {
            // although we need to mark delete current row, we still need to read missing columns
            // for this row, we need to ensure that each column is aligned
            _mow_context->delete_bitmap->add(
                    {_opts.rowset_ctx->rowset_id, _segment_id, DeleteBitmap::TEMP_VERSION_COMMON},
                    segment_pos);
        } else {
            _mow_context->delete_bitmap->add(
                    {loc.rowset_id, loc.segment_id, DeleteBitmap::TEMP_VERSION_COMMON}, loc.row_id);
        }
    }
    CHECK(use_default_or_null_flag.size() == num_rows);

    if (config::enable_merge_on_write_correctness_check) {
        tablet->add_sentinel_mark_to_delete_bitmap(_mow_context->delete_bitmap.get(),
                                                   _mow_context->rowset_ids);
    }

    // read and fill block
    auto mutable_full_columns = full_block.mutate_columns();
    auto token = StorageEngine::instance()->check_primary_keys_executor()->create_token();
    if (config::enable_check_primary_keys) {
        RETURN_IF_ERROR(token->submit(tablet, &_rssid_to_rid, &_rsid_to_rowset, this, &key_columns,
                                      row_pos));
    }
    RETURN_IF_ERROR(fill_missing_columns(mutable_full_columns, use_default_or_null_flag,
                                         has_default_or_nullable, segment_start_pos));
    if (config::enable_check_primary_keys) {
        RETURN_IF_ERROR(token->wait());
    }
    // row column should be filled here
    if (_tablet_schema->store_row_column()) {
        // convert block to row store format
        _serialize_block_to_row_column(full_block);
    }

    // convert missing columns and send to column writer
    auto cids_missing = _opts.rowset_ctx->partial_update_info->missing_cids;
    _olap_data_convertor->set_source_content_with_specifid_columns(&full_block, row_pos, num_rows,
                                                                   cids_missing);
    for (auto cid : cids_missing) {
        auto converted_result = _olap_data_convertor->convert_column_data(cid);
        if (!converted_result.first.ok()) {
            return converted_result.first;
        }
        if (_tablet_schema->has_sequence_col() && !have_input_seq_column &&
            cid == _tablet_schema->sequence_col_idx()) {
            DCHECK_EQ(seq_column, nullptr);
            seq_column = converted_result.second;
        }
        RETURN_IF_ERROR(_column_writers[cid]->append(converted_result.second->get_nullmap(),
                                                     converted_result.second->get_data(),
                                                     num_rows));
    }

    _num_rows_filtered += num_rows_filtered;
    if (_tablet_schema->has_sequence_col() && !have_input_seq_column) {
        DCHECK_NE(seq_column, nullptr);
        DCHECK_EQ(_num_rows_written, row_pos)
                << "_num_rows_written: " << _num_rows_written << ", row_pos" << row_pos;
        DCHECK_EQ(_primary_key_index_builder->num_rows(), _num_rows_written)
                << "primary key index builder num rows(" << _primary_key_index_builder->num_rows()
                << ") not equal to segment writer's num rows written(" << _num_rows_written << ")";
        if (_num_rows_written != row_pos ||
            _primary_key_index_builder->num_rows() != _num_rows_written) {
            return Status::InternalError(
                    "Correctness check failed, _num_rows_written: {}, row_pos: {}, primary key "
                    "index builder num rows: {}",
                    _num_rows_written, row_pos, _primary_key_index_builder->num_rows());
        }
        for (size_t block_pos = row_pos; block_pos < row_pos + num_rows; block_pos++) {
            std::string key = full_encode_keys(key_columns, block_pos - row_pos);
            _encode_seq_column(seq_column, block_pos - row_pos, &key);
            RETURN_IF_ERROR(_primary_key_index_builder->add_item(key));
        }
    }

    _num_rows_written += num_rows;
    DCHECK_EQ(_primary_key_index_builder->num_rows(), _num_rows_written)
            << "primary key index builder num rows(" << _primary_key_index_builder->num_rows()
            << ") not equal to segment writer's num rows written(" << _num_rows_written << ")";
    _olap_data_convertor->clear_source_content();
    return Status::OK();
}

Status SegmentWriter::fill_missing_columns(vectorized::MutableColumns& mutable_full_columns,
                                           const std::vector<bool>& use_default_or_null_flag,
                                           bool has_default_or_nullable,
                                           const size_t& segment_start_pos) {
    if (config::cloud_mode) [[unlikely]] {
        return Status::NotSupported("fill_missing_columns");
    }
    auto tablet = static_cast<Tablet*>(_tablet.get());
    // create old value columns
    std::vector<uint32_t> cids_missing = _opts.rowset_ctx->partial_update_info->missing_cids;
    auto old_value_block = _tablet_schema->create_block_by_cids(cids_missing);
    CHECK(cids_missing.size() == old_value_block.columns());
    auto mutable_old_columns = old_value_block.mutate_columns();
    bool has_row_column = _tablet_schema->store_row_column();
    // record real pos, key is input line num, value is old_block line num
    std::map<uint32_t, uint32_t> read_index;
    size_t read_idx = 0;
    for (auto rs_it : _rssid_to_rid) {
        for (auto seg_it : rs_it.second) {
            auto rowset = _rsid_to_rowset[rs_it.first];
            CHECK(rowset);
            std::vector<uint32_t> rids;
            for (auto id_and_pos : seg_it.second) {
                rids.emplace_back(id_and_pos.rid);
                read_index[id_and_pos.pos] = read_idx++;
            }
            if (has_row_column) {
                auto st = tablet->fetch_value_through_row_column(rowset, seg_it.first, rids,
                                                                 cids_missing, old_value_block);
                if (!st.ok()) {
                    LOG(WARNING) << "failed to fetch value through row column";
                    return st;
                }
                continue;
            }
            for (size_t cid = 0; cid < mutable_old_columns.size(); ++cid) {
                TabletColumn tablet_column = _tablet_schema->column(cids_missing[cid]);
                auto st = tablet->fetch_value_by_rowids(rowset, seg_it.first, rids, tablet_column,
                                                        mutable_old_columns[cid]);
                // set read value to output block
                if (!st.ok()) {
                    LOG(WARNING) << "failed to fetch value by rowids";
                    return st;
                }
            }
        }
    }
    // build default value columns
    auto default_value_block = old_value_block.clone_empty();
    auto mutable_default_value_columns = default_value_block.mutate_columns();

    const vectorized::Int8* delete_sign_column_data = nullptr;
    if (const vectorized::ColumnWithTypeAndName* delete_sign_column =
                old_value_block.try_get_by_name(DELETE_SIGN);
        delete_sign_column != nullptr && _tablet_schema->has_sequence_col()) {
        auto& delete_sign_col =
                reinterpret_cast<const vectorized::ColumnInt8&>(*(delete_sign_column->column));
        delete_sign_column_data = delete_sign_col.get_data().data();
    }

    if (has_default_or_nullable || delete_sign_column_data != nullptr) {
        for (auto i = 0; i < cids_missing.size(); ++i) {
            const auto& column = _tablet_schema->column(cids_missing[i]);
            if (column.has_default_value()) {
                auto default_value = _tablet_schema->column(cids_missing[i]).default_value();
                vectorized::ReadBuffer rb(const_cast<char*>(default_value.c_str()),
                                          default_value.size());
                static_cast<void>(old_value_block.get_by_position(i).type->from_string(
                        rb, mutable_default_value_columns[i].get()));
            }
        }
    }

    // fill all missing value from mutable_old_columns, need to consider default value and null value
    for (auto idx = 0; idx < use_default_or_null_flag.size(); idx++) {
        // `use_default_or_null_flag[idx] == true` doesn't mean that we should read values from the old row
        // for the missing columns. For example, if a table has sequence column, the rows with DELETE_SIGN column
        // marked will not be marked in delete bitmap(see https://github.com/apache/doris/pull/24011), so it will
        // be found in Tablet::lookup_row_key() and `use_default_or_null_flag[idx]` will be false. But we should not
        // read values from old rows for missing values in this occasion. So we should read the DELETE_SIGN column
        // to check if a row REALLY exists in the table.
        if (use_default_or_null_flag[idx] ||
            (delete_sign_column_data != nullptr &&
             delete_sign_column_data[read_index[idx + segment_start_pos]] != 0)) {
            for (auto i = 0; i < cids_missing.size(); ++i) {
                // if the column has default value, fiil it with default value
                // otherwise, if the column is nullable, fill it with null value
                const auto& tablet_column = _tablet_schema->column(cids_missing[i]);
                if (tablet_column.has_default_value()) {
                    mutable_full_columns[cids_missing[i]]->insert_from(
                            *mutable_default_value_columns[i].get(), 0);
                } else if (tablet_column.is_nullable()) {
                    auto nullable_column = assert_cast<vectorized::ColumnNullable*>(
                            mutable_full_columns[cids_missing[i]].get());
                    nullable_column->insert_null_elements(1);
                } else {
                    // If the control flow reaches this branch, the column neither has default value
                    // nor is nullable. It means that the row's delete sign is marked, and the value
                    // columns are useless and won't be read. So we can just put arbitary values in the cells
                    mutable_full_columns[cids_missing[i]]->insert_default();
                }
            }
            continue;
        }
        auto pos_in_old_block = read_index[idx + segment_start_pos];
        for (auto i = 0; i < cids_missing.size(); ++i) {
            mutable_full_columns[cids_missing[i]]->insert_from(
                    *old_value_block.get_columns_with_type_and_name()[i].column.get(),
                    pos_in_old_block);
        }
    }
    return Status::OK();
}

Status SegmentWriter::append_block(const vectorized::Block* block, size_t row_pos,
                                   size_t num_rows) {
    if (_opts.rowset_ctx->partial_update_info &&
        _opts.rowset_ctx->partial_update_info->is_partial_update &&
        _opts.write_type == DataWriteType::TYPE_DIRECT &&
        !_opts.rowset_ctx->is_transient_rowset_writer) {
        RETURN_IF_ERROR(append_block_with_partial_content(block, row_pos, num_rows));
        return Status::OK();
    }
    CHECK(block->columns() >= _column_writers.size())
            << ", block->columns()=" << block->columns()
            << ", _column_writers.size()=" << _column_writers.size();
    // Row column should be filled here when it's a directly write from memtable
    // or it's schema change write(since column data type maybe changed, so we should reubild)
    if (_tablet_schema->store_row_column() &&
        (_opts.write_type == DataWriteType::TYPE_DIRECT ||
         _opts.write_type == DataWriteType::TYPE_SCHEMA_CHANGE)) {
        _serialize_block_to_row_column(*const_cast<vectorized::Block*>(block));
    }

    if (_opts.write_type == DataWriteType::TYPE_DIRECT && _opts.enable_unique_key_merge_on_write &&
        !_tablet_schema->has_sequence_col() && _tablet_schema->delete_sign_idx() != -1) {
        const vectorized::ColumnWithTypeAndName& delete_sign_column =
                block->get_by_position(_tablet_schema->delete_sign_idx());
        auto& delete_sign_col =
                reinterpret_cast<const vectorized::ColumnInt8&>(*(delete_sign_column.column));
        if (delete_sign_col.size() >= row_pos + num_rows) {
            const vectorized::Int8* delete_sign_column_data = delete_sign_col.get_data().data();
            uint32_t segment_start_pos =
                    _column_writers[_tablet_schema->delete_sign_idx()]->get_next_rowid();
            for (size_t block_pos = row_pos, seg_pos = segment_start_pos;
                 seg_pos < segment_start_pos + num_rows; block_pos++, seg_pos++) {
                // we can directly use delete bitmap to mark the rows with delete sign as deleted
                // if sequence column doesn't exist to eliminate reading delete sign columns in later reads
                if (delete_sign_column_data[block_pos]) {
                    _mow_context->delete_bitmap->add({_opts.rowset_ctx->rowset_id, _segment_id,
                                                      DeleteBitmap::TEMP_VERSION_FOR_DELETE_SIGN},
                                                     seg_pos);
                }
            }
        }
    }

    _olap_data_convertor->set_source_content(block, row_pos, num_rows);

    // find all row pos for short key indexes
    std::vector<size_t> short_key_pos;
    if (_has_key) {
        // We build a short key index every `_opts.num_rows_per_block` rows. Specifically, we
        // build a short key index using 1st rows for first block and `_short_key_row_pos - _row_count`
        // for next blocks.
        // Ensure we build a short key index using 1st rows only for the first block (ISSUE-9766).
        if (UNLIKELY(_short_key_row_pos == 0 && _num_rows_written == 0)) {
            short_key_pos.push_back(0);
        }
        while (_short_key_row_pos + _opts.num_rows_per_block < _num_rows_written + num_rows) {
            _short_key_row_pos += _opts.num_rows_per_block;
            short_key_pos.push_back(_short_key_row_pos - _num_rows_written);
        }
    }

    // convert column data from engine format to storage layer format
    std::vector<vectorized::IOlapColumnDataAccessor*> key_columns;
    vectorized::IOlapColumnDataAccessor* seq_column = nullptr;
    for (size_t id = 0; id < _column_writers.size(); ++id) {
        // olap data convertor alway start from id = 0
        auto converted_result = _olap_data_convertor->convert_column_data(id);
        if (!converted_result.first.ok()) {
            return converted_result.first;
        }
        auto cid = _column_ids[id];
        if (_has_key && cid < _num_key_columns) {
            key_columns.push_back(converted_result.second);
        } else if (_has_key && _tablet_schema->has_sequence_col() &&
                   cid == _tablet_schema->sequence_col_idx()) {
            seq_column = converted_result.second;
        }
        RETURN_IF_ERROR(_column_writers[id]->append(converted_result.second->get_nullmap(),
                                                    converted_result.second->get_data(), num_rows));
    }
    if (_has_key) {
        if (_tablet_schema->keys_type() == UNIQUE_KEYS && _opts.enable_unique_key_merge_on_write) {
            // create primary indexes
            std::string last_key;
            for (size_t pos = 0; pos < num_rows; pos++) {
                std::string key = full_encode_keys(key_columns, pos);
                if (_tablet_schema->has_sequence_col()) {
                    _encode_seq_column(seq_column, pos, &key);
                }
                DCHECK(key.compare(last_key) > 0)
                        << "found duplicate key or key is not sorted! current key: " << key
                        << ", last key" << last_key;
                RETURN_IF_ERROR(_primary_key_index_builder->add_item(key));
                _maybe_invalid_row_cache(key);
                last_key = std::move(key);
            }
        } else {
            // create short key indexes'
            // for min_max key
            set_min_key(full_encode_keys(key_columns, 0));
            set_max_key(full_encode_keys(key_columns, num_rows - 1));

            key_columns.resize(_num_short_key_columns);
            for (const auto pos : short_key_pos) {
                RETURN_IF_ERROR(_short_key_index_builder->add_item(_encode_keys(key_columns, pos)));
            }
        }
    }

    _num_rows_written += num_rows;
    _olap_data_convertor->clear_source_content();
    return Status::OK();
}

int64_t SegmentWriter::max_row_to_add(size_t row_avg_size_in_bytes) {
    auto segment_size = estimate_segment_size();
    if (PREDICT_FALSE(segment_size >= MAX_SEGMENT_SIZE ||
                      _num_rows_written >= _max_row_per_segment)) {
        return 0;
    }
    int64_t size_rows = ((int64_t)MAX_SEGMENT_SIZE - (int64_t)segment_size) / row_avg_size_in_bytes;
    int64_t count_rows = (int64_t)_max_row_per_segment - _num_rows_written;

    return std::min(size_rows, count_rows);
}

std::string SegmentWriter::full_encode_keys(
        const std::vector<vectorized::IOlapColumnDataAccessor*>& key_columns, size_t pos,
        bool null_first) {
    assert(_key_index_size.size() == _num_key_columns);
    assert(key_columns.size() == _num_key_columns && _key_coders.size() == _num_key_columns);

    std::string encoded_keys;
    size_t cid = 0;
    for (const auto& column : key_columns) {
        auto field = column->get_data_at(pos);
        if (UNLIKELY(!field)) {
            if (null_first) {
                encoded_keys.push_back(KEY_NULL_FIRST_MARKER);
            } else {
                encoded_keys.push_back(KEY_NULL_LAST_MARKER);
            }
            ++cid;
            continue;
        }
        encoded_keys.push_back(KEY_NORMAL_MARKER);
        _key_coders[cid]->full_encode_ascending(field, &encoded_keys);
        ++cid;
    }
    return encoded_keys;
}

void SegmentWriter::_encode_seq_column(const vectorized::IOlapColumnDataAccessor* seq_column,
                                       size_t pos, string* encoded_keys) {
    auto field = seq_column->get_data_at(pos);
    // To facilitate the use of the primary key index, encode the seq column
    // to the minimum value of the corresponding length when the seq column
    // is null
    if (UNLIKELY(!field)) {
        encoded_keys->push_back(KEY_NULL_FIRST_MARKER);
        size_t seq_col_length = _tablet_schema->column(_tablet_schema->sequence_col_idx()).length();
        encoded_keys->append(seq_col_length, KEY_MINIMAL_MARKER);
        return;
    }
    encoded_keys->push_back(KEY_NORMAL_MARKER);
    _seq_coder->full_encode_ascending(field, encoded_keys);
}

std::string SegmentWriter::_encode_keys(
        const std::vector<vectorized::IOlapColumnDataAccessor*>& key_columns, size_t pos,
        bool null_first) {
    assert(key_columns.size() == _num_short_key_columns);

    std::string encoded_keys;
    size_t cid = 0;
    for (const auto& column : key_columns) {
        auto field = column->get_data_at(pos);
        if (UNLIKELY(!field)) {
            if (null_first) {
                encoded_keys.push_back(KEY_NULL_FIRST_MARKER);
            } else {
                encoded_keys.push_back(KEY_NULL_LAST_MARKER);
            }
            ++cid;
            continue;
        }
        encoded_keys.push_back(KEY_NORMAL_MARKER);
        _key_coders[cid]->encode_ascending(field, _key_index_size[cid], &encoded_keys);
        ++cid;
    }
    return encoded_keys;
}

template <typename RowType>
Status SegmentWriter::append_row(const RowType& row) {
    for (size_t cid = 0; cid < _column_writers.size(); ++cid) {
        auto cell = row.cell(cid);
        RETURN_IF_ERROR(_column_writers[cid]->append(cell));
    }
    std::string full_encoded_key;
    encode_key<RowType, true, true>(&full_encoded_key, row, _num_key_columns);
    if (_tablet_schema->has_sequence_col()) {
        full_encoded_key.push_back(KEY_NORMAL_MARKER);
        auto cid = _tablet_schema->sequence_col_idx();
        auto cell = row.cell(cid);
        row.schema()->column(cid)->full_encode_ascending(cell.cell_ptr(), &full_encoded_key);
    }

    if (_tablet_schema->keys_type() == UNIQUE_KEYS && _opts.enable_unique_key_merge_on_write) {
        RETURN_IF_ERROR(_primary_key_index_builder->add_item(full_encoded_key));
    } else {
        // At the beginning of one block, so add a short key index entry
        if ((_num_rows_written % _opts.num_rows_per_block) == 0) {
            std::string encoded_key;
            encode_key(&encoded_key, row, _num_short_key_columns);
            RETURN_IF_ERROR(_short_key_index_builder->add_item(encoded_key));
        }
        set_min_max_key(full_encoded_key);
    }
    ++_num_rows_written;
    return Status::OK();
}

template Status SegmentWriter::append_row(const RowCursor& row);

// TODO(lingbin): Currently this function does not include the size of various indexes,
// We should make this more precise.
// NOTE: This function will be called when any row of data is added, so we need to
// make this function efficient.
uint64_t SegmentWriter::estimate_segment_size() {
    // footer_size(4) + checksum(4) + segment_magic(4)
    uint64_t size = 12;
    for (auto& column_writer : _column_writers) {
        size += column_writer->estimate_buffer_size();
    }
    if (_tablet_schema->keys_type() == UNIQUE_KEYS && _opts.enable_unique_key_merge_on_write) {
        size += _primary_key_index_builder->size();
    } else {
        size += _short_key_index_builder->size();
    }

    // update the mem_tracker of segment size
    _mem_tracker->consume(size - _mem_tracker->consumption());
    return size;
}

size_t SegmentWriter::try_get_inverted_index_file_size() {
    size_t total_size = 0;
    for (auto& column_writer : _column_writers) {
        total_size += column_writer->get_inverted_index_size();
    }
    return total_size;
}

Status SegmentWriter::finalize_columns_data() {
    if (_has_key) {
        _row_count = _num_rows_written;
    } else {
        DCHECK(_row_count == _num_rows_written)
                << "_row_count != _num_rows_written:" << _row_count << " vs. " << _num_rows_written;
        if (_row_count != _num_rows_written) {
            std::stringstream ss;
            ss << "_row_count != _num_rows_written:" << _row_count << " vs. " << _num_rows_written;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
    }
    _num_rows_written = 0;

    for (auto& column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->finish());
    }
    RETURN_IF_ERROR(_write_data());

    return Status::OK();
}

Status SegmentWriter::finalize_columns_index(uint64_t* index_size) {
    uint64_t index_start = _file_writer->bytes_appended();
    RETURN_IF_ERROR(_write_ordinal_index());
    RETURN_IF_ERROR(_write_zone_map());
    RETURN_IF_ERROR(_write_bitmap_index());
    RETURN_IF_ERROR(_write_inverted_index());
    RETURN_IF_ERROR(_write_bloom_filter_index());

    *index_size = _file_writer->bytes_appended() - index_start;
    if (_has_key) {
        if (_tablet_schema->keys_type() == UNIQUE_KEYS && _opts.enable_unique_key_merge_on_write) {
            RETURN_IF_ERROR(_write_primary_key_index());
            // IndexedColumnWriter write data pages mixed with segment data, we should use
            // the stat from primary key index builder.
            *index_size += _primary_key_index_builder->disk_size();
        } else {
            RETURN_IF_ERROR(_write_short_key_index());
            *index_size = _file_writer->bytes_appended() - index_start;
        }
    }
    _inverted_index_file_size = try_get_inverted_index_file_size();
    // reset all column writers and data_conveter
    clear();

    return Status::OK();
}

Status SegmentWriter::finalize_footer(uint64_t* segment_file_size) {
    RETURN_IF_ERROR(_write_footer());
    // finish
    RETURN_IF_ERROR(_file_writer->finalize());
    *segment_file_size = _file_writer->bytes_appended();
    if (*segment_file_size == 0) {
        return Status::Corruption("Bad segment, file size = 0");
    }
    return Status::OK();
}

Status SegmentWriter::finalize(uint64_t* segment_file_size, uint64_t* index_size) {
    MonotonicStopWatch timer;
    timer.start();
    // check disk capacity
    if (_data_dir != nullptr && _data_dir->reach_capacity_limit((int64_t)estimate_segment_size())) {
        return Status::Error<DISK_REACH_CAPACITY_LIMIT>("disk {} exceed capacity limit.",
                                                        _data_dir->path_hash());
    }
    // write data
    RETURN_IF_ERROR(finalize_columns_data());
    // write index
    RETURN_IF_ERROR(finalize_columns_index(index_size));
    // write footer
    RETURN_IF_ERROR(finalize_footer(segment_file_size));

    if (timer.elapsed_time() > 5000000000l) {
        LOG(INFO) << "segment flush consumes a lot time_ns " << timer.elapsed_time()
                  << ", segmemt_size " << *segment_file_size;
    }
    return Status::OK();
}

void SegmentWriter::clear() {
    for (auto& column_writer : _column_writers) {
        column_writer.reset();
    }
    _column_writers.clear();
    _column_ids.clear();
    _olap_data_convertor.reset();
}

// write column data to file one by one
Status SegmentWriter::_write_data() {
    for (auto& column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->write_data());
    }
    return Status::OK();
}

// write ordinal index after data has been written
Status SegmentWriter::_write_ordinal_index() {
    for (auto& column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->write_ordinal_index());
    }
    return Status::OK();
}

Status SegmentWriter::_write_zone_map() {
    for (auto& column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->write_zone_map());
    }
    return Status::OK();
}

Status SegmentWriter::_write_bitmap_index() {
    for (auto& column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->write_bitmap_index());
    }
    return Status::OK();
}

Status SegmentWriter::_write_inverted_index() {
    for (auto& column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->write_inverted_index());
    }
    return Status::OK();
}

Status SegmentWriter::_write_bloom_filter_index() {
    for (auto& column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->write_bloom_filter_index());
    }
    return Status::OK();
}

Status SegmentWriter::_write_short_key_index() {
    std::vector<Slice> body;
    PageFooterPB footer;
    RETURN_IF_ERROR(_short_key_index_builder->finalize(_row_count, &body, &footer));
    PagePointer pp;
    // short key index page is not compressed right now
    RETURN_IF_ERROR(PageIO::write_page(_file_writer, body, footer, &pp));
    pp.to_proto(_footer.mutable_short_key_index_page());
    return Status::OK();
}

Status SegmentWriter::_write_primary_key_index() {
    CHECK(_primary_key_index_builder->num_rows() == _row_count);
    return _primary_key_index_builder->finalize(_footer.mutable_primary_key_index_meta());
}

Status SegmentWriter::_write_footer() {
    _footer.set_num_rows(_row_count);

    // Footer := SegmentFooterPB, FooterPBSize(4), FooterPBChecksum(4), MagicNumber(4)
    std::string footer_buf;
    if (!_footer.SerializeToString(&footer_buf)) {
        return Status::InternalError("failed to serialize segment footer");
    }

    faststring fixed_buf;
    // footer's size
    put_fixed32_le(&fixed_buf, footer_buf.size());
    // footer's checksum
    uint32_t checksum = crc32c::Value(footer_buf.data(), footer_buf.size());
    put_fixed32_le(&fixed_buf, checksum);
    // Append magic number. we don't write magic number in the header because
    // that will need an extra seek when reading
    fixed_buf.append(k_segment_magic, k_segment_magic_length);

    std::vector<Slice> slices {footer_buf, fixed_buf};
    return _write_raw_data(slices);
}

Status SegmentWriter::_write_raw_data(const std::vector<Slice>& slices) {
    RETURN_IF_ERROR(_file_writer->appendv(&slices[0], slices.size()));
    return Status::OK();
}

Slice SegmentWriter::min_encoded_key() {
    return (_primary_key_index_builder == nullptr) ? Slice(_min_key.data(), _min_key.size())
                                                   : _primary_key_index_builder->min_key();
}
Slice SegmentWriter::max_encoded_key() {
    return (_primary_key_index_builder == nullptr) ? Slice(_max_key.data(), _max_key.size())
                                                   : _primary_key_index_builder->max_key();
}

void SegmentWriter::set_min_max_key(const Slice& key) {
    if (UNLIKELY(_is_first_row)) {
        _min_key.append(key.get_data(), key.get_size());
        _is_first_row = false;
    }
    if (key.compare(_max_key) > 0) {
        _max_key.clear();
        _max_key.append(key.get_data(), key.get_size());
    }
}

void SegmentWriter::set_min_key(const Slice& key) {
    if (UNLIKELY(_is_first_row)) {
        _min_key.append(key.get_data(), key.get_size());
        _is_first_row = false;
    }
}

void SegmentWriter::set_max_key(const Slice& key) {
    _max_key.clear();
    _max_key.append(key.get_data(), key.get_size());
}

void SegmentWriter::set_mow_context(std::shared_ptr<MowContext> mow_context) {
    _mow_context = mow_context;
}

} // namespace segment_v2
} // namespace doris
