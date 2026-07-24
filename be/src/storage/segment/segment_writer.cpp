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

#include "storage/segment/segment_writer.h"

#include <assert.h>
#include <gen_cpp/segment_v2.pb.h>
#include <parallel_hashmap/phmap.h>

#include <algorithm>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include <crc32c/crc32c.h>

#include "cloud/config.h"
#include "common/cast_set.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/logging.h" // LOG
#include "common/status.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_nullable.h"
#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "core/types.h"
#include "core/value/vdatetime_value.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker.h"
#include "storage/data_dir.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/index_writer.h"
#include "storage/index/inverted/inverted_index_fs_directory.h"
#include "storage/index/primary_key_index.h"
#include "storage/index/short_key_index.h"
#include "storage/iterator/olap_data_convertor.h"
#include "storage/key_coder.h"
#include "storage/mow/key_probe.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/rowset/rowset_writer_context.h" // RowsetWriterContext
#include "storage/rowset/segment_creator.h"
#include "storage/segment/column_writer.h" // ColumnWriter
#include "storage/segment/encoding_info.h"
#include "storage/segment/external_col_meta_util.h"
#include "storage/segment/page_io.h"
#include "storage/segment/page_pointer.h"
#include "storage/segment/variant/variant_ext_meta_writer.h"
#include "storage/segment/variant_stats_calculator.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"
#include "util/coding.h"
#include "util/faststring.h"
#include "util/simd/bits.h"
namespace doris {
namespace segment_v2 {

using namespace ErrorCode;

const char* k_segment_magic = "D0R1";
const uint32_t k_segment_magic_length = 4;

inline std::string segment_mem_tracker_name(uint32_t segment_id) {
    return "SegmentWriter:Segment-" + std::to_string(segment_id);
}

SegmentWriter::SegmentWriter(io::FileWriter* file_writer, uint32_t segment_id,
                             TabletSchemaSPtr tablet_schema, BaseTabletSPtr tablet,
                             DataDir* data_dir, const SegmentWriterOptions& opts,
                             IndexFileWriter* index_file_writer)
        : _segment_id(segment_id),
          _tablet_schema(std::move(tablet_schema)),
          _tablet(std::move(tablet)),
          _data_dir(data_dir),
          _opts(opts),
          _file_writer(file_writer),
          _index_file_writer(index_file_writer),
          _mem_tracker(std::make_unique<MemTracker>(segment_mem_tracker_name(segment_id))),
          _key_encoder(*_tablet_schema, _is_mow()) {
    CHECK_NOTNULL(file_writer);
    _num_short_key_columns = _tablet_schema->num_short_key_columns();
}

SegmentWriter::~SegmentWriter() {
    _mem_tracker->release(_mem_tracker->consumption());
}

void SegmentWriter::init_column_meta(ColumnMetaPB* meta, uint32_t column_id,
                                     const TabletColumn& column, const ColumnWriterOptions& opts) {
    meta->set_column_id(column_id);
    meta->set_type(int(column.type()));
    meta->set_length(column.length());
    meta->set_encoding(EncodingInfo::resolve_default_encoding(opts.storage_format, column));
    meta->set_compression(_opts.compression_type);
    meta->set_is_nullable(column.is_nullable());
    meta->set_default_value(column.default_value());
    meta->set_precision(column.precision());
    meta->set_frac(column.frac());
    if (column.has_path_info()) {
        column.path_info_ptr()->to_protobuf(meta->mutable_column_path_info(),
                                            column.parent_unique_id());
    }
    meta->set_unique_id(column.unique_id());
    for (uint32_t i = 0; i < column.get_subtype_count(); ++i) {
        init_column_meta(meta->add_children_columns(), column_id, column.get_sub_column(i), opts);
    }
    meta->set_result_is_nullable(column.get_result_is_nullable());
    meta->set_function_name(column.get_aggregation_name());
    meta->set_be_exec_version(column.get_be_exec_version());
    if (column.is_variant_type()) {
        meta->set_variant_max_subcolumns_count(column.variant_max_subcolumns_count());
        meta->set_variant_enable_doc_mode(column.variant_enable_doc_mode());
    }
}

Status SegmentWriter::init() {
    std::vector<uint32_t> column_ids;
    auto column_cnt = cast_set<int>(_tablet_schema->num_columns());
    for (uint32_t i = 0; i < column_cnt; ++i) {
        column_ids.emplace_back(i);
    }
    return init(column_ids, true);
}

Status SegmentWriter::_create_column_writer(uint32_t cid, const TabletColumn& column,
                                            const TabletSchemaSPtr& schema) {
    ColumnWriterOptions opts;
    opts.meta = _footer.add_columns();
    opts.storage_format = schema->storage_format();

    init_column_meta(opts.meta, cid, column, opts);

    // now we create zone map for key columns in AGG_KEYS or all column in UNIQUE_KEYS or DUP_KEYS
    // except for columns whose type don't support zone map.
    opts.need_zone_map = column.is_key() || schema->keys_type() != KeysType::AGG_KEYS;
    opts.need_bloom_filter = column.is_bf_column();
    if (opts.need_bloom_filter) {
        opts.bf_options.fpp = schema->has_bf_fpp() ? schema->bloom_filter_fpp() : 0.05;
    }
    auto* tablet_index = schema->get_ngram_bf_index(column.unique_id());
    if (tablet_index) {
        opts.need_bloom_filter = true;
        opts.is_ngram_bf_index = true;
        //narrow convert from int32_t to uint8_t and uint16_t which is dangerous
        auto gram_size = tablet_index->get_gram_size();
        auto gram_bf_size = tablet_index->get_gram_bf_size();
        if (gram_size > 256 || gram_size < 1) {
            return Status::NotSupported("Do not support ngram bloom filter for ngram_size: ",
                                        gram_size);
        }
        if (gram_bf_size > 65535 || gram_bf_size < 64) {
            return Status::NotSupported("Do not support ngram bloom filter for bf_size: ",
                                        gram_bf_size);
        }
        opts.gram_size = cast_set<uint8_t>(gram_size);
        opts.gram_bf_size = cast_set<uint16_t>(gram_bf_size);
    }

    bool skip_inverted_index = false;
    if (_opts.rowset_ctx != nullptr) {
        // skip write inverted index for index compaction column
        skip_inverted_index =
                _opts.rowset_ctx->columns_to_do_index_compaction.count(column.unique_id()) > 0;
    }
    // skip write inverted index on load if skip_write_index_on_load is true
    if (_opts.write_type == DataWriteType::TYPE_DIRECT && schema->skip_write_index_on_load()) {
        skip_inverted_index = true;
    }
    // indexes for this column
    if (!skip_inverted_index) {
        auto inverted_indexs = schema->inverted_indexs(column);
        if (!inverted_indexs.empty()) {
            opts.inverted_indexes = inverted_indexs;
            opts.need_inverted_index = true;
            DCHECK(_index_file_writer != nullptr);
        }
    }
    // indexes for this column
    if (const auto& index = schema->ann_index(column); index != nullptr) {
        opts.ann_index = index;
        opts.need_ann_index = true;
        DCHECK(_index_file_writer != nullptr);
    }

    opts.index_file_writer = _index_file_writer;

#define DISABLE_INDEX_IF_FIELD_TYPE(TYPE)                     \
    if (column.type() == FieldType::OLAP_FIELD_TYPE_##TYPE) { \
        opts.need_zone_map = false;                           \
        opts.need_bloom_filter = false;                       \
    }

    DISABLE_INDEX_IF_FIELD_TYPE(STRUCT)
    DISABLE_INDEX_IF_FIELD_TYPE(ARRAY)
    DISABLE_INDEX_IF_FIELD_TYPE(JSONB)
    DISABLE_INDEX_IF_FIELD_TYPE(AGG_STATE)
    DISABLE_INDEX_IF_FIELD_TYPE(MAP)
    DISABLE_INDEX_IF_FIELD_TYPE(BITMAP)
    DISABLE_INDEX_IF_FIELD_TYPE(HLL)
    DISABLE_INDEX_IF_FIELD_TYPE(QUANTILE_STATE)
    DISABLE_INDEX_IF_FIELD_TYPE(VARIANT)

#undef DISABLE_INDEX_IF_FIELD_TYPE

    int64_t storage_page_size = _tablet_schema->storage_page_size();
    // storage_page_size must be between 4KB and 10MB.
    if (storage_page_size >= 4096 && storage_page_size <= 10485760) {
        opts.data_page_size = storage_page_size;
    }
    opts.dict_page_size = _tablet_schema->storage_dict_page_size();
    DBUG_EXECUTE_IF("VerticalSegmentWriter._create_column_writer.storage_page_size", {
        auto table_id = DebugPoints::instance()->get_debug_param_or_default<int64_t>(
                "VerticalSegmentWriter._create_column_writer.storage_page_size", "table_id",
                INT_MIN);
        auto target_data_page_size = DebugPoints::instance()->get_debug_param_or_default<int64_t>(
                "VerticalSegmentWriter._create_column_writer.storage_page_size",
                "storage_page_size", INT_MIN);
        if (table_id == INT_MIN || target_data_page_size == INT_MIN) {
            return Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "Debug point parameters missing: either 'table_id' or 'storage_page_size' not "
                    "set.");
        }
        if (table_id == _tablet_schema->table_id() &&
            opts.data_page_size != target_data_page_size) {
            return Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "Mismatch in 'storage_page_size': expected size does not match the current "
                    "data page size. "
                    "Expected: " +
                    std::to_string(target_data_page_size) +
                    ", Actual: " + std::to_string(opts.data_page_size) + ".");
        }
    })
    if (column.is_row_store_column()) {
        // smaller page size for row store column; encoding is already set to PLAIN /
        // PLAIN_V2 by init_column_meta via resolve_default_encoding().
        auto page_size = _tablet_schema->row_store_page_size();
        opts.data_page_size =
                (page_size > 0) ? page_size : segment_v2::ROW_STORE_PAGE_SIZE_DEFAULT_VALUE;
    }

    opts.rowset_ctx = _opts.rowset_ctx;
    opts.file_writer = _file_writer;
    opts.compression_type = _opts.compression_type;
    opts.footer = &_footer;
    if (_opts.rowset_ctx != nullptr) {
        opts.input_rs_readers = _opts.rowset_ctx->input_rs_readers;
    }

    std::unique_ptr<ColumnWriter> writer;
    RETURN_IF_ERROR(ColumnWriter::create(opts, &column, _file_writer, &writer));
    RETURN_IF_ERROR(writer->init());
    _column_writers.push_back(std::move(writer));

    _olap_data_convertor->add_column_data_convertor(column);
    return Status::OK();
}

Status SegmentWriter::init(const std::vector<uint32_t>& col_ids, bool has_key) {
    DCHECK(_column_writers.empty());
    DCHECK(_column_ids.empty());
    _has_key = has_key;
    _column_writers.reserve(_tablet_schema->columns().size());
    _column_ids.insert(_column_ids.end(), col_ids.begin(), col_ids.end());
    _olap_data_convertor = std::make_unique<OlapBlockDataConvertor>();
    if (_opts.compression_type == UNKNOWN_COMPRESSION) {
        _opts.compression_type = _tablet_schema->compression_type();
    }

    // Vertical compaction calls init() multiple times against the same writer; the footer accumulates entries
    // across calls, so this init()'s slice of footer columns starts at the current size.
    const int variant_stats_footer_offset = _footer.columns_size();
    RETURN_IF_ERROR(_create_writers(_tablet_schema, col_ids));

    // Initialize variant statistics calculator
    _variant_stats_calculator = std::make_unique<VariantStatsCaculator>(
            &_footer, _tablet_schema, col_ids, variant_stats_footer_offset);

    // we don't need the short key index for unique key merge on write table.
    if (_has_key) {
        if (_is_mow()) {
            size_t seq_col_length = 0;
            if (_tablet_schema->has_sequence_col()) {
                seq_col_length =
                        _tablet_schema->column(_tablet_schema->sequence_col_idx()).length() + 1;
            }
            size_t rowid_length = 0;
            if (_is_mow_with_cluster_key()) {
                rowid_length = PrimaryKeyIndexReader::ROW_ID_LENGTH;
                _short_key_index_builder.reset(
                        new ShortKeyIndexBuilder(_segment_id, _opts.num_rows_per_block));
            }
            _primary_key_index_builder.reset(
                    new PrimaryKeyIndexBuilder(_file_writer, seq_col_length, rowid_length));
            RETURN_IF_ERROR(_primary_key_index_builder->init());
        } else {
            _short_key_index_builder.reset(
                    new ShortKeyIndexBuilder(_segment_id, _opts.num_rows_per_block));
        }
    }
    return Status::OK();
}

Status SegmentWriter::_create_writers(const TabletSchemaSPtr& tablet_schema,
                                      const std::vector<uint32_t>& col_ids) {
    _olap_data_convertor->reserve(col_ids.size());
    for (auto& cid : col_ids) {
        RETURN_IF_ERROR(_create_column_writer(cid, tablet_schema->column(cid), tablet_schema));
    }
    return Status::OK();
}

Status SegmentWriter::append_block(const Block* block, size_t row_pos, size_t num_rows) {
    if (block->columns() < _column_writers.size()) {
        return Status::InternalError(
                "block->columns() < _column_writers.size(), block->columns()=" +
                std::to_string(block->columns()) +
                ", _column_writers.size()=" + std::to_string(_column_writers.size()) +
                ", _tablet_schema->dump_structure()=" + _tablet_schema->dump_structure());
    }
    CHECK(block->columns() >= _column_writers.size())
            << ", block->columns()=" << block->columns()
            << ", _column_writers.size()=" << _column_writers.size()
            << ", _tablet_schema->dump_structure()=" << _tablet_schema->dump_structure();
    _olap_data_convertor->set_source_content(block, row_pos, num_rows);

    // convert column data from engine format to storage layer format
    std::vector<IOlapColumnDataAccessor*> key_columns;
    IOlapColumnDataAccessor* seq_column = nullptr;
    for (size_t id = 0; id < _column_writers.size(); ++id) {
        // olap data convertor alway start from id = 0
        auto converted_result = _olap_data_convertor->convert_column_data(id);
        if (!converted_result.first.ok()) {
            return converted_result.first;
        }
        auto cid = _column_ids[id];
        if (_has_key && cid < _tablet_schema->num_key_columns()) {
            key_columns.push_back(converted_result.second);
        } else if (_has_key && _tablet_schema->has_sequence_col() &&
                   cid == _tablet_schema->sequence_col_idx()) {
            seq_column = converted_result.second;
        }
        RETURN_IF_ERROR(_column_writers[id]->append(converted_result.second->get_nullmap(),
                                                    converted_result.second->get_data(), num_rows));
    }
    if (_opts.write_type == DataWriteType::TYPE_COMPACTION) {
        RETURN_IF_ERROR(
                _variant_stats_calculator->calculate_variant_stats(block, row_pos, num_rows));
    }

    RETURN_IF_ERROR(build_key_index(key_columns, seq_column, num_rows));

    _num_rows_written += num_rows;
    _olap_data_convertor->clear_source_content();
    return Status::OK();
}

Status SegmentWriter::build_key_index(std::vector<IOlapColumnDataAccessor*>& key_columns,
                                      IOlapColumnDataAccessor* seq_column, size_t num_rows) {
    if (!_has_key) {
        return Status::OK();
    }

    // find all row pos for short key indexes
    std::vector<size_t> short_key_pos;
    if (UNLIKELY(_short_key_row_pos == 0 && _num_rows_written == 0)) {
        short_key_pos.push_back(0);
    }
    while (_short_key_row_pos + _opts.num_rows_per_block < _num_rows_written + num_rows) {
        _short_key_row_pos += _opts.num_rows_per_block;
        short_key_pos.push_back(_short_key_row_pos - _num_rows_written);
    }

    if (_is_mow_with_cluster_key()) {
        // For CLUSTER BY tables:
        // 1) generate primary key index (unique keys)
        RETURN_IF_ERROR(_generate_primary_key_index(key_columns, seq_column, num_rows, true));
        // 2) generate short key index (cluster keys)
        key_columns.clear();
        for (const auto& cid : _tablet_schema->cluster_key_uids()) {
            auto cluster_key_index = _tablet_schema->field_index(cid);
            if (cluster_key_index == -1) {
                return Status::InternalError("could not find cluster key column with unique_id=" +
                                             std::to_string(cid) + " in tablet schema");
            }
            bool found = false;
            for (auto i = 0; i < _column_ids.size(); ++i) {
                if (_column_ids[i] == cluster_key_index) {
                    auto converted_result = _olap_data_convertor->convert_column_data(i);
                    if (!converted_result.first.ok()) {
                        return converted_result.first;
                    }
                    key_columns.push_back(converted_result.second);
                    found = true;
                    break;
                }
            }
            if (!found) {
                return Status::InternalError(
                        "could not found cluster key column with unique_id=" + std::to_string(cid) +
                        ", tablet schema index=" + std::to_string(cluster_key_index));
            }
        }
        return _generate_short_key_index(key_columns, num_rows, short_key_pos);
    }
    if (_is_mow()) {
        return _generate_primary_key_index(key_columns, seq_column, num_rows, false);
    }
    return _generate_short_key_index(key_columns, num_rows, short_key_pos);
}

int64_t SegmentWriter::max_row_to_add(size_t row_avg_size_in_bytes) {
    auto segment_size = estimate_segment_size();
    if (segment_size >= MAX_SEGMENT_SIZE || _num_rows_written >= _opts.max_rows_per_segment)
            [[unlikely]] {
        return 0;
    }
    int64_t size_rows = ((int64_t)MAX_SEGMENT_SIZE - (int64_t)segment_size) / row_avg_size_in_bytes;
    int64_t count_rows = (int64_t)_opts.max_rows_per_segment - _num_rows_written;

    return std::min(size_rows, count_rows);
}

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
    if (_is_mow_with_cluster_key()) {
        size += _primary_key_index_builder->size() + _short_key_index_builder->size();
    } else if (_is_mow()) {
        size += _primary_key_index_builder->size();
    } else {
        size += _short_key_index_builder->size();
    }

    // update the mem_tracker of segment size
    _mem_tracker->consume(size - _mem_tracker->consumption());
    return size;
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
    // Record each index range separately. Vertical compaction writes column groups as
    // data+index pairs, so a single [first index, EOF) range would include later column data.
    // This SegmentWriter path is shared by cloud load, non-vertical compaction, schema change
    // final output, and vertical compaction via VerticalBetaRowsetWriter.
    RETURN_IF_ERROR(_write_ordinal_index());
    RETURN_IF_ERROR(_write_zone_map());
    RETURN_IF_ERROR(_write_inverted_index());
    RETURN_IF_ERROR(_write_ann_index());
    RETURN_IF_ERROR(_write_bloom_filter_index());

    *index_size = _file_writer->bytes_appended() - index_start;
    if (_has_key) {
        if (_is_mow_with_cluster_key()) {
            // 1. sort primary keys
            std::sort(_primary_keys.begin(), _primary_keys.end());
            // 2. write primary keys index
            std::string last_key;
            for (const auto& key : _primary_keys) {
                DCHECK(key.compare(last_key) > 0)
                        << "found duplicate key or key is not sorted! current key: " << key
                        << ", last key: " << last_key;
                RETURN_IF_ERROR(_primary_key_index_builder->add_item(key));
                last_key = key;
            }

            RETURN_IF_ERROR(_write_short_key_index());
            *index_size = _file_writer->bytes_appended() - index_start;
            RETURN_IF_ERROR(_write_primary_key_index());
            *index_size += _primary_key_index_builder->disk_size();
        } else if (_is_mow()) {
            RETURN_IF_ERROR(_write_primary_key_index());
            // IndexedColumnWriter write data pages mixed with segment data, we should use
            // the stat from primary key index builder.
            *index_size += _primary_key_index_builder->disk_size();
        } else {
            RETURN_IF_ERROR(_write_short_key_index());
            *index_size = _file_writer->bytes_appended() - index_start;
        }
    }
    uint64_t file_index_end = _file_writer->bytes_appended();
    _index_file_cache_info.add_index_range(index_start, file_index_end - index_start);
    // reset all column writers and data_conveter
    clear();

    return Status::OK();
}

Status SegmentWriter::finalize_footer(uint64_t* segment_file_size,
                                      SegmentIndexFileCacheInfo* index_file_cache_info) {
    uint64_t footer_start = _file_writer->bytes_appended();
    RETURN_IF_ERROR(_write_footer());
    // finish
    RETURN_IF_ERROR(_file_writer->close(true));
    *segment_file_size = _file_writer->bytes_appended();
    // The closed size completes the preload range recorded above. Local temporary rowsets, such as
    // schema-change internal sorting output, are filtered by SegmentIndexFileCacheLoader.
    _index_file_cache_info.segment_file_size = *segment_file_size;
    _index_file_cache_info.add_index_range(footer_start, *segment_file_size - footer_start);
    if (index_file_cache_info != nullptr) {
        *index_file_cache_info = _index_file_cache_info;
    }
    if (*segment_file_size == 0) {
        return Status::Corruption("Bad segment, file size = 0");
    }
    return Status::OK();
}

Status SegmentWriter::finalize(uint64_t* segment_file_size, uint64_t* index_size,
                               SegmentIndexFileCacheInfo* index_file_cache_info) {
    MonotonicStopWatch timer;
    timer.start();
    // check disk capacity
    if (_data_dir != nullptr && _data_dir->reach_capacity_limit((int64_t)estimate_segment_size())) {
        return Status::Error<DISK_REACH_CAPACITY_LIMIT>("disk {} exceed capacity limit, path: {}",
                                                        _data_dir->path_hash(), _data_dir->path());
    }
    // write data
    RETURN_IF_ERROR(finalize_columns_data());
    // write index
    RETURN_IF_ERROR(finalize_columns_index(index_size));
    // write footer
    RETURN_IF_ERROR(finalize_footer(segment_file_size, index_file_cache_info));

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

        auto* column_meta = column_writer->get_column_meta();
        DCHECK(column_meta != nullptr);
        column_meta->set_compressed_data_bytes(
                (column_meta->has_compressed_data_bytes() ? column_meta->compressed_data_bytes()
                                                          : 0) +
                column_writer->get_total_compressed_data_pages_bytes());
        column_meta->set_uncompressed_data_bytes(
                (column_meta->has_uncompressed_data_bytes() ? column_meta->uncompressed_data_bytes()
                                                            : 0) +
                column_writer->get_total_uncompressed_data_pages_bytes());
        column_meta->set_raw_data_bytes(
                (column_meta->has_raw_data_bytes() ? column_meta->raw_data_bytes() : 0) +
                column_writer->get_raw_data_bytes());
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

Status SegmentWriter::_write_inverted_index() {
    for (auto& column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->write_inverted_index());
    }
    return Status::OK();
}

Status SegmentWriter::_write_ann_index() {
    for (auto& column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->write_ann_index());
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
    CHECK_EQ(_primary_key_index_builder->num_rows(), _row_count);
    return _primary_key_index_builder->finalize(_footer.mutable_primary_key_index_meta());
}

Status SegmentWriter::_write_footer() {
    _footer.set_num_rows(_row_count);
    // Decide whether to externalize ColumnMetaPB by tablet default, and stamp footer version
    if (_tablet_schema->storage_format() == TabletStorageFormatPB::TABLET_STORAGE_FORMAT_V3) {
        _footer.set_version(SEGMENT_FOOTER_VERSION_V3_EXT_COL_META);
        VLOG_DEBUG << "use external column meta";
        // External ColumnMetaPB writing (optional)
        RETURN_IF_ERROR(ExternalColMetaUtil::write_external_column_meta(
                _file_writer, &_footer, _opts.compression_type,
                [this](const std::vector<Slice>& slices) { return _write_raw_data(slices); }));
    }

    // Footer := SegmentFooterPB, FooterPBSize(4), FooterPBChecksum(4), MagicNumber(4)
    std::string footer_buf;
    VLOG_DEBUG << "footer " << _footer.DebugString();
    if (!_footer.SerializeToString(&footer_buf)) {
        return Status::InternalError("failed to serialize segment footer");
    }

    faststring fixed_buf;
    // footer's size
    put_fixed32_le(&fixed_buf, cast_set<uint32_t>(footer_buf.size()));
    // footer's checksum
    uint32_t checksum = crc32c::Crc32c(footer_buf.data(), footer_buf.size());
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

Status SegmentWriter::_generate_primary_key_index(
        const std::vector<IOlapColumnDataAccessor*>& primary_key_columns,
        IOlapColumnDataAccessor* seq_column, size_t num_rows, bool need_sort) {
    if (!need_sort) { // mow table without cluster key
        std::string last_key;
        for (size_t pos = 0; pos < num_rows; pos++) {
            std::string key = segment_v2::encode_mow_key_invalidate_cache(
                    _key_encoder, primary_key_columns, seq_column, pos,
                    _tablet_schema->has_sequence_col(), _opts.rowset_ctx->tablet_id,
                    *_tablet_schema, _opts.write_type);
            DCHECK(key.compare(last_key) > 0)
                    << "found duplicate key or key is not sorted! current key: " << key
                    << ", last key: " << last_key;
            RETURN_IF_ERROR(_primary_key_index_builder->add_item(key));
            last_key = std::move(key);
        }
    } else { // mow table with cluster key
        // generate primary keys in memory
        for (uint32_t pos = 0; pos < num_rows; pos++) {
            std::string key = _key_encoder.full_encode_primary_keys(primary_key_columns, pos);
            MowKeyProbe::maybe_invalidate_row_cache(_opts.rowset_ctx->tablet_id, *_tablet_schema,
                                                    _opts.write_type, key);
            if (_tablet_schema->has_sequence_col()) {
                _key_encoder.append_seq_suffix(&key, seq_column, pos);
            }
            _key_encoder.append_rowid_suffix(&key, pos + _num_rows_written);
            _primary_keys_size += key.size();
            _primary_keys.emplace_back(std::move(key));
        }
    }
    return Status::OK();
}

Status SegmentWriter::_generate_short_key_index(std::vector<IOlapColumnDataAccessor*>& key_columns,
                                                size_t num_rows,
                                                const std::vector<size_t>& short_key_pos) {
    set_min_key(_key_encoder.full_encode(key_columns, 0));
    set_max_key(_key_encoder.full_encode(key_columns, num_rows - 1));
    DCHECK(Slice(_max_key.data(), _max_key.size())
                   .compare(Slice(_min_key.data(), _min_key.size())) >= 0)
            << "key is not sorted! min key: " << _min_key << ", max key: " << _max_key;

    key_columns.resize(_num_short_key_columns);
    std::string last_key;
    for (const auto pos : short_key_pos) {
        std::string key = _key_encoder.encode_short_keys(key_columns, pos);
        DCHECK(key.compare(last_key) >= 0)
                << "key is not sorted! current key: " << key << ", last key: " << last_key;
        RETURN_IF_ERROR(_short_key_index_builder->add_item(key));
        last_key = std::move(key);
    }
    return Status::OK();
}

} // namespace segment_v2
} // namespace doris
