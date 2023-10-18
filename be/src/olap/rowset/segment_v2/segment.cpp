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

#include "olap/rowset/segment_v2/segment.h"

#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/segment_v2.pb.h>
#include <string.h>

#include <algorithm>
#include <memory>
#include <utility>

#include "common/logging.h"
#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "io/io_common.h"
#include "olap/block_column_predicate.h"
#include "olap/column_predicate.h"
#include "olap/iterators.h"
#include "olap/primary_key_index.h"
#include "olap/rowset/rowset_reader_context.h"
#include "olap/rowset/segment_v2/empty_segment_iterator.h"
#include "olap/rowset/segment_v2/indexed_column_reader.h"
#include "olap/rowset/segment_v2/page_io.h"
#include "olap/rowset/segment_v2/page_pointer.h"
#include "olap/rowset/segment_v2/segment_iterator.h"
#include "olap/rowset/segment_v2/segment_writer.h" // k_segment_magic_length
#include "olap/schema.h"
#include "olap/short_key_index.h"
#include "olap/storage_engine.h"
#include "olap/tablet_schema.h"
#include "olap/types.h"
#include "olap/utils.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/query_context.h"
#include "runtime/runtime_predicate.h"
#include "runtime/runtime_state.h"
#include "util/bvar_helper.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/slice.h" // Slice
#include "vec/columns/column.h"
#include "vec/common/string_ref.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/olap/vgeneric_iterators.h"

namespace doris {

namespace segment_v2 {
class InvertedIndexIterator;

Status Segment::open(io::FileSystemSPtr fs, const std::string& path, uint32_t segment_id,
                     RowsetId rowset_id, TabletSchemaSPtr tablet_schema,
                     const io::FileReaderOptions& reader_options,
                     std::shared_ptr<Segment>* output) {
    io::FileReaderSPtr file_reader;
    RETURN_IF_ERROR(fs->open_file(path, &file_reader, &reader_options));
    std::shared_ptr<Segment> segment(new Segment(segment_id, rowset_id, tablet_schema));
    segment->_file_reader = std::move(file_reader);
    RETURN_IF_ERROR(segment->_open());
    *output = std::move(segment);
    return Status::OK();
}

Segment::Segment(uint32_t segment_id, RowsetId rowset_id, TabletSchemaSPtr tablet_schema)
        : _segment_id(segment_id),
          _meta_mem_usage(0),
          _rowset_id(rowset_id),
          _tablet_schema(tablet_schema),
          _segment_meta_mem_tracker(StorageEngine::instance()->segment_meta_mem_tracker()) {}

Segment::~Segment() {
#ifndef BE_TEST
    _segment_meta_mem_tracker->release(_meta_mem_usage);
#endif
}

Status Segment::_open() {
    SegmentFooterPB footer;
    RETURN_IF_ERROR(_parse_footer(&footer));
    RETURN_IF_ERROR(_create_column_readers(footer));
    _pk_index_meta.reset(footer.has_primary_key_index_meta()
                                 ? new PrimaryKeyIndexMetaPB(footer.primary_key_index_meta())
                                 : nullptr);
    // delete_bitmap_calculator_test.cpp
    // DCHECK(footer.has_short_key_index_page());
    _sk_index_page = footer.short_key_index_page();
    _num_rows = footer.num_rows();
    return Status::OK();
}

Status Segment::new_iterator(SchemaSPtr schema, const StorageReadOptions& read_options,
                             std::unique_ptr<RowwiseIterator>* iter) {
    read_options.stats->total_segment_number++;
    // trying to prune the current segment by segment-level zone map
    for (auto& entry : read_options.col_id_to_predicates) {
        int32_t column_id = entry.first;
        // schema change
        if (_tablet_schema->num_columns() <= column_id) {
            continue;
        }
        int32_t uid = read_options.tablet_schema->column(column_id).unique_id();
        if (_column_readers.count(uid) < 1 || !_column_readers.at(uid)->has_zone_map()) {
            continue;
        }
        if (read_options.col_id_to_predicates.count(column_id) > 0 &&
            !_column_readers.at(uid)->match_condition(entry.second.get())) {
            // any condition not satisfied, return.
            iter->reset(new EmptySegmentIterator(*schema));
            read_options.stats->filtered_segment_number++;
            return Status::OK();
        }
    }
    if (read_options.use_topn_opt) {
        auto query_ctx = read_options.runtime_state->get_query_ctx();
        auto runtime_predicate = query_ctx->get_runtime_predicate().get_predictate();
        if (runtime_predicate) {
            int32_t uid =
                    read_options.tablet_schema->column(runtime_predicate->column_id()).unique_id();
            AndBlockColumnPredicate and_predicate;
            auto single_predicate = new SingleColumnBlockPredicate(runtime_predicate.get());
            and_predicate.add_column_predicate(single_predicate);
            if (!_column_readers.at(uid)->match_condition(&and_predicate)) {
                // any condition not satisfied, return.
                iter->reset(new EmptySegmentIterator(*schema));
                read_options.stats->filtered_segment_number++;
                return Status::OK();
            }
        }
    }

    RETURN_IF_ERROR(load_index());
    if (read_options.delete_condition_predicates->num_of_column_predicate() == 0 &&
        read_options.push_down_agg_type_opt != TPushAggOp::NONE &&
        read_options.push_down_agg_type_opt != TPushAggOp::COUNT_ON_INDEX) {
        iter->reset(vectorized::new_vstatistics_iterator(this->shared_from_this(), *schema));
    } else {
        iter->reset(new SegmentIterator(this->shared_from_this(), schema));
    }

    if (config::ignore_always_true_predicate_for_segment &&
        read_options.io_ctx.reader_type == ReaderType::READER_QUERY &&
        !read_options.column_predicates.empty()) {
        auto pruned_predicates = read_options.column_predicates;
        auto pruned = false;
        for (auto& it : _column_readers) {
            if (it.second->prune_predicates_by_zone_map(pruned_predicates, it.first)) {
                pruned = true;
            }
        }

        if (pruned) {
            auto options_with_pruned_predicates = read_options;
            options_with_pruned_predicates.column_predicates = pruned_predicates;
            LOG(INFO) << "column_predicates pruned from " << read_options.column_predicates.size()
                      << " to " << pruned_predicates.size();
            return iter->get()->init(options_with_pruned_predicates);
        }
    }
    return iter->get()->init(read_options);
}

Status Segment::_parse_footer(SegmentFooterPB* footer) {
    // Footer := SegmentFooterPB, FooterPBSize(4), FooterPBChecksum(4), MagicNumber(4)
    auto file_size = _file_reader->size();
    if (file_size < 12) {
        return Status::Corruption("Bad segment file {}: file size {} < 12",
                                  _file_reader->path().native(), file_size);
    }

    uint8_t fixed_buf[12];
    size_t bytes_read = 0;
    // TODO(plat1ko): Support session variable `enable_file_cache`
    io::IOContext io_ctx {.is_index_data = true};
    RETURN_IF_ERROR(
            _file_reader->read_at(file_size - 12, Slice(fixed_buf, 12), &bytes_read, &io_ctx));
    DCHECK_EQ(bytes_read, 12);

    if (memcmp(fixed_buf + 8, k_segment_magic, k_segment_magic_length) != 0) {
        return Status::Corruption("Bad segment file {}: magic number not match",
                                  _file_reader->path().native());
    }

    // read footer PB
    uint32_t footer_length = decode_fixed32_le(fixed_buf);
    if (file_size < 12 + footer_length) {
        return Status::Corruption("Bad segment file {}: file size {} < {}",
                                  _file_reader->path().native(), file_size, 12 + footer_length);
    }

    std::string footer_buf;
    footer_buf.resize(footer_length);
    RETURN_IF_ERROR(_file_reader->read_at(file_size - 12 - footer_length, footer_buf, &bytes_read,
                                          &io_ctx));
    DCHECK_EQ(bytes_read, footer_length);

    // validate footer PB's checksum
    uint32_t expect_checksum = decode_fixed32_le(fixed_buf + 4);
    uint32_t actual_checksum = crc32c::Value(footer_buf.data(), footer_buf.size());
    if (actual_checksum != expect_checksum) {
        return Status::Corruption(
                "Bad segment file {}: footer checksum not match, actual={} vs expect={}",
                _file_reader->path().native(), actual_checksum, expect_checksum);
    }

    // deserialize footer PB
    if (!footer->ParseFromString(footer_buf)) {
        return Status::Corruption("Bad segment file {}: failed to parse SegmentFooterPB",
                                  _file_reader->path().native());
    }
    return Status::OK();
}

Status Segment::_load_pk_bloom_filter() {
    DCHECK(_tablet_schema->keys_type() == UNIQUE_KEYS);
    DCHECK(_pk_index_meta != nullptr);
    DCHECK(_pk_index_reader != nullptr);
    auto status = [this]() {
        return _load_pk_bf_once.call([this] {
            RETURN_IF_ERROR(_pk_index_reader->parse_bf(_file_reader, *_pk_index_meta));
            _meta_mem_usage += _pk_index_reader->get_bf_memory_size();
            _segment_meta_mem_tracker->consume(_pk_index_reader->get_bf_memory_size());
            return Status::OK();
        });
    }();
    if (!status.ok()) {
        remove_from_segment_cache();
    }
    return status;
}

void Segment::remove_from_segment_cache() const {
    if (config::disable_segment_cache) {
        return;
    }
    SegmentCache::CacheKey cache_key(_rowset_id, _segment_id);
    SegmentLoader::instance()->erase_segment(cache_key);
}

Status Segment::load_pk_index_and_bf() {
    RETURN_IF_ERROR(load_index());
    RETURN_IF_ERROR(_load_pk_bloom_filter());
    return Status::OK();
}
Status Segment::load_index() {
    auto status = [this]() { return _load_index_impl(); }();
    if (!status.ok()) {
        remove_from_segment_cache();
    }
    return status;
}

Status Segment::_load_index_impl() {
    return _load_index_once.call([this] {
        if (_tablet_schema->keys_type() == UNIQUE_KEYS && _pk_index_meta != nullptr) {
            _pk_index_reader.reset(new PrimaryKeyIndexReader());
            RETURN_IF_ERROR(_pk_index_reader->parse_index(_file_reader, *_pk_index_meta));
            _meta_mem_usage += _pk_index_reader->get_memory_size();
            _segment_meta_mem_tracker->consume(_pk_index_reader->get_memory_size());
            return Status::OK();
        } else {
            // read and parse short key index page
            OlapReaderStatistics tmp_stats;
            PageReadOptions opts {
                    .use_page_cache = true,
                    .type = INDEX_PAGE,
                    .file_reader = _file_reader.get(),
                    .page_pointer = PagePointer(_sk_index_page),
                    // short key index page uses NO_COMPRESSION for now
                    .codec = nullptr,
                    .stats = &tmp_stats,
                    .io_ctx = io::IOContext {.is_index_data = true},
            };
            Slice body;
            PageFooterPB footer;
            RETURN_IF_ERROR(
                    PageIO::read_and_decompress_page(opts, &_sk_index_handle, &body, &footer));
            DCHECK_EQ(footer.type(), SHORT_KEY_PAGE);
            DCHECK(footer.has_short_key_page_footer());

            _meta_mem_usage += body.get_size();
            _segment_meta_mem_tracker->consume(body.get_size());
            _sk_index_decoder.reset(new ShortKeyIndexDecoder);
            return _sk_index_decoder->parse(body, footer.short_key_page_footer());
        }
    });
}

Status Segment::_create_column_readers(const SegmentFooterPB& footer) {
    std::unordered_map<uint32_t, uint32_t> column_id_to_footer_ordinal;

    for (uint32_t ordinal = 0; ordinal < footer.columns().size(); ++ordinal) {
        auto& column_pb = footer.columns(ordinal);
        column_id_to_footer_ordinal.emplace(column_pb.unique_id(), ordinal);
    }

    for (uint32_t ordinal = 0; ordinal < _tablet_schema->num_columns(); ++ordinal) {
        auto& column = _tablet_schema->column(ordinal);
        auto iter = column_id_to_footer_ordinal.find(column.unique_id());
        if (iter == column_id_to_footer_ordinal.end()) {
            continue;
        }

        ColumnReaderOptions opts {
                .kept_in_memory = _tablet_schema->is_in_memory(),
        };
        std::unique_ptr<ColumnReader> reader;
        RETURN_IF_ERROR(ColumnReader::create(opts, footer.columns(iter->second), footer.num_rows(),
                                             _file_reader, &reader));
        _column_readers.emplace(column.unique_id(), std::move(reader));
    }
    return Status::OK();
}

// Not use cid anymore, for example original table schema is colA int, then user do following actions
// 1.add column b
// 2. drop column b
// 3. add column c
// in the new schema column c's cid == 2
// but in the old schema column b's cid == 2
// but they are not the same column
Status Segment::new_column_iterator(const TabletColumn& tablet_column,
                                    std::unique_ptr<ColumnIterator>* iter) {
    if (_column_readers.count(tablet_column.unique_id()) < 1) {
        if (!tablet_column.has_default_value() && !tablet_column.is_nullable()) {
            return Status::InternalError("invalid nonexistent column without default value.");
        }
        auto type_info = get_type_info(&tablet_column);
        std::unique_ptr<DefaultValueColumnIterator> default_value_iter(
                new DefaultValueColumnIterator(tablet_column.has_default_value(),
                                               tablet_column.default_value(),
                                               tablet_column.is_nullable(), std::move(type_info),
                                               tablet_column.precision(), tablet_column.frac()));
        ColumnIteratorOptions iter_opts;

        RETURN_IF_ERROR(default_value_iter->init(iter_opts));
        *iter = std::move(default_value_iter);
        return Status::OK();
    }
    ColumnIterator* it;
    RETURN_IF_ERROR(_column_readers.at(tablet_column.unique_id())->new_iterator(&it));
    iter->reset(it);
    return Status::OK();
}

Status Segment::new_bitmap_index_iterator(const TabletColumn& tablet_column,
                                          std::unique_ptr<BitmapIndexIterator>* iter) {
    auto col_unique_id = tablet_column.unique_id();
    if (_column_readers.count(col_unique_id) > 0 &&
        _column_readers.at(col_unique_id)->has_bitmap_index()) {
        BitmapIndexIterator* it;
        RETURN_IF_ERROR(_column_readers.at(col_unique_id)->new_bitmap_index_iterator(&it));
        iter->reset(it);
        return Status::OK();
    }
    return Status::OK();
}

Status Segment::new_inverted_index_iterator(const TabletColumn& tablet_column,
                                            const TabletIndex* index_meta,
                                            const StorageReadOptions& read_options,
                                            std::unique_ptr<InvertedIndexIterator>* iter) {
    auto col_unique_id = tablet_column.unique_id();
    if (_column_readers.count(col_unique_id) > 0 && index_meta) {
        RETURN_IF_ERROR(_column_readers.at(col_unique_id)
                                ->new_inverted_index_iterator(index_meta, read_options, iter));
        return Status::OK();
    }
    return Status::OK();
}

Status Segment::lookup_row_key(const Slice& key, bool with_seq_col, RowLocation* row_location) {
    RETURN_IF_ERROR(load_pk_index_and_bf());
    bool has_seq_col = _tablet_schema->has_sequence_col();
    size_t seq_col_length = 0;
    if (has_seq_col) {
        seq_col_length = _tablet_schema->column(_tablet_schema->sequence_col_idx()).length() + 1;
    }

    Slice key_without_seq =
            Slice(key.get_data(), key.get_size() - (with_seq_col ? seq_col_length : 0));

    DCHECK(_pk_index_reader != nullptr);
    if (!_pk_index_reader->check_present(key_without_seq)) {
        return Status::Error<ErrorCode::KEY_NOT_FOUND>("Can't find key in the segment");
    }
    bool exact_match = false;
    std::unique_ptr<segment_v2::IndexedColumnIterator> index_iterator;
    RETURN_IF_ERROR(_pk_index_reader->new_iterator(&index_iterator));
    auto st = index_iterator->seek_at_or_after(&key_without_seq, &exact_match);
    if (!st.ok() && !st.is<ErrorCode::ENTRY_NOT_FOUND>()) {
        return st;
    }
    if (st.is<ErrorCode::ENTRY_NOT_FOUND>() || (!has_seq_col && !exact_match)) {
        return Status::Error<ErrorCode::KEY_NOT_FOUND>("Can't find key in the segment");
    }
    row_location->row_id = index_iterator->get_current_ordinal();
    row_location->segment_id = _segment_id;
    row_location->rowset_id = _rowset_id;

    if (has_seq_col) {
        size_t num_to_read = 1;
        auto index_type = vectorized::DataTypeFactory::instance().create_data_type(
                _pk_index_reader->type_info()->type(), 1, 0);
        auto index_column = index_type->create_column();
        size_t num_read = num_to_read;
        RETURN_IF_ERROR(index_iterator->next_batch(&num_read, index_column));
        DCHECK(num_to_read == num_read);

        Slice sought_key =
                Slice(index_column->get_data_at(0).data, index_column->get_data_at(0).size);
        Slice sought_key_without_seq =
                Slice(sought_key.get_data(), sought_key.get_size() - seq_col_length);

        // compare key
        if (key_without_seq.compare(sought_key_without_seq) != 0) {
            return Status::Error<ErrorCode::KEY_NOT_FOUND>("Can't find key in the segment");
        }

        if (!with_seq_col) {
            return Status::OK();
        }

        // compare sequence id
        Slice sequence_id =
                Slice(key.get_data() + key_without_seq.get_size() + 1, seq_col_length - 1);
        Slice previous_sequence_id = Slice(
                sought_key.get_data() + sought_key_without_seq.get_size() + 1, seq_col_length - 1);
        if (sequence_id.compare(previous_sequence_id) < 0) {
            return Status::Error<ErrorCode::KEY_ALREADY_EXISTS>(
                    "key with higher sequence id exists");
        }
    }

    return Status::OK();
}

Status Segment::read_key_by_rowid(uint32_t row_id, std::string* key) {
    RETURN_IF_ERROR(load_pk_index_and_bf());
    std::unique_ptr<segment_v2::IndexedColumnIterator> iter;
    RETURN_IF_ERROR(_pk_index_reader->new_iterator(&iter));

    auto index_type = vectorized::DataTypeFactory::instance().create_data_type(
            _pk_index_reader->type_info()->type(), 1, 0);
    auto index_column = index_type->create_column();
    RETURN_IF_ERROR(iter->seek_to_ordinal(row_id));
    size_t num_read = 1;
    RETURN_IF_ERROR(iter->next_batch(&num_read, index_column));
    CHECK(num_read == 1);
    *key = index_column->get_data_at(0).to_string();
    return Status::OK();
}

} // namespace segment_v2
} // namespace doris
