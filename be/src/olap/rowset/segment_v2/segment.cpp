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

#include <gen_cpp/olap_file.pb.h>

#include <memory>
#include <utility>

#include "common/config.h"
#include "common/logging.h" // LOG
#include "io/cache/file_cache_manager.h"
#include "io/fs/file_system.h"
#include "olap/rowset/segment_v2/column_reader.h" // ColumnReader
#include "olap/rowset/segment_v2/empty_segment_iterator.h"
#include "olap/rowset/segment_v2/page_io.h"
#include "olap/rowset/segment_v2/segment_iterator.h"
#include "olap/rowset/segment_v2/segment_writer.h" // k_segment_magic_length
#include "olap/storage_engine.h"
#include "olap/tablet_schema.h"
#include "util/crc32c.h"
#include "util/slice.h" // Slice
#include "vec/olap/vgeneric_iterators.h"

namespace doris {
namespace segment_v2 {

using io::FileCacheManager;

Status Segment::open(io::FileSystem* fs, const std::string& path, const std::string& cache_path,
                     uint32_t segment_id, TabletSchemaSPtr tablet_schema,
                     std::shared_ptr<Segment>* output) {
    std::shared_ptr<Segment> segment(new Segment(segment_id, tablet_schema));
    io::FileReaderSPtr file_reader;
#ifndef BE_TEST
    RETURN_IF_ERROR(fs->open_file(path, &file_reader));
#else
    // be ut use local file reader instead of remote file reader while use remote cache
    if (!config::file_cache_type.empty()) {
        RETURN_IF_ERROR(io::global_local_filesystem()->open_file(path, &file_reader));
    } else {
        RETURN_IF_ERROR(fs->open_file(path, &file_reader));
    }
#endif
    if (fs->type() != io::FileSystemType::LOCAL && !config::file_cache_type.empty()) {
        io::FileCachePtr cache_reader = FileCacheManager::instance()->new_file_cache(
                cache_path, config::file_cache_alive_time_sec, file_reader,
                config::file_cache_type);
        segment->_file_reader = cache_reader;
        FileCacheManager::instance()->add_file_cache(cache_path, cache_reader);
    } else {
        segment->_file_reader = std::move(file_reader);
    }
    RETURN_IF_ERROR(segment->_open());
    *output = std::move(segment);
    return Status::OK();
}

Segment::Segment(uint32_t segment_id, TabletSchemaSPtr tablet_schema)
        : _segment_id(segment_id), _tablet_schema(tablet_schema), _meta_mem_usage(0) {}

Segment::~Segment() {
#ifndef BE_TEST
    StorageEngine::instance()->segment_meta_mem_tracker()->release(_meta_mem_usage);
#endif
}

Status Segment::_open() {
    RETURN_IF_ERROR(_parse_footer());
    RETURN_IF_ERROR(_create_column_readers());
    return Status::OK();
}

Status Segment::new_iterator(const Schema& schema, const StorageReadOptions& read_options,
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
            iter->reset(new EmptySegmentIterator(schema));
            read_options.stats->filtered_segment_number++;
            return Status::OK();
        }
    }

    RETURN_IF_ERROR(load_index());
    if (read_options.col_id_to_del_predicates.empty() &&
        read_options.push_down_agg_type_opt != TPushAggOp::NONE) {
        iter->reset(vectorized::new_vstatistics_iterator(this->shared_from_this(), schema));
    } else {
        iter->reset(new SegmentIterator(this->shared_from_this(), schema));
    }
    iter->get()->init(read_options);
    return Status::OK();
}

Status Segment::_parse_footer() {
    // Footer := SegmentFooterPB, FooterPBSize(4), FooterPBChecksum(4), MagicNumber(4)
    auto file_size = _file_reader->size();
    if (file_size < 12) {
        return Status::Corruption("Bad segment file {}: file size {} < 12",
                                  _file_reader->path().native(), file_size);
    }

    uint8_t fixed_buf[12];
    size_t bytes_read = 0;
    RETURN_IF_ERROR(_file_reader->read_at(file_size - 12, Slice(fixed_buf, 12), &bytes_read));
    DCHECK_EQ(bytes_read, 12);

    // validate magic number
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
    _meta_mem_usage += footer_length;
    StorageEngine::instance()->segment_meta_mem_tracker()->consume(footer_length);

    std::string footer_buf;
    footer_buf.resize(footer_length);
    RETURN_IF_ERROR(_file_reader->read_at(file_size - 12 - footer_length, footer_buf, &bytes_read));
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
    if (!_footer.ParseFromString(footer_buf)) {
        return Status::Corruption("Bad segment file {}: failed to parse SegmentFooterPB",
                                  _file_reader->path().native());
    }
    return Status::OK();
}

Status Segment::_load_pk_bloom_filter() {
    DCHECK(_tablet_schema->keys_type() == UNIQUE_KEYS);
    DCHECK(_footer.has_primary_key_index_meta());
    DCHECK(_pk_index_reader != nullptr);
    return _load_pk_bf_once.call([this] {
        RETURN_IF_ERROR(_pk_index_reader->parse_bf(_file_reader, _footer.primary_key_index_meta()));
        _meta_mem_usage += _pk_index_reader->get_bf_memory_size();
        return Status::OK();
    });
}

Status Segment::load_pk_index_and_bf() {
    RETURN_IF_ERROR(load_index());
    RETURN_IF_ERROR(_load_pk_bloom_filter());
    return Status::OK();
}
Status Segment::load_index() {
    return _load_index_once.call([this] {
        if (_tablet_schema->keys_type() == UNIQUE_KEYS && _footer.has_primary_key_index_meta()) {
            _pk_index_reader.reset(new PrimaryKeyIndexReader());
            RETURN_IF_ERROR(
                    _pk_index_reader->parse_index(_file_reader, _footer.primary_key_index_meta()));
            _meta_mem_usage += _pk_index_reader->get_memory_size();
            return Status::OK();
        } else {
            // read and parse short key index page
            PageReadOptions opts;
            opts.file_reader = _file_reader.get();
            opts.page_pointer = PagePointer(_footer.short_key_index_page());
            opts.codec = nullptr; // short key index page uses NO_COMPRESSION for now
            OlapReaderStatistics tmp_stats;
            opts.stats = &tmp_stats;
            opts.type = INDEX_PAGE;
            Slice body;
            PageFooterPB footer;
            RETURN_IF_ERROR(
                    PageIO::read_and_decompress_page(opts, &_sk_index_handle, &body, &footer));
            DCHECK_EQ(footer.type(), SHORT_KEY_PAGE);
            DCHECK(footer.has_short_key_page_footer());

            _meta_mem_usage += body.get_size();
            StorageEngine::instance()->segment_meta_mem_tracker()->consume(body.get_size());
            _sk_index_decoder.reset(new ShortKeyIndexDecoder);
            return _sk_index_decoder->parse(body, footer.short_key_page_footer());
        }
    });
}

Status Segment::_create_column_readers() {
    for (uint32_t ordinal = 0; ordinal < _footer.columns().size(); ++ordinal) {
        auto& column_pb = _footer.columns(ordinal);
        _column_id_to_footer_ordinal.emplace(column_pb.unique_id(), ordinal);
    }

    for (uint32_t ordinal = 0; ordinal < _tablet_schema->num_columns(); ++ordinal) {
        auto& column = _tablet_schema->column(ordinal);
        auto iter = _column_id_to_footer_ordinal.find(column.unique_id());
        if (iter == _column_id_to_footer_ordinal.end()) {
            continue;
        }

        ColumnReaderOptions opts;
        opts.kept_in_memory = _tablet_schema->is_in_memory();
        std::unique_ptr<ColumnReader> reader;
        RETURN_IF_ERROR(ColumnReader::create(opts, _footer.columns(iter->second),
                                             _footer.num_rows(), _file_reader, &reader));
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
Status Segment::new_column_iterator(const TabletColumn& tablet_column, ColumnIterator** iter) {
    if (_column_readers.count(tablet_column.unique_id()) < 1) {
        if (!tablet_column.has_default_value() && !tablet_column.is_nullable()) {
            return Status::InternalError("invalid nonexistent column without default value.");
        }
        auto type_info = get_type_info(&tablet_column);
        std::unique_ptr<DefaultValueColumnIterator> default_value_iter(
                new DefaultValueColumnIterator(
                        tablet_column.has_default_value(), tablet_column.default_value(),
                        tablet_column.is_nullable(), std::move(type_info), tablet_column.length(),
                        tablet_column.precision(), tablet_column.frac()));
        ColumnIteratorOptions iter_opts;

        RETURN_IF_ERROR(default_value_iter->init(iter_opts));
        *iter = default_value_iter.release();
        return Status::OK();
    }
    return _column_readers.at(tablet_column.unique_id())->new_iterator(iter);
}

Status Segment::new_bitmap_index_iterator(const TabletColumn& tablet_column,
                                          BitmapIndexIterator** iter) {
    auto col_unique_id = tablet_column.unique_id();
    if (_column_readers.count(col_unique_id) > 0 &&
        _column_readers.at(col_unique_id)->has_bitmap_index()) {
        return _column_readers.at(col_unique_id)->new_bitmap_index_iterator(iter);
    }
    return Status::OK();
}

Status Segment::lookup_row_key(const Slice& key, RowLocation* row_location) {
    RETURN_IF_ERROR(load_pk_index_and_bf());
    bool has_seq_col = _tablet_schema->has_sequence_col();
    size_t seq_col_length = 0;
    if (has_seq_col) {
        seq_col_length = _tablet_schema->column(_tablet_schema->sequence_col_idx()).length() + 1;
    }
    Slice key_without_seq = Slice(key.get_data(), key.get_size() - seq_col_length);

    DCHECK(_pk_index_reader != nullptr);
    if (!_pk_index_reader->check_present(key_without_seq)) {
        return Status::NotFound("Can't find key in the segment");
    }
    bool exact_match = false;
    std::unique_ptr<segment_v2::IndexedColumnIterator> index_iterator;
    RETURN_IF_ERROR(_pk_index_reader->new_iterator(&index_iterator));
    RETURN_IF_ERROR(index_iterator->seek_at_or_after(&key_without_seq, &exact_match));
    if (!has_seq_col && !exact_match) {
        return Status::NotFound("Can't find key in the segment");
    }
    row_location->row_id = index_iterator->get_current_ordinal();
    row_location->segment_id = _segment_id;

    if (has_seq_col) {
        MemPool pool;
        size_t num_to_read = 1;
        std::unique_ptr<ColumnVectorBatch> cvb;
        RETURN_IF_ERROR(ColumnVectorBatch::create(num_to_read, false, _pk_index_reader->type_info(),
                                                  nullptr, &cvb));
        ColumnBlock block(cvb.get(), &pool);
        ColumnBlockView column_block_view(&block);
        size_t num_read = num_to_read;
        RETURN_IF_ERROR(index_iterator->next_batch(&num_read, &column_block_view));
        DCHECK(num_to_read == num_read);

        const Slice* sought_key = reinterpret_cast<const Slice*>(cvb->cell_ptr(0));
        Slice sought_key_without_seq =
                Slice(sought_key->get_data(), sought_key->get_size() - seq_col_length);

        // compare key
        if (key_without_seq.compare(sought_key_without_seq) != 0) {
            return Status::NotFound("Can't find key in the segment");
        }

        // compare sequence id
        Slice sequence_id =
                Slice(key.get_data() + key_without_seq.get_size() + 1, seq_col_length - 1);
        Slice previous_sequence_id = Slice(
                sought_key->get_data() + sought_key_without_seq.get_size() + 1, seq_col_length - 1);
        if (sequence_id.compare(previous_sequence_id) < 0) {
            return Status::AlreadyExist("key with higher sequence id exists");
        }
    }

    return Status::OK();
}

} // namespace segment_v2
} // namespace doris
