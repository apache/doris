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
#include "olap/olap_common.h"
#include "olap/primary_key_index.h"
#include "olap/rowset/rowset_reader_context.h"
#include "olap/rowset/segment_v2/empty_segment_iterator.h"
#include "olap/rowset/segment_v2/hierarchical_data_reader.h"
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
#include "runtime/define_primitive_type.h"
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
#include "vec/core/field.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_object.h"
#include "vec/json/path_in_data.h"
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
        // TODO handle var path
        int32_t uid = read_options.tablet_schema->column(column_id).unique_id();
        if (_column_readers.count(uid) < 1 || !_column_readers.at(uid)->has_zone_map()) {
            continue;
        }
        if (read_options.col_id_to_predicates.count(column_id) > 0 &&
            can_apply_predicate_safely(column_id,
                                       read_options.col_id_to_predicates.at(column_id).get(),
                                       *schema, read_options.io_ctx.reader_type) &&
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
            // TODO handle var path
            int32_t uid =
                    read_options.tablet_schema->column(runtime_predicate->column_id()).unique_id();
            AndBlockColumnPredicate and_predicate;
            auto single_predicate = new SingleColumnBlockPredicate(runtime_predicate.get());
            and_predicate.add_column_predicate(single_predicate);
            if (_column_readers.count(uid) >= 1 &&
                can_apply_predicate_safely(runtime_predicate->column_id(), runtime_predicate.get(),
                                           *schema, read_options.io_ctx.reader_type) &&
                !_column_readers.at(uid)->match_condition(&and_predicate)) {
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
            const auto uid = it.first;
            const auto column_id = read_options.tablet_schema->field_index(uid);
            if (it.second->prune_predicates_by_zone_map(pruned_predicates, column_id)) {
                pruned = true;
            }
        }

        if (pruned) {
            auto options_with_pruned_predicates = read_options;
            options_with_pruned_predicates.column_predicates = pruned_predicates;
            //because column_predicates is changed, we need to rebuild col_id_to_predicates so that inverted index will not go through it.
            options_with_pruned_predicates.col_id_to_predicates.clear();
            for (auto* pred : options_with_pruned_predicates.column_predicates) {
                if (!options_with_pruned_predicates.col_id_to_predicates.contains(
                            pred->column_id())) {
                    options_with_pruned_predicates.col_id_to_predicates.insert(
                            {pred->column_id(), std::make_shared<AndBlockColumnPredicate>()});
                }
                auto* single_column_block_predicate = new SingleColumnBlockPredicate(pred);
                options_with_pruned_predicates.col_id_to_predicates[pred->column_id()]
                        ->add_column_predicate(single_column_block_predicate);
            }
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

// Return the storage datatype of related column to field.
// Return nullptr meaning no such storage infomation for this column
vectorized::DataTypePtr Segment::get_data_type_of(vectorized::PathInData path, bool is_nullable,
                                                  bool ignore_children) const {
    // Path has higher priority
    if (!path.empty()) {
        auto node = _sub_column_tree.find_leaf(path);
        if (node) {
            if (ignore_children || node->children.empty()) {
                return node->data.file_column_type;
            }
        }
        // it contains children or column missing in storage, so treat it as variant
        return is_nullable
                       ? vectorized::make_nullable(std::make_shared<vectorized::DataTypeObject>())
                       : std::make_shared<vectorized::DataTypeObject>();
    }
    // TODO support normal column type
    return nullptr;
}
Status Segment::_create_column_readers(const SegmentFooterPB& footer) {
    std::unordered_map<uint32_t, uint32_t> column_id_to_footer_ordinal;
    std::unordered_map<vectorized::PathInData, uint32_t, vectorized::PathInData::Hash>
            column_path_to_footer_ordinal;
    for (uint32_t ordinal = 0; ordinal < footer.columns().size(); ++ordinal) {
        auto& column_pb = footer.columns(ordinal);
        // column path for accessing subcolumns of variant
        if (column_pb.has_column_path_info()) {
            vectorized::PathInData path;
            path.from_protobuf(column_pb.column_path_info());
            column_path_to_footer_ordinal.emplace(path, ordinal);
        }
        // unique_id is unsigned, -1 meaning no unique id(e.g. an extracted column from variant)
        if (static_cast<int>(column_pb.unique_id()) >= 0) {
            // unique id
            column_id_to_footer_ordinal.emplace(column_pb.unique_id(), ordinal);
        }
    }
    // init by unique_id
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

    // init by column path
    for (uint32_t ordinal = 0; ordinal < _tablet_schema->num_columns(); ++ordinal) {
        auto& column = _tablet_schema->column(ordinal);
        auto iter = column_path_to_footer_ordinal.find(column.path_info());
        if (iter == column_path_to_footer_ordinal.end()) {
            continue;
        }
        ColumnReaderOptions opts;
        opts.kept_in_memory = _tablet_schema->is_in_memory();
        std::unique_ptr<ColumnReader> reader;
        RETURN_IF_ERROR(ColumnReader::create(opts, footer.columns(iter->second), footer.num_rows(),
                                             _file_reader, &reader));
        _sub_column_tree.add(
                iter->first,
                SubcolumnReader {std::move(reader),
                                 vectorized::DataTypeFactory::instance().create_data_type(
                                         footer.columns(iter->second))});
    }
    return Status::OK();
}

static Status new_default_iterator(const TabletColumn& tablet_column,
                                   std::unique_ptr<ColumnIterator>* iter) {
    if (!tablet_column.has_default_value() && !tablet_column.is_nullable()) {
        return Status::InternalError("invalid nonexistent column without default value.");
    }
    auto type_info = get_type_info(&tablet_column);
    std::unique_ptr<DefaultValueColumnIterator> default_value_iter(new DefaultValueColumnIterator(
            tablet_column.has_default_value(), tablet_column.default_value(),
            tablet_column.is_nullable(), std::move(type_info), tablet_column.precision(),
            tablet_column.frac()));
    ColumnIteratorOptions iter_opts;

    RETURN_IF_ERROR(default_value_iter->init(iter_opts));
    *iter = std::move(default_value_iter);
    return Status::OK();
}

Status Segment::new_column_iterator_with_path(const TabletColumn& tablet_column,
                                              std::unique_ptr<ColumnIterator>* iter,
                                              const StorageReadOptions* opt) {
    vectorized::PathInData root_path;
    if (tablet_column.path_info().empty()) {
        // Missing path info, but need read the whole variant column
        root_path = vectorized::PathInData(tablet_column.name_lower_case());
    } else {
        root_path = vectorized::PathInData({tablet_column.path_info().get_parts()[0]});
    }
    auto root = _sub_column_tree.find_leaf(root_path);
    auto node = _sub_column_tree.find_exact(tablet_column.path_info());
    if (opt != nullptr && opt->io_ctx.reader_type == ReaderType::READER_ALTER_TABLE) {
        CHECK(tablet_column.is_variant_type());
        if (node == nullptr) {
            // No such variant column in this segment, get a default one
            RETURN_IF_ERROR(new_default_iterator(tablet_column, iter));
            return Status::OK();
        }
        bool output_as_raw_json = true;
        // Alter table operation should read the whole variant column, since it does not aware of
        // subcolumns of variant during processing rewriting rowsets.
        // This is slow, since it needs to read all sub columns and merge them into a single column
        RETURN_IF_ERROR(HierarchicalDataReader::create(iter, tablet_column.path_info(), node, root,
                                                       output_as_raw_json));
        return Status::OK();
    }

    if (opt == nullptr || opt->io_ctx.reader_type != ReaderType::READER_QUERY) {
        // Could be compaction ..etc and read flat leaves nodes data
        auto node = _sub_column_tree.find_leaf(tablet_column.path_info());
        if (!node) {
            RETURN_IF_ERROR(new_default_iterator(tablet_column, iter));
            return Status::OK();
        }
        ColumnIterator* it;
        RETURN_IF_ERROR(node->data.reader->new_iterator(&it));
        iter->reset(it);
        return Status::OK();
    }

    // Init iterators with extra path info.
    // TODO If this segment does not contain any data correspond to the relatate path,
    // then we could optimize to generate a default iterator
    // This file doest not contain this column, so only read from sparse column
    // to avoid read amplification
    if (node != nullptr && node->is_scalar() && node->children.empty()) {
        // Direct read extracted columns
        const auto* node = _sub_column_tree.find_leaf(tablet_column.path_info());
        ColumnIterator* it;
        RETURN_IF_ERROR(node->data.reader->new_iterator(&it));
        iter->reset(it);
    } else if (node != nullptr && !node->children.empty()) {
        // Create reader with hirachical data
        RETURN_IF_ERROR(
                HierarchicalDataReader::create(iter, tablet_column.path_info(), node, root));
    } else {
        // If file only exist column `v.a` and `v` but target path is `v.b`, read only read and parse root column
        if (root == nullptr) {
            // No such variant column in this segment, get a default one
            RETURN_IF_ERROR(new_default_iterator(tablet_column, iter));
            return Status::OK();
        }
        ColumnIterator* it;
        RETURN_IF_ERROR(root->data.reader->new_iterator(&it));
        auto stream_iter = new ExtractReader(
                tablet_column,
                std::make_unique<StreamReader>(root->data.file_column_type->create_column(),
                                               std::unique_ptr<ColumnIterator>(it),
                                               root->data.file_column_type));
        iter->reset(stream_iter);
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
                                    std::unique_ptr<ColumnIterator>* iter,
                                    const StorageReadOptions* opt) {
    // init column iterator by path info
    if (!tablet_column.path_info().empty() || tablet_column.is_variant_type()) {
        return new_column_iterator_with_path(tablet_column, iter, opt);
    }
    // init default iterator
    if (_column_readers.count(tablet_column.unique_id()) < 1) {
        RETURN_IF_ERROR(new_default_iterator(tablet_column, iter));
        return Status::OK();
    }
    // init iterator by unique id
    ColumnIterator* it;
    RETURN_IF_ERROR(_column_readers.at(tablet_column.unique_id())->new_iterator(&it));
    iter->reset(it);

    if (config::enable_column_type_check &&
        tablet_column.type() != _column_readers.at(tablet_column.unique_id())->get_meta_type()) {
        LOG(WARNING) << "different type between schema and column reader,"
                     << " column schema name: " << tablet_column.name()
                     << " column schema type: " << int(tablet_column.type())
                     << " column reader meta type"
                     << int(_column_readers.at(tablet_column.unique_id())->get_meta_type());
        return Status::InternalError("different type between schema and column reader");
    }
    return Status::OK();
}

Status Segment::new_column_iterator(int32_t unique_id, std::unique_ptr<ColumnIterator>* iter) {
    ColumnIterator* it;
    RETURN_IF_ERROR(_column_readers.at(unique_id)->new_iterator(&it));
    iter->reset(it);
    return Status::OK();
}

ColumnReader* Segment::_get_column_reader(const TabletColumn& col) {
    // init column iterator by path info
    if (!col.path_info().empty() || col.is_variant_type()) {
        auto node = _sub_column_tree.find_exact(col.path_info());
        if (node != nullptr) {
            return node->data.reader.get();
        }
        return nullptr;
    }
    auto col_unique_id = col.unique_id();
    if (_column_readers.count(col_unique_id) > 0) {
        return _column_readers[col_unique_id].get();
    }
    return nullptr;
}

Status Segment::new_bitmap_index_iterator(const TabletColumn& tablet_column,
                                          std::unique_ptr<BitmapIndexIterator>* iter) {
    ColumnReader* reader = _get_column_reader(tablet_column);
    if (reader != nullptr && reader->has_bitmap_index()) {
        BitmapIndexIterator* it;
        RETURN_IF_ERROR(reader->new_bitmap_index_iterator(&it));
        iter->reset(it);
        return Status::OK();
    }
    return Status::OK();
}

Status Segment::new_inverted_index_iterator(const TabletColumn& tablet_column,
                                            const TabletIndex* index_meta,
                                            const StorageReadOptions& read_options,
                                            std::unique_ptr<InvertedIndexIterator>* iter) {
    ColumnReader* reader = _get_column_reader(tablet_column);
    if (reader != nullptr && index_meta) {
        RETURN_IF_ERROR(reader->new_inverted_index_iterator(index_meta, read_options, iter));
        return Status::OK();
    }
    return Status::OK();
}

Status Segment::lookup_row_key(const Slice& key, bool with_seq_col, bool with_rowid,
                               RowLocation* row_location) {
    RETURN_IF_ERROR(load_pk_index_and_bf());
    bool has_seq_col = _tablet_schema->has_sequence_col();
    bool has_rowid = !_tablet_schema->cluster_key_idxes().empty();
    size_t seq_col_length = 0;
    if (has_seq_col) {
        seq_col_length = _tablet_schema->column(_tablet_schema->sequence_col_idx()).length() + 1;
    }
    size_t rowid_length = has_rowid ? PrimaryKeyIndexReader::ROW_ID_LENGTH : 0;

    Slice key_without_seq =
            Slice(key.get_data(), key.get_size() - (with_seq_col ? seq_col_length : 0) -
                                          (with_rowid ? rowid_length : 0));

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
    if (st.is<ErrorCode::ENTRY_NOT_FOUND>() || (!has_seq_col && !has_rowid && !exact_match)) {
        return Status::Error<ErrorCode::KEY_NOT_FOUND>("Can't find key in the segment");
    }
    row_location->row_id = index_iterator->get_current_ordinal();
    row_location->segment_id = _segment_id;
    row_location->rowset_id = _rowset_id;

    size_t num_to_read = 1;
    auto index_type = vectorized::DataTypeFactory::instance().create_data_type(
            _pk_index_reader->type_info()->type(), 1, 0);
    auto index_column = index_type->create_column();
    size_t num_read = num_to_read;
    RETURN_IF_ERROR(index_iterator->next_batch(&num_read, index_column));
    DCHECK(num_to_read == num_read);

    Slice sought_key = Slice(index_column->get_data_at(0).data, index_column->get_data_at(0).size);

    if (has_seq_col) {
        Slice sought_key_without_seq =
                Slice(sought_key.get_data(), sought_key.get_size() - seq_col_length - rowid_length);

        // compare key
        if (key_without_seq.compare(sought_key_without_seq) != 0) {
            return Status::Error<ErrorCode::KEY_NOT_FOUND>("Can't find key in the segment");
        }

        if (with_seq_col) {
            // compare sequence id
            Slice sequence_id =
                    Slice(key.get_data() + key_without_seq.get_size() + 1, seq_col_length - 1);
            Slice previous_sequence_id =
                    Slice(sought_key.get_data() + sought_key_without_seq.get_size() + 1,
                          seq_col_length - 1);
            if (sequence_id.compare(previous_sequence_id) < 0) {
                return Status::Error<ErrorCode::KEY_ALREADY_EXISTS>(
                        "key with higher sequence id exists");
            }
        }
    } else if (has_rowid) {
        Slice sought_key_without_rowid =
                Slice(sought_key.get_data(), sought_key.get_size() - rowid_length);
        // compare key
        if (key_without_seq.compare(sought_key_without_rowid) != 0) {
            return Status::Error<ErrorCode::KEY_NOT_FOUND>("Can't find key in the segment");
        }
    }
    // found the key, use rowid in pk index if necessary.
    if (has_rowid) {
        Slice sought_key_without_seq =
                Slice(sought_key.get_data(), sought_key.get_size() - seq_col_length - rowid_length);
        Slice rowid_slice = Slice(
                sought_key.get_data() + sought_key_without_seq.get_size() + seq_col_length + 1,
                rowid_length - 1);
        const auto* type_info = get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT>();
        auto rowid_coder = get_key_coder(type_info->type());
        RETURN_IF_ERROR(rowid_coder->decode_ascending(&rowid_slice, rowid_length,
                                                      (uint8_t*)&row_location->row_id));
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
    // trim row id
    if (_tablet_schema->cluster_key_idxes().empty()) {
        *key = index_column->get_data_at(0).to_string();
    } else {
        Slice sought_key =
                Slice(index_column->get_data_at(0).data, index_column->get_data_at(0).size);
        Slice sought_key_without_rowid =
                Slice(sought_key.get_data(),
                      sought_key.get_size() - PrimaryKeyIndexReader::ROW_ID_LENGTH);
        *key = sought_key_without_rowid.to_string();
    }
    return Status::OK();
}

bool Segment::same_with_storage_type(int32_t cid, const Schema& schema,
                                     bool ignore_children) const {
    auto file_column_type = get_data_type_of(schema.column(cid)->path(),
                                             schema.column(cid)->is_nullable(), ignore_children);
    auto expected_type = Schema::get_data_type_ptr(*schema.column(cid));
#ifndef NDEBUG
    if (file_column_type && !file_column_type->equals(*expected_type)) {
        VLOG_DEBUG << fmt::format("Get column {}, file column type {}, exepected type {}",
                                  schema.column(cid)->name(), file_column_type->get_name(),
                                  expected_type->get_name());
    }
#endif
    bool same =
            (!file_column_type) || (file_column_type && file_column_type->equals(*expected_type));
    return same;
}

Status Segment::seek_and_read_by_rowid(const TabletSchema& schema, SlotDescriptor* slot,
                                       uint32_t row_id, vectorized::MutableColumnPtr& result,
                                       OlapReaderStatistics& stats,
                                       std::unique_ptr<ColumnIterator>& iterator_hint) {
    StorageReadOptions storage_read_opt;
    storage_read_opt.io_ctx.reader_type = ReaderType::READER_QUERY;
    segment_v2::ColumnIteratorOptions opt {
            .use_page_cache = !config::disable_storage_page_cache,
            .file_reader = file_reader().get(),
            .stats = &stats,
            .io_ctx = io::IOContext {.reader_type = ReaderType::READER_QUERY},
    };
    std::vector<segment_v2::rowid_t> single_row_loc {row_id};
    if (!slot->column_paths().empty()) {
        vectorized::PathInData path(schema.column_by_uid(slot->col_unique_id()).name_lower_case(),
                                    slot->column_paths());
        auto storage_type = get_data_type_of(path, slot->is_nullable(), false);
        vectorized::MutableColumnPtr file_storage_column = storage_type->create_column();
        DCHECK(storage_type != nullptr);
        TabletColumn column = TabletColumn::create_materialized_variant_column(
                schema.column_by_uid(slot->col_unique_id()).name_lower_case(), slot->column_paths(),
                slot->col_unique_id());
        if (iterator_hint == nullptr) {
            RETURN_IF_ERROR(new_column_iterator(column, &iterator_hint, &storage_read_opt));
            RETURN_IF_ERROR(iterator_hint->init(opt));
        }
        RETURN_IF_ERROR(
                iterator_hint->read_by_rowids(single_row_loc.data(), 1, file_storage_column));
        // iterator_hint.reset(nullptr);
        // Get it's inner field, for JSONB case
        vectorized::Field field = remove_nullable(storage_type)->get_default();
        file_storage_column->get(0, field);
        result->insert(field);
    } else {
        int index = (slot->col_unique_id() >= 0) ? schema.field_index(slot->col_unique_id())
                                                 : schema.field_index(slot->col_name());
        if (index < 0) {
            std::stringstream ss;
            ss << "field name is invalid. field=" << slot->col_name()
               << ", field_name_to_index=" << schema.get_all_field_names();
            return Status::InternalError(ss.str());
        }
        storage_read_opt.io_ctx.reader_type = ReaderType::READER_QUERY;
        if (iterator_hint == nullptr) {
            RETURN_IF_ERROR(
                    new_column_iterator(schema.column(index), &iterator_hint, &storage_read_opt));
            RETURN_IF_ERROR(iterator_hint->init(opt));
        }
        RETURN_IF_ERROR(iterator_hint->read_by_rowids(single_row_loc.data(), 1, result));
    }
    return Status::OK();
}

} // namespace segment_v2
} // namespace doris
