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

#include "storage/segment/variant/hierarchical_data_iterator.h"

#include <memory>
#include <span>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/define_primitive_type.h"
#include "io/io_common.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/column_reader_cache.h"
#include "storage/segment/variant/nested_group_path.h"
#include "storage/segment/variant/v2/variant_assembler.h"
#include "storage/segment/variant/v2/variant_column_reader.h"
#include "util/json/path_in_data.h"

namespace doris::segment_v2 {

HierarchicalDataIterator::HierarchicalDataIterator(const PathInData& path) : _path(path) {}

HierarchicalDataIterator::~HierarchicalDataIterator() = default;

Status HierarchicalDataIterator::create(ColumnIteratorUPtr* reader, int32_t col_uid,
                                        PathInData path, const SubcolumnColumnMetaInfo::Node* node,
                                        std::unique_ptr<SubstreamIterator>&& binary_column_reader,
                                        std::unique_ptr<SubstreamIterator>&& root_column_reader,
                                        ColumnReaderCache* column_reader_cache,
                                        OlapReaderStatistics* stats, ReadType read_type,
                                        bool use_variant_v2, const io::IOContext* io_ctx) {
    DORIS_CHECK(use_variant_v2);
    // None leave node need merge with root
    std::unique_ptr<HierarchicalDataIterator> stream_iter(new HierarchicalDataIterator(path));
    if (node != nullptr && read_type == ReadType::SUBCOLUMNS_AND_SPARSE) {
        std::vector<const SubcolumnColumnMetaInfo::Node*> leaves;
        PathsInData leaves_paths;
        SubcolumnColumnMetaInfo::get_leaves_of_node(node, leaves, leaves_paths);
        for (size_t i = 0; i < leaves_paths.size(); ++i) {
            if (leaves_paths[i].empty()) {
                // use set_root to share instead
                continue;
            }
            // Skip NestedGroup subcolumns (columns with ___DOR_ng___. prefix in path).
            // NestedGroup columns only contain rows that have the nested array, not all rows.
            // They need special handling via NestedGroupWholeIterator, not regular hierarchical merge.
            const auto& leaf_path = leaves_paths[i].get_path();
            if (contains_nested_group_marker(leaf_path)) {
                VLOG_DEBUG << "Skipping NestedGroup subcolumn: " << leaf_path;
                continue;
            }
            RETURN_IF_ERROR(stream_iter->add_stream(col_uid, leaves[i], column_reader_cache, stats,
                                                    io_ctx));
        }
    }
    // need read from root column if not null
    stream_iter->_root_reader = std::move(root_column_reader);
    // need read from sparse column if not null
    stream_iter->_binary_column_reader = std::move(binary_column_reader);
    stream_iter->_stats = stats;

    variant_v2::VariantAssemblerOptions assembler_options;
    assembler_options.requested_path = path;
    if (stream_iter->_binary_column_reader) {
        assembler_options.storage_map_kind = read_type == ReadType::SUBCOLUMNS_AND_SPARSE
                                                     ? variant_v2::StorageMapKind::SPARSE
                                                     : variant_v2::StorageMapKind::DOC;
    }
    assembler_options.has_root = stream_iter->_root_reader != nullptr;
    RETURN_IF_ERROR(stream_iter->tranverse([&](SubstreamReaderTree::Node& stream) {
        assembler_options.materialized_paths.push_back(
                {.path = stream.path, .type = stream.data.type});
        return Status::OK();
    }));
    stream_iter->_variant_v2_assembler =
            DORIS_TRY(variant_v2::VariantAssembler::create(std::move(assembler_options)));
    *reader = std::move(stream_iter);

    return Status::OK();
}

Status HierarchicalDataIterator::init(const ColumnIteratorOptions& opts) {
    RETURN_IF_ERROR(tranverse([&](SubstreamReaderTree::Node& node) {
        RETURN_IF_ERROR(node.data.iterator->init(opts));
        node.data.inited = true;
        return Status::OK();
    }));
    if (_root_reader && !_root_reader->inited) {
        RETURN_IF_ERROR(_root_reader->iterator->init(opts));
        _root_reader->inited = true;
    }
    if (_binary_column_reader && !_binary_column_reader->inited) {
        RETURN_IF_ERROR(_binary_column_reader->iterator->init(opts));
        _binary_column_reader->inited = true;
    }
    return Status::OK();
}

Status HierarchicalDataIterator::seek_to_ordinal(ordinal_t ord) {
    RETURN_IF_ERROR(tranverse([&](SubstreamReaderTree::Node& node) {
        RETURN_IF_ERROR(node.data.iterator->seek_to_ordinal(ord));
        return Status::OK();
    }));
    if (_root_reader) {
        DCHECK(_root_reader->inited);
        RETURN_IF_ERROR(_root_reader->iterator->seek_to_ordinal(ord));
    }
    if (_binary_column_reader) {
        DCHECK(_binary_column_reader->inited);
        RETURN_IF_ERROR(_binary_column_reader->iterator->seek_to_ordinal(ord));
    }
    return Status::OK();
}

Status HierarchicalDataIterator::next_batch(size_t* n, MutableColumnPtr& dst, bool* has_null) {
    const size_t requested_rows = *n;
    size_t actual_rows = 0;
    RETURN_IF_ERROR(process_read(
            [&](SubstreamIterator& reader, const PathInData& path, const DataTypePtr& type,
                bool* stream_has_null) {
                CHECK(reader.inited);
                size_t stream_rows = requested_rows;
                RETURN_IF_ERROR(
                        reader.iterator->next_batch(&stream_rows, reader.column, stream_has_null));
                if (stream_rows != reader.column->size()) {
                    return Status::Corruption("Variant stream {} reported {} rows but produced {}",
                                              path.get_path(), stream_rows, reader.column->size());
                }
                VLOG_DEBUG << fmt::format("{} next_batch {} rows, type={}", path.get_path(),
                                          stream_rows, type ? type->get_name() : "null");
                return Status::OK();
            },
            dst, requested_rows, true, &actual_rows, has_null));
    *n = actual_rows;
    return Status::OK();
}

Status HierarchicalDataIterator::read_by_rowids(const rowid_t* rowids, const size_t count,
                                                MutableColumnPtr& dst) {
    size_t actual_rows = 0;
    return process_read(
            [&](SubstreamIterator& reader, const PathInData& path, const DataTypePtr& type,
                bool* /*stream_has_null*/) {
                CHECK(reader.inited);
                RETURN_IF_ERROR(reader.iterator->read_by_rowids(rowids, count, reader.column));
                VLOG_DEBUG << fmt::format("{} read_by_rowids {} rows, type={}", path.get_path(),
                                          count, type ? type->get_name() : "null");
                return Status::OK();
            },
            dst, count, false, &actual_rows, nullptr);
}

Status HierarchicalDataIterator::_assemble_variant_v2(MutableColumnPtr& dst, size_t nrows,
                                                      bool* has_null) {
    DORIS_CHECK(_variant_v2_assembler != nullptr);
    DorisVector<const IColumn*> materialized;
    materialized.reserve(_substream_reader.size());
    for (const auto& entry : _substream_reader) {
        materialized.push_back(entry->data.column.get());
    }

    const ColumnMap* storage_map = nullptr;
    if (_binary_column_reader) {
        storage_map = check_and_get_column<ColumnMap>(_binary_column_reader->column.get());
        if (storage_map == nullptr) {
            return Status::Corruption("Variant V2 binary stream is not Map<String,String>");
        }
    }

    variant_v2::VariantAssemblerBatchView batch;
    batch.num_rows = nrows;
    batch.root_jsonb = _root_reader ? _root_reader->column.get() : nullptr;
    batch.materialized_columns = materialized;
    batch.storage_map = storage_map;
    ColumnNullable::MutablePtr assembled;
    RETURN_IF_ERROR(_variant_v2_assembler->assemble(batch, &assembled));
    if (has_null != nullptr) {
        *has_null = assembled->has_null();
    }
    return variant_v2::append_assembled_variant(dst, std::move(assembled));
}

void HierarchicalDataIterator::_clear_read_columns() {
    for (const auto& entry : _substream_reader) {
        entry->data.column->clear();
    }
    if (_binary_column_reader) {
        _binary_column_reader->column->clear();
    }
    if (_root_reader) {
        _root_reader->column->clear();
    }
}

Status HierarchicalDataIterator::add_stream(int32_t col_uid,
                                            const SubcolumnColumnMetaInfo::Node* node,
                                            ColumnReaderCache* column_reader_cache,
                                            OlapReaderStatistics* stats,
                                            const io::IOContext* io_ctx) {
    if (_substream_reader.find_leaf(node->path)) {
        VLOG_DEBUG << "Already exist sub column " << node->path.get_path();
        return Status::OK();
    }
    CHECK(node);
    ColumnIteratorUPtr it;
    std::shared_ptr<ColumnReader> column_reader;
    RETURN_IF_ERROR(column_reader_cache->get_path_column_reader(col_uid, node->path, &column_reader,
                                                                stats, node, io_ctx));
    RETURN_IF_ERROR(column_reader->new_iterator(&it, nullptr));
    SubstreamIterator reader(node->data.file_column_type->create_column(), std::move(it),
                             node->data.file_column_type);
    bool added = _substream_reader.add(node->path, std::move(reader));
    if (!added) {
        return Status::InternalError("Failed to add node path {}", node->path.get_path());
    }
    VLOG_DEBUG << fmt::format("Add substream {} for {}", node->path.get_path(), _path.get_path());
    return Status::OK();
}

ordinal_t HierarchicalDataIterator::get_current_ordinal() const {
    if (_substream_reader.begin() != _substream_reader.end()) {
        return (*_substream_reader.begin())->data.iterator->get_current_ordinal();
    }
    if (_root_reader) {
        return _root_reader->iterator->get_current_ordinal();
    }
    DORIS_CHECK(_binary_column_reader != nullptr);
    return _binary_column_reader->iterator->get_current_ordinal();
}

Status HierarchicalDataIterator::init_prefetcher(const SegmentPrefetchParams& params) {
    RETURN_IF_ERROR(tranverse([&](SubstreamReaderTree::Node& node) {
        RETURN_IF_ERROR(node.data.iterator->init_prefetcher(params));
        return Status::OK();
    }));
    if (_root_reader) {
        DCHECK(_root_reader->inited);
        RETURN_IF_ERROR(_root_reader->iterator->init_prefetcher(params));
    }
    if (_binary_column_reader) {
        DCHECK(_binary_column_reader->inited);
        RETURN_IF_ERROR(_binary_column_reader->iterator->init_prefetcher(params));
    }
    return Status::OK();
}

void HierarchicalDataIterator::collect_prefetchers(
        std::map<PrefetcherInitMethod, std::vector<SegmentPrefetcher*>>& prefetchers,
        PrefetcherInitMethod init_method) {
    static_cast<void>(tranverse([&](SubstreamReaderTree::Node& node) {
        node.data.iterator->collect_prefetchers(prefetchers, init_method);
        return Status::OK();
    }));
    if (_root_reader) {
        DCHECK(_root_reader->inited);
        _root_reader->iterator->collect_prefetchers(prefetchers, init_method);
    }
    if (_binary_column_reader) {
        DCHECK(_binary_column_reader->inited);
        _binary_column_reader->iterator->collect_prefetchers(prefetchers, init_method);
    }
}

} // namespace doris::segment_v2
