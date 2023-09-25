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

#include "olap/rowset/segment_v2/hierarchical_data_reader.h"

#include "common/status.h"
#include "io/io_common.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "vec/columns/column.h"
#include "vec/columns/column_object.h"
#include "vec/common/assert_cast.h"
#include "vec/common/schema_util.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/json/path_in_data.h"

namespace doris {
namespace segment_v2 {

Status HierarchicalDataReader::init(const ColumnIteratorOptions& opts) {
    RETURN_IF_ERROR(tranverse([&](SubstreamReaderTree::Node& node) {
        RETURN_IF_ERROR(node.data.iterator->init(opts));
        node.data.inited = true;
        return Status::OK();
    }));
    if (_root_reader && !_root_reader->inited) {
        RETURN_IF_ERROR(_root_reader->iterator->init(opts));
        _root_reader->inited = true;
    }
    return Status::OK();
}

Status HierarchicalDataReader::seek_to_first() {
    LOG(FATAL) << "Not implemented";
}

Status HierarchicalDataReader::seek_to_ordinal(ordinal_t ord) {
    RETURN_IF_ERROR(tranverse([&](SubstreamReaderTree::Node& node) {
        RETURN_IF_ERROR(node.data.iterator->seek_to_ordinal(ord));
        return Status::OK();
    }));
    if (_root_reader) {
        DCHECK(_root_reader->inited);
        RETURN_IF_ERROR(_root_reader->iterator->seek_to_ordinal(ord));
    }
    return Status::OK();
}

Status HierarchicalDataReader::next_batch(size_t* n, vectorized::MutableColumnPtr& dst,
                                          bool* has_null) {
    return process_read(
            [&](StreamReader& reader, const vectorized::PathInData& path,
                const vectorized::DataTypePtr& type) {
                CHECK(reader.inited);
                RETURN_IF_ERROR(reader.iterator->next_batch(n, reader.column, has_null));
                VLOG_DEBUG << fmt::format("{} next_batch {} rows, type={}", path.get_path(), *n,
                                          type->get_name());
                reader.rows_read += *n;
                return Status::OK();
            },
            dst, *n);
}

Status HierarchicalDataReader::read_by_rowids(const rowid_t* rowids, const size_t count,
                                              vectorized::MutableColumnPtr& dst) {
    return process_read(
            [&](StreamReader& reader, const vectorized::PathInData& path,
                const vectorized::DataTypePtr& type) {
                CHECK(reader.inited);
                RETURN_IF_ERROR(reader.iterator->read_by_rowids(rowids, count, reader.column));
                VLOG_DEBUG << fmt::format("{} read_by_rowids {} rows, type={}", path.get_path(),
                                          count, type->get_name());
                reader.rows_read += count;
                return Status::OK();
            },
            dst, count);
}

Status HierarchicalDataReader::add_stream(const SubcolumnColumnReaders::Node* node) {
    if (_substream_reader.find_leaf(node->path)) {
        VLOG_DEBUG << "Already exist sub column " << node->path.get_path();
        return Status::OK();
    }
    CHECK(node);
    ColumnIterator* it;
    RETURN_IF_ERROR(node->data.reader->new_iterator(&it));
    std::unique_ptr<ColumnIterator> it_ptr;
    it_ptr.reset(it);
    StreamReader reader(node->data.file_column_type->create_column(), std::move(it_ptr),
                        node->data.file_column_type);
    bool added = _substream_reader.add(node->path, std::move(reader));
    if (!added) {
        return Status::InternalError("Failed to add node path {}", node->path.get_path());
    }
    VLOG_DEBUG << fmt::format("Add substream {} for {}", node->path.get_path(),
                              _col.path_info().get_path());
    return Status::OK();
}

ordinal_t HierarchicalDataReader::get_current_ordinal() const {
    return (*_substream_reader.begin())->data.iterator->get_current_ordinal();
}

Status ExtractReader::init(const ColumnIteratorOptions& opts) {
    if (!_root_reader->inited) {
        RETURN_IF_ERROR(_root_reader->iterator->init(opts));
        _root_reader->inited = true;
    }
    return Status::OK();
}

Status ExtractReader::seek_to_first() {
    LOG(FATAL) << "Not implemented";
}

Status ExtractReader::seek_to_ordinal(ordinal_t ord) {
    CHECK(_root_reader->inited);
    return _root_reader->iterator->seek_to_ordinal(ord);
}

Status ExtractReader::extract_to(vectorized::MutableColumnPtr& dst, size_t nrows) {
    DCHECK(_root_reader);
    DCHECK(_root_reader->inited);
    vectorized::MutableColumnPtr extracted_column;
    const auto& root = assert_cast<const vectorized::ColumnObject&>(*_root_reader->column);
    // extract root value with path, we can't modify the original root column
    // since some other column may depend on it.
    RETURN_IF_ERROR(root.extract_root( // trim the root name, eg. v.a.b -> a.b
            _col.path_info().pop_front(), extracted_column));
    if (dst->is_variant()) {
        auto& dst_var = assert_cast<vectorized::ColumnObject&>(*dst);
        if (dst_var.empty() || dst_var.is_null_root()) {
            dst_var.create_root(root.get_root_type(), std::move(extracted_column));
        } else {
            vectorized::ColumnPtr cast_column;
            const auto& expected_type = dst_var.get_root_type();
            RETURN_IF_ERROR(vectorized::schema_util::cast_column(
                    {extracted_column->get_ptr(),
                     vectorized::make_nullable(
                             std::make_shared<vectorized::ColumnObject::MostCommonType>()),
                     ""},
                    expected_type, &cast_column));
            dst_var.get_root()->insert_range_from(*cast_column, 0, nrows);
            dst_var.set_num_rows(dst_var.get_root()->size());
        }
    } else {
        CHECK(false) << "Not implemented extract to type " << dst->get_name();
    }
#ifndef NDEBUG
    assert_cast<vectorized::ColumnObject&>(*dst).check_consistency();
#endif
    return Status::OK();
}

Status ExtractReader::next_batch(size_t* n, vectorized::MutableColumnPtr& dst, bool* has_null) {
    static_cast<vectorized::ColumnObject*>(_root_reader->column.get())->clear_subcolumns_data();
    RETURN_IF_ERROR(_root_reader->iterator->next_batch(n, _root_reader->column));
    RETURN_IF_ERROR(extract_to(dst, *n));
    return Status::OK();
}

Status ExtractReader::read_by_rowids(const rowid_t* rowids, const size_t count,
                                     vectorized::MutableColumnPtr& dst) {
    static_cast<vectorized::ColumnObject*>(_root_reader->column.get())->clear_subcolumns_data();
    RETURN_IF_ERROR(_root_reader->iterator->read_by_rowids(rowids, count, _root_reader->column));
    RETURN_IF_ERROR(extract_to(dst, count));
    return Status::OK();
}

ordinal_t ExtractReader::get_current_ordinal() const {
    return _root_reader->iterator->get_current_ordinal();
}

} // namespace segment_v2
} // namespace doris
