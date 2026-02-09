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

#include "olap/rowset/segment_v2/variant/binary_column_reader.h"

#include <algorithm>
#include <tuple>

#include "olap/rowset/segment_v2/segment.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_variant.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/common/variant_util.h"

namespace doris::segment_v2 {

#include "common/compile_check_begin.h"

Status DummyBinaryColumnReader::new_binary_column_iterator(ColumnIteratorUPtr* iter) const {
    static const TabletColumn binary_column = []() {
        TabletColumn binary_column;
        binary_column.set_name("binary_column");
        binary_column.set_type(FieldType::OLAP_FIELD_TYPE_MAP);
        binary_column.set_default_value("NULL");
        TabletColumn child_tcolumn;
        child_tcolumn.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
        binary_column.add_sub_column(child_tcolumn);
        binary_column.add_sub_column(child_tcolumn);
        binary_column.set_is_nullable(false);
        return binary_column;
    }();
    RETURN_IF_ERROR(Segment::new_default_iterator(binary_column, iter));
    return Status::OK();
}

Status DummyBinaryColumnReader::add_binary_column_reader(std::shared_ptr<ColumnReader> reader,
                                                         uint32_t index) {
    throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                           "DummyBinaryColumnReader does not support add_binary_column_reader");
}

std::pair<std::shared_ptr<ColumnReader>, std::string>
DummyBinaryColumnReader::select_reader_and_cache_key(const std::string& relative_path) const {
    throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                           "DummyBinaryColumnReader does not support select_reader_and_cache_key");
}

std::shared_ptr<ColumnReader> DummyBinaryColumnReader::select_reader(uint32_t index) const {
    throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                           "DummyBinaryColumnReader does not support select_reader");
}

uint32_t DummyBinaryColumnReader::num_buckets() const {
    throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                           "DummyBinaryColumnReader does not support num_buckets");
}

BinaryColumnType DummyBinaryColumnReader::get_type() const {
    return BinaryColumnType::DUMMY;
}

Status SingleSparseColumnReader::add_binary_column_reader(std::shared_ptr<ColumnReader> reader,
                                                          uint32_t /*index*/) {
    if (_single_reader) {
        return Status::AlreadyExist("Single sparse column reader already exists");
    }
    _single_reader = std::move(reader);
    return Status::OK();
}

std::pair<std::shared_ptr<ColumnReader>, std::string>
SingleSparseColumnReader::select_reader_and_cache_key(const std::string& /*relative_path*/) const {
    return {_single_reader, std::string(SPARSE_COLUMN_PATH)};
}

Status SingleSparseColumnReader::new_binary_column_iterator(ColumnIteratorUPtr* iter) const {
    return _single_reader->new_iterator(iter, nullptr);
}

std::shared_ptr<ColumnReader> SingleSparseColumnReader::select_reader(uint32_t /*index*/) const {
    return _single_reader;
}

uint32_t SingleSparseColumnReader::num_buckets() const {
    return 1;
}

BinaryColumnType SingleSparseColumnReader::get_type() const {
    return BinaryColumnType::SINGLE_SPARSE;
}

Status MultipleBinaryColumnReader::new_binary_column_iterator(ColumnIteratorUPtr* iter) const {
    std::vector<std::unique_ptr<ColumnIterator>> iters;
    iters.reserve(_multiple_column_readers.size());
    for (const auto& [index, reader] : _multiple_column_readers) {
        if (!reader) {
            return Status::NotFound("No column reader available, binary column index is: ", index);
        }
        ColumnIteratorUPtr it;
        RETURN_IF_ERROR(reader->new_iterator(&it, nullptr));
        iters.emplace_back(std::move(it));
    }
    *iter = std::make_unique<CombineMultipleBinaryColumnIterator>(std::move(iters));
    return Status::OK();
}

Status MultipleBinaryColumnReader::add_binary_column_reader(std::shared_ptr<ColumnReader> reader,
                                                            uint32_t index) {
    if (_multiple_column_readers.find(index) != _multiple_column_readers.end()) {
        return Status::AlreadyExist(
                "Multiple sparse column reader already exists, binary column index is: ", index);
    }
    _multiple_column_readers.emplace(index, std::move(reader));
    return Status::OK();
}

uint32_t MultipleBinaryColumnReader::num_buckets() const {
    return static_cast<uint32_t>(_multiple_column_readers.size());
}

std::shared_ptr<ColumnReader> MultipleBinaryColumnReader::select_reader(uint32_t index) const {
    auto it = _multiple_column_readers.find(index);
    if (it == _multiple_column_readers.end()) {
        return nullptr;
    }
    std::shared_ptr<ColumnReader> reader = it->second;
    return reader;
}

uint32_t MultipleBinaryColumnReader::pick_index(const std::string& relative_path) const {
    uint32_t N = static_cast<uint32_t>(_multiple_column_readers.size());
    uint32_t bucket_index = vectorized::variant_util::variant_binary_shard_of(
            StringRef {relative_path.data(), relative_path.size()}, N);
    DCHECK(bucket_index < N);
    return bucket_index;
}

std::pair<std::shared_ptr<ColumnReader>, std::string>
MultipleSparseColumnReader::select_reader_and_cache_key(const std::string& relative_path) const {
    uint32_t bucket_index = pick_index(relative_path);
    std::string key = std::string(SPARSE_COLUMN_PATH) + ".b" + std::to_string(bucket_index);
    std::shared_ptr<ColumnReader> reader = select_reader(bucket_index);
    return {std::move(reader), key};
}

BinaryColumnType MultipleSparseColumnReader::get_type() const {
    return BinaryColumnType::MULTIPLE_SPARSE;
}

std::pair<std::shared_ptr<ColumnReader>, std::string>
MultipleDocColumnReader::select_reader_and_cache_key(const std::string& relative_path) const {
    uint32_t bucket_index = pick_index(relative_path);
    std::string key = std::string(DOC_VALUE_COLUMN_PATH) + ".b" + std::to_string(bucket_index);
    std::shared_ptr<ColumnReader> reader = select_reader(bucket_index);
    return {std::move(reader), key};
}

BinaryColumnType MultipleDocColumnReader::get_type() const {
    return BinaryColumnType::MULTIPLE_DOC_VALUE;
}

Status CombineMultipleBinaryColumnIterator::init(const ColumnIteratorOptions& opts) {
    for (auto& it : _iters) {
        RETURN_IF_ERROR(it->init(opts));
    }
    return Status::OK();
}

Status CombineMultipleBinaryColumnIterator::seek_to_ordinal(ordinal_t ord_idx) {
    for (auto& it : _iters) {
        RETURN_IF_ERROR(it->seek_to_ordinal(ord_idx));
    }
    return Status::OK();
}

Status CombineMultipleBinaryColumnIterator::next_batch(size_t* n, vectorized::MutableColumnPtr& dst,
                                                       bool* has_null) {
    // Read each bucket into temp maps.
    _binary_column_data.clear();
    _binary_column_data.reserve(_iters.size());
    for (auto& it : _iters) {
        vectorized::MutableColumnPtr m = vectorized::ColumnVariant::create_binary_column_fn();
        RETURN_IF_ERROR(it->next_batch(n, m, has_null));
        _binary_column_data.emplace_back(std::move(m));
    }
    _collect_sparse_data_from_buckets(*dst);
    return Status::OK();
}

Status CombineMultipleBinaryColumnIterator::read_by_rowids(const rowid_t* rowids,
                                                           const size_t count,
                                                           vectorized::MutableColumnPtr& dst) {
    _binary_column_data.clear();
    _binary_column_data.reserve(_iters.size());
    for (auto& it : _iters) {
        vectorized::MutableColumnPtr m = vectorized::ColumnVariant::create_binary_column_fn();
        RETURN_IF_ERROR(it->read_by_rowids(rowids, count, m));
        _binary_column_data.emplace_back(std::move(m));
    }
    _collect_sparse_data_from_buckets(*dst);
    return Status::OK();
}

ordinal_t CombineMultipleBinaryColumnIterator::get_current_ordinal() const {
    return _iters.empty() ? 0 : _iters.front()->get_current_ordinal();
}

void CombineMultipleBinaryColumnIterator::_collect_sparse_data_from_buckets(
        vectorized::IColumn& binary_data_column) {
    using namespace vectorized;

    // Get path, value, offset from all buckets.
    auto& column_map = assert_cast<ColumnMap&>(binary_data_column);
    auto& dst_paths = assert_cast<ColumnString&>(column_map.get_keys());
    auto& dst_values = assert_cast<ColumnString&>(column_map.get_values());
    auto& dst_offsets = assert_cast<ColumnArray::Offsets64&>(column_map.get_offsets());

    std::vector<const ColumnString*> src_paths(_binary_column_data.size());
    std::vector<const ColumnString*> src_values(_binary_column_data.size());
    std::vector<const ColumnArray::Offsets64*> src_offsets(_binary_column_data.size());
    for (size_t i = 0; i != _binary_column_data.size(); ++i) {
        const auto& src_map = assert_cast<const ColumnMap&>(*_binary_column_data[i]);
        src_paths[i] = assert_cast<const ColumnString*>(&src_map.get_keys());
        src_values[i] = assert_cast<const ColumnString*>(&src_map.get_values());
        src_offsets[i] = assert_cast<const ColumnArray::Offsets64*>(&src_map.get_offsets());
    }

    size_t num_rows = _binary_column_data[0]->size();
    for (size_t i = 0; i != num_rows; ++i) {
        // Sparse data contains paths in sorted order in each row.
        // Collect all paths from all buckets in this row and sort them.
        // Save each path bucket and index to be able find corresponding value later.
        std::vector<std::tuple<std::string_view, size_t, size_t>> all_paths;
        for (size_t bucket = 0; bucket != _binary_column_data.size(); ++bucket) {
            size_t offset_start = (*src_offsets[bucket])[ssize_t(i) - 1];
            size_t offset_end = (*src_offsets[bucket])[ssize_t(i)];

            // collect all paths.
            for (size_t j = offset_start; j != offset_end; ++j) {
                auto path = src_paths[bucket]->get_data_at(j).to_string_view();
                all_paths.emplace_back(path, bucket, j);
            }
        }

        std::sort(all_paths.begin(), all_paths.end());
        for (const auto& [path, bucket, offset] : all_paths) {
            dst_paths.insert_data(path.data(), path.size());
            dst_values.insert_from(*src_values[bucket], offset);
        }

        dst_offsets.push_back(dst_paths.size());
    }
}

#include "common/compile_check_end.h"
} // namespace doris::segment_v2