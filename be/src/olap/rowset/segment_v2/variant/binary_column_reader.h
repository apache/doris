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

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/status.h"
#include "olap/rowset/segment_v2/column_reader.h"

namespace doris::segment_v2 {

#include "common/compile_check_begin.h"

enum class BinaryColumnType {
    SINGLE_SPARSE = 0,
    MULTIPLE_SPARSE = 1,
    MULTIPLE_DOC_VALUE = 2,
    DUMMY = 3,
};

// Combine multiple bucket binary iterators into one logical iterator.
class CombineMultipleBinaryColumnIterator;

class BinaryColumnReader {
public:
    virtual ~BinaryColumnReader() = default;

    virtual Status new_binary_column_iterator(ColumnIteratorUPtr* iter) const = 0;

    virtual Status add_binary_column_reader(std::shared_ptr<ColumnReader> reader,
                                            uint32_t index) = 0;

    virtual std::pair<std::shared_ptr<ColumnReader>, std::string> select_reader_and_cache_key(
            const std::string& relative_path) const = 0;

    virtual std::shared_ptr<ColumnReader> select_reader(uint32_t index) const = 0;

    virtual uint32_t num_buckets() const = 0;

    virtual BinaryColumnType get_type() const = 0;
};

// Dummy binary column reader for variant column without any binary data.
// for example, old version variant column without any binary data.
class DummyBinaryColumnReader : public BinaryColumnReader {
public:
    Status new_binary_column_iterator(ColumnIteratorUPtr* iter) const override;
    Status add_binary_column_reader(std::shared_ptr<ColumnReader> reader, uint32_t index) override;
    std::pair<std::shared_ptr<ColumnReader>, std::string> select_reader_and_cache_key(
            const std::string& relative_path) const override;
    std::shared_ptr<ColumnReader> select_reader(uint32_t index) const override;
    uint32_t num_buckets() const override;
    BinaryColumnType get_type() const override;
};

class SingleSparseColumnReader : public BinaryColumnReader {
public:
    Status add_binary_column_reader(std::shared_ptr<ColumnReader> reader, uint32_t index) override;

    std::pair<std::shared_ptr<ColumnReader>, std::string> select_reader_and_cache_key(
            const std::string& relative_path) const override;

    Status new_binary_column_iterator(ColumnIteratorUPtr* iter) const override;

    std::shared_ptr<ColumnReader> select_reader(uint32_t index) const override;

    uint32_t num_buckets() const override;

    BinaryColumnType get_type() const override;

private:
    std::shared_ptr<ColumnReader> _single_reader;
};

class MultipleBinaryColumnReader : public BinaryColumnReader {
public:
    Status new_binary_column_iterator(ColumnIteratorUPtr* iter) const override;

    Status add_binary_column_reader(std::shared_ptr<ColumnReader> reader, uint32_t index) override;

    uint32_t num_buckets() const override;

    std::shared_ptr<ColumnReader> select_reader(uint32_t index) const override;

protected:
    uint32_t pick_index(const std::string& relative_path) const;
    std::unordered_map<uint32_t, std::shared_ptr<ColumnReader>> _multiple_column_readers;
};

class MultipleSparseColumnReader : public MultipleBinaryColumnReader {
public:
    std::pair<std::shared_ptr<ColumnReader>, std::string> select_reader_and_cache_key(
            const std::string& relative_path) const override;

    BinaryColumnType get_type() const override;
};

class MultipleDocColumnReader : public MultipleBinaryColumnReader {
public:
    std::pair<std::shared_ptr<ColumnReader>, std::string> select_reader_and_cache_key(
            const std::string& relative_path) const override;

    BinaryColumnType get_type() const override;
};

// Combine multiple bucket sparse iterators into one logical sparse iterator by row-wise merging.
class CombineMultipleBinaryColumnIterator : public ColumnIterator {
public:
    explicit CombineMultipleBinaryColumnIterator(
            std::vector<std::unique_ptr<ColumnIterator>>&& iters)
            : _iters(std::move(iters)) {}

    Status init(const ColumnIteratorOptions& opts) override;

    Status seek_to_ordinal(ordinal_t ord_idx) override;

    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst, bool* has_null) override;

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          vectorized::MutableColumnPtr& dst) override;

    ordinal_t get_current_ordinal() const override;

private:
    void _collect_sparse_data_from_buckets(vectorized::IColumn& binary_data_column);

    std::vector<std::unique_ptr<ColumnIterator>> _iters;
    std::vector<vectorized::MutableColumnPtr> _binary_column_data;
};

#include "common/compile_check_end.h"
} // namespace doris::segment_v2