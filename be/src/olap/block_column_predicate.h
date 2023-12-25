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

#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <ostream>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "olap/column_predicate.h"
#include "olap/olap_common.h"
#include "vec/columns/column.h"

namespace roaring {
class Roaring;
} // namespace roaring

namespace doris {
class WrapperField;

namespace segment_v2 {
class BloomFilter;
class InvertedIndexIterator;
} // namespace segment_v2

// Block Column Predicate support do column predicate and support OR and AND predicate
// Block Column Predicate will replace column predicate as a unified external vectorized interface
// in the future
// TODO: support do predicate on Bitmap and ZoneMap, So we can use index of column to do predicate on
// page and segment
class BlockColumnPredicate {
public:
    BlockColumnPredicate() = default;
    virtual ~BlockColumnPredicate() = default;

    virtual void get_all_column_ids(std::set<ColumnId>& column_id_set) const = 0;

    virtual void get_all_column_predicate(
            std::set<const ColumnPredicate*>& predicate_set) const = 0;

    virtual uint16_t evaluate(vectorized::MutableColumns& block, uint16_t* sel,
                              uint16_t selected_size) const {
        return selected_size;
    }
    virtual void evaluate_and(vectorized::MutableColumns& block, uint16_t* sel,
                              uint16_t selected_size, bool* flags) const {}
    virtual void evaluate_or(vectorized::MutableColumns& block, uint16_t* sel,
                             uint16_t selected_size, bool* flags) const {}

    virtual void evaluate_vec(vectorized::MutableColumns& block, uint16_t size, bool* flags) const {
    }

    virtual bool can_do_apply_safely(PrimitiveType input_type, bool is_null) const {
        LOG(FATAL) << "should not reach here";
        return true;
    }

    virtual bool support_zonemap() const { return true; }

    virtual bool evaluate_and(const std::pair<WrapperField*, WrapperField*>& statistic) const {
        LOG(FATAL) << "should not reach here";
        return true;
    }

    virtual bool evaluate_and(const segment_v2::BloomFilter* bf) const {
        LOG(FATAL) << "should not reach here";
        return true;
    }

    virtual bool evaluate_and(const StringRef* dict_words, const size_t dict_num) const {
        LOG(FATAL) << "should not reach here";
        return true;
    }

    virtual bool can_do_bloom_filter(bool ngram) const { return false; }

    //evaluate predicate on inverted
    virtual Status evaluate(const std::string& column_name, InvertedIndexIterator* iterator,
                            uint32_t num_rows, roaring::Roaring* bitmap) const {
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_IMPLEMENTED>(
                "Not Implemented evaluate with inverted index, please check the predicate");
    }
};

class SingleColumnBlockPredicate : public BlockColumnPredicate {
public:
    explicit SingleColumnBlockPredicate(const ColumnPredicate* pre) : _predicate(pre) {}

    void get_all_column_ids(std::set<ColumnId>& column_id_set) const override {
        column_id_set.insert(_predicate->column_id());
    }

    void get_all_column_predicate(std::set<const ColumnPredicate*>& predicate_set) const override {
        predicate_set.insert(_predicate);
    }

    uint16_t evaluate(vectorized::MutableColumns& block, uint16_t* sel,
                      uint16_t selected_size) const override;
    void evaluate_and(vectorized::MutableColumns& block, uint16_t* sel, uint16_t selected_size,
                      bool* flags) const override;
    bool support_zonemap() const override { return _predicate->support_zonemap(); }
    bool evaluate_and(const std::pair<WrapperField*, WrapperField*>& statistic) const override;
    bool evaluate_and(const segment_v2::BloomFilter* bf) const override;
    bool evaluate_and(const StringRef* dict_words, const size_t dict_num) const override;
    void evaluate_or(vectorized::MutableColumns& block, uint16_t* sel, uint16_t selected_size,
                     bool* flags) const override;

    void evaluate_vec(vectorized::MutableColumns& block, uint16_t size, bool* flags) const override;

    bool can_do_bloom_filter(bool ngram) const override {
        return _predicate->can_do_bloom_filter(ngram);
    }

    bool can_do_apply_safely(PrimitiveType input_type, bool is_null) const override {
        return _predicate->can_do_apply_safely(input_type, is_null);
    }

private:
    const ColumnPredicate* _predicate = nullptr;
};

class MutilColumnBlockPredicate : public BlockColumnPredicate {
public:
    MutilColumnBlockPredicate() = default;

    ~MutilColumnBlockPredicate() override {
        for (auto ptr : _block_column_predicate_vec) {
            delete ptr;
        }
    }

    bool support_zonemap() const override {
        for (const auto* child_block_predicate : _block_column_predicate_vec) {
            if (!child_block_predicate->support_zonemap()) {
                return false;
            }
        }

        return true;
    }

    void add_column_predicate(const BlockColumnPredicate* column_predicate) {
        _block_column_predicate_vec.push_back(column_predicate);
    }

    size_t num_of_column_predicate() const { return _block_column_predicate_vec.size(); }

    void get_all_column_ids(std::set<ColumnId>& column_id_set) const override {
        for (auto child_block_predicate : _block_column_predicate_vec) {
            child_block_predicate->get_all_column_ids(column_id_set);
        }
    }

    void get_all_column_predicate(std::set<const ColumnPredicate*>& predicate_set) const override {
        for (auto child_block_predicate : _block_column_predicate_vec) {
            child_block_predicate->get_all_column_predicate(predicate_set);
        }
    }

protected:
    std::vector<const BlockColumnPredicate*> _block_column_predicate_vec;
};

class OrBlockColumnPredicate : public MutilColumnBlockPredicate {
public:
    uint16_t evaluate(vectorized::MutableColumns& block, uint16_t* sel,
                      uint16_t selected_size) const override;
    void evaluate_and(vectorized::MutableColumns& block, uint16_t* sel, uint16_t selected_size,
                      bool* flags) const override;
    void evaluate_or(vectorized::MutableColumns& block, uint16_t* sel, uint16_t selected_size,
                     bool* flags) const override;

    // note(wb) we didnt't implement evaluate_vec method here, because storage layer only support AND predicate now;
};

class AndBlockColumnPredicate : public MutilColumnBlockPredicate {
public:
    uint16_t evaluate(vectorized::MutableColumns& block, uint16_t* sel,
                      uint16_t selected_size) const override;
    void evaluate_and(vectorized::MutableColumns& block, uint16_t* sel, uint16_t selected_size,
                      bool* flags) const override;
    void evaluate_or(vectorized::MutableColumns& block, uint16_t* sel, uint16_t selected_size,
                     bool* flags) const override;

    void evaluate_vec(vectorized::MutableColumns& block, uint16_t size, bool* flags) const override;

    bool evaluate_and(const std::pair<WrapperField*, WrapperField*>& statistic) const override;

    bool evaluate_and(const segment_v2::BloomFilter* bf) const override;

    bool evaluate_and(const StringRef* dict_words, const size_t dict_num) const override;

    bool can_do_bloom_filter(bool ngram) const override {
        for (auto& pred : _block_column_predicate_vec) {
            if (!pred->can_do_bloom_filter(ngram)) {
                return false;
            }
        }
        return true;
    }

    bool can_do_apply_safely(PrimitiveType input_type, bool is_null) const override {
        for (auto& pred : _block_column_predicate_vec) {
            if (!pred->can_do_apply_safely(input_type, is_null)) {
                return false;
            }
        }
        return true;
    }

    Status evaluate(const std::string& column_name, InvertedIndexIterator* iterator,
                    uint32_t num_rows, roaring::Roaring* bitmap) const override;
};

} //namespace doris
