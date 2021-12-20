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

#ifndef DORIS_BE_SRC_OLAP_BLOCK_COLUMN_PREDICATE_H
#define DORIS_BE_SRC_OLAP_BLOCK_COLUMN_PREDICATE_H

#include <vector>

#include "olap/column_predicate.h"

namespace doris {

// Block Column Predicate support do column predicate in RowBlockV2 and support OR and AND predicate
// Block Column Predicate will replace column predicate as a unified external vectorized interface
// in the future
// TODO: support do predicate on Bitmap and ZoneMap, So we can use index of column to do predicate on
// page and segment

class BlockColumnPredicate {
public:
    BlockColumnPredicate() = default;
    virtual ~BlockColumnPredicate() = default;

    // evaluate all predicate on Block
    virtual void evaluate(RowBlockV2* block, uint16_t* selected_size) const = 0;
    // evaluate and semantics in all child block column predicate, flags as temporary variable identification
    // to mark whether select vector is selected in evaluate the column predicate
    virtual void evaluate_and(RowBlockV2* block, uint16_t selected_size, bool* flags) const = 0;
    // evaluate or semantics in all child block column predicate
    virtual void evaluate_or(RowBlockV2* block, uint16_t selected_size, bool* flags) const = 0;

    virtual void get_all_column_ids(std::set<ColumnId>& column_id_set) const = 0;
};

class SingleColumnBlockPredicate : public BlockColumnPredicate {
public:
    explicit SingleColumnBlockPredicate(const ColumnPredicate* pre):_predicate(pre) {};

    void evaluate(RowBlockV2* block, uint16_t* selected_size) const override;
    void evaluate_and(RowBlockV2* block, uint16_t selected_size, bool* flags) const override;
    void evaluate_or(RowBlockV2* block, uint16_t selected_size, bool* flags) const override;

    void get_all_column_ids(std::set<ColumnId>& column_id_set) const override {
        column_id_set.insert(_predicate->column_id());
    };
private:
    const ColumnPredicate* _predicate;
};

class MutilColumnBlockPredicate : public BlockColumnPredicate {
public:
    MutilColumnBlockPredicate() = default;

    ~MutilColumnBlockPredicate() override {
        for (auto ptr : _block_column_predicate_vec) {
            delete ptr;
        }
    }

    void add_column_predicate(const BlockColumnPredicate* column_predicate) {
        _block_column_predicate_vec.push_back(column_predicate);
    }

    size_t num_of_column_predicate() const {
        return _block_column_predicate_vec.size();
    }

    void get_all_column_ids(std::set<ColumnId>& column_id_set) const override {
        for (auto child_block_predicate : _block_column_predicate_vec) {
            child_block_predicate->get_all_column_ids(column_id_set);
        }
    };

protected:
    std::vector<const BlockColumnPredicate*> _block_column_predicate_vec;
};

class OrBlockColumnPredicate : public MutilColumnBlockPredicate {
public:
    void evaluate(RowBlockV2* block, uint16_t* selected_size) const override;

    // It's kind of confusing here, when OrBlockColumnPredicate as a child of AndBlockColumnPredicate:
    // 1.OrBlockColumnPredicate need evaluate all child BlockColumnPredicate OR SEMANTICS inside first
    // 2.Do AND SEMANTICS in flags use 1 result to get proper select flags
    void evaluate_and(RowBlockV2* block, uint16_t selected_size, bool* flags) const override;
    void evaluate_or(RowBlockV2* block, uint16_t selected_size, bool* flags) const override;
};

class AndBlockColumnPredicate : public MutilColumnBlockPredicate {
public:
    void evaluate(RowBlockV2* block, uint16_t* selected_size) const override;
    void evaluate_and(RowBlockV2* block, uint16_t selected_size, bool* flags) const override;

    // It's kind of confusing here, when AndBlockColumnPredicate as a child of OrBlockColumnPredicate:
    // 1.AndBlockColumnPredicate need evaluate all child BlockColumnPredicate AND SEMANTICS inside first
    // 2.Evaluate OR SEMANTICS in flags use 1 result to get proper select flags
    void evaluate_or(RowBlockV2* block, uint16_t selected_size, bool* flags) const override;
};

} //namespace doris

#endif //DORIS_BE_SRC_OLAP_COLUMN_PREDICATE_H
