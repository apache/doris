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

#include "block_column_predicate.h"

#include "olap/row_block2.h"

namespace doris {

void SingleColumnBlockPredicate::evaluate(RowBlockV2* block, uint16_t* selected_size) const {
    auto column_id = _predicate->column_id();
    auto column_block = block->column_block(column_id);
    _predicate->evaluate(&column_block, block->selection_vector(), selected_size);
}

void SingleColumnBlockPredicate::evaluate_and(RowBlockV2 *block, uint16_t selected_size, bool *flags) const {
    auto column_id = _predicate->column_id();
    auto column_block = block->column_block(column_id);
    _predicate->evaluate_and(&column_block, block->selection_vector(), selected_size, flags);
}

void SingleColumnBlockPredicate::evaluate_or(RowBlockV2 *block, uint16_t selected_size, bool *flags) const {
    auto column_id = _predicate->column_id();
    auto column_block = block->column_block(column_id);
    _predicate->evaluate_or(&column_block, block->selection_vector(), selected_size, flags);
}

void OrBlockColumnPredicate::evaluate(RowBlockV2* block, uint16_t* selected_size) const {
    if (num_of_column_predicate() == 1) {
        _block_column_predicate_vec[0]->evaluate(block, selected_size);
    } else {
        bool flags[*selected_size];
        memset(flags, false, *selected_size);
        for (int i = 0; i < num_of_column_predicate(); ++i) {
            auto column_predicate = _block_column_predicate_vec[i];
            column_predicate->evaluate_or(block, *selected_size, flags);
        }

        uint16_t new_size = 0;
        for (int i = 0; i < *selected_size; ++i) {
            if (flags[i]) {
                block->selection_vector()[new_size++] = block->selection_vector()[i];
            }
        }
        *selected_size = new_size;
    }
}

void OrBlockColumnPredicate::evaluate_or(RowBlockV2 *block, uint16_t selected_size, bool* flags) const {
    for (auto block_column_predicate : _block_column_predicate_vec) {
        block_column_predicate->evaluate_or(block, selected_size, flags);
    }
}

void OrBlockColumnPredicate::evaluate_and(RowBlockV2 *block, uint16_t selected_size, bool* flags) const {
    if (num_of_column_predicate() == 1) {
        _block_column_predicate_vec[0]->evaluate_and(block, selected_size, flags);
    } else {
        bool new_flags[selected_size];
        memset(new_flags, false, selected_size);
        for (int i = 0; i < num_of_column_predicate(); ++i) {
            auto column_predicate = _block_column_predicate_vec[i];
            column_predicate->evaluate_or(block, selected_size, new_flags);
        }

        for (int i = 0; i < selected_size; ++i) {
            flags[i] &= new_flags[i];
        }
    }
}

void AndBlockColumnPredicate::evaluate(RowBlockV2* block, uint16_t* selected_size) const {
    for (auto block_column_predicate : _block_column_predicate_vec) {
        block_column_predicate->evaluate(block, selected_size);
    }
}

void AndBlockColumnPredicate::evaluate_and(RowBlockV2 *block, uint16_t selected_size, bool* flags) const {
    for (auto block_column_predicate : _block_column_predicate_vec) {
        block_column_predicate->evaluate_and(block, selected_size, flags);
    }
}

void AndBlockColumnPredicate::evaluate_or(RowBlockV2 *block, uint16_t selected_size, bool* flags) const {
    if (num_of_column_predicate() == 1) {
        _block_column_predicate_vec[0]->evaluate_or(block, selected_size, flags);
    } else {
        bool new_flags[selected_size];
        memset(new_flags, true, selected_size);

        for (int i = 0; i < num_of_column_predicate(); ++i) {
            auto column_predicate = _block_column_predicate_vec[i];
            column_predicate->evaluate_and(block, selected_size, new_flags);
        }

        for (int i = 0; i < selected_size; ++i) {
            flags[i] |= new_flags[i];
        }
    }
}

} // namespace doris
