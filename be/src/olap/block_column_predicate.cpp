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

#include <string.h>

namespace roaring {
class Roaring;
} // namespace roaring

namespace doris {
class WrapperField;
namespace segment_v2 {
class InvertedIndexIterator;
} // namespace segment_v2

uint16_t SingleColumnBlockPredicate::evaluate(vectorized::MutableColumns& block, uint16_t* sel,
                                              uint16_t selected_size) const {
    auto column_id = _predicate->column_id();
    auto& column = block[column_id];
    return _predicate->evaluate(*column, sel, selected_size);
}

void SingleColumnBlockPredicate::evaluate_and(vectorized::MutableColumns& block, uint16_t* sel,
                                              uint16_t selected_size, bool* flags) const {
    auto column_id = _predicate->column_id();
    auto& column = block[column_id];
    _predicate->evaluate_and(*column, sel, selected_size, flags);
}

bool SingleColumnBlockPredicate::evaluate_and(
        const std::pair<WrapperField*, WrapperField*>& statistic) const {
    return _predicate->evaluate_and(statistic);
}

bool SingleColumnBlockPredicate::evaluate_and(const segment_v2::BloomFilter* bf) const {
    return _predicate->evaluate_and(bf);
}

bool SingleColumnBlockPredicate::evaluate_and(const StringRef* dict_words,
                                              const size_t dict_num) const {
    return _predicate->evaluate_and(dict_words, dict_num);
}

void SingleColumnBlockPredicate::evaluate_or(vectorized::MutableColumns& block, uint16_t* sel,
                                             uint16_t selected_size, bool* flags) const {
    auto column_id = _predicate->column_id();
    auto& column = block[column_id];
    _predicate->evaluate_or(*column, sel, selected_size, flags);
}

void SingleColumnBlockPredicate::evaluate_vec(vectorized::MutableColumns& block, uint16_t size,
                                              bool* flags) const {
    auto column_id = _predicate->column_id();
    auto& column = block[column_id];

    // Dictionary column should do something to initial.
    if (PredicateTypeTraits::is_range(_predicate->type())) {
        column->convert_dict_codes_if_necessary();
    } else if (PredicateTypeTraits::is_bloom_filter(_predicate->type())) {
        column->initialize_hash_values_for_runtime_filter();
    }

    _predicate->evaluate_vec(*column, size, flags);
}

uint16_t OrBlockColumnPredicate::evaluate(vectorized::MutableColumns& block, uint16_t* sel,
                                          uint16_t selected_size) const {
    if (num_of_column_predicate() == 1) {
        return _block_column_predicate_vec[0]->evaluate(block, sel, selected_size);
    } else {
        if (!selected_size) {
            return 0;
        }
        bool ret_flags[selected_size];
        memset(ret_flags, false, selected_size);
        for (int i = 0; i < num_of_column_predicate(); ++i) {
            auto column_predicate = _block_column_predicate_vec[i];
            column_predicate->evaluate_or(block, sel, selected_size, ret_flags);
        }

        uint16_t new_size = 0;
        for (int i = 0; i < selected_size; ++i) {
            if (ret_flags[i]) {
                sel[new_size++] = sel[i];
            }
        }
        return new_size;
    }
}

void OrBlockColumnPredicate::evaluate_or(vectorized::MutableColumns& block, uint16_t* sel,
                                         uint16_t selected_size, bool* flags) const {
    for (auto block_column_predicate : _block_column_predicate_vec) {
        block_column_predicate->evaluate_or(block, sel, selected_size, flags);
    }
}

void OrBlockColumnPredicate::evaluate_and(vectorized::MutableColumns& block, uint16_t* sel,
                                          uint16_t selected_size, bool* flags) const {
    if (num_of_column_predicate() == 1) {
        _block_column_predicate_vec[0]->evaluate_and(block, sel, selected_size, flags);
    } else {
        bool ret_flags[selected_size];
        memset(ret_flags, false, selected_size);
        for (int i = 0; i < num_of_column_predicate(); ++i) {
            auto column_predicate = _block_column_predicate_vec[i];
            column_predicate->evaluate_or(block, sel, selected_size, ret_flags);
        }

        for (int i = 0; i < selected_size; ++i) {
            flags[i] &= ret_flags[i];
        }
    }
}

uint16_t AndBlockColumnPredicate::evaluate(vectorized::MutableColumns& block, uint16_t* sel,
                                           uint16_t selected_size) const {
    for (auto block_column_predicate : _block_column_predicate_vec) {
        selected_size = block_column_predicate->evaluate(block, sel, selected_size);
    }
    return selected_size;
}

void AndBlockColumnPredicate::evaluate_and(vectorized::MutableColumns& block, uint16_t* sel,
                                           uint16_t selected_size, bool* flags) const {
    for (auto block_column_predicate : _block_column_predicate_vec) {
        block_column_predicate->evaluate_and(block, sel, selected_size, flags);
    }
}

bool AndBlockColumnPredicate::evaluate_and(
        const std::pair<WrapperField*, WrapperField*>& statistic) const {
    for (auto block_column_predicate : _block_column_predicate_vec) {
        if (!block_column_predicate->evaluate_and(statistic)) {
            return false;
        }
    }
    return true;
}

bool AndBlockColumnPredicate::evaluate_and(const segment_v2::BloomFilter* bf) const {
    for (auto block_column_predicate : _block_column_predicate_vec) {
        if (!block_column_predicate->evaluate_and(bf)) {
            return false;
        }
    }
    return true;
}

bool AndBlockColumnPredicate::evaluate_and(const StringRef* dict_words,
                                           const size_t dict_num) const {
    for (auto* predicate : _block_column_predicate_vec) {
        if (!predicate->evaluate_and(dict_words, dict_num)) {
            return false;
        }
    }
    return true;
}

void AndBlockColumnPredicate::evaluate_or(vectorized::MutableColumns& block, uint16_t* sel,
                                          uint16_t selected_size, bool* flags) const {
    if (num_of_column_predicate() == 1) {
        _block_column_predicate_vec[0]->evaluate_or(block, sel, selected_size, flags);
    } else {
        bool new_flags[selected_size];
        memset(new_flags, true, selected_size);

        for (auto block_column_predicate : _block_column_predicate_vec) {
            block_column_predicate->evaluate_and(block, sel, selected_size, new_flags);
        }

        for (uint16_t i = 0; i < selected_size; i++) {
            flags[i] |= new_flags[i];
        }
    }
}

// todo(wb) Can the 'and' of multiple bitmaps be vectorized?
void AndBlockColumnPredicate::evaluate_vec(vectorized::MutableColumns& block, uint16_t size,
                                           bool* flags) const {
    if (num_of_column_predicate() == 1) {
        _block_column_predicate_vec[0]->evaluate_vec(block, size, flags);
    } else {
        bool new_flags[size];
        bool initialized = false;
        for (auto block_column_predicate : _block_column_predicate_vec) {
            if (initialized) {
                block_column_predicate->evaluate_vec(block, size, new_flags);
                for (uint16_t j = 0; j < size; j++) {
                    flags[j] &= new_flags[j];
                }
            } else {
                block_column_predicate->evaluate_vec(block, size, flags);
                initialized = true;
            }
        }
    }
}

Status AndBlockColumnPredicate::evaluate(const std::string& column_name,
                                         InvertedIndexIterator* iterator, uint32_t num_rows,
                                         roaring::Roaring* bitmap) const {
    return Status::Error<ErrorCode::INVERTED_INDEX_NOT_IMPLEMENTED>(
            "Not Implemented evaluate with inverted index, please check the predicate");
}

} // namespace doris
