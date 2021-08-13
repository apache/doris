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

#ifndef DORIS_BE_SRC_OLAP_IN_LIST_PREDICATE_H
#define DORIS_BE_SRC_OLAP_IN_LIST_PREDICATE_H

#include <stdint.h>

#include <roaring/roaring.hh>
#include <parallel_hashmap/phmap.h>

#include "decimal12.h"
#include "olap/column_predicate.h"
#include "uint24.h"
#include "util/murmur_hash3.h"

namespace std {
// for string value
template <>
struct hash<doris::StringValue> {
    uint64_t operator()(const doris::StringValue& rhs) const { return hash_value(rhs); }
};

template <>
struct equal_to<doris::StringValue> {
    bool operator()(const doris::StringValue& lhs, const doris::StringValue& rhs) const {
        return lhs == rhs;
    }
};
// for decimal12_t
template <>
struct hash<doris::decimal12_t> {
    int64_t operator()(const doris::decimal12_t& rhs) const {
        return hash<int64_t>()(rhs.integer) ^ hash<int32_t>()(rhs.fraction);
    }
};

template <>
struct equal_to<doris::decimal12_t> {
    bool operator()(const doris::decimal12_t& lhs, const doris::decimal12_t& rhs) const {
        return lhs == rhs;
    }
};
// for uint24_t
template <>
struct hash<doris::uint24_t> {
    size_t operator()(const doris::uint24_t& rhs) const {
        uint32_t val(rhs);
        return hash<int>()(val);
    }
};

template <>
struct equal_to<doris::uint24_t> {
    bool operator()(const doris::uint24_t& lhs, const doris::uint24_t& rhs) const {
        return lhs == rhs;
    }
};
} // namespace std

namespace doris {

class VectorizedRowBatch;

#define IN_LIST_PRED_CLASS_DEFINE(CLASS)                                                        \
    template <class type>                                                                       \
    class CLASS : public ColumnPredicate {                                                      \
    public:                                                                                     \
        CLASS(uint32_t column_id, phmap::flat_hash_set<type>&& values, bool is_opposite = false); \
        virtual void evaluate(VectorizedRowBatch* batch) const override;                        \
        void evaluate(ColumnBlock* block, uint16_t* sel, uint16_t* size) const override;        \
        void evaluate_or(ColumnBlock* block, uint16_t* sel, uint16_t size,                      \
                         bool* flags) const override;                                           \
        void evaluate_and(ColumnBlock* block, uint16_t* sel, uint16_t size,                     \
                          bool* flags) const override;                                          \
        virtual Status evaluate(const Schema& schema,                                           \
                                const std::vector<BitmapIndexIterator*>& iterators,             \
                                uint32_t num_rows, Roaring* bitmap) const override;             \
                                                                                                \
    private:                                                                                    \
        phmap::flat_hash_set<type> _values;                                                       \
    };

IN_LIST_PRED_CLASS_DEFINE(InListPredicate)
IN_LIST_PRED_CLASS_DEFINE(NotInListPredicate)

} //namespace doris

#endif //DORIS_BE_SRC_OLAP_IN_LIST_PREDICATE_H
