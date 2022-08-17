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

#include "olap/bloom_filter_predicate.h"

#include "exprs/create_predicate_function.h"

#define APPLY_FOR_PRIMTYPE(M) \
    M(TYPE_TINYINT)           \
    M(TYPE_SMALLINT)          \
    M(TYPE_INT)               \
    M(TYPE_BIGINT)            \
    M(TYPE_LARGEINT)          \
    M(TYPE_FLOAT)             \
    M(TYPE_DOUBLE)            \
    M(TYPE_CHAR)              \
    M(TYPE_DATE)              \
    M(TYPE_DATETIME)          \
    M(TYPE_DATEV2)            \
    M(TYPE_DATETIMEV2)        \
    M(TYPE_VARCHAR)           \
    M(TYPE_STRING)            \
    M(TYPE_DECIMAL32)         \
    M(TYPE_DECIMAL64)         \
    M(TYPE_DECIMAL128)

namespace doris {
ColumnPredicate* BloomFilterColumnPredicateFactory::create_column_predicate(
        uint32_t column_id, const std::shared_ptr<IBloomFilterFuncBase>& bloom_filter,
        FieldType type) {
    std::shared_ptr<IBloomFilterFuncBase> filter;
    switch (type) {
#define M(NAME)                                                         \
    case OLAP_FIELD_##NAME: {                                           \
        filter.reset(create_bloom_filter(NAME));                        \
        filter->light_copy(bloom_filter.get());                         \
        return new BloomFilterColumnPredicate<NAME>(column_id, filter); \
    }
        APPLY_FOR_PRIMTYPE(M)
#undef M
    case OLAP_FIELD_TYPE_DECIMAL: {
        filter.reset(create_bloom_filter(TYPE_DECIMALV2));
        filter->light_copy(bloom_filter.get());
        return new BloomFilterColumnPredicate<TYPE_DECIMALV2>(column_id, filter);
    }
    case OLAP_FIELD_TYPE_BOOL: {
        filter.reset(create_bloom_filter(TYPE_BOOLEAN));
        filter->light_copy(bloom_filter.get());
        return new BloomFilterColumnPredicate<TYPE_BOOLEAN>(column_id, filter);
    }
    default:
        return nullptr;
    }
}
} //namespace doris
