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

#include "olap/aggregate_func.h"

namespace std {
namespace {
// algorithm from boost: http://www.boost.org/doc/libs/1_61_0/doc/html/hash/reference.html#boost.hash_combine
template <class T>
inline void hash_combine(std::size_t& seed, T const& v) {
    seed ^= std::hash<T>()(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
}

template <class Tuple, size_t Index = std::tuple_size<Tuple>::value - 1>
struct HashValueImpl {
    static void apply(size_t& seed, Tuple const& tuple) {
        HashValueImpl<Tuple, Index - 1>::apply(seed, tuple);
        hash_combine(seed, std::get<Index>(tuple));
    }
};

template <class Tuple>
struct HashValueImpl<Tuple, 0> {
    static void apply(size_t& seed, Tuple const& tuple) { hash_combine(seed, std::get<0>(tuple)); }
};
} // namespace

template <typename... TT>
struct hash<std::tuple<TT...>> {
    size_t operator()(std::tuple<TT...> const& tt) const {
        size_t seed = 0;
        HashValueImpl<std::tuple<TT...>>::apply(seed, tt);
        return seed;
    }
};
} // namespace std

namespace doris {

template <typename Traits>
AggregateInfo::AggregateInfo(const Traits& traits)
        : _init_fn(traits.init),
          _update_fn(traits.update),
          _finalize_fn(traits.finalize),
          _agg_method(traits.agg_method) {}

class AggregateFuncResolver {
    DECLARE_SINGLETON(AggregateFuncResolver);

public:
    const AggregateInfo* get_aggregate_info(const FieldAggregationMethod agg_method,
                                            const FieldType field_type,
                                            const FieldType sub_type) const {
        auto pair = _infos_mapping.find(std::make_tuple(agg_method, field_type, sub_type));
        if (pair != _infos_mapping.end()) {
            return pair->second;
        } else {
            return nullptr;
        }
    }

    template <FieldAggregationMethod agg_method, FieldType field_type,
              FieldType sub_type = OLAP_FIELD_TYPE_NONE>
    void add_aggregate_mapping() {
        _infos_mapping.emplace(
                std::make_tuple(agg_method, field_type, sub_type),
                new AggregateInfo(AggregateTraits<agg_method, field_type, sub_type>()));
    }

private:
    typedef std::tuple<FieldAggregationMethod, FieldType, FieldType> key_t;
    std::unordered_map<key_t, const AggregateInfo*> _infos_mapping;

    DISALLOW_COPY_AND_ASSIGN(AggregateFuncResolver);
};

AggregateFuncResolver::AggregateFuncResolver() {
    // None Aggregate Function, no-ops
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_TINYINT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_SMALLINT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_INT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_BIGINT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_LARGEINT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_FLOAT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_DOUBLE>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_DECIMAL>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_DATE>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_DATETIME>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_CHAR>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_VARCHAR>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_STRING>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_BOOL>();
    // array types has sub type like array<int>  field type is array, subtype is int
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_ARRAY,
                          OLAP_FIELD_TYPE_TINYINT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_ARRAY,
                          OLAP_FIELD_TYPE_SMALLINT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_ARRAY,
                          OLAP_FIELD_TYPE_INT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_ARRAY,
                          OLAP_FIELD_TYPE_BIGINT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_ARRAY,
                          OLAP_FIELD_TYPE_LARGEINT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_ARRAY,
                          OLAP_FIELD_TYPE_VARCHAR>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_NONE, OLAP_FIELD_TYPE_ARRAY,
                          OLAP_FIELD_TYPE_CHAR>();

    // Min Aggregate Function
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_MIN, OLAP_FIELD_TYPE_TINYINT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_MIN, OLAP_FIELD_TYPE_SMALLINT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_MIN, OLAP_FIELD_TYPE_INT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_MIN, OLAP_FIELD_TYPE_BIGINT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_MIN, OLAP_FIELD_TYPE_LARGEINT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_MIN, OLAP_FIELD_TYPE_FLOAT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_MIN, OLAP_FIELD_TYPE_DOUBLE>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_MIN, OLAP_FIELD_TYPE_DECIMAL>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_MIN, OLAP_FIELD_TYPE_DATE>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_MIN, OLAP_FIELD_TYPE_DATETIME>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_MIN, OLAP_FIELD_TYPE_CHAR>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_MIN, OLAP_FIELD_TYPE_VARCHAR>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_MIN, OLAP_FIELD_TYPE_STRING>();

    // Max Aggregate Function
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_MAX, OLAP_FIELD_TYPE_TINYINT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_MAX, OLAP_FIELD_TYPE_SMALLINT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_MAX, OLAP_FIELD_TYPE_INT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_MAX, OLAP_FIELD_TYPE_BIGINT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_MAX, OLAP_FIELD_TYPE_LARGEINT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_MAX, OLAP_FIELD_TYPE_FLOAT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_MAX, OLAP_FIELD_TYPE_DOUBLE>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_MAX, OLAP_FIELD_TYPE_DECIMAL>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_MAX, OLAP_FIELD_TYPE_DATE>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_MAX, OLAP_FIELD_TYPE_DATETIME>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_MAX, OLAP_FIELD_TYPE_CHAR>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_MAX, OLAP_FIELD_TYPE_VARCHAR>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_MAX, OLAP_FIELD_TYPE_STRING>();

    // Sum Aggregate Function
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_SUM, OLAP_FIELD_TYPE_TINYINT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_SUM, OLAP_FIELD_TYPE_SMALLINT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_SUM, OLAP_FIELD_TYPE_INT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_SUM, OLAP_FIELD_TYPE_BIGINT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_SUM, OLAP_FIELD_TYPE_LARGEINT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_SUM, OLAP_FIELD_TYPE_FLOAT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_SUM, OLAP_FIELD_TYPE_DOUBLE>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_SUM, OLAP_FIELD_TYPE_DECIMAL>();

    // Replace Aggregate Function
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_REPLACE, OLAP_FIELD_TYPE_BOOL>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_REPLACE, OLAP_FIELD_TYPE_TINYINT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_REPLACE, OLAP_FIELD_TYPE_SMALLINT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_REPLACE, OLAP_FIELD_TYPE_INT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_REPLACE, OLAP_FIELD_TYPE_BIGINT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_REPLACE, OLAP_FIELD_TYPE_LARGEINT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_REPLACE, OLAP_FIELD_TYPE_FLOAT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_REPLACE, OLAP_FIELD_TYPE_DOUBLE>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_REPLACE, OLAP_FIELD_TYPE_DECIMAL>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_REPLACE, OLAP_FIELD_TYPE_DATE>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_REPLACE, OLAP_FIELD_TYPE_DATETIME>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_REPLACE, OLAP_FIELD_TYPE_CHAR>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_REPLACE, OLAP_FIELD_TYPE_VARCHAR>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_REPLACE, OLAP_FIELD_TYPE_STRING>();

    // ReplaceIfNotNull Aggregate Function
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL, OLAP_FIELD_TYPE_BOOL>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL, OLAP_FIELD_TYPE_TINYINT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL, OLAP_FIELD_TYPE_SMALLINT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL, OLAP_FIELD_TYPE_INT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL, OLAP_FIELD_TYPE_BIGINT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL, OLAP_FIELD_TYPE_LARGEINT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL, OLAP_FIELD_TYPE_FLOAT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL, OLAP_FIELD_TYPE_DOUBLE>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL, OLAP_FIELD_TYPE_DECIMAL>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL, OLAP_FIELD_TYPE_DATE>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL, OLAP_FIELD_TYPE_DATETIME>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL, OLAP_FIELD_TYPE_CHAR>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL, OLAP_FIELD_TYPE_VARCHAR>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL, OLAP_FIELD_TYPE_STRING>();

    // Hyperloglog Aggregate Function
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_HLL_UNION, OLAP_FIELD_TYPE_HLL>();

    // Bitmap Aggregate Function
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_BITMAP_UNION, OLAP_FIELD_TYPE_OBJECT>();
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_BITMAP_UNION,
                          OLAP_FIELD_TYPE_VARCHAR>(); //for backward compatibility
}

AggregateFuncResolver::~AggregateFuncResolver() {
    for (auto& iter : _infos_mapping) {
        delete iter.second;
    }
}

const AggregateInfo* get_aggregate_info(const FieldAggregationMethod agg_method,
                                        const FieldType field_type, const FieldType sub_type) {
    return AggregateFuncResolver::instance()->get_aggregate_info(agg_method, field_type, sub_type);
}

} // namespace doris
