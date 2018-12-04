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

namespace doris {

struct AggregateFuncMapHash {
    size_t operator()(const std::pair<FieldAggregationMethod, FieldType>& pair) const {
        return (pair.first + 31) ^ pair.second;
    }
};

class AggregateFuncResolver {
DECLARE_SINGLETON(AggregateFuncResolver);
public:
    AggregateFunc get_aggregate_func(const FieldAggregationMethod agg_method,
                                     const FieldType field_type) {
        auto pair = _aggregate_mapping.find(std::make_pair(agg_method, field_type));
        if (pair != _aggregate_mapping.end()) {
            return pair->second;
        } else {
            return nullptr;
        }
    }

    FinalizeFunc get_finalize_func(const FieldAggregationMethod agg_method,
                                     const FieldType field_type) {
        auto pair = _finalize_mapping.find(std::make_pair(agg_method, field_type));
        if (pair != _finalize_mapping.end()) {
            return pair->second;
        } else {
            return nullptr;
        }
    }

    template<FieldAggregationMethod agg_method, FieldType field_type>
    void add_aggregate_mapping() {
        _aggregate_mapping.insert(std::make_pair(std::make_pair(agg_method, field_type),
                         &AggregateFuncTraits<agg_method, field_type>::aggregate));
    }

    template<FieldAggregationMethod agg_method, FieldType field_type>
    void add_finalize_mapping() {
        _finalize_mapping.insert(std::make_pair(std::make_pair(agg_method, field_type),
                         &AggregateFuncTraits<agg_method, field_type>::finalize));
    }
private:
    typedef std::pair<FieldAggregationMethod, FieldType> key_t;
    std::unordered_map<key_t, AggregateFunc, AggregateFuncMapHash> _aggregate_mapping;
    std::unordered_map<key_t, FinalizeFunc, AggregateFuncMapHash> _finalize_mapping;

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

    // Hyperloglog Aggregate Function
    add_aggregate_mapping<OLAP_FIELD_AGGREGATION_HLL_UNION, OLAP_FIELD_TYPE_HLL>();


    // Finalize Function for hyperloglog Function
    add_finalize_mapping<OLAP_FIELD_AGGREGATION_HLL_UNION, OLAP_FIELD_TYPE_HLL>();
}

AggregateFuncResolver::~AggregateFuncResolver() {}

AggregateFunc get_aggregate_func(const FieldAggregationMethod agg_method,
                                 const FieldType field_type) {
    return AggregateFuncResolver::instance()->get_aggregate_func(agg_method, field_type);
}

FinalizeFunc get_finalize_func(const FieldAggregationMethod agg_method,
                                 const FieldType field_type) {
    return AggregateFuncResolver::instance()->get_finalize_func(agg_method, field_type);
}

} // namespace doris
