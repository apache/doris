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

#include "olap/predicate_creator.h"

namespace doris {

std::shared_ptr<ColumnPredicate> create_bloom_filter_predicate(
        const uint32_t cid, const std::string col_name, const vectorized::DataTypePtr& data_type,
        const std::shared_ptr<BloomFilterFuncBase>& filter) {
    // Do the necessary type conversion, for CAST(STRING AS CHAR), we do nothing here but change the data type to the target type CHAR
    std::shared_ptr<BloomFilterFuncBase> filter_olap;
    filter_olap.reset(create_bloom_filter(data_type->get_primitive_type(), false));
    filter_olap->light_copy(filter.get());
    switch (data_type->get_primitive_type()) {
    case TYPE_TINYINT: {
        return BloomFilterColumnPredicate<TYPE_TINYINT>::create_shared(cid, col_name, filter_olap);
    }
    case TYPE_SMALLINT: {
        return BloomFilterColumnPredicate<TYPE_SMALLINT>::create_shared(cid, col_name, filter_olap);
    }
    case TYPE_INT: {
        return BloomFilterColumnPredicate<TYPE_INT>::create_shared(cid, col_name, filter_olap);
    }
    case TYPE_BIGINT: {
        return BloomFilterColumnPredicate<TYPE_BIGINT>::create_shared(cid, col_name, filter_olap);
    }
    case TYPE_LARGEINT: {
        return BloomFilterColumnPredicate<TYPE_LARGEINT>::create_shared(cid, col_name, filter_olap);
    }
    case TYPE_FLOAT: {
        return BloomFilterColumnPredicate<TYPE_FLOAT>::create_shared(cid, col_name, filter_olap);
    }
    case TYPE_DOUBLE: {
        return BloomFilterColumnPredicate<TYPE_DOUBLE>::create_shared(cid, col_name, filter_olap);
    }
    case TYPE_DECIMALV2: {
        return BloomFilterColumnPredicate<TYPE_DECIMALV2>::create_shared(cid, col_name,
                                                                         filter_olap);
    }
    case TYPE_DECIMAL32: {
        return BloomFilterColumnPredicate<TYPE_DECIMAL32>::create_shared(cid, col_name,
                                                                         filter_olap);
    }
    case TYPE_DECIMAL64: {
        return BloomFilterColumnPredicate<TYPE_DECIMAL64>::create_shared(cid, col_name,
                                                                         filter_olap);
    }
    case TYPE_DECIMAL128I: {
        return BloomFilterColumnPredicate<TYPE_DECIMAL128I>::create_shared(cid, col_name,
                                                                           filter_olap);
    }
    case TYPE_DECIMAL256: {
        return BloomFilterColumnPredicate<TYPE_DECIMAL256>::create_shared(cid, col_name,
                                                                          filter_olap);
    }
    case TYPE_CHAR: {
        return BloomFilterColumnPredicate<TYPE_CHAR>::create_shared(cid, col_name, filter_olap);
    }
    case TYPE_VARCHAR: {
        return BloomFilterColumnPredicate<TYPE_VARCHAR>::create_shared(cid, col_name, filter_olap);
    }
    case TYPE_STRING: {
        return BloomFilterColumnPredicate<TYPE_STRING>::create_shared(cid, col_name, filter_olap);
    }
    case TYPE_DATE: {
        return BloomFilterColumnPredicate<TYPE_DATE>::create_shared(cid, col_name, filter_olap);
    }
    case TYPE_DATEV2: {
        return BloomFilterColumnPredicate<TYPE_DATEV2>::create_shared(cid, col_name, filter_olap);
    }
    case TYPE_DATETIME: {
        return BloomFilterColumnPredicate<TYPE_DATETIME>::create_shared(cid, col_name, filter_olap);
    }
    case TYPE_DATETIMEV2: {
        return BloomFilterColumnPredicate<TYPE_DATETIMEV2>::create_shared(cid, col_name,
                                                                          filter_olap);
    }
    case TYPE_TIMESTAMPTZ: {
        return BloomFilterColumnPredicate<TYPE_TIMESTAMPTZ>::create_shared(cid, col_name,
                                                                           filter_olap);
    }
    case TYPE_BOOLEAN: {
        return BloomFilterColumnPredicate<TYPE_BOOLEAN>::create_shared(cid, col_name, filter_olap);
    }
    case TYPE_IPV4: {
        return BloomFilterColumnPredicate<TYPE_IPV4>::create_shared(cid, col_name, filter_olap);
    }
    case TYPE_IPV6: {
        return BloomFilterColumnPredicate<TYPE_IPV6>::create_shared(cid, col_name, filter_olap);
    }
    default:
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        fmt::format("Cannot use bloom filter for type: {}",
                                    type_to_string(data_type->get_primitive_type())));
        return nullptr;
    }
}

std::shared_ptr<ColumnPredicate> create_bitmap_filter_predicate(
        const uint32_t cid, const std::string col_name, const vectorized::DataTypePtr& data_type,
        const std::shared_ptr<BitmapFilterFuncBase>& filter) {
    switch (data_type->get_primitive_type()) {
    case TYPE_TINYINT: {
        return BitmapFilterColumnPredicate<TYPE_TINYINT>::create_shared(cid, col_name, filter);
    }
    case TYPE_SMALLINT: {
        return BitmapFilterColumnPredicate<TYPE_SMALLINT>::create_shared(cid, col_name, filter);
    }
    case TYPE_INT: {
        return BitmapFilterColumnPredicate<TYPE_INT>::create_shared(cid, col_name, filter);
    }
    case TYPE_BIGINT: {
        return BitmapFilterColumnPredicate<TYPE_BIGINT>::create_shared(cid, col_name, filter);
    }
    default:
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        fmt::format("Cannot use bitmap filter for type: {}",
                                    type_to_string(data_type->get_primitive_type())));
        return nullptr;
    }
}

} // namespace doris
