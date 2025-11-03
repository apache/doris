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

#include "common/status.h"
#include "exprs/block_bloom_filter.hpp"
#include "exprs/filter_base.h"
#include "exprs/hybrid_set.h"
#include "runtime/primitive_type.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/common/string_ref.h"

namespace doris {

class BloomFilterAdaptor : public FilterBase {
public:
    BloomFilterAdaptor(bool null_aware) : FilterBase(null_aware) {
        _bloom_filter = std::make_shared<doris::BlockBloomFilter>();
    }

    static BloomFilterAdaptor* create(bool null_aware) {
        return new BloomFilterAdaptor(null_aware);
    }

    Status merge(BloomFilterAdaptor* other) { return _bloom_filter->merge(*other->_bloom_filter); }

    Status init(int64_t len) {
        int log_space = (int)log2(len);
        return _bloom_filter->init(log_space, /*hash_seed*/ 0);
    }

    Status init(butil::IOBufAsZeroCopyInputStream* data, const size_t data_size) {
        int log_space = (int)log2(data_size);
        return _bloom_filter->init_from_directory(log_space, data, data_size, false, 0);
    }

    char* data() { return (char*)_bloom_filter->directory().data; }

    size_t size() { return _bloom_filter->directory().size; }

    bool test(uint32_t data) const { return _bloom_filter->find(data); }

    template <typename fixed_len_to_uint32_method, typename T>
    bool test_element(T element) const {
        if constexpr (std::is_same_v<T, StringRef>) {
            return _bloom_filter->find(element);
        } else {
            return _bloom_filter->find(fixed_len_to_uint32_method()(element));
        }
    }

    template <typename fixed_len_to_uint32_method, typename T>
    void add_element(T element) {
        if constexpr (std::is_same_v<T, StringRef>) {
            _bloom_filter->insert(element);
        } else {
            _bloom_filter->insert(fixed_len_to_uint32_method()(element));
        }
    }

private:
    std::shared_ptr<doris::BlockBloomFilter> _bloom_filter;
};

template <typename fixed_len_to_uint32_method, class T>
struct CommonFindOp {
    static uint16_t find_batch_olap_engine(const BloomFilterAdaptor& bloom_filter, const char* data,
                                           const uint8_t* nullmap, uint16_t* offsets, int number,
                                           const bool is_parse_column) {
        return find_batch_olap<fixed_len_to_uint32_method, T>(bloom_filter, data, nullmap, offsets,
                                                              number, is_parse_column);
    }

    static void insert_batch(BloomFilterAdaptor& bloom_filter, const vectorized::ColumnPtr& column,
                             size_t start) {
        const auto size = column->size();
        if (column->is_nullable()) {
            const auto* nullable = assert_cast<const vectorized::ColumnNullable*>(column.get());
            const auto& col = nullable->get_nested_column();
            const auto& nullmap =
                    assert_cast<const vectorized::ColumnUInt8&>(nullable->get_null_map_column())
                            .get_data();

            const T* data = (T*)col.get_raw_data().data;
            for (size_t i = start; i < size; i++) {
                if (!nullmap[i]) {
                    bloom_filter.add_element<fixed_len_to_uint32_method>(*(data + i));
                } else {
                    bloom_filter.set_contain_null(true);
                }
            }
        } else {
            const T* data = (T*)column->get_raw_data().data;
            for (size_t i = start; i < size; i++) {
                bloom_filter.add_element<fixed_len_to_uint32_method>(*(data + i));
            }
        }
    }

    static void insert_set(BloomFilterAdaptor& bloom_filter, std::shared_ptr<HybridSetBase> set) {
        auto* it = set->begin();
        while (it->has_next()) {
            bloom_filter.add_element<fixed_len_to_uint32_method>(*(T*)it->get_value());
            it->next();
        }
    }

    static void find_batch(const BloomFilterAdaptor& bloom_filter,
                           const vectorized::ColumnPtr& column, uint8_t* results) {
        const T* __restrict data = nullptr;
        const uint8_t* __restrict nullmap = nullptr;
        if (column->is_nullable()) {
            const auto* nullable = assert_cast<const vectorized::ColumnNullable*>(column.get());
            if (nullable->has_null()) {
                nullmap =
                        assert_cast<const vectorized::ColumnUInt8&>(nullable->get_null_map_column())
                                .get_data()
                                .data();
            }
            data = (T*)nullable->get_nested_column().get_raw_data().data;
        } else {
            data = (T*)column->get_raw_data().data;
        }

        const auto size = column->size();
        if (nullmap) {
            for (size_t i = 0; i < size; i++) {
                if (!nullmap[i]) {
                    results[i] = bloom_filter.test_element<fixed_len_to_uint32_method>(data[i]);
                } else {
                    results[i] = bloom_filter.contain_null();
                }
            }
        } else {
            for (size_t i = 0; i < size; i++) {
                results[i] = bloom_filter.test_element<fixed_len_to_uint32_method>(data[i]);
            }
        }
    }
};

template <typename fixed_len_to_uint32_method>
struct StringFindOp : CommonFindOp<fixed_len_to_uint32_method, StringRef> {
    static void insert_batch(BloomFilterAdaptor& bloom_filter, const vectorized::ColumnPtr& column,
                             size_t start) {
        auto _insert_batch_col_str = [&](const auto& col, const uint8_t* __restrict nullmap,
                                         size_t start, size_t size) {
            for (size_t i = start; i < size; i++) {
                if (nullmap == nullptr || !nullmap[i]) {
                    bloom_filter.add_element<fixed_len_to_uint32_method>(col.get_data_at(i));
                } else {
                    bloom_filter.set_contain_null(true);
                }
            }
        };

        if (column->is_nullable()) {
            const auto* nullable = assert_cast<const vectorized::ColumnNullable*>(column.get());
            const auto& nullmap =
                    assert_cast<const vectorized::ColumnUInt8&>(nullable->get_null_map_column())
                            .get_data();
            if (nullable->get_nested_column().is_column_string64()) {
                _insert_batch_col_str(assert_cast<const vectorized::ColumnString64&>(
                                              nullable->get_nested_column()),
                                      nullmap.data(), start, nullmap.size());
            } else {
                _insert_batch_col_str(
                        assert_cast<const vectorized::ColumnString&>(nullable->get_nested_column()),
                        nullmap.data(), start, nullmap.size());
            }
        } else {
            if (column->is_column_string64()) {
                _insert_batch_col_str(assert_cast<const vectorized::ColumnString64&>(*column),
                                      nullptr, start, column->size());
            } else {
                _insert_batch_col_str(assert_cast<const vectorized::ColumnString&>(*column),
                                      nullptr, start, column->size());
            }
        }
    }

    static void find_batch(const BloomFilterAdaptor& bloom_filter,
                           const vectorized::ColumnPtr& column, uint8_t* results) {
        if (column->is_nullable()) {
            const auto* nullable = assert_cast<const vectorized::ColumnNullable*>(column.get());
            const auto& col =
                    assert_cast<const vectorized::ColumnString&>(nullable->get_nested_column());
            const auto& nullmap =
                    assert_cast<const vectorized::ColumnUInt8&>(nullable->get_null_map_column())
                            .get_data();

            if (nullable->has_null()) {
                for (size_t i = 0; i < col.size(); i++) {
                    if (!nullmap[i]) {
                        results[i] = bloom_filter.test_element<fixed_len_to_uint32_method>(
                                col.get_data_at(i));
                    } else {
                        results[i] = bloom_filter.contain_null();
                    }
                }
            } else {
                for (size_t i = 0; i < col.size(); i++) {
                    results[i] = bloom_filter.test_element<fixed_len_to_uint32_method>(
                            col.get_data_at(i));
                }
            }
        } else {
            const auto& col = assert_cast<const vectorized::ColumnString*>(column.get());
            for (size_t i = 0; i < col->size(); i++) {
                results[i] =
                        bloom_filter.test_element<fixed_len_to_uint32_method>(col->get_data_at(i));
            }
        }
    }
};

// We do not need to judge whether data is empty, because null will not appear
// when filer used by the storage engine
template <typename fixed_len_to_uint32_method>
struct FixedStringFindOp : public StringFindOp<fixed_len_to_uint32_method> {
    static uint16_t find_batch_olap_engine(const BloomFilterAdaptor& bloom_filter, const char* data,
                                           const uint8_t* nullmap, uint16_t* offsets, int number,
                                           const bool is_parse_column) {
        return find_batch_olap<fixed_len_to_uint32_method, StringRef, true>(
                bloom_filter, data, nullmap, offsets, number, is_parse_column);
    }
};

template <typename fixed_len_to_uint32_method, PrimitiveType type>
struct BloomFilterTypeTraits {
    using T = typename PrimitiveTypeTraits<type>::CppType;
    using FindOp = CommonFindOp<fixed_len_to_uint32_method, T>;
};

template <typename fixed_len_to_uint32_method>
struct BloomFilterTypeTraits<fixed_len_to_uint32_method, TYPE_CHAR> {
    using FindOp = FixedStringFindOp<fixed_len_to_uint32_method>;
};

template <typename fixed_len_to_uint32_method>
struct BloomFilterTypeTraits<fixed_len_to_uint32_method, TYPE_VARCHAR> {
    using FindOp = StringFindOp<fixed_len_to_uint32_method>;
};

template <typename fixed_len_to_uint32_method>
struct BloomFilterTypeTraits<fixed_len_to_uint32_method, TYPE_STRING> {
    using FindOp = StringFindOp<fixed_len_to_uint32_method>;
};

} // namespace doris
