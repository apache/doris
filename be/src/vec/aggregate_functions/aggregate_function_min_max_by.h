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

#include "common/logging.h"
#include "util/bitmap_value.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_min_max.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

/// For bitmap value
struct BitmapValueData {
private:
    using Self = BitmapValueData;
    bool has_value = false;
    BitmapValue value;

public:
    BitmapValueData() = default;
    BitmapValueData(bool has_value_, BitmapValue value_) : has_value(has_value_), value(value_) {}
    [[nodiscard]] bool has() const { return has_value; }

    void insert_result_into(IColumn& to) const {
        if (has()) {
            assert_cast<ColumnBitmap&>(to).get_data().push_back(value);
        } else {
            assert_cast<ColumnBitmap&>(to).insert_default();
        }
    }

    void reset() {
        if (has()) {
            has_value = false;
        }
    }

    void write(BufferWritable& buf) const {
        write_binary(has(), buf);
        if (has()) {
            DataTypeBitMap::serialize_as_stream(value, buf);
        }
    }

    void read(BufferReadable& buf, Arena* arena) {
        read_binary(has_value, buf);
        if (has()) {
            DataTypeBitMap::deserialize_as_stream(value, buf);
        }
    }

    void change(const IColumn& column, size_t row_num, Arena*) {
        has_value = true;
        value = assert_cast<const ColumnBitmap&, TypeCheckOnRelease::DISABLE>(column)
                        .get_data()[row_num];
    }

    void change(const Self& to, Arena*) {
        has_value = true;
        value = to.value;
    }
};

template <typename VT, typename KT>
struct AggregateFunctionMinMaxByBaseData {
protected:
    VT value;
    KT key;

public:
    void insert_result_into(IColumn& to) const { value.insert_result_into(to); }

    void reset() {
        value.reset();
        key.reset();
    }
    void write(BufferWritable& buf) const {
        value.write(buf);
        key.write(buf);
    }

    void read(BufferReadable& buf, Arena* arena) {
        value.read(buf, arena);
        key.read(buf, arena);
    }
};

template <typename VT, typename KT>
struct AggregateFunctionMaxByData : public AggregateFunctionMinMaxByBaseData<VT, KT> {
    using Self = AggregateFunctionMaxByData;
    void change_if_better(const IColumn& value_column, const IColumn& key_column, size_t row_num,
                          Arena* arena) {
        if (this->key.change_if_greater(key_column, row_num, arena)) {
            this->value.change(value_column, row_num, arena);
        }
    }

    void change_if_better(const Self& to, Arena* arena) {
        if (this->key.change_if_greater(to.key, arena)) {
            this->value.change(to.value, arena);
        }
    }

    static const char* name() { return "max_by"; }
};

template <typename VT, typename KT>
struct AggregateFunctionMinByData : public AggregateFunctionMinMaxByBaseData<VT, KT> {
    using Self = AggregateFunctionMinByData;
    void change_if_better(const IColumn& value_column, const IColumn& key_column, size_t row_num,
                          Arena* arena) {
        if (this->key.change_if_less(key_column, row_num, arena)) {
            this->value.change(value_column, row_num, arena);
        }
    }

    void change_if_better(const Self& to, Arena* arena) {
        if (this->key.change_if_less(to.key, arena)) {
            this->value.change(to.value, arena);
        }
    }

    static const char* name() { return "min_by"; }
};

template <typename Data>
class AggregateFunctionsMinMaxBy final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionsMinMaxBy<Data>> {
private:
    DataTypePtr& value_type;
    DataTypePtr& key_type;

public:
    AggregateFunctionsMinMaxBy(const DataTypes& arguments)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionsMinMaxBy<Data>>(
                      {arguments[0], arguments[1]}),
              value_type(this->argument_types[0]),
              key_type(this->argument_types[1]) {
        if (StringRef(Data::name()) == StringRef("min_by") ||
            StringRef(Data::name()) == StringRef("max_by")) {
            CHECK(key_type->is_comparable());
        }
    }

    String get_name() const override { return Data::name(); }

    DataTypePtr get_return_type() const override { return value_type; }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena* arena) const override {
        this->data(place).change_if_better(*columns[0], *columns[1], row_num, arena);
    }

    void reset(AggregateDataPtr place) const override { this->data(place).reset(); }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena* arena) const override {
        this->data(place).change_if_better(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena* arena) const override {
        this->data(place).read(buf, arena);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        this->data(place).insert_result_into(to);
    }
};

template <template <typename> class AggregateFunctionTemplate,
          template <typename, typename> class Data, typename VT>
AggregateFunctionPtr create_aggregate_function_min_max_by_impl(const DataTypes& argument_types,
                                                               const bool result_is_nullable) {
    WhichDataType which(remove_nullable(argument_types[1]));

#define DISPATCH(TYPE)                                                            \
    if (which.idx == TypeIndex::TYPE)                                             \
        return creator_without_type::create<                                      \
                AggregateFunctionTemplate<Data<VT, SingleValueDataFixed<TYPE>>>>( \
                argument_types, result_is_nullable);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

#define DISPATCH(TYPE)                                                              \
    if (which.idx == TypeIndex::TYPE)                                               \
        return creator_without_type::create<                                        \
                AggregateFunctionTemplate<Data<VT, SingleValueDataDecimal<TYPE>>>>( \
                argument_types, result_is_nullable);
    FOR_DECIMAL_TYPES(DISPATCH)
#undef DISPATCH

    if (which.idx == TypeIndex::String) {
        return creator_without_type::create<
                AggregateFunctionTemplate<Data<VT, SingleValueDataString>>>(argument_types,
                                                                            result_is_nullable);
    }
    if (which.idx == TypeIndex::DateTime || which.idx == TypeIndex::Date) {
        return creator_without_type::create<
                AggregateFunctionTemplate<Data<VT, SingleValueDataFixed<Int64>>>>(
                argument_types, result_is_nullable);
    }
    if (which.idx == TypeIndex::DateV2) {
        return creator_without_type::create<
                AggregateFunctionTemplate<Data<VT, SingleValueDataFixed<UInt32>>>>(
                argument_types, result_is_nullable);
    }
    if (which.idx == TypeIndex::DateTimeV2) {
        return creator_without_type::create<
                AggregateFunctionTemplate<Data<VT, SingleValueDataFixed<UInt64>>>>(
                argument_types, result_is_nullable);
    }
    return nullptr;
}

template <template <typename> class AggregateFunctionTemplate,
          template <typename, typename> class Data>
AggregateFunctionPtr create_aggregate_function_min_max_by(const String& name,
                                                          const DataTypes& argument_types,
                                                          const bool result_is_nullable) {
    if (argument_types.size() != 2) {
        return nullptr;
    }

    WhichDataType which(remove_nullable(argument_types[0]));
#define DISPATCH(TYPE)                                                                    \
    if (which.idx == TypeIndex::TYPE)                                                     \
        return create_aggregate_function_min_max_by_impl<AggregateFunctionTemplate, Data, \
                                                         SingleValueDataFixed<TYPE>>(     \
                argument_types, result_is_nullable);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

#define DISPATCH(TYPE)                                                                    \
    if (which.idx == TypeIndex::TYPE)                                                     \
        return create_aggregate_function_min_max_by_impl<AggregateFunctionTemplate, Data, \
                                                         SingleValueDataDecimal<TYPE>>(   \
                argument_types, result_is_nullable);
    FOR_DECIMAL_TYPES(DISPATCH)
#undef DISPATCH

    if (which.idx == TypeIndex::String) {
        return create_aggregate_function_min_max_by_impl<AggregateFunctionTemplate, Data,
                                                         SingleValueDataString>(argument_types,
                                                                                result_is_nullable);
    }
    if (which.idx == TypeIndex::DateTime || which.idx == TypeIndex::Date) {
        return create_aggregate_function_min_max_by_impl<AggregateFunctionTemplate, Data,
                                                         SingleValueDataFixed<Int64>>(
                argument_types, result_is_nullable);
    }
    if (which.idx == TypeIndex::DateV2) {
        return create_aggregate_function_min_max_by_impl<AggregateFunctionTemplate, Data,
                                                         SingleValueDataFixed<UInt32>>(
                argument_types, result_is_nullable);
    }
    if (which.idx == TypeIndex::DateTimeV2) {
        return create_aggregate_function_min_max_by_impl<AggregateFunctionTemplate, Data,
                                                         SingleValueDataFixed<UInt64>>(
                argument_types, result_is_nullable);
    }
    if (which.idx == TypeIndex::BitMap) {
        return create_aggregate_function_min_max_by_impl<AggregateFunctionTemplate, Data,
                                                         BitmapValueData>(argument_types,
                                                                          result_is_nullable);
    }
    return nullptr;
}

} // namespace doris::vectorized
