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
#include "core/assert_cast.h"
#include "core/call_on_type_index.h"
#include "core/column/column_complex.h"
#include "core/column/column_decimal.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_bitmap.h"
#include "core/data_type/primitive_type.h"
#include "core/value/bitmap_value.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/aggregate/aggregate_function_min_max.h"
#include "exprs/aggregate/helpers.h"

namespace doris {

struct MaxMinValueBase {
    virtual ~MaxMinValueBase() = default;
    virtual void write(BufferWritable& buf, const DataTypePtr& data_type,
                       int be_exec_version) const = 0;
    virtual void read(BufferReadable& buf, const DataTypePtr& data_type, int be_exec_version,
                      Arena& arena) = 0;
    virtual void insert_result_into(IColumn& to) const = 0;
    virtual void reset() = 0;
    virtual void set(const IColumn& column, size_t row_num, Arena& arena) = 0;
    virtual void set(const MaxMinValueBase& to, Arena& arena) = 0;
};

template <typename VT>
struct MaxMinValue : public MaxMinValueBase {
    VT value;

    MaxMinValue() = default;

    ~MaxMinValue() override = default;

    void write(BufferWritable& buf, const DataTypePtr& data_type,
               int be_exec_version) const override {
        value.write(buf, data_type, be_exec_version);
    }

    void read(BufferReadable& buf, const DataTypePtr& data_type, int be_exec_version,
              Arena& arena) override {
        value.read(buf, data_type, be_exec_version, arena);
    }

    void insert_result_into(IColumn& to) const override { value.insert_result_into(to); }

    void reset() override { value.reset(); }

    void set(const IColumn& column, size_t row_num, Arena& arena) override {
        value.set(column, row_num, arena);
    }

    void set(const MaxMinValueBase& to, Arena& arena) override {
        const auto& derived = assert_cast<const MaxMinValue&>(to);
        value.set(derived.value, arena);
    }
};

std::unique_ptr<MaxMinValueBase> create_max_min_value(const DataTypePtr& type);

/// For bitmap value
struct BitmapValueData {
private:
    using Self = BitmapValueData;
    bool has_value = false;
    BitmapValue value;

public:
    BitmapValueData() = default;
    [[nodiscard]] bool has() const { return has_value; }

    void insert_result_into(IColumn& to) const {
        if (has()) {
            assert_cast<ColumnBitmap&, TypeCheckOnRelease::DISABLE>(to).get_data().push_back(value);
        } else {
            assert_cast<ColumnBitmap&, TypeCheckOnRelease::DISABLE>(to).insert_default();
        }
    }

    void reset() {
        if (has()) {
            has_value = false;
        }
    }

    void write(BufferWritable& buf, const DataTypePtr&, int) const {
        buf.write_binary(has());
        if (has()) {
            DataTypeBitMap::serialize_as_stream(value, buf);
        }
    }

    void read(BufferReadable& buf, const DataTypePtr&, int, Arena&) {
        buf.read_binary(has_value);
        if (has()) {
            DataTypeBitMap::deserialize_as_stream(value, buf);
        }
    }

    void set(const IColumn& column, size_t row_num, Arena&) {
        has_value = true;
        value = assert_cast<const ColumnBitmap&, TypeCheckOnRelease::DISABLE>(column)
                        .get_data()[row_num];
    }

    void set(const Self& to, Arena&) {
        DORIS_CHECK(to.has());
        has_value = true;
        value = to.value;
    }
};

/**
 * The template parameter KT is introduced here primarily for performance reasons.
 *
 * Without using a template parameter, the key type would have to be
 * std::unique_ptr<MaxMinValueBase>. Since MaxMinValueBase is a polymorphic base
 * class with virtual methods, comparing keys would inevitably involve virtual
 * function calls, which can introduce significant runtime overhead.
 *
 * By making KT a template parameter, the concrete key type is known at compile
 * time, allowing static dispatch and avoiding virtual function calls. This
 * substantially reduces the cost of key comparisons.
 *
 * In contrast, the value type VT is intentionally not made a template parameter.
 * On one hand, templating both key and value types would lead to an n × n
 * explosion in template instantiations, increasing compile time and code size.
 * On the other hand, value objects typically only invoke the set method; for
 * random data, this method is called approximately log(x) times (where x is the
 * data size), making the overhead acceptable.
 */
template <typename KT>
struct AggregateFunctionMinMaxByBaseData {
protected:
    std::unique_ptr<MaxMinValueBase> value;
    KT key;

public:
    AggregateFunctionMinMaxByBaseData() {}

    explicit AggregateFunctionMinMaxByBaseData(const DataTypes& argument_types) {
        value = create_max_min_value(argument_types[0]);
    }

    void insert_result_into(IColumn& to) const { value->insert_result_into(to); }

    void reset() {
        value->reset();
        key.reset();
    }
    void write(BufferWritable& buf, const DataTypePtr& value_type, const DataTypePtr& key_type,
               int be_exec_version) const {
        value->write(buf, value_type, be_exec_version);
        key.write(buf, key_type, be_exec_version);
    }

    void read(BufferReadable& buf, const DataTypePtr& value_type, const DataTypePtr& key_type,
              int be_exec_version, Arena& arena) {
        value->read(buf, value_type, be_exec_version, arena);
        key.read(buf, key_type, be_exec_version, arena);
    }
};

template <typename KT>
struct AggregateFunctionMaxByData : public AggregateFunctionMinMaxByBaseData<KT> {
    using Self = AggregateFunctionMaxByData;

    AggregateFunctionMaxByData() {}

    explicit AggregateFunctionMaxByData(const DataTypes& argument_types)
            : AggregateFunctionMinMaxByBaseData<KT>(argument_types) {}

    void change_if_better(const IColumn& value_column, const IColumn& key_column, size_t row_num,
                          Arena& arena) {
        if (this->key.set_if_greater(key_column, row_num, arena)) {
            this->value->set(value_column, row_num, arena);
        }
    }

    void change_if_better_batch(const IColumn& value_column, const IColumn& key_column,
                                size_t batch_size, Arena& arena) {
        size_t best_pos = batch_size;
        for (size_t i = 0; i < batch_size; ++i) {
            if (this->key.set_if_greater(key_column, i, arena)) {
                best_pos = i;
            }
        }
        if (best_pos < batch_size) {
            this->value->set(value_column, best_pos, arena);
        }
    }

    void change_if_better(const Self& to, Arena& arena) {
        if (this->key.set_if_greater(to.key, arena)) {
            this->value->set(*to.value, arena);
        }
    }

    static const char* name() { return "max_by"; }
};

template <typename KT>
struct AggregateFunctionMinByData : public AggregateFunctionMinMaxByBaseData<KT> {
    using Self = AggregateFunctionMinByData;

    AggregateFunctionMinByData() {}

    explicit AggregateFunctionMinByData(const DataTypes& argument_types)
            : AggregateFunctionMinMaxByBaseData<KT>(argument_types) {}

    void change_if_better(const IColumn& value_column, const IColumn& key_column, size_t row_num,
                          Arena& arena) {
        if (this->key.set_if_smaller(key_column, row_num, arena)) {
            this->value->set(value_column, row_num, arena);
        }
    }

    void change_if_better_batch(const IColumn& value_column, const IColumn& key_column,
                                size_t batch_size, Arena& arena) {
        size_t best_pos = batch_size;
        for (size_t i = 0; i < batch_size; ++i) {
            if (this->key.set_if_smaller(key_column, i, arena)) {
                best_pos = i;
            }
        }
        if (best_pos < batch_size) {
            this->value->set(value_column, best_pos, arena);
        }
    }

    void change_if_better(const Self& to, Arena& arena) {
        if (this->key.set_if_smaller(to.key, arena)) {
            this->value->set(*to.value, arena);
        }
    }

    static const char* name() { return "min_by"; }
};

template <typename Data>
class AggregateFunctionsMinMaxBy final
        : public IAggregateFunctionDataHelper<Data, AggregateFunctionsMinMaxBy<Data>>,
          MultiExpression,
          NullableAggregateFunction {
private:
    const DataTypePtr& _value_type;
    const DataTypePtr& _key_type;

public:
    AggregateFunctionsMinMaxBy(const DataTypes& arguments)
            : IAggregateFunctionDataHelper<Data, AggregateFunctionsMinMaxBy<Data>>(
                      {arguments[0], arguments[1]}),
              _value_type(this->argument_types[0]),
              _key_type(this->argument_types[1]) {}

    void create(AggregateDataPtr __restrict place) const override {
        new (place) Data(IAggregateFunction::argument_types);
    }

    String get_name() const override { return Data::name(); }

    DataTypePtr get_return_type() const override { return _value_type; }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena& arena) const override {
        this->data(place).change_if_better(*columns[0], *columns[1], row_num, arena);
    }

    void add_batch_single_place(size_t batch_size, AggregateDataPtr place, const IColumn** columns,
                                Arena& arena) const override {
        this->data(place).change_if_better_batch(*columns[0], *columns[1], batch_size, arena);
    }

    void reset(AggregateDataPtr place) const override { this->data(place).reset(); }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena& arena) const override {
        this->data(place).change_if_better(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf, _value_type, _key_type, IAggregateFunction::version);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena& arena) const override {
        this->data(place).read(buf, _value_type, _key_type, IAggregateFunction::version, arena);
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        this->data(place).insert_result_into(to);
    }
};

template <template <typename> class Data>
AggregateFunctionPtr create_aggregate_function_min_max_by(const String& name,
                                                          const DataTypes& argument_types,
                                                          const DataTypePtr& result_type,
                                                          const bool result_is_nullable,
                                                          const AggregateFunctionAttr& attr) {
    if (argument_types.size() != 2) {
        return nullptr;
    }

    AggregateFunctionPtr result;
    auto call = [&](const auto& dispatch_type) -> bool {
        using DispatchType = std::decay_t<decltype(dispatch_type)>;
        constexpr auto PT = DispatchType::PType;
        result = creator_without_type::create_multi_arguments<
                AggregateFunctionsMinMaxBy<Data<SingleValueDataFixed<PT>>>>(
                argument_types, result_is_nullable, attr);
        return true;
    };
    // Keep nano dispatch local because the shared scalar dispatcher is also used by templates
    // that assume arithmetic support.
    if (argument_types[1]->get_primitive_type() == TYPE_TIMESTAMP_NS) {
        call(DispatchDataType<TYPE_TIMESTAMP_NS>());
        return result;
    }
    if (dispatch_switch_scalar(argument_types[1]->get_primitive_type(), call)) {
        return result;
    }

    switch (argument_types[1]->get_primitive_type()) {
    case PrimitiveType::TYPE_CHAR:
    case PrimitiveType::TYPE_VARCHAR:
    case PrimitiveType::TYPE_STRING:
        return creator_without_type::create_multi_arguments<
                AggregateFunctionsMinMaxBy<Data<SingleValueDataString>>>(argument_types,
                                                                         result_is_nullable, attr);
    case PrimitiveType::TYPE_ARRAY:
        return creator_without_type::create_multi_arguments<
                AggregateFunctionsMinMaxBy<Data<SingleValueDataColumn>>>(argument_types,
                                                                         result_is_nullable, attr);
    default:
        return nullptr;
    }
}

} // namespace doris
