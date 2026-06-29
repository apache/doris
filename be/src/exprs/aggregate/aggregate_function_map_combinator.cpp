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

#include "exprs/aggregate/aggregate_function_map_combinator.h"

#include <string>
#include <string_view>

#include "agent/be_exec_version_manager.h"
#include "core/call_on_type_index.h"
#include "core/column/column_const.h"
#include "core/column/column_decimal.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/string_buffer.hpp"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "exprs/aggregate/factory_helpers.h"
#include "exprs/aggregate/helpers.h"
#include "exprs/function/function_helpers.h"

namespace doris {
namespace {

struct MapStringHash {
    using is_transparent = void;

    size_t operator()(const StringRef& key) const { return StringRefHash()(key); }

    size_t operator()(const std::string& key) const {
        return StringRefHash()(StringRef(key.data(), key.size()));
    }
};

struct MapStringEqual {
    using is_transparent = void;

    static std::string_view to_view(const StringRef& key) {
        return {key.size == 0 ? "" : key.data, key.size};
    }
    static std::string_view to_view(const std::string& key) { return key; }

    template <typename Lhs, typename Rhs>
    bool operator()(const Lhs& lhs, const Rhs& rhs) const {
        return to_view(lhs) == to_view(rhs);
    }
};

std::string nested_function_name(const std::string& name) {
    if (name == "sum_map") {
        return "sum";
    }
    if (name == "avg_map") {
        return "avg";
    }
    if (name == "min_map") {
        return "min";
    }
    if (name == "max_map") {
        return "max";
    }
    if (name == "count_map") {
        return "count";
    }
    throw Exception(ErrorCode::INTERNAL_ERROR, "Unknown map aggregate function {}", name);
}

const DataTypeMap* get_map_type(const DataTypePtr& type) {
    return type ? check_and_get_data_type<DataTypeMap>(remove_nullable(type).get()) : nullptr;
}

DataTypePtr result_value_type_or_argument_value_type(const DataTypePtr& result_type,
                                                     const DataTypeMap& argument_map_type) {
    const auto* result_map_type = get_map_type(result_type);
    if (result_map_type != nullptr) {
        return result_map_type->get_value_type();
    }
    return argument_map_type.get_value_type();
}

AggregateFunctionPtr create_nested_function(const std::string& name, const DataTypeMap& map_type,
                                            const DataTypePtr& result_value_type,
                                            const AggregateFunctionAttr& attr) {
    DataTypes nested_argument_types {map_type.get_value_type()};
    DataTypePtr nested_result_type = remove_nullable(result_value_type);
    const bool nested_result_is_nullable = result_value_type->is_nullable();

    auto nested_function = AggregateFunctionSimpleFactory::instance().get(
            nested_function_name(name), nested_argument_types, nested_result_type,
            nested_result_is_nullable, BeExecVersionManager::get_newest_version(), attr);
    if (nested_function == nullptr) {
        throw Exception(ErrorCode::INTERNAL_ERROR,
                        "Can not create nested aggregate function for {}", name);
    }
    return nested_function;
}

template <PrimitiveType KeyType>
struct AggregateFunctionMapCombinatorDataTyped {
    using KeyColumnType = typename PrimitiveTypeTraits<KeyType>::ColumnType;
    using Key = typename PrimitiveTypeTraits<KeyType>::CppType;
    using StoredKey = std::conditional_t<is_string_type(KeyType), std::string, Key>;
    using LookupKey = std::conditional_t<is_string_type(KeyType), StringRef, Key>;
    using Map = std::conditional_t<
            is_string_type(KeyType),
            flat_hash_map<StoredKey, AggregateDataPtr, MapStringHash, MapStringEqual>,
            flat_hash_map<StoredKey, AggregateDataPtr>>;

    AggregateFunctionMapCombinatorDataTyped() = default;

    void clear() {
        key_to_place.clear();
        null_place = nullptr;
    }

    Map key_to_place;
    AggregateDataPtr null_place = nullptr;
};

template <PrimitiveType KeyType>
class AggregateFunctionMapCombinatorTyped final
        : public IAggregateFunctionDataHelper<AggregateFunctionMapCombinatorDataTyped<KeyType>,
                                              AggregateFunctionMapCombinatorTyped<KeyType>>,
          UnaryExpression,
          NotNullableAggregateFunction {
public:
    using Data = AggregateFunctionMapCombinatorDataTyped<KeyType>;
    using Base = IAggregateFunctionDataHelper<Data, AggregateFunctionMapCombinatorTyped<KeyType>>;
    using KeyColumnType = typename Data::KeyColumnType;
    using LookupKey = typename Data::LookupKey;
    using StoredKey = typename Data::StoredKey;

    AggregateFunctionMapCombinatorTyped(std::string name_, AggregateFunctionPtr nested_function_,
                                        const DataTypes& argument_types_)
            : Base(argument_types_),
              _name(std::move(name_)),
              _nested_function(std::move(nested_function_)) {
        const auto* map_type =
                assert_cast<const DataTypeMap*>(remove_nullable(this->argument_types[0]).get());
        _key_type = make_nullable(map_type->get_key_type());
        _value_type = make_nullable(_nested_function->get_return_type());
    }

    void set_version(const int version_) override {
        Base::set_version(version_);
        _nested_function->set_version(version_);
    }

    String get_name() const override { return _name; }

    DataTypePtr get_return_type() const override {
        return std::make_shared<DataTypeMap>(_key_type, _value_type);
    }

    void create(AggregateDataPtr __restrict place) const override { new (place) Data; }

    void destroy(AggregateDataPtr __restrict place) const noexcept override {
        auto& data_ = this->data(place);
        destroy_nested_places(data_);
        data_.~Data();
    }

    void reset(AggregateDataPtr place) const override {
        auto& data_ = this->data(place);
        destroy_nested_places(data_);
        data_.clear();
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena& arena) const override {
        const IColumn* column = columns[0];
        ssize_t actual_row = row_num;
        if (const auto* const_column = check_and_get_column<ColumnConst>(*column)) {
            column = &const_column->get_data_column();
            actual_row = 0;
        }

        const auto& map_column = assert_cast<const ColumnMap&>(*column);
        const auto& offsets = map_column.get_offsets();
        const size_t offset = actual_row == 0 ? 0 : offsets[actual_row - 1];
        const size_t size = offsets[actual_row] - offset;
        const auto& key_column = map_column.get_keys();
        const auto& value_column = map_column.get_values();
        const IColumn* nested_columns[1] = {&value_column};

        auto& data_ = this->data(place);
        for (size_t i = 0; i != size; ++i) {
            const size_t row = offset + i;
            AggregateDataPtr nested_place = get_or_create_place(data_, key_column, row, arena);
            _nested_function->add(nested_place, nested_columns, row, arena);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena& arena) const override {
        auto& data_ = this->data(place);
        const auto& rhs_data = this->data(rhs);

        if (rhs_data.null_place != nullptr) {
            AggregateDataPtr nested_place = get_or_create_null_place(data_, arena);
            _nested_function->merge(nested_place, rhs_data.null_place, arena);
        }

        for (const auto& [key, rhs_place] : rhs_data.key_to_place) {
            AggregateDataPtr nested_place = get_or_create_place_by_key(data_, key, arena);
            _nested_function->merge(nested_place, rhs_place, arena);
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        const auto& data_ = this->data(place);

        buf.write_binary(data_.null_place != nullptr);
        if (data_.null_place != nullptr) {
            _nested_function->serialize(data_.null_place, buf);
        }

        buf.write_var_uint(data_.key_to_place.size());
        for (const auto& [key, nested_place] : data_.key_to_place) {
            write_key(key, buf);
            _nested_function->serialize(nested_place, buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena& arena) const override {
        auto& data_ = this->data(place);
        DCHECK(data_.key_to_place.empty());
        DCHECK_EQ(data_.null_place, nullptr);

        bool has_null_key = false;
        buf.read_binary(has_null_key);
        if (has_null_key) {
            data_.null_place = create_nested_place(arena);
            _nested_function->deserialize(data_.null_place, buf, arena);
        }

        UInt64 size = 0;
        buf.read_var_uint(size);
        for (UInt64 i = 0; i != size; ++i) {
            StoredKey key;
            read_key(key, buf);
            AggregateDataPtr nested_place = create_nested_place(arena);
            _nested_function->deserialize(nested_place, buf, arena);
            data_.key_to_place.emplace(std::move(key), nested_place);
        }
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& map_column = assert_cast<ColumnMap&>(to);
        auto& key_column = map_column.get_keys();
        auto& value_column = map_column.get_values();
        auto& offsets = map_column.get_offsets();

        const auto& data_ = this->data(place);
        if (data_.null_place != nullptr) {
            assert_cast<ColumnNullable&, TypeCheckOnRelease::DISABLE>(key_column).insert_default();
            insert_nested_result(data_.null_place, value_column);
        }

        for (const auto& [key, nested_place] : data_.key_to_place) {
            insert_key(key, key_column);
            insert_nested_result(nested_place, value_column);
        }

        offsets.push_back(value_column.size());
    }

private:
    AggregateDataPtr create_nested_place(Arena& arena) const {
        auto* nested_place = arena.aligned_alloc(_nested_function->size_of_data(),
                                                 _nested_function->align_of_data());
        _nested_function->create(nested_place);
        return nested_place;
    }

    void destroy_nested_places(Data& data) const noexcept {
        if (data.null_place != nullptr) {
            _nested_function->destroy(data.null_place);
        }

        for (auto& [_, nested_place] : data.key_to_place) {
            _nested_function->destroy(nested_place);
        }
    }

    static bool is_null_key(const IColumn& key_column, size_t row) {
        if (const auto* nullable_column = check_and_get_column<ColumnNullable>(key_column)) {
            return nullable_column->is_null_at(row);
        }
        return false;
    }

    static const IColumn& nested_key_column(const IColumn& key_column) {
        if (const auto* nullable_column = check_and_get_column<ColumnNullable>(key_column)) {
            return nullable_column->get_nested_column();
        }
        return key_column;
    }

    static LookupKey get_key(const IColumn& key_column, size_t row) {
        const auto& typed_column =
                assert_cast<const KeyColumnType&, TypeCheckOnRelease::DISABLE>(key_column);
        if constexpr (is_string_type(KeyType)) {
            return typed_column.get_data_at(row);
        } else {
            return typed_column.get_data()[row];
        }
    }

    static void insert_key(const StoredKey& key, IColumn& key_column) {
        auto& nullable_column =
                assert_cast<ColumnNullable&, TypeCheckOnRelease::DISABLE>(key_column);
        insert_non_null_key(key, nullable_column.get_nested_column());
        nullable_column.get_null_map_data().push_back(0);
    }

    static void insert_non_null_key(const StoredKey& key, IColumn& key_column) {
        auto& typed_column = assert_cast<KeyColumnType&, TypeCheckOnRelease::DISABLE>(key_column);
        if constexpr (is_string_type(KeyType)) {
            typed_column.insert_data(key.data(), key.size());
        } else {
            typed_column.get_data().push_back(key);
        }
    }

    static void write_key(const StoredKey& key, BufferWritable& buf) { buf.write_binary(key); }

    static void read_key(StoredKey& key, BufferReadable& buf) { buf.read_binary(key); }

    static StoredKey stored_key_from_lookup_key(const LookupKey& key) {
        if constexpr (is_string_type(KeyType)) {
            return {key.size == 0 ? "" : key.data, key.size};
        } else {
            return key;
        }
    }

    AggregateDataPtr get_or_create_null_place(Data& data_, Arena& arena) const {
        if (data_.null_place != nullptr) {
            return data_.null_place;
        }

        data_.null_place = create_nested_place(arena);
        return data_.null_place;
    }

    AggregateDataPtr get_or_create_place_by_key(Data& data_, const StoredKey& key,
                                                Arena& arena) const {
        AggregateDataPtr nested_place = nullptr;
        bool inserted = false;
        // Use lazy_emplace to avoid hashing once for find and again for insert.
        auto it = data_.key_to_place.lazy_emplace(key, [&](const auto& ctor) {
            inserted = true;
            nested_place = create_nested_place(arena);
            ctor(key, nested_place);
        });
        if (!inserted) {
            return it->second;
        }
        return nested_place;
    }

    AggregateDataPtr get_or_create_place(Data& data_, const IColumn& key_column, size_t row,
                                         Arena& arena) const {
        if (is_null_key(key_column, row)) {
            return get_or_create_null_place(data_, arena);
        }

        auto key = get_key(nested_key_column(key_column), row);
        AggregateDataPtr nested_place = nullptr;
        bool inserted = false;
        // Use lazy_emplace to avoid hashing once for find and again for insert.
        auto it = data_.key_to_place.lazy_emplace(key, [&](const auto& ctor) {
            inserted = true;
            nested_place = create_nested_place(arena);
            ctor(stored_key_from_lookup_key(key), nested_place);
        });
        if (!inserted) {
            return it->second;
        }

        return nested_place;
    }

    void insert_nested_result(ConstAggregateDataPtr nested_place, IColumn& value_column) const {
        if (_nested_function->get_return_type()->is_nullable()) {
            _nested_function->insert_result_into(nested_place, value_column);
            return;
        }

        if (auto* nullable_column = check_and_get_column<ColumnNullable>(value_column)) {
            _nested_function->insert_result_into(nested_place,
                                                 nullable_column->get_nested_column());
            nullable_column->get_null_map_data().push_back(0);
            return;
        }

        _nested_function->insert_result_into(nested_place, value_column);
    }

    std::string _name;
    AggregateFunctionPtr _nested_function;
    DataTypePtr _key_type;
    DataTypePtr _value_type;
};

template <PrimitiveType KeyType>
AggregateFunctionPtr create_aggregate_function_map_combinator_typed(
        const std::string& name, const DataTypes& argument_types, const bool result_is_nullable,
        const AggregateFunctionAttr& attr, AggregateFunctionPtr nested_function) {
    return creator_without_type::create<AggregateFunctionMapCombinatorTyped<KeyType>>(
            argument_types, result_is_nullable, attr, name, nested_function);
}

AggregateFunctionPtr create_aggregate_function_map_combinator(const std::string& name,
                                                              const DataTypes& argument_types,
                                                              const DataTypePtr& result_type,
                                                              const bool result_is_nullable,
                                                              const AggregateFunctionAttr& attr) {
    assert_arity_range(name, argument_types, 1, 1);

    const auto* map_type = get_map_type(argument_types[0]);
    if (map_type == nullptr) {
        LOG(WARNING) << fmt::format("unsupported input type {} for aggregate function {}",
                                    argument_types[0]->get_name(), name);
        return nullptr;
    }

    DataTypePtr result_value_type =
            result_value_type_or_argument_value_type(result_type, *map_type);
    AggregateFunctionPtr nested_function =
            create_nested_function(name, *map_type, result_value_type, attr);

    AggregateFunctionPtr typed_function;
    auto call = [&](const auto& type) -> bool {
        using DispatchType = std::decay_t<decltype(type)>;
        typed_function = create_aggregate_function_map_combinator_typed<DispatchType::PType>(
                name, argument_types, result_is_nullable, attr, nested_function);
        return true;
    };

    const PrimitiveType key_type = remove_nullable(map_type->get_key_type())->get_primitive_type();
    DORIS_CHECK(dispatch_switch_all(key_type, call))
            << fmt::format("unsupported map key type {} for aggregate function {}",
                           map_type->get_key_type()->get_name(), name);
    return typed_function;
}

} // namespace

void register_aggregate_function_map_combinator(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("sum_map", create_aggregate_function_map_combinator);
    factory.register_function_both("avg_map", create_aggregate_function_map_combinator);
    factory.register_function_both("min_map", create_aggregate_function_map_combinator);
    factory.register_function_both("max_map", create_aggregate_function_map_combinator);
    factory.register_function_both("count_map", create_aggregate_function_map_combinator);
}

} // namespace doris
