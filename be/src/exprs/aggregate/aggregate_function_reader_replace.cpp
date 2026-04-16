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

// Self-contained implementation for replace/replace_if_not_null reader/load aggregation.
// Deliberately does NOT include aggregate_function_reader_first_last.h to avoid pulling in
// heavy template machinery shared with the window-function path.

#include "core/column/column_nullable.h"
#include "core/field.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/aggregate/aggregate_function_reader.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"

namespace doris {

// ---------------------------------------------------------------------------
// Storage layer: PointerStore / CopyStore
// Uniform interface: is_null(), set_value<SkipNull>(), insert_into(), reset().
// set_value returns false when SkipNull && the row is null (caller should not
// update _has_value); returns true otherwise.
// ---------------------------------------------------------------------------

// Zero-copy storage: keeps a pointer into the source column (reader path).
template <bool ArgIsNullable>
struct PointerStore {
    const IColumn* _ptr = nullptr;
    size_t _offset = 0;

    bool is_null() const {
        if (_ptr == nullptr) {
            return true;
        }
        if constexpr (ArgIsNullable) {
            return assert_cast<const ColumnNullable*, TypeCheckOnRelease::DISABLE>(_ptr)
                    ->is_null_at(_offset);
        }
        return false;
    }

    template <bool SkipNull>
    bool set_value(const IColumn* column, size_t row) {
        if constexpr (SkipNull && ArgIsNullable) {
            const auto* nc =
                    assert_cast<const ColumnNullable*, TypeCheckOnRelease::DISABLE>(column);
            if (nc->is_null_at(row)) {
                return false;
            }
        }
        _ptr = column;
        _offset = row;
        return true;
    }

    void insert_into(IColumn& to) const {
        if constexpr (ArgIsNullable) {
            const auto* nc = assert_cast<const ColumnNullable*, TypeCheckOnRelease::DISABLE>(_ptr);
            to.insert_from(nc->get_nested_column(), _offset);
        } else {
            to.insert_from(*_ptr, _offset);
        }
    }

    void reset() {
        _ptr = nullptr;
        _offset = 0;
    }
};

// Deep-copy storage: copies the value into a Field (load path).
template <bool ArgIsNullable>
struct CopyStore {
    Field _value;
    bool _is_null = true;

    bool is_null() const { return _is_null; }

    template <bool SkipNull>
    bool set_value(const IColumn* column, size_t row) {
        if constexpr (ArgIsNullable) {
            const auto* nc =
                    assert_cast<const ColumnNullable*, TypeCheckOnRelease::DISABLE>(column);
            if (nc->is_null_at(row)) {
                if constexpr (SkipNull) {
                    return false;
                }
                _is_null = true;
                return true;
            }
            nc->get_nested_column().get(row, _value);
        } else {
            column->get(row, _value);
        }
        _is_null = false;
        return true;
    }

    void insert_into(IColumn& to) const { to.insert(_value); }

    void reset() {
        _is_null = true;
        _value = {};
    }
};

// ---------------------------------------------------------------------------
// Data layer: ReaderReplaceData
// Template params: IsFirst, SkipNull, ArgIsNullable
// IsCopy is derived: reader (IsFirst=true) deep-copies via Field because the source
// column will be reused; load (IsFirst=false) keeps a zero-copy pointer because
// insert_result_into is called while the column is still alive.
// ---------------------------------------------------------------------------
template <bool IsFirst, bool SkipNull, bool ArgIsNullable>
struct ReaderReplaceData {
    static constexpr bool IsCopy = IsFirst;
    using Store = std::conditional_t<IsCopy, CopyStore<ArgIsNullable>, PointerStore<ArgIsNullable>>;

    Store _store;
    bool _has_value = false;

    void add(int64_t row, const IColumn** columns) {
        if constexpr (IsFirst) {
            if (_has_value) {
                return;
            }
        }
        if (_store.template set_value<SkipNull>(columns[0], row)) {
            _has_value = true;
        }
    }

    void insert_result_into(IColumn& to, bool result_is_nullable) const {
        if (result_is_nullable) {
            auto& nullable_col = assert_cast<ColumnNullable&>(to);
            if (!_has_value || _store.is_null()) {
                nullable_col.insert_default();
            } else {
                nullable_col.get_null_map_data().push_back(0);
                _store.insert_into(nullable_col.get_nested_column());
            }
        } else {
            _store.insert_into(to);
        }
    }

    void reset() {
        _has_value = false;
        _store.reset();
    }
};

// ---------------------------------------------------------------------------
// Aggregate function class
// ---------------------------------------------------------------------------
template <bool IsFirst, bool SkipNull, bool ArgIsNullable>
class ReaderReplaceFunction final
        : public IAggregateFunctionDataHelper<
                  ReaderReplaceData<IsFirst, SkipNull, ArgIsNullable>,
                  ReaderReplaceFunction<IsFirst, SkipNull, ArgIsNullable>> {
    using Data = ReaderReplaceData<IsFirst, SkipNull, ArgIsNullable>;

public:
    ReaderReplaceFunction(const DataTypes& argument_types_, bool result_is_nullable)
            : IAggregateFunctionDataHelper<Data,
                                           ReaderReplaceFunction<IsFirst, SkipNull, ArgIsNullable>>(
                      argument_types_),
              _argument_type(argument_types_[0]),
              _result_is_nullable(result_is_nullable) {}

    String get_name() const override { return "reader_replace"; }

    DataTypePtr get_return_type() const override {
        if (_result_is_nullable) {
            return make_nullable(_argument_type);
        }
        return _argument_type;
    }

    void add(AggregateDataPtr place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        this->data(place).add(row_num, columns);
    }

    void insert_result_into(ConstAggregateDataPtr place, IColumn& to) const override {
        this->data(place).insert_result_into(to, _result_is_nullable);
    }

    void reset(AggregateDataPtr place) const override { this->data(place).reset(); }

    void add_range_single_place(int64_t, int64_t, int64_t, int64_t, AggregateDataPtr,
                                const IColumn**, Arena&, UInt8*, UInt8*) const override {
        throw doris::Exception(Status::FatalError(
                "ReaderReplaceFunction does not support add_range_single_place"));
    }
    void merge(AggregateDataPtr, ConstAggregateDataPtr, Arena&) const override {
        throw doris::Exception(Status::FatalError("ReaderReplaceFunction does not support merge"));
    }
    void serialize(ConstAggregateDataPtr, BufferWritable&) const override {
        throw doris::Exception(
                Status::FatalError("ReaderReplaceFunction does not support serialize"));
    }
    void deserialize(AggregateDataPtr, BufferReadable&, Arena&) const override {
        throw doris::Exception(
                Status::FatalError("ReaderReplaceFunction does not support deserialize"));
    }

private:
    DataTypePtr _argument_type;
    bool _result_is_nullable;
};

// ---------------------------------------------------------------------------
// Factory helpers
// ---------------------------------------------------------------------------
template <bool IsFirst, bool SkipNull, bool ArgIsNullable>
static AggregateFunctionPtr create_reader_replace(const std::string& /*name*/,
                                                  const DataTypes& argument_types_,
                                                  const DataTypePtr& /*result_type*/,
                                                  bool result_is_nullable,
                                                  const AggregateFunctionAttr& /*attr*/) {
    return std::make_shared<ReaderReplaceFunction<IsFirst, SkipNull, ArgIsNullable>>(
            argument_types_, result_is_nullable);
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

// only replace function in load/reader do different agg operation.
// because Doris can ensure that the data is globally ordered in reader, but cannot in load
// 1. reader: get the first value of input data  (IsFirst=true  → CopyStore, deep copy)
// 2. load:   get the last  value of input data  (IsFirst=false → PointerStore, zero-copy)
void register_aggregate_function_replace_reader_load(AggregateFunctionSimpleFactory& factory) {
    auto reg = [&](const std::string& name, const std::string& suffix,
                   const AggregateFunctionCreator& creator,
                   bool nullable) { factory.register_function(name + suffix, creator, nullable); };

    //                                         IsFirst SkipNull ArgNullable
    // replace – reader (first, pointer, accept null)
    reg("replace", AGG_READER_SUFFIX, create_reader_replace<true, false, false>, false);
    reg("replace", AGG_READER_SUFFIX, create_reader_replace<true, false, true>, true);

    // replace – load (last, copy, accept null)
    reg("replace", AGG_LOAD_SUFFIX, create_reader_replace<false, false, false>, false);
    reg("replace", AGG_LOAD_SUFFIX, create_reader_replace<false, false, true>, true);

    // replace_if_not_null – reader (first, pointer, skip null)
    reg("replace_if_not_null", AGG_READER_SUFFIX, create_reader_replace<true, true, false>, false);
    reg("replace_if_not_null", AGG_READER_SUFFIX, create_reader_replace<true, true, true>, true);

    // replace_if_not_null – load (last, copy, skip null)
    reg("replace_if_not_null", AGG_LOAD_SUFFIX, create_reader_replace<false, true, false>, false);
    reg("replace_if_not_null", AGG_LOAD_SUFFIX, create_reader_replace<false, true, true>, true);
}

} // namespace doris
