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

#include <array>
#include <cmath>
#include <cstdint>
#include <limits>
#include <tuple>
#include <type_traits>

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/field.h"
#include "core/types.h"
#include "exprs/aggregate/aggregate_function.h"

namespace doris {

enum class RegrFunctionKind : uint8_t {
    regr_avgx,
    regr_avgy,
    regr_count,
    regr_slope,
    regr_intercept,
    regr_sxx,
    regr_syy,
    regr_sxy,
    regr_r2,
};

template <RegrFunctionKind kind>
struct RegrTraits;

template <>
struct RegrTraits<RegrFunctionKind::regr_avgx> {
    static constexpr auto name = "regr_avgx";
    static constexpr size_t sx_level = 1;
    static constexpr size_t sy_level = 0;
    static constexpr bool use_sxy = false;
};

template <>
struct RegrTraits<RegrFunctionKind::regr_avgy> {
    static constexpr auto name = "regr_avgy";
    static constexpr size_t sx_level = 0;
    static constexpr size_t sy_level = 1;
    static constexpr bool use_sxy = false;
};

template <>
struct RegrTraits<RegrFunctionKind::regr_count> {
    static constexpr auto name = "regr_count";
    static constexpr size_t sx_level = 0;
    static constexpr size_t sy_level = 0;
    static constexpr bool use_sxy = false;
};

template <>
struct RegrTraits<RegrFunctionKind::regr_slope> {
    static constexpr auto name = "regr_slope";
    static constexpr size_t sx_level = 2;
    static constexpr size_t sy_level = 1;
    static constexpr bool use_sxy = true;
};

template <>
struct RegrTraits<RegrFunctionKind::regr_intercept> {
    static constexpr auto name = "regr_intercept";
    static constexpr size_t sx_level = 2;
    // Keep `syy` to preserve regr_intercept's historical serialized state layout.
    static constexpr size_t sy_level = 2;
    static constexpr bool use_sxy = true;
};

template <>
struct RegrTraits<RegrFunctionKind::regr_sxx> {
    static constexpr auto name = "regr_sxx";
    static constexpr size_t sx_level = 2;
    static constexpr size_t sy_level = 0;
    static constexpr bool use_sxy = false;
};

template <>
struct RegrTraits<RegrFunctionKind::regr_syy> {
    static constexpr auto name = "regr_syy";
    static constexpr size_t sx_level = 0;
    static constexpr size_t sy_level = 2;
    static constexpr bool use_sxy = false;
};

template <>
struct RegrTraits<RegrFunctionKind::regr_sxy> {
    static constexpr auto name = "regr_sxy";
    static constexpr size_t sx_level = 1;
    static constexpr size_t sy_level = 1;
    static constexpr bool use_sxy = true;
};

template <>
struct RegrTraits<RegrFunctionKind::regr_r2> {
    static constexpr auto name = "regr_r2";
    static constexpr size_t sx_level = 2;
    static constexpr size_t sy_level = 2;
    static constexpr bool use_sxy = true;
};

template <PrimitiveType T, RegrFunctionKind kind>
struct AggregateFunctionRegrData {
    using Traits = RegrTraits<kind>;
    static constexpr PrimitiveType Type = T;

    static_assert(Traits::sx_level <= 2 && Traits::sy_level <= 2, "sx/sy level must be <= 2");
    static_assert(!Traits::use_sxy || (Traits::sx_level > 0 && Traits::sy_level > 0),
                  "sxy requires sx_level > 0 and sy_level > 0");

    static constexpr bool has_sx = Traits::sx_level > 0;
    static constexpr bool has_sy = Traits::sy_level > 0;
    static constexpr bool has_sxx = Traits::sx_level > 1;
    static constexpr bool has_syy = Traits::sy_level > 1;
    static constexpr bool has_sxy = Traits::use_sxy;

    static constexpr size_t k_num_moments = Traits::sx_level + Traits::sy_level + size_t {has_sxy};
    static constexpr bool has_moments = k_num_moments > 0;
    static_assert(k_num_moments <= 5, "Unexpected size of regr moment array");

    using State = std::conditional_t<has_moments,
                                     // (n, moments)
                                     std::tuple<UInt64, std::array<Float64, k_num_moments>>,
                                     // (n)
                                     std::tuple<UInt64>>;

    /**
     * The aggregate state stores:
     *     N   = count
     *
     * The following moments are stored only when enabled by RegrTraits<kind>:
     *     Sx  = sum(X)
     *     Sy  = sum(Y)
     *     Sxx = sum((X-Sx/N)^2)
     *     Syy = sum((Y-Sy/N)^2)
     *     Sxy = sum((X-Sx/N)*(Y-Sy/N))
     */
    State state {};

    auto& moments() {
        static_assert(has_moments, "moments not enabled");
        return std::get<1>(state);
    }

    const auto& moments() const {
        static_assert(has_moments, "moments not enabled");
        return std::get<1>(state);
    }

    static constexpr size_t sx_index() {
        static_assert(has_sx, "sx not enabled");
        return 0;
    }

    static constexpr size_t sy_index() {
        static_assert(has_sy, "sy not enabled");
        return size_t {has_sx};
    }

    static constexpr size_t sxx_index() {
        static_assert(has_sxx, "sxx not enabled");
        return size_t {has_sx + has_sy};
    }

    static constexpr size_t syy_index() {
        static_assert(has_syy, "syy not enabled");
        return size_t {has_sx + has_sy + has_sxx};
    }

    static constexpr size_t sxy_index() {
        static_assert(has_sxy, "sxy not enabled");
        return size_t {has_sx + has_sy + has_sxx + has_syy};
    }

    UInt64& n() { return std::get<0>(state); }
    Float64& sx() { return moments()[sx_index()]; }
    Float64& sy() { return moments()[sy_index()]; }
    Float64& sxx() { return moments()[sxx_index()]; }
    Float64& syy() { return moments()[syy_index()]; }
    Float64& sxy() { return moments()[sxy_index()]; }

    const UInt64& n() const { return std::get<0>(state); }
    const Float64& sx() const { return moments()[sx_index()]; }
    const Float64& sy() const { return moments()[sy_index()]; }
    const Float64& sxx() const { return moments()[sxx_index()]; }
    const Float64& syy() const { return moments()[syy_index()]; }
    const Float64& sxy() const { return moments()[sxy_index()]; }

    void write(BufferWritable& buf) const {
        if constexpr (has_sx) {
            buf.write_binary(sx());
        }
        if constexpr (has_sy) {
            buf.write_binary(sy());
        }
        if constexpr (has_sxx) {
            buf.write_binary(sxx());
        }
        if constexpr (has_syy) {
            buf.write_binary(syy());
        }
        if constexpr (has_sxy) {
            buf.write_binary(sxy());
        }
        buf.write_binary(n());
    }

    void read(BufferReadable& buf) {
        if constexpr (has_sx) {
            buf.read_binary(sx());
        }
        if constexpr (has_sy) {
            buf.read_binary(sy());
        }
        if constexpr (has_sxx) {
            buf.read_binary(sxx());
        }
        if constexpr (has_syy) {
            buf.read_binary(syy());
        }
        if constexpr (has_sxy) {
            buf.read_binary(sxy());
        }
        buf.read_binary(n());
    }

    void reset() {
        if constexpr (has_moments) {
            moments().fill({});
        }
        n() = {};
    }

    /**
     * The merge function uses the Youngs-Cramer algorithm:
     *     N   = N1 + N2
     *     Sx  = Sx1 + Sx2
     *     Sy  = Sy1 + Sy2
     *     Sxx = Sxx1 + Sxx2 + N1 * N2 * (Sx1/N1 - Sx2/N2)^2 / N
     *     Syy = Syy1 + Syy2 + N1 * N2 * (Sy1/N1 - Sy2/N2)^2 / N
     *     Sxy = Sxy1 + Sxy2 + N1 * N2 * (Sx1/N1 - Sx2/N2) * (Sy1/N1 - Sy2/N2) / N
     */
    void merge(const AggregateFunctionRegrData& rhs) {
        if (rhs.n() == 0) {
            return;
        }
        if (n() == 0) {
            *this = rhs;
            return;
        }
        const auto n1 = static_cast<Float64>(n());
        const auto n2 = static_cast<Float64>(rhs.n());
        const auto nsum = n1 + n2;

        Float64 dx {};
        Float64 dy {};
        if constexpr (has_sxx || has_sxy) {
            dx = sx() / n1 - rhs.sx() / n2;
        }
        if constexpr (has_syy || has_sxy) {
            dy = sy() / n1 - rhs.sy() / n2;
        }

        n() += rhs.n();
        if constexpr (has_sx) {
            sx() += rhs.sx();
        }
        if constexpr (has_sy) {
            sy() += rhs.sy();
        }
        if constexpr (has_sxx) {
            sxx() += rhs.sxx() + n1 * n2 * dx * dx / nsum;
        }
        if constexpr (has_syy) {
            syy() += rhs.syy() + n1 * n2 * dy * dy / nsum;
        }
        if constexpr (has_sxy) {
            sxy() += rhs.sxy() + n1 * n2 * dx * dy / nsum;
        }
    }

    /**
     * N   = count
     * Sx  = sum(X)
     * Sy  = sum(Y)
     * Sxx = sum((X-Sx/N)^2)
     * Syy = sum((Y-Sy/N)^2)
     * Sxy = sum((X-Sx/N)*(Y-Sy/N))
     */
    void add(typename PrimitiveTypeTraits<Type>::CppType value_y,
             typename PrimitiveTypeTraits<Type>::CppType value_x) {
        const auto x = static_cast<Float64>(value_x);
        const auto y = static_cast<Float64>(value_y);

        if constexpr (has_sx) {
            sx() += x;
        }
        if constexpr (has_sy) {
            sy() += y;
        }

        if (n() == 0) [[unlikely]] {
            n() = 1;
            return;
        }
        const auto n_old = static_cast<Float64>(n());
        const auto n_new = n_old + 1;
        const auto scale = 1.0 / (n_new * n_old);
        n() += 1;

        Float64 tmp_x {};
        Float64 tmp_y {};
        if constexpr (has_sxx || has_sxy) {
            tmp_x = x * n_new - sx();
        }
        if constexpr (has_syy || has_sxy) {
            tmp_y = y * n_new - sy();
        }

        if constexpr (has_sxx) {
            sxx() += tmp_x * tmp_x * scale;
        }
        if constexpr (has_syy) {
            syy() += tmp_y * tmp_y * scale;
        }
        if constexpr (has_sxy) {
            sxy() += tmp_x * tmp_y * scale;
        }
    }

    auto get_result() const {
        if constexpr (kind == RegrFunctionKind::regr_count) {
            return static_cast<Int64>(n());
        } else if constexpr (kind == RegrFunctionKind::regr_avgx) {
            if (n() < 1) {
                return std::numeric_limits<Float64>::quiet_NaN();
            }
            return sx() / static_cast<Float64>(n());
        } else if constexpr (kind == RegrFunctionKind::regr_avgy) {
            if (n() < 1) {
                return std::numeric_limits<Float64>::quiet_NaN();
            }
            return sy() / static_cast<Float64>(n());
        } else if constexpr (kind == RegrFunctionKind::regr_slope) {
            if (n() < 1 || sxx() == 0.0) {
                return std::numeric_limits<Float64>::quiet_NaN();
            }
            return sxy() / sxx();
        } else if constexpr (kind == RegrFunctionKind::regr_intercept) {
            if (n() < 1 || sxx() == 0.0) {
                return std::numeric_limits<Float64>::quiet_NaN();
            }
            return (sy() - sx() * sxy() / sxx()) / static_cast<Float64>(n());
        } else if constexpr (kind == RegrFunctionKind::regr_sxx) {
            if (n() < 1) {
                return std::numeric_limits<Float64>::quiet_NaN();
            }
            return sxx();
        } else if constexpr (kind == RegrFunctionKind::regr_syy) {
            if (n() < 1) {
                return std::numeric_limits<Float64>::quiet_NaN();
            }
            return syy();
        } else if constexpr (kind == RegrFunctionKind::regr_sxy) {
            if (n() < 1) {
                return std::numeric_limits<Float64>::quiet_NaN();
            }
            return sxy();
        } else if constexpr (kind == RegrFunctionKind::regr_r2) {
            if (n() < 1 || sxx() == 0.0) {
                return std::numeric_limits<Float64>::quiet_NaN();
            }
            if (syy() == 0.0) {
                return 1.0;
            }
            return (sxy() * sxy()) / (sxx() * syy());
        } else {
            __builtin_unreachable();
        }
    }
};

template <PrimitiveType T, RegrFunctionKind kind, bool y_is_nullable, bool x_is_nullable,
          typename Derived>
class AggregateFunctionRegrBase
        : public IAggregateFunctionDataHelper<AggregateFunctionRegrData<T, kind>, Derived> {
public:
    using Data = AggregateFunctionRegrData<T, kind>;
    using InputCol = typename PrimitiveTypeTraits<Data::Type>::ColumnType;

    explicit AggregateFunctionRegrBase(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<Data, Derived>(argument_types_) {
        DCHECK(argument_types_.size() == 2);
    }

    String get_name() const override { return RegrTraits<kind>::name; }

    // Regr aggregates only consume rows where both y and x are non-null.
    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        const auto* y_column = get_nested_column_or_null<y_is_nullable>(columns[0], row_num);
        if constexpr (y_is_nullable) {
            if (y_column == nullptr) {
                return;
            }
        }
        const auto* x_column = get_nested_column_or_null<x_is_nullable>(columns[1], row_num);
        if constexpr (x_is_nullable) {
            if (x_column == nullptr) {
                return;
            }
        }

        this->data(place).add(y_column->get_data()[row_num], x_column->get_data()[row_num]);
    }

    void reset(AggregateDataPtr __restrict place) const override { this->data(place).reset(); }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs,
               Arena&) const override {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, BufferWritable& buf) const override {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, BufferReadable& buf,
                     Arena&) const override {
        this->data(place).read(buf);
    }

protected:
    using IAggregateFunctionDataHelper<Data, Derived>::data;

private:
    template <bool is_nullable>
    static ALWAYS_INLINE const InputCol* get_nested_column_or_null(const IColumn* col,
                                                                   ssize_t row_num) {
        if constexpr (is_nullable) {
            const auto& nullable_column =
                    assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(*col);
            if (nullable_column.is_null_at(row_num)) {
                return nullptr;
            }
            return assert_cast<const InputCol*, TypeCheckOnRelease::DISABLE>(
                    nullable_column.get_nested_column_ptr().get());
        } else {
            return assert_cast<const InputCol*, TypeCheckOnRelease::DISABLE>(col->get_ptr().get());
        }
    }
};

template <PrimitiveType T, RegrFunctionKind kind, bool y_is_nullable, bool x_is_nullable>
class AggregateFunctionRegr final
        : public AggregateFunctionRegrBase<
                  T, kind, y_is_nullable, x_is_nullable,
                  AggregateFunctionRegr<T, kind, y_is_nullable, x_is_nullable>> {
public:
    static_assert(kind != RegrFunctionKind::regr_count,
                  "regr_count uses the AggregateFunctionRegr specialization");

    using ResultDataType = ColumnFloat64;
    using Base =
            AggregateFunctionRegrBase<T, kind, y_is_nullable, x_is_nullable, AggregateFunctionRegr>;

    explicit AggregateFunctionRegr(const DataTypes& argument_types_) : Base(argument_types_) {}

    DataTypePtr get_return_type() const override {
        return make_nullable(std::make_shared<DataTypeFloat64>());
    }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& nullable_column = assert_cast<ColumnNullable&>(to);
        auto& nested_column = assert_cast<ResultDataType&>(nullable_column.get_nested_column());
        const Float64 result = this->data(place).get_result();
        if (std::isnan(result)) {
            nullable_column.get_null_map_data().push_back(1);
            nested_column.insert_default();
        } else {
            nullable_column.get_null_map_data().push_back(0);
            nested_column.get_data().push_back(result);
        }
    }
};

template <PrimitiveType T, bool y_is_nullable, bool x_is_nullable>
class AggregateFunctionRegr<T, RegrFunctionKind::regr_count, y_is_nullable, x_is_nullable> final
        : public AggregateFunctionRegrBase<T, RegrFunctionKind::regr_count, y_is_nullable,
                                           x_is_nullable,
                                           AggregateFunctionRegr<T, RegrFunctionKind::regr_count,
                                                                 y_is_nullable, x_is_nullable>> {
public:
    using ResultDataType = ColumnInt64;
    using Base = AggregateFunctionRegrBase<T, RegrFunctionKind::regr_count, y_is_nullable,
                                           x_is_nullable, AggregateFunctionRegr>;

    explicit AggregateFunctionRegr(const DataTypes& argument_types_) : Base(argument_types_) {}

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        auto& result_column = assert_cast<ResultDataType&>(to);
        result_column.get_data().push_back(this->data(place).get_result());
    }
};

} // namespace doris
