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

#include <cmath>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <PrimitiveType T,
          // requires Sx and Sy
          bool NeedSxy,
          // level 1: Sx
          // level 2: Sxx
          size_t SxLevel = size_t {NeedSxy},
          // level 1: Sy
          // level 2: Syy
          size_t SyLevel = size_t {NeedSxy}>
struct AggregateFunctionRegrData {
    static constexpr PrimitiveType Type = T;

    static_assert(!NeedSxy || (SxLevel > 0 && SyLevel > 0),
                  "NeedSxy requires SxLevel > 0 and SyLevel > 0");
    static_assert(SxLevel <= 2 && SyLevel <= 2, "Sx/Sy level must be <= 2");

    static constexpr bool need_sx = SxLevel > 0;
    static constexpr bool need_sy = SyLevel > 0;
    static constexpr bool need_sxx = SxLevel > 1;
    static constexpr bool need_syy = SyLevel > 1;
    static constexpr bool need_sxy = NeedSxy;

    static constexpr size_t kMomentSize = SxLevel + SyLevel + size_t {need_sxy};
    static_assert(kMomentSize > 0 && kMomentSize <= 5, "Unexpected size of regr moment array");

    /**
     * The moments array is:
     *     Sx  = sum(X)
     *     Sy  = sum(Y)
     *     Sxx = sum((X-Sx/N)^2)
     *     Syy = sum((Y-Sy/N)^2)
     *     Sxy = sum((X-Sx/N)*(Y-Sy/N))
    */
    std::array<Float64, kMomentSize> moments {};
    UInt64 n {};

    static constexpr size_t idx_sx() {
        static_assert(need_sx, "sx not enabled");
        return 0;
    }
    static constexpr size_t idx_sy() {
        static_assert(need_sy, "sy not enabled");
        return size_t {need_sx};
    }
    static constexpr size_t idx_sxx() {
        static_assert(need_sxx, "sxx not enabled");
        return size_t {need_sx + need_sy};
    }
    static constexpr size_t idx_syy() {
        static_assert(need_syy, "syy not enabled");
        return size_t {need_sx + need_sy + need_sxx};
    }
    static constexpr size_t idx_sxy() {
        static_assert(need_sxy, "sxy not enabled");
        return size_t {need_sx + need_sy + need_sxx + need_syy};
    }

    Float64& sx() { return moments[idx_sx()]; }
    Float64& sy() { return moments[idx_sy()]; }
    Float64& sxx() { return moments[idx_sxx()]; }
    Float64& syy() { return moments[idx_syy()]; }
    Float64& sxy() { return moments[idx_sxy()]; }

    const Float64& sx() const { return moments[idx_sx()]; }
    const Float64& sy() const { return moments[idx_sy()]; }
    const Float64& sxx() const { return moments[idx_sxx()]; }
    const Float64& syy() const { return moments[idx_syy()]; }
    const Float64& sxy() const { return moments[idx_sxy()]; }

    void write(BufferWritable& buf) const {
        if constexpr (need_sx) {
            buf.write_binary(sx());
        }
        if constexpr (need_sy) {
            buf.write_binary(sy());
        }
        if constexpr (need_sxx) {
            buf.write_binary(sxx());
        }
        if constexpr (need_syy) {
            buf.write_binary(syy());
        }
        if constexpr (need_sxy) {
            buf.write_binary(sxy());
        }
        buf.write_binary(n);
    }

    void read(BufferReadable& buf) {
        if constexpr (need_sx) {
            buf.read_binary(sx());
        }
        if constexpr (need_sy) {
            buf.read_binary(sy());
        }
        if constexpr (need_sxx) {
            buf.read_binary(sxx());
        }
        if constexpr (need_syy) {
            buf.read_binary(syy());
        }
        if constexpr (need_sxy) {
            buf.read_binary(sxy());
        }
        buf.read_binary(n);
    }

    void reset() {
        moments.fill({});
        n = {};
    }

    /**
     * The merge function uses the Youngs–Cramer algorithm:
     *     N   = N1 + N2
     *     Sx  = Sx1 + Sx2
     *     Sy  = Sy1 + Sy2
     *     Sxx = Sxx1 + Sxx2 + N1 * N2 * (Sx1/N1 - Sx2/N2)^2 / N
     *     Syy = Syy1 + Syy2 + N1 * N2 * (Sy1/N1 - Sy2/N2)^2 / N
     *     Sxy = Sxy1 + Sxy2 + N1 * N2 * (Sx1/N1 - Sx2/N2) * (Sy1/N1 - Sy2/N2) / N
     */
    void merge(const AggregateFunctionRegrData& rhs) {
        if (rhs.n == 0) {
            return;
        }
        if (n == 0) {
            *this = rhs;
            return;
        }
        const auto n1 = static_cast<Float64>(n);
        const auto n2 = static_cast<Float64>(rhs.n);
        const auto nsum = n1 + n2;

        Float64 dx {};
        Float64 dy {};
        if constexpr (need_sxx || need_sxy) {
            dx = sx() / n1 - rhs.sx() / n2;
        }
        if constexpr (need_syy || need_sxy) {
            dy = sy() / n1 - rhs.sy() / n2;
        }

        n += rhs.n;
        if constexpr (need_sx) {
            sx() += rhs.sx();
        }
        if constexpr (need_sy) {
            sy() += rhs.sy();
        }
        if constexpr (need_sxx) {
            sxx() += rhs.sxx() + n1 * n2 * dx * dx / nsum;
        }
        if constexpr (need_syy) {
            syy() += rhs.syy() + n1 * n2 * dy * dy / nsum;
        }
        if constexpr (need_sxy) {
            sxy() += rhs.sxy() + n1 * n2 * dx * dy / nsum;
        }
    }

    /**
     * N
     * Sx  = sum(X)
     * Sy  = sum(Y)
     * Sxx = sum((X-Sx/N)^2)
     * Syy = sum((Y-Sy/N)^2)
     * Sxy = sum((X-Sx/N)*(Y-Sy/N))
     */
    void add(typename PrimitiveTypeTraits<T>::CppType value_y,
             typename PrimitiveTypeTraits<T>::CppType value_x) {
        const auto x = static_cast<Float64>(value_x);
        const auto y = static_cast<Float64>(value_y);

        if constexpr (need_sx) {
            sx() += x;
        }
        if constexpr (need_sy) {
            sy() += y;
        }

        if (n == 0) [[unlikely]] {
            n = 1;
            return;
        }
        const auto n_old = static_cast<Float64>(n);
        const auto n_new = n_old + 1;
        const auto scale = 1.0 / (n_new * n_old);
        n += 1;

        Float64 tmp_x {};
        Float64 tmp_y {};
        if constexpr (need_sxx || need_sxy) {
            tmp_x = x * n_new - sx();
        }
        if constexpr (need_syy || need_sxy) {
            tmp_y = y * n_new - sy();
        }

        if constexpr (need_sxx) {
            sxx() += tmp_x * tmp_x * scale;
        }
        if constexpr (need_syy) {
            syy() += tmp_y * tmp_y * scale;
        }
        if constexpr (need_sxy) {
            sxy() += tmp_x * tmp_y * scale;
        }
    }
};

template <PrimitiveType T>
struct RegrSlopeFunc : AggregateFunctionRegrData<T, true, 2, 1> {
    static constexpr const char* name = "regr_slope";

    Float64 get_result() const {
        if (this->n < 1 || this->sxx() == 0.0) {
            return std::numeric_limits<Float64>::quiet_NaN();
        }
        return this->sxy() / this->sxx();
    }
};

template <PrimitiveType T>
struct RegrInterceptFunc : AggregateFunctionRegrData<T, true, 2, 2> {
    static constexpr const char* name = "regr_intercept";

    Float64 get_result() const {
        if (this->n < 1 || this->sxx() == 0.0) {
            return std::numeric_limits<Float64>::quiet_NaN();
        }
        return (this->sy() - this->sx() * this->sxy() / this->sxx()) /
               static_cast<Float64>(this->n);
    }
};

template <PrimitiveType T>
struct RegrSxxFunc : AggregateFunctionRegrData<T, false, 2, 0> {
    static constexpr const char* name = "regr_sxx";

    Float64 get_result() const {
        if (this->n < 1) {
            return std::numeric_limits<Float64>::quiet_NaN();
        }
        return this->sxx();
    }
};

template <PrimitiveType T>
struct RegrSyyFunc : AggregateFunctionRegrData<T, false, 0, 2> {
    static constexpr const char* name = "regr_syy";

    Float64 get_result() const {
        if (this->n < 1) {
            return std::numeric_limits<Float64>::quiet_NaN();
        }
        return this->syy();
    }
};

template <PrimitiveType T>
struct RegrSxyFunc : AggregateFunctionRegrData<T, true, 1, 1> {
    static constexpr const char* name = "regr_sxy";

    Float64 get_result() const {
        if (this->n < 1) {
            return std::numeric_limits<Float64>::quiet_NaN();
        }
        return this->sxy();
    }
};

template <typename RegrFunc, bool y_nullable, bool x_nullable>
class AggregateFunctionRegrSimple
        : public IAggregateFunctionDataHelper<
                  RegrFunc, AggregateFunctionRegrSimple<RegrFunc, y_nullable, x_nullable>> {
public:
    using InputCol = typename PrimitiveTypeTraits<RegrFunc::Type>::ColumnType;
    using ResultCol = ColumnFloat64;

    explicit AggregateFunctionRegrSimple(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper<
                      RegrFunc, AggregateFunctionRegrSimple<RegrFunc, y_nullable, x_nullable>>(
                      argument_types_) {
        DCHECK(!argument_types_.empty());
    }

    String get_name() const override { return RegrFunc::name; }

    DataTypePtr get_return_type() const override {
        return make_nullable(std::make_shared<DataTypeFloat64>());
    }

    void add(AggregateDataPtr __restrict place, const IColumn** columns, ssize_t row_num,
             Arena&) const override {
        const auto* y_col = nested_or_null<y_nullable>(columns[0], row_num);
        if constexpr (y_nullable) {
            if (y_col == nullptr) {
                return;
            }
        }
        const auto* x_col = nested_or_null<x_nullable>(columns[1], row_num);
        if constexpr (x_nullable) {
            if (x_col == nullptr) {
                return;
            }
        }

        this->data(place).add(y_col->get_data()[row_num], x_col->get_data()[row_num]);
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

    void insert_result_into(ConstAggregateDataPtr __restrict place, IColumn& to) const override {
        const auto& data = this->data(place);
        auto& dst_column_with_nullable = assert_cast<ColumnNullable&>(to);
        auto& dst_column = assert_cast<ResultCol&>(dst_column_with_nullable.get_nested_column());
        Float64 result = data.get_result();
        if (std::isnan(result)) {
            dst_column_with_nullable.get_null_map_data().push_back(1);
            dst_column.insert_default();
        } else {
            dst_column_with_nullable.get_null_map_data().push_back(0);
            dst_column.get_data().push_back(result);
        }
    }

private:
    template <bool Nullable>
    static ALWAYS_INLINE const InputCol* nested_or_null(const IColumn* col, ssize_t row_num) {
        if constexpr (Nullable) {
            const auto& c = assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(*col);
            if (c.is_null_at(row_num)) {
                return nullptr;
            }
            return assert_cast<const InputCol*, TypeCheckOnRelease::DISABLE>(
                    c.get_nested_column_ptr().get());
        } else {
            return assert_cast<const InputCol*, TypeCheckOnRelease::DISABLE>(col->get_ptr().get());
        }
    }
};
} // namespace doris::vectorized

#include "common/compile_check_end.h"
