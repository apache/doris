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

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_binary.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/core/types.h"

namespace doris::vectorized {

template <typename T>
struct CorrMoment {
    T m0 {};
    T x1 {};
    T y1 {};
    T xy {};
    T x2 {};
    T y2 {};

    void add(T x, T y) {
        ++m0;
        x1 += x;
        y1 += y;
        xy += x * y;
        x2 += x * x;
        y2 += y * y;
    }

    void merge(const CorrMoment& rhs) {
        m0 += rhs.m0;
        x1 += rhs.x1;
        y1 += rhs.y1;
        xy += rhs.xy;
        x2 += rhs.x2;
        y2 += rhs.y2;
    }

    void write(BufferWritable& buf) const {
        write_binary(m0, buf);
        write_binary(x1, buf);
        write_binary(y1, buf);
        write_binary(xy, buf);
        write_binary(x2, buf);
        write_binary(y2, buf);
    }

    void read(BufferReadable& buf) {
        read_binary(m0, buf);
        read_binary(x1, buf);
        read_binary(y1, buf);
        read_binary(xy, buf);
        read_binary(x2, buf);
        read_binary(y2, buf);
    }

    T get() const {
        // avoid float error(silent nan) when x or y is constant
        if (m0 * x2 <= x1 * x1 || m0 * y2 <= y1 * y1) [[unlikely]] {
            return 0;
        }
        return (m0 * xy - x1 * y1) / sqrt((m0 * x2 - x1 * x1) * (m0 * y2 - y1 * y1));
    }

    static String name() { return "corr"; }
};

AggregateFunctionPtr create_aggregate_corr_function(const std::string& name,
                                                    const DataTypes& argument_types,
                                                    const bool result_is_nullable) {
    assert_binary(name, argument_types);
    return create_with_two_basic_numeric_types<CorrMoment>(argument_types[0], argument_types[1],
                                                           argument_types, result_is_nullable);
}

void register_aggregate_functions_corr(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("corr", create_aggregate_corr_function);
}

} // namespace doris::vectorized
