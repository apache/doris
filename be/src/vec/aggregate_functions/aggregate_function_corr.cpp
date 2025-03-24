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
    T meanX {};
    T meanY {};
    T c2 {};
    T m2X {};
    T m2Y {};
    int64_t count {0};

    void add(T x, T y) {
        ++count;
        T deltaX = x - meanX;
        T deltaY = y - meanY;
        meanX += deltaX / count;
        meanY += deltaY / count;
        c2 += deltaX * (y - meanY);
        m2X += deltaX * (x - meanX);
        m2Y += deltaY * (y - meanY);
    }

    void merge(const CorrMoment& rhs) {
        if (rhs.count == 0) return;
        if (count == 0) {
            *this = rhs;
            return;
        }

        T deltaX = rhs.meanX - meanX;
        T deltaY = rhs.meanY - meanY;
        int64_t total_count = count + rhs.count;

        meanX = (meanX * count + rhs.meanX * rhs.count) / total_count;
        meanY = (meanY * count + rhs.meanY * rhs.count) / total_count;

        c2 += rhs.c2 + deltaX * deltaY * count * rhs.count / total_count;
        m2X += rhs.m2X + deltaX * deltaX * count * rhs.count / total_count;
        m2Y += rhs.m2Y + deltaY * deltaY * count * rhs.count / total_count;

        count = total_count;
    }

    void write(BufferWritable& buf) const {
        write_binary(meanX, buf);
        write_binary(meanY, buf);
        write_binary(c2, buf);
        write_binary(m2X, buf);
        write_binary(m2Y, buf);
        write_binary(count, buf);
    }

    void read(BufferReadable& buf) {
        read_binary(meanX, buf);
        read_binary(meanY, buf);
        read_binary(c2, buf);
        read_binary(m2X, buf);
        read_binary(m2Y, buf);
        read_binary(count, buf);
    }

    T get() const {
        if (m2X == 0 || m2Y == 0) {
            return 0;
        }
        return c2 / sqrt(m2X * m2Y);
    }

    static String name() { return "corr"; }

    void reset() {
        meanX = {};
        meanY = {};
        c2 = {};
        m2X = {};
        m2Y = {};
        count = 0;
    }
};

AggregateFunctionPtr create_aggregate_corr_function(const std::string& name,
                                                    const DataTypes& argument_types,
                                                    const bool result_is_nullable,
                                                    const AggregateFunctionAttr& attr) {
    assert_binary(name, argument_types);
    return create_with_two_basic_numeric_types<CorrMoment>(argument_types[0], argument_types[1],
                                                           argument_types, result_is_nullable);
}

void register_aggregate_functions_corr(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("corr", create_aggregate_corr_function);
}

} // namespace doris::vectorized
