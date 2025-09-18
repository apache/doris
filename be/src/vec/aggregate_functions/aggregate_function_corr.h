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

#include "vec/core/types.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

template <PrimitiveType Type>
struct CorrMoment {
    using T = typename PrimitiveTypeTraits<Type>::CppType;
    static_assert(std::is_same_v<T, double>, "CorrMoment only support double");
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
        buf.write_binary(m0);
        buf.write_binary(x1);
        buf.write_binary(y1);
        buf.write_binary(xy);
        buf.write_binary(x2);
        buf.write_binary(y2);
    }

    void read(BufferReadable& buf) {
        buf.read_binary(m0);
        buf.read_binary(x1);
        buf.read_binary(y1);
        buf.read_binary(xy);
        buf.read_binary(x2);
        buf.read_binary(y2);
    }

    T get() const {
        // avoid float error(silent nan) when x or y is constant
        if (m0 * x2 <= x1 * x1 || m0 * y2 <= y1 * y1) [[unlikely]] {
            return 0;
        }
        return (m0 * xy - x1 * y1) / sqrt((m0 * x2 - x1 * x1) * (m0 * y2 - y1 * y1));
    }

    static String name() { return "corr"; }

    void reset() {
        m0 = {};
        x1 = {};
        y1 = {};
        xy = {};
        x2 = {};
        y2 = {};
    }
};

template <PrimitiveType T>
struct CorrMomentWelford {
    static_assert(std::is_same_v<typename PrimitiveTypeTraits<T>::CppType, double>,
                  "CorrMomentWelford only support double");
    double meanX {};
    double meanY {};
    double c2 {};
    double m2X {};
    double m2Y {};
    int64_t count {0};

    void add(double x, double y) {
        ++count;
        double deltaX = x - meanX;
        double deltaY = y - meanY;
        meanX += deltaX / count;
        meanY += deltaY / count;
        c2 += deltaX * (y - meanY);
        m2X += deltaX * (x - meanX);
        m2Y += deltaY * (y - meanY);
    }

    void merge(const CorrMomentWelford& rhs) {
        if (rhs.count == 0) return;
        if (count == 0) {
            *this = rhs;
            return;
        }

        double deltaX = rhs.meanX - meanX;
        double deltaY = rhs.meanY - meanY;
        int64_t total_count = count + rhs.count;

        meanX = (meanX * count + rhs.meanX * rhs.count) / total_count;
        meanY = (meanY * count + rhs.meanY * rhs.count) / total_count;

        c2 += rhs.c2 + deltaX * deltaY * count * rhs.count / total_count;
        m2X += rhs.m2X + deltaX * deltaX * count * rhs.count / total_count;
        m2Y += rhs.m2Y + deltaY * deltaY * count * rhs.count / total_count;

        count = total_count;
    }

    void write(BufferWritable& buf) const {
        buf.write_binary(meanX);
        buf.write_binary(meanY);
        buf.write_binary(c2);
        buf.write_binary(m2X);
        buf.write_binary(m2Y);
        buf.write_binary(count);
    }

    void read(BufferReadable& buf) {
        buf.read_binary(meanX);
        buf.read_binary(meanY);
        buf.read_binary(c2);
        buf.read_binary(m2X);
        buf.read_binary(m2Y);
        buf.read_binary(count);
    }

    double get() const {
        if (m2X == 0 || m2Y == 0) {
            return 0;
        }
        return c2 / sqrt(m2X * m2Y);
    }

    static String name() { return "corr_welford"; }

    void reset() {
        meanX = {};
        meanY = {};
        c2 = {};
        m2X = {};
        m2Y = {};
        count = 0;
    }
};
} // namespace doris::vectorized
