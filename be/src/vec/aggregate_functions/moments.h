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

#include <stddef.h>

#include "common/exception.h"
#include "common/status.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

class BufferReadable;
class BufferWritable;

template <typename T, size_t _level>
struct VarMoments {
    // m[1] = sum(x)
    // m[2] = sum(x^2)
    // m[3] = sum(x^3)
    // m[4] = sum(x^4)
    T m[_level + 1] {};

    void add(T x) {
        ++m[0];
        m[1] += x;
        m[2] += x * x;
        if constexpr (_level >= 3) m[3] += x * x * x;
        if constexpr (_level >= 4) m[4] += x * x * x * x;
    }

    void merge(const VarMoments& rhs) {
        m[0] += rhs.m[0];
        m[1] += rhs.m[1];
        m[2] += rhs.m[2];
        if constexpr (_level >= 3) m[3] += rhs.m[3];
        if constexpr (_level >= 4) m[4] += rhs.m[4];
    }

    void write(BufferWritable& buf) const { write_binary(*this, buf); }

    void read(BufferReadable& buf) { read_binary(*this, buf); }

    T get() const {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "Variation moments should be obtained by 'get_population' method");
    }

    T get_population() const {
        if (m[0] == 0) return std::numeric_limits<T>::quiet_NaN();

        /// Due to numerical errors, the result can be slightly less than zero,
        /// but it should be impossible. Trim to zero.

        return std::max(T {}, (m[2] - m[1] * m[1] / m[0]) / m[0]);
    }

    T get_sample() const {
        if (m[0] <= 1) return std::numeric_limits<T>::quiet_NaN();
        return std::max(T {}, (m[2] - m[1] * m[1] / m[0]) / (m[0] - 1));
    }

    T get_moment_3() const {
        if constexpr (_level < 3) {
            throw doris::Exception(
                    ErrorCode::INTERNAL_ERROR,
                    "Variation moments should be obtained by 'get_population' method");
        } else {
            if (m[0] == 0) return std::numeric_limits<T>::quiet_NaN();
            // to avoid accuracy problem
            if (m[0] == 1) return 0;
            /// \[ \frac{1}{m_0} (m_3 - (3 * m_2 - \frac{2 * {m_1}^2}{m_0}) * \frac{m_1}{m_0});\]
            return (m[3] - (3 * m[2] - 2 * m[1] * m[1] / m[0]) * m[1] / m[0]) / m[0];
        }
    }

    T get_moment_4() const {
        if constexpr (_level < 4) {
            throw doris::Exception(
                    ErrorCode::INTERNAL_ERROR,
                    "Variation moments should be obtained by 'get_population' method");
        } else {
            if (m[0] == 0) return std::numeric_limits<T>::quiet_NaN();
            // to avoid accuracy problem
            if (m[0] == 1) return 0;
            /// \[ \frac{1}{m_0}(m_4 - (4 * m_3 - (6 * m_2 - \frac{3 * m_1^2}{m_0} ) \frac{m_1}{m_0})\frac{m_1}{m_0})\]
            return (m[4] -
                    (4 * m[3] - (6 * m[2] - 3 * m[1] * m[1] / m[0]) * m[1] / m[0]) * m[1] / m[0]) /
                   m[0];
        }
    }

    void reset() {
        m = {};
        return;
    }
};

} // namespace doris::vectorized