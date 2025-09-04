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

#include <cmath>

#include "common/compiler_util.h"
#include "runtime/type_limit.h"

#pragma once
namespace doris {
#include "common/compile_check_begin.h"
struct Compare {
    template <typename T>
    static bool less(const T& a, const T& b) {
        return a < b;
    }
    template <typename T>
    static bool greater(const T& a, const T& b) {
        return a > b;
    }

    template <typename T>
    static bool less_equal(const T& a, const T& b) {
        return a <= b;
    }
    template <typename T>
    static bool greater_equal(const T& a, const T& b) {
        return a >= b;
    }
    template <typename T>
    static bool equal(const T& a, const T& b) {
        return a == b;
    }
    template <typename T>
    static bool not_equal(const T& a, const T& b) {
        return a != b;
    }

    template <typename T>
    static int compare(const T& a, const T& b) {
        return a > b ? 1 : (a < b ? -1 : 0);
    }

    template <typename T>
    static T min(const T& a, const T& b) {
        return less(a, b) ? a : b;
    }

    template <typename T>
    static T max(const T& a, const T& b) {
        return greater(a, b) ? a : b;
    }

    template <typename T>
    static T min_value() {
        return type_limit<T>::min();
    }

    template <typename T>
    static T max_value() {
        return type_limit<T>::max();
    }
};

template <typename T>
bool EqualsFloat(T left, T right) {
    if (UNLIKELY(std::isnan(left) && std::isnan(right))) {
        return true;
    }
    return left == right;
}

template <typename T>
bool GreaterThanFloat(T left, T right) {
    // nan is always bigger than everything else
    bool left_is_nan = std::isnan(left);
    bool right_is_nan = std::isnan(right);
    if (UNLIKELY(right_is_nan)) {
        return false;
    }
    if (UNLIKELY(left_is_nan)) {
        return true;
    }
    return left > right;
}

template <typename T>
bool GreaterThanEqualsFloat(T left, T right) {
    // nan is always bigger than everything else
    bool left_is_nan = std::isnan(left);
    bool right_is_nan = std::isnan(right);
    if (UNLIKELY(right_is_nan)) {
        return left_is_nan;
    }
    if (UNLIKELY(left_is_nan)) {
        return true;
    }
    return left >= right;
}

template <typename T>
int CompareFloat(T left, T right) {
    // nan is always bigger than everything else
    bool left_is_nan = std::isnan(left);
    bool right_is_nan = std::isnan(right);
    if (UNLIKELY(left_is_nan || right_is_nan)) {
        if (left_is_nan && right_is_nan) {
            return 0;
        }
        if (left_is_nan) {
            return 1;
        } else {
            return -1;
        }
    }
    return left > right ? 1 : (left < right ? -1 : 0);
};

// float
template <>
inline bool Compare::less(const float& a, const float& b) {
    return GreaterThanFloat(b, a);
}
template <>
inline bool Compare::greater(const float& a, const float& b) {
    return GreaterThanFloat(a, b);
}
template <>
inline bool Compare::less_equal(const float& a, const float& b) {
    return GreaterThanEqualsFloat(b, a);
}
template <>
inline bool Compare::greater_equal(const float& a, const float& b) {
    return GreaterThanEqualsFloat(a, b);
}
template <>
inline bool Compare::equal(const float& a, const float& b) {
    return EqualsFloat(a, b);
}
template <>
inline bool Compare::not_equal(const float& a, const float& b) {
    return !EqualsFloat(a, b);
}
template <>
inline int Compare::compare(const float& a, const float& b) {
    return CompareFloat(a, b);
}
template <>
inline float Compare::max_value<float>() {
    return std::numeric_limits<float>::quiet_NaN();
}

// double
template <>
inline bool Compare::less(const double& a, const double& b) {
    return GreaterThanFloat(b, a);
}
template <>
inline bool Compare::greater(const double& a, const double& b) {
    return GreaterThanFloat(a, b);
}
template <>
inline bool Compare::less_equal(const double& a, const double& b) {
    return GreaterThanEqualsFloat(b, a);
}
template <>
inline bool Compare::greater_equal(const double& a, const double& b) {
    return GreaterThanEqualsFloat(a, b);
}
template <>
inline bool Compare::equal(const double& a, const double& b) {
    return EqualsFloat(a, b);
}
template <>
inline bool Compare::not_equal(const double& a, const double& b) {
    return !EqualsFloat(a, b);
}
template <>
inline int Compare::compare(const double& a, const double& b) {
    return CompareFloat(a, b);
}
template <>
inline double Compare::max_value<double>() {
    return std::numeric_limits<double>::quiet_NaN();
}
} // namespace doris

#include "common/compile_check_end.h"