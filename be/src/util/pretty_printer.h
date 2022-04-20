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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/pretty-printer.h
// and modified by Doris

#ifndef IMPALA_UTIL_PRETTY_PRINTER_H
#define IMPALA_UTIL_PRETTY_PRINTER_H

#include <boost/algorithm/string.hpp>
#include <cmath>
#include <iomanip>
#include <sstream>

#include "gen_cpp/RuntimeProfile_types.h"
#include "util/binary_cast.hpp"
#include "util/cpu_info.h"

/// Truncate a double to offset decimal places.
#define DOUBLE_TRUNCATE(val, offset) floor(val* pow(10, offset)) / pow(10, offset)

namespace doris {

/// Methods for printing numeric values with optional units, or other types with an
/// applicable operator<<.
class PrettyPrinter {
public:
    static std::string print(bool value, TUnit::type ignored, bool verbose = false) {
        std::stringstream ss;
        ss << std::boolalpha << value;
        return ss.str();
    }

    /// Prints the 'value' in a human friendly format depending on the data type.
    /// i.e. for bytes: 3145728 -> 3MB
    /// If verbose is true, this also prints the raw value (before unit conversion) for
    /// types where this is applicable.
    template <typename T>
    static typename std::enable_if<std::is_arithmetic<T>::value, std::string>::type print(
            T value, TUnit::type unit, bool verbose = false) {
        std::stringstream ss;
        ss.flags(std::ios::fixed);
        switch (unit) {
        case TUnit::NONE: {
            ss << value;
            return ss.str();
        }

        case TUnit::UNIT: {
            std::string unit;
            double output = get_unit(value, &unit);
            if (unit.empty()) {
                ss << value;
            } else {
                ss << std::setprecision(PRECISION) << output << unit;
            }
            if (verbose) ss << " (" << value << ")";
            break;
        }

        case TUnit::UNIT_PER_SECOND: {
            std::string unit;
            double output = get_unit(value, &unit);
            if (output == 0) {
                ss << "0";
            } else {
                ss << std::setprecision(PRECISION) << output << " " << unit << "/sec";
            }
            break;
        }

        case TUnit::CPU_TICKS: {
            if (value < CpuInfo::cycles_per_ms()) {
                ss << std::setprecision(PRECISION) << (value / 1000.) << "K clock cycles";
            } else {
                value /= CpuInfo::cycles_per_ms();
                print_timems(value, &ss);
            }
            break;
        }

        case TUnit::TIME_NS: {
            ss << std::setprecision(TIME_NS_PRECISION);
            if (value >= BILLION) {
                /// If the time is over a second, print it up to ms.
                value /= MILLION;
                print_timems(value, &ss);
            } else if (value >= MILLION) {
                /// if the time is over a ms, print it up to microsecond in the unit of ms.
                ss << DOUBLE_TRUNCATE(static_cast<double>(value) / MILLION, TIME_NS_PRECISION)
                   << "ms";
            } else if (value > 1000) {
                /// if the time is over a microsecond, print it using unit microsecond
                ss << DOUBLE_TRUNCATE(static_cast<double>(value) / 1000, TIME_NS_PRECISION) << "us";
            } else {
                ss << DOUBLE_TRUNCATE(value, TIME_NS_PRECISION) << "ns";
            }
            break;
        }

        case TUnit::TIME_MS: {
            print_timems(value, &ss);
            break;
        }

        case TUnit::TIME_S: {
            print_timems(value * 1000, &ss);
            break;
        }

        case TUnit::BYTES: {
            std::string unit;
            double output = get_byte_unit(value, &unit);
            if (output == 0) {
                ss << "0";
            } else {
                ss << std::setprecision(PRECISION) << output << " " << unit;
                if (verbose) ss << " (" << value << ")";
            }
            break;
        }

        case TUnit::BYTES_PER_SECOND: {
            std::string unit;
            double output = get_byte_unit(value, &unit);
            ss << std::setprecision(PRECISION) << output << " " << unit << "/sec";
            break;
        }

        /// TODO: Remove DOUBLE_VALUE. IMPALA-1649
        case TUnit::DOUBLE_VALUE: {
            double output = binary_cast<T, double>(value);
            ss << std::setprecision(PRECISION) << output << " ";
            break;
        }

        default:
            DCHECK(false) << "Unsupported TUnit: " << value;
            break;
        }
        return ss.str();
    }

    /// For non-arithmetics, just write the value as a string and return it.
    //
    /// TODO: There's no good is_string equivalent, so there's a needless copy for strings
    /// here.
    template <typename T>
    static typename std::enable_if<!std::is_arithmetic<T>::value, std::string>::type print(
            const T& value, TUnit::type unit) {
        std::stringstream ss;
        ss << std::boolalpha << value;
        return ss.str();
    }

    /// Utility method to print an iterable type to a stringstream like [v1, v2, v3]
    template <typename I>
    static void print_stringList(const I& iterable, TUnit::type unit, std::stringstream* out) {
        std::vector<std::string> strings;
        for (typename I::const_iterator it = iterable.begin(); it != iterable.end(); ++it) {
            std::stringstream ss;
            ss << PrettyPrinter::print(*it, unit);
            strings.push_back(ss.str());
        }

        (*out) << "[" << boost::algorithm::join(strings, ", ") << "]";
    }

    /// Convenience method
    static std::string print_bytes(int64_t value) {
        return PrettyPrinter::print(value, TUnit::BYTES);
    }

private:
    static const int PRECISION = 2;
    static const int TIME_NS_PRECISION = 3;
    static const int64_t KILOBYTE = 1024;
    static const int64_t MEGABYTE = KILOBYTE * 1024;
    static const int64_t GIGABYTE = MEGABYTE * 1024;

    static const int64_t SECOND = 1000;
    static const int64_t MINUTE = SECOND * 60;
    static const int64_t HOUR = MINUTE * 60;

    static const int64_t THOUSAND = 1000;
    static const int64_t MILLION = THOUSAND * 1000;
    static const int64_t BILLION = MILLION * 1000;

    template <typename T>
    static double get_byte_unit(T value, std::string* unit) {
        if (value == 0) {
            *unit = "";
            return value;
        } else if (value >= GIGABYTE) {
            *unit = "GB";
            return value / (double)GIGABYTE;
        } else if (value >= MEGABYTE) {
            *unit = "MB";
            return value / (double)MEGABYTE;
        } else if (value >= KILOBYTE) {
            *unit = "KB";
            return value / (double)KILOBYTE;
        } else {
            *unit = "B";
            return value;
        }
    }

    template <typename T>
    static double get_unit(T value, std::string* unit) {
        if (value >= BILLION) {
            *unit = "B";
            return value / (1. * BILLION);
        } else if (value >= MILLION) {
            *unit = "M";
            return value / (1. * MILLION);
        } else if (value >= THOUSAND) {
            *unit = "K";
            return value / (1. * THOUSAND);
        } else {
            *unit = "";
            return value;
        }
    }

    /// Utility to perform integer modulo if T is integral, otherwise to use fmod().
    template <typename T>
    static typename boost::enable_if_c<boost::is_integral<T>::value, int64_t>::type mod(
            const T& value, const int modulus) {
        return value % modulus;
    }

    template <typename T>
    static typename boost::enable_if_c<!boost::is_integral<T>::value, double>::type mod(
            const T& value, int modulus) {
        return fmod(value, 1. * modulus);
    }

    /// Print the value (time in ms) to ss
    template <typename T>
    static void print_timems(T value, std::stringstream* ss) {
        DCHECK_GE(value, static_cast<T>(0));
        if (value == 0) {
            *ss << "0";
        } else {
            bool hour = false;
            bool minute = false;
            bool second = false;
            if (value >= HOUR) {
                *ss << static_cast<int64_t>(value / HOUR) << "h";
                value = mod(value, HOUR);
                hour = true;
            }
            if (value >= MINUTE) {
                *ss << static_cast<int64_t>(value / MINUTE) << "m";
                value = mod(value, MINUTE);
                minute = true;
            }
            if (!hour && value >= SECOND) {
                *ss << static_cast<int64_t>(value / SECOND) << "s";
                value = mod(value, SECOND);
                second = true;
            }
            if (!hour && !minute) {
                if (second) *ss << std::setw(3) << std::setfill('0');
                *ss << static_cast<int64_t>(value) << "ms";
            }
        }
    }
};

} // namespace doris

#endif
