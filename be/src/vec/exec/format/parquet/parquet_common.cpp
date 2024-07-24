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

#include "parquet_common.h"

#include <glog/logging.h>

#include "util/simd/bits.h"
#include "vec/core/types.h"

namespace doris::vectorized {

const int32_t ParquetInt96::JULIAN_EPOCH_OFFSET_DAYS = 2440588;
const int64_t ParquetInt96::MICROS_IN_DAY = 86400000000;
const int64_t ParquetInt96::NANOS_PER_MICROSECOND = 1000;

ColumnSelectVector::ColumnSelectVector(const uint8_t* filter_map, size_t filter_map_size,
                                       bool filter_all) {
    build(filter_map, filter_map_size, filter_all);
}

void ColumnSelectVector::build(const uint8_t* filter_map, size_t filter_map_size, bool filter_all) {
    _filter_all = filter_all;
    _filter_map = filter_map;
    _filter_map_size = filter_map_size;
    if (filter_all) {
        _has_filter = true;
        _filter_ratio = 1;
    } else if (filter_map == nullptr) {
        _has_filter = false;
        _filter_ratio = 0;
    } else {
        size_t filter_count =
                simd::count_zero_num(reinterpret_cast<const int8_t*>(filter_map), filter_map_size);
        if (filter_count == filter_map_size) {
            _has_filter = true;
            _filter_all = true;
            _filter_ratio = 1;
        } else if (filter_count > 0 && filter_map_size > 0) {
            _has_filter = true;
            _filter_ratio = (double)filter_count / filter_map_size;
        } else {
            _has_filter = false;
            _filter_ratio = 0;
        }
    }
}

void ColumnSelectVector::set_run_length_null_map(const std::vector<uint16_t>& run_length_null_map,
                                                 size_t num_values, NullMap* null_map) {
    _num_values = num_values;
    _num_nulls = 0;
    _read_index = 0;
    size_t map_index = 0;
    bool is_null = false;
    if (_has_filter) {
        // No run length null map is generated when _filter_all = true
        DCHECK(!_filter_all);
        _data_map.resize(num_values);
        for (auto& run_length : run_length_null_map) {
            if (is_null) {
                _num_nulls += run_length;
                for (int i = 0; i < run_length; ++i) {
                    _data_map[map_index++] = FILTERED_NULL;
                }
            } else {
                for (int i = 0; i < run_length; ++i) {
                    _data_map[map_index++] = FILTERED_CONTENT;
                }
            }
            is_null = !is_null;
        }
        size_t num_read = 0;
        DCHECK_LE(_filter_map_index + num_values, _filter_map_size);
        for (size_t i = 0; i < num_values; ++i) {
            if (_filter_map[_filter_map_index++]) {
                _data_map[i] = _data_map[i] == FILTERED_NULL ? NULL_DATA : CONTENT;
                num_read++;
            }
        }
        _num_filtered = num_values - num_read;
        if (null_map != nullptr && num_read > 0) {
            NullMap& map_data_column = *null_map;
            auto null_map_index = map_data_column.size();
            map_data_column.resize(null_map_index + num_read);
            if (_num_nulls == 0) {
                memset(map_data_column.data() + null_map_index, 0, num_read);
            } else if (_num_nulls == num_values) {
                memset(map_data_column.data() + null_map_index, 1, num_read);
            } else {
                for (size_t i = 0; i < num_values; ++i) {
                    if (_data_map[i] == CONTENT) {
                        map_data_column[null_map_index++] = (UInt8) false;
                    } else if (_data_map[i] == NULL_DATA) {
                        map_data_column[null_map_index++] = (UInt8) true;
                    }
                }
            }
        }
    } else {
        _num_filtered = 0;
        _run_length_null_map = &run_length_null_map;
        if (null_map != nullptr) {
            NullMap& map_data_column = *null_map;
            auto null_map_index = map_data_column.size();
            map_data_column.resize(null_map_index + num_values);

            for (auto& run_length : run_length_null_map) {
                if (is_null) {
                    memset(map_data_column.data() + null_map_index, 1, run_length);
                    null_map_index += run_length;
                    _num_nulls += run_length;
                } else {
                    memset(map_data_column.data() + null_map_index, 0, run_length);
                    null_map_index += run_length;
                }
                is_null = !is_null;
            }
        } else {
            for (auto& run_length : run_length_null_map) {
                if (is_null) {
                    _num_nulls += run_length;
                }
                is_null = !is_null;
            }
        }
    }
}

bool ColumnSelectVector::can_filter_all(size_t remaining_num_values) {
    if (!_has_filter) {
        return false;
    }
    if (_filter_all) {
        // all data in normal columns can be skipped when _filter_all = true,
        // so the remaining_num_values should be less than the remaining filter map size.
        DCHECK_LE(remaining_num_values + _filter_map_index, _filter_map_size);
        // return true always, to make sure that the data in normal columns can be skipped.
        return true;
    }
    if (remaining_num_values + _filter_map_index > _filter_map_size) {
        return false;
    }
    return simd::count_zero_num(reinterpret_cast<const int8_t*>(_filter_map + _filter_map_index),
                                remaining_num_values) == remaining_num_values;
}

void ColumnSelectVector::skip(size_t num_values) {
    _filter_map_index += num_values;
}

std::atomic<bool> CorruptStatistics::already_logged {false};
const SemanticVersion CorruptStatistics::PARQUET_251_FIXED_VERSION(1, 8, 0);
const SemanticVersion CorruptStatistics::CDH_5_PARQUET_251_FIXED_START(1, 5, 0, std::nullopt,
                                                                       "cdh5.5.0", std::nullopt);
const SemanticVersion CorruptStatistics::CDH_5_PARQUET_251_FIXED_END(1, 5, 0);

SemanticVersion::NumberOrString::NumberOrString(const std::string& numberOrString)
        : original(numberOrString) {
    const static std::regex NUMERIC("\\d+");
    isNumeric = std::regex_match(original, NUMERIC);
    number = -1;
    if (isNumeric) {
        try {
            number = std::stoi(original);
        } catch (const std::invalid_argument&) {
            // Handle invalid argument if needed
        }
    }
}

int SemanticVersion::NumberOrString::compareTo(const SemanticVersion::NumberOrString& that) const {
    if (this->isNumeric != that.isNumeric) {
        return this->isNumeric ? -1 : 1;
    }

    if (isNumeric) {
        return this->number - that.number;
    }

    return this->original.compare(that.original);
}

std::string SemanticVersion::NumberOrString::toString() const {
    return original;
}

bool SemanticVersion::NumberOrString::operator<(const SemanticVersion::NumberOrString& that) const {
    return compareTo(that) < 0;
}

bool SemanticVersion::NumberOrString::operator==(
        const SemanticVersion::NumberOrString& that) const {
    return compareTo(that) == 0;
}

bool SemanticVersion::NumberOrString::operator!=(
        const SemanticVersion::NumberOrString& that) const {
    return !(*this == that);
}

bool SemanticVersion::NumberOrString::operator>(const SemanticVersion::NumberOrString& that) const {
    return compareTo(that) > 0;
}

bool SemanticVersion::NumberOrString::operator<=(
        const SemanticVersion::NumberOrString& that) const {
    return !(*this > that);
}

bool SemanticVersion::NumberOrString::operator>=(
        const SemanticVersion::NumberOrString& that) const {
    return !(*this < that);
}

std::vector<std::string> SemanticVersion::Prerelease::split(const std::string& s,
                                                            const std::regex& delimiter) {
    //    std::sregex_token_iterator iter(s.begin(), s.end(), delimiter, -1);
    //    std::sregex_token_iterator end;
    //    std::vector<std::string> tokens(iter, end);
    //    return tokens;

    std::sregex_token_iterator iter(s.begin(), s.end(), delimiter, -1);
    std::sregex_token_iterator end;

    std::vector<std::string> tokens;
    tokens.reserve(std::distance(iter, end));

    for (; iter != end; ++iter) {
        tokens.emplace_back(*iter);
    }

    return tokens;
}

SemanticVersion::Prerelease::Prerelease(std::string original) : _original(std::move(original)) {
    static const std::regex DOT("\\.");
    auto parts = split(_original, DOT);
    for (const auto& part : parts) {
        NumberOrString number_or_string(part);
        _identifiers.emplace_back(number_or_string);
    }
}

int SemanticVersion::Prerelease::compareTo(const Prerelease& that) const {
    int size = std::min(this->_identifiers.size(), that._identifiers.size());
    for (int i = 0; i < size; ++i) {
        int cmp = this->_identifiers[i].compareTo(that._identifiers[i]);
        if (cmp != 0) {
            return cmp;
        }
    }
    return static_cast<int>(this->_identifiers.size()) - static_cast<int>(that._identifiers.size());
}

std::string SemanticVersion::Prerelease::toString() const {
    return _original;
}

bool SemanticVersion::Prerelease::operator<(const Prerelease& that) const {
    return compareTo(that) < 0;
}

bool SemanticVersion::Prerelease::operator==(const Prerelease& that) const {
    return compareTo(that) == 0;
}

bool SemanticVersion::Prerelease::operator!=(const Prerelease& that) const {
    return !(*this == that);
}

bool SemanticVersion::Prerelease::operator>(const Prerelease& that) const {
    return compareTo(that) > 0;
}

bool SemanticVersion::Prerelease::operator<=(const Prerelease& that) const {
    return !(*this > that);
}

bool SemanticVersion::Prerelease::operator>=(const Prerelease& that) const {
    return !(*this < that);
}

} // namespace doris::vectorized
