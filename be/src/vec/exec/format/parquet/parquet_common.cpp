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

ParsedVersion::ParsedVersion(std::string application, std::optional<std::string> version,
                             std::optional<std::string> app_build_hash)
        : _application(std::move(application)),
          _version(std::move(version)),
          _app_build_hash(std::move(app_build_hash)) {}

bool ParsedVersion::operator==(const ParsedVersion& other) const {
    return _application == other._application && _version == other._version &&
           _app_build_hash == other._app_build_hash;
}

bool ParsedVersion::operator!=(const ParsedVersion& other) const {
    return !(*this == other);
}

size_t ParsedVersion::hash() const {
    std::hash<std::string> hasher;
    return hasher(_application) ^ (_version ? hasher(*_version) : 0) ^
           (_app_build_hash ? hasher(*_app_build_hash) : 0);
}

std::string ParsedVersion::to_string() const {
    return "ParsedVersion(application=" + _application +
           ", semver=" + (_version ? *_version : "null") +
           ", app_build_hash=" + (_app_build_hash ? *_app_build_hash : "null") + ")";
}

Status VersionParser::parse(const std::string& created_by,
                            std::unique_ptr<ParsedVersion>* parsed_version) {
    static const std::string FORMAT =
            "(.*?)\\s+version\\s*(?:([^(]*?)\\s*(?:\\(\\s*build\\s*([^)]*?)\\s*\\))?)?";
    static const std::regex PATTERN(FORMAT);

    std::smatch matcher;
    if (!std::regex_match(created_by, matcher, PATTERN)) {
        return Status::InternalError(fmt::format("Could not parse created_by: {}, using format: {}",
                                                 created_by, FORMAT));
    }

    std::string application = matcher[1].str();
    if (application.empty()) {
        return Status::InternalError("application cannot be null or empty");
    }
    std::optional<std::string> semver =
            matcher[2].str().empty() ? std::nullopt : std::optional<std::string>(matcher[2].str());
    std::optional<std::string> app_build_hash =
            matcher[3].str().empty() ? std::nullopt : std::optional<std::string>(matcher[3].str());
    *parsed_version = std::make_unique<ParsedVersion>(application, semver, app_build_hash);
    return Status::OK();
}

SemanticVersion::SemanticVersion(int major, int minor, int patch)
        : _major(major),
          _minor(minor),
          _patch(patch),
          _prerelease(false),
          _unknown(std::nullopt),
          _pre(std::nullopt),
          _build_info(std::nullopt) {}

#ifdef BE_TEST
SemanticVersion::SemanticVersion(int major, int minor, int patch, bool has_unknown)
        : _major(major),
          _minor(minor),
          _patch(patch),
          _prerelease(has_unknown),
          _unknown(std::nullopt),
          _pre(std::nullopt),
          _build_info(std::nullopt) {}
#endif

SemanticVersion::SemanticVersion(int major, int minor, int patch,
                                 std::optional<std::string> unknown, std::optional<std::string> pre,
                                 std::optional<std::string> build_info)
        : _major(major),
          _minor(minor),
          _patch(patch),
          _prerelease(unknown.has_value() && !unknown.value().empty()),
          _unknown(std::move(unknown)),
          _pre(pre.has_value() ? std::optional<Prerelease>(Prerelease(std::move(pre.value())))
                               : std::nullopt),
          _build_info(std::move(build_info)) {}

Status SemanticVersion::parse(const std::string& version,
                              std::unique_ptr<SemanticVersion>* semantic_version) {
    static const std::regex pattern(R"(^(\d+)\.(\d+)\.(\d+)([^-+]*)?(?:-([^+]*))?(?:\+(.*))?$)");
    std::smatch match;

    if (!std::regex_match(version, match, pattern)) {
        return Status::InternalError(version + " does not match format");
    }

    int major = std::stoi(match[1].str());
    int minor = std::stoi(match[2].str());
    int patch = std::stoi(match[3].str());
    std::optional<std::string> unknown =
            match[4].str().empty() ? std::nullopt : std::optional<std::string>(match[4].str());
    std::optional<std::string> prerelease =
            match[5].str().empty() ? std::nullopt : std::optional<std::string>(match[5].str());
    std::optional<std::string> build_info =
            match[6].str().empty() ? std::nullopt : std::optional<std::string>(match[6].str());
    if (major < 0 || minor < 0 || patch < 0) {
        return Status::InternalError("major({}), minor({}), and patch({}) must all be >= 0", major,
                                     minor, patch);
    }
    *semantic_version =
            std::make_unique<SemanticVersion>(major, minor, patch, unknown, prerelease, build_info);
    return Status::OK();
}

int SemanticVersion::compare_to(const SemanticVersion& other) const {
    if (int cmp = _compare_integers(_major, other._major); cmp != 0) {
        return cmp;
    }
    if (int cmp = _compare_integers(_minor, other._minor); cmp != 0) {
        return cmp;
    }
    if (int cmp = _compare_integers(_patch, other._patch); cmp != 0) {
        return cmp;
    }
    if (int cmp = _compare_booleans(other._prerelease, _prerelease); cmp != 0) {
        return cmp;
    }
    if (_pre.has_value()) {
        if (other._pre.has_value()) {
            return _pre.value().compare_to(other._pre.value());
        } else {
            return -1;
        }
    } else if (other._pre.has_value()) {
        return 1;
    }
    return 0;
}

bool SemanticVersion::operator==(const SemanticVersion& other) const {
    return compare_to(other) == 0;
}

bool SemanticVersion::operator!=(const SemanticVersion& other) const {
    return !(*this == other);
}

std::string SemanticVersion::to_string() const {
    std::string result =
            std::to_string(_major) + "." + std::to_string(_minor) + "." + std::to_string(_patch);
    if (_prerelease && _unknown) result += _unknown.value();
    if (_pre) result += _pre.value().to_string();
    if (_build_info) result += _build_info.value();
    return result;
}

SemanticVersion::NumberOrString::NumberOrString(const std::string& value_string)
        : _original(value_string) {
    const static std::regex NUMERIC("\\d+");
    _is_numeric = std::regex_match(_original, NUMERIC);
    _number = -1;
    if (_is_numeric) {
        _number = std::stoi(_original);
    }
}

SemanticVersion::NumberOrString::NumberOrString(const NumberOrString& other)
        : _original(other._original), _is_numeric(other._is_numeric), _number(other._number) {}

int SemanticVersion::NumberOrString::compare_to(const SemanticVersion::NumberOrString& that) const {
    if (this->_is_numeric != that._is_numeric) {
        return this->_is_numeric ? -1 : 1;
    }

    if (_is_numeric) {
        return this->_number - that._number;
    }

    return this->_original.compare(that._original);
}

std::string SemanticVersion::NumberOrString::to_string() const {
    return _original;
}

bool SemanticVersion::NumberOrString::operator<(const SemanticVersion::NumberOrString& that) const {
    return compare_to(that) < 0;
}

bool SemanticVersion::NumberOrString::operator==(
        const SemanticVersion::NumberOrString& that) const {
    return compare_to(that) == 0;
}

bool SemanticVersion::NumberOrString::operator!=(
        const SemanticVersion::NumberOrString& that) const {
    return !(*this == that);
}

bool SemanticVersion::NumberOrString::operator>(const SemanticVersion::NumberOrString& that) const {
    return compare_to(that) > 0;
}

bool SemanticVersion::NumberOrString::operator<=(
        const SemanticVersion::NumberOrString& that) const {
    return !(*this > that);
}

bool SemanticVersion::NumberOrString::operator>=(
        const SemanticVersion::NumberOrString& that) const {
    return !(*this < that);
}

int SemanticVersion::_compare_integers(int x, int y) {
    return (x < y) ? -1 : ((x == y) ? 0 : 1);
}

int SemanticVersion::_compare_booleans(bool x, bool y) {
    return (x == y) ? 0 : (x ? 1 : -1);
}

std::vector<std::string> SemanticVersion::Prerelease::_split(const std::string& s,
                                                             const std::regex& delimiter) {
    std::sregex_token_iterator iter(s.begin(), s.end(), delimiter, -1);
    std::sregex_token_iterator end;
    std::vector<std::string> tokens(iter, end);
    return tokens;
}

SemanticVersion::Prerelease::Prerelease(std::string original) : _original(std::move(original)) {
    static const std::regex DOT("\\.");
    auto parts = _split(_original, DOT);
    for (const auto& part : parts) {
        NumberOrString number_or_string(part);
        _identifiers.emplace_back(number_or_string);
    }
}

int SemanticVersion::Prerelease::compare_to(const Prerelease& that) const {
    int size = std::min(this->_identifiers.size(), that._identifiers.size());
    for (int i = 0; i < size; ++i) {
        int cmp = this->_identifiers[i].compare_to(that._identifiers[i]);
        if (cmp != 0) {
            return cmp;
        }
    }
    return static_cast<int>(this->_identifiers.size()) - static_cast<int>(that._identifiers.size());
}

std::string SemanticVersion::Prerelease::to_string() const {
    return _original;
}

bool SemanticVersion::Prerelease::operator<(const Prerelease& that) const {
    return compare_to(that) < 0;
}

bool SemanticVersion::Prerelease::operator==(const Prerelease& that) const {
    return compare_to(that) == 0;
}

bool SemanticVersion::Prerelease::operator!=(const Prerelease& that) const {
    return !(*this == that);
}

bool SemanticVersion::Prerelease::operator>(const Prerelease& that) const {
    return compare_to(that) > 0;
}

bool SemanticVersion::Prerelease::operator<=(const Prerelease& that) const {
    return !(*this > that);
}

bool SemanticVersion::Prerelease::operator>=(const Prerelease& that) const {
    return !(*this < that);
}

const SemanticVersion CorruptStatistics::PARQUET_251_FIXED_VERSION(1, 8, 0);
const SemanticVersion CorruptStatistics::CDH_5_PARQUET_251_FIXED_START(1, 5, 0, std::nullopt,
                                                                       "cdh5.5.0", std::nullopt);
const SemanticVersion CorruptStatistics::CDH_5_PARQUET_251_FIXED_END(1, 5, 0);

bool CorruptStatistics::should_ignore_statistics(const std::string& created_by,
                                                 tparquet::Type::type physical_type) {
    if (physical_type != tparquet::Type::BYTE_ARRAY &&
        physical_type != tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
        // The bug only applies to binary columns
        return false;
    }

    if (created_by.empty()) {
        // created_by is not populated
        VLOG_DEBUG
                << "Ignoring statistics because created_by is null or empty! See PARQUET-251 and "
                   "PARQUET-297";
        return true;
    }

    Status status;
    std::unique_ptr<ParsedVersion> parsed_version;
    status = VersionParser::parse(created_by, &parsed_version);
    if (!status.ok()) {
        VLOG_DEBUG << "Ignoring statistics because created_by could not be parsed (see "
                      "PARQUET-251)."
                      " CreatedBy: "
                   << created_by << ", msg: " << status.msg();
        return true;
    }

    if (parsed_version->application() != "parquet-mr") {
        // Assume other applications don't have this bug
        return false;
    }

    if ((!parsed_version->version().has_value()) || parsed_version->version().value().empty()) {
        VLOG_DEBUG << "Ignoring statistics because created_by did not contain a semver (see "
                      "PARQUET-251): "
                   << created_by;
        return true;
    }

    std::unique_ptr<SemanticVersion> semantic_version;
    status = SemanticVersion::parse(parsed_version->version().value(), &semantic_version);
    if (!status.ok()) {
        VLOG_DEBUG << "Ignoring statistics because created_by could not be parsed (see "
                      "PARQUET-251)."
                      " CreatedBy: "
                   << created_by << ", msg: " << status.msg();
        return true;
    }
    if (semantic_version->compare_to(PARQUET_251_FIXED_VERSION) < 0 &&
        !(semantic_version->compare_to(CDH_5_PARQUET_251_FIXED_START) >= 0 &&
          semantic_version->compare_to(CDH_5_PARQUET_251_FIXED_END) < 0)) {
        VLOG_DEBUG
                << "Ignoring statistics because this file was created prior to the fixed version, "
                   "see PARQUET-251";
        return true;
    }

    // This file was created after the fix
    return false;
}

} // namespace doris::vectorized
