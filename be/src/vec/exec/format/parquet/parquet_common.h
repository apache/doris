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

#include <gen_cpp/parquet_types.h>
#include <stddef.h>

#include <cstdint>
#include <ostream>
#include <regex>
#include <string>
#include <vector>

#include "vec/columns/column_nullable.h"

namespace doris::vectorized {

using level_t = int16_t;

struct RowRange {
    RowRange() = default;
    RowRange(int64_t first, int64_t last) : first_row(first), last_row(last) {}

    int64_t first_row;
    int64_t last_row;

    bool operator<(const RowRange& range) const { return first_row < range.first_row; }

    std::string debug_string() const {
        std::stringstream ss;
        ss << "[" << first_row << "," << last_row << ")";
        return ss.str();
    }
};

#pragma pack(1)
struct ParquetInt96 {
    int64_t lo; // time of nanoseconds in a day
    int32_t hi; // days from julian epoch

    inline int64_t to_timestamp_micros() const {
        return (hi - JULIAN_EPOCH_OFFSET_DAYS) * MICROS_IN_DAY + lo / NANOS_PER_MICROSECOND;
    }
    inline __int128 to_int128() const {
        __int128 ans = 0;
        ans = (((__int128)hi) << 64) + lo;
        return ans;
    }

    static const int32_t JULIAN_EPOCH_OFFSET_DAYS;
    static const int64_t MICROS_IN_DAY;
    static const int64_t NANOS_PER_MICROSECOND;
};
#pragma pack()
static_assert(sizeof(ParquetInt96) == 12, "The size of ParquetInt96 is not 12.");

class ColumnSelectVector {
public:
    enum DataReadType : uint8_t { CONTENT = 0, NULL_DATA, FILTERED_CONTENT, FILTERED_NULL };

    ColumnSelectVector(const uint8_t* filter_map, size_t filter_map_size, bool filter_all);

    ColumnSelectVector() = default;

    void build(const uint8_t* filter_map, size_t filter_map_size, bool filter_all);

    const uint8_t* filter_map() { return _filter_map; }

    size_t num_values() const { return _num_values; }

    size_t num_nulls() const { return _num_nulls; }

    size_t num_filtered() const { return _num_filtered; }

    double filter_ratio() const { return _has_filter ? _filter_ratio : 0; }

    void fallback_filter() { _has_filter = false; }

    bool has_filter() const { return _has_filter; }

    bool can_filter_all(size_t remaining_num_values);

    bool filter_all() const { return _filter_all; }

    void skip(size_t num_values);

    void reset() {
        if (_has_filter) {
            _filter_map_index = 0;
        }
    }

    template <bool has_filter>
    size_t get_next_run(DataReadType* data_read_type) {
        DCHECK_EQ(_has_filter, has_filter);
        if constexpr (has_filter) {
            if (_read_index == _num_values) {
                return 0;
            }
            const DataReadType& type = _data_map[_read_index++];
            size_t run_length = 1;
            while (_read_index < _num_values) {
                if (_data_map[_read_index] == type) {
                    run_length++;
                    _read_index++;
                } else {
                    break;
                }
            }
            *data_read_type = type;
            return run_length;
        } else {
            size_t run_length = 0;
            while (run_length == 0) {
                if (_read_index == (*_run_length_null_map).size()) {
                    return 0;
                }
                *data_read_type = _read_index % 2 == 0 ? CONTENT : NULL_DATA;
                run_length = (*_run_length_null_map)[_read_index++];
            }
            return run_length;
        }
    }

    void set_run_length_null_map(const std::vector<uint16_t>& run_length_null_map,
                                 size_t num_values, NullMap* null_map = nullptr);

private:
    std::vector<DataReadType> _data_map;
    // the length of non-null values and null values are arranged in turn.
    const std::vector<uint16_t>* _run_length_null_map;
    bool _has_filter = false;
    // only used when the whole batch is skipped
    bool _filter_all = false;
    const uint8_t* _filter_map = nullptr;
    size_t _filter_map_size = 0;
    double _filter_ratio = 0;
    size_t _filter_map_index = 0;

    // generated in set_run_length_null_map
    size_t _num_values;
    size_t _num_nulls;
    size_t _num_filtered;
    size_t _read_index;
};

enum class ColumnOrderName { UNDEFINED, TYPE_DEFINED_ORDER };

enum class SortOrder { SIGNED, UNSIGNED, UNKNOWN };

class ParsedVersion {
public:
    ParsedVersion(std::string application, std::optional<std::string> version,
                  std::optional<std::string> appBuildHash)
            : application(std::move(application)),
              version(std::move(version)),
              appBuildHash(std::move(appBuildHash)) {}

    bool operator==(const ParsedVersion& other) const {
        return application == other.application && version == other.version &&
               appBuildHash == other.appBuildHash;
    }

    bool operator!=(const ParsedVersion& other) const { return !(*this == other); }

    size_t hash() const {
        std::hash<std::string> hasher;
        return hasher(application) ^ (version ? hasher(*version) : 0) ^
               (appBuildHash ? hasher(*appBuildHash) : 0);
    }

    std::string toString() const {
        return "ParsedVersion(application=" + application +
               ", semver=" + (version ? *version : "null") +
               ", appBuildHash=" + (appBuildHash ? *appBuildHash : "null") + ")";
    }

public:
    std::string application;
    std::optional<std::string> version;
    std::optional<std::string> appBuildHash;
};

class VersionParser {
public:
    static Status parse(const std::string& createdBy,
                        std::unique_ptr<ParsedVersion>* parsedVersion) {
        static const std::string FORMAT =
                "(.*?)\\s+version\\s*(?:([^(]*?)\\s*(?:\\(\\s*build\\s*([^)]*?)\\s*\\))?)?";
        static const std::regex PATTERN(FORMAT);

        std::smatch matcher;
        if (!std::regex_match(createdBy, matcher, PATTERN)) {
            return Status::InternalError(fmt::format(
                    "Could not parse created_by: {}, using format: {}", createdBy, FORMAT));
        }

        std::string application = matcher[1].str();
        if (application.empty()) {
            return Status::InternalError("application cannot be null or empty");
        }
        std::optional<std::string> semver = matcher[2].str().empty()
                                                    ? std::nullopt
                                                    : std::optional<std::string>(matcher[2].str());
        std::optional<std::string> appBuildHash =
                matcher[3].str().empty() ? std::nullopt
                                         : std::optional<std::string>(matcher[3].str());
        *parsedVersion = std::make_unique<ParsedVersion>(application, semver, appBuildHash);
        return Status::OK();
    }
};

class SemanticVersion {
public:
    SemanticVersion(int major, int minor, int patch)
            : _major(major),
              _minor(minor),
              _patch(patch),
              _prerelease(false),
              _unknown(std::nullopt),
              _pre(std::nullopt),
              _build_info(std::nullopt) {}

#ifdef BE_TEST
    SemanticVersion(int major, int minor, int patch, bool has_unknown)
            : _major(major),
              _minor(minor),
              _patch(patch),
              _prerelease(has_unknown),
              _unknown(std::nullopt),
              _pre(std::nullopt),
              _build_info(std::nullopt) {}
#endif

    SemanticVersion(int major, int minor, int patch, std::optional<std::string> unknown,
                    std::optional<std::string> pre, std::optional<std::string> build_info)
            : _major(major),
              _minor(minor),
              _patch(patch),
              _prerelease(unknown.has_value() && !unknown.value().empty()),
              _unknown(std::move(unknown)),
              _pre(pre.has_value() ? std::optional<Prerelease>(Prerelease(std::move(pre.value())))
                                   : std::nullopt),
              _build_info(std::move(build_info)) {}

    static Status parse(const std::string& version,
                        std::unique_ptr<SemanticVersion>* semantic_version) {
        static const std::regex pattern(
                R"(^(\d+)\.(\d+)\.(\d+)([^-+]*)?(?:-([^+]*))?(?:\+(.*))?$)");
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
            return Status::InternalError("major({}), minor({}), and patch({}) must all be >= 0",
                                         major, minor, patch);
        }
        *semantic_version = std::make_unique<SemanticVersion>(major, minor, patch, unknown,
                                                              prerelease, build_info);
        return Status::OK();
    }

    int compareTo(const SemanticVersion& other) const {
        if (int cmp = compareIntegers(_major, other._major); cmp != 0) return cmp;
        if (int cmp = compareIntegers(_minor, other._minor); cmp != 0) return cmp;
        if (int cmp = compareIntegers(_patch, other._patch); cmp != 0) return cmp;
        if (int cmp = compareBooleans(other._prerelease, _prerelease); cmp != 0) return cmp;
        if (_pre.has_value()) {
            if (other._pre.has_value()) {
                return _pre.value().compareTo(other._pre.value());
            } else {
                return -1;
            }
        } else if (other._pre.has_value()) {
            return 1;
        }
        return 0;
    }

    bool operator==(const SemanticVersion& other) const { return compareTo(other) == 0; }

    bool operator!=(const SemanticVersion& other) const { return !(*this == other); }

    std::string toString() const {
        std::string result = std::to_string(_major) + "." + std::to_string(_minor) + "." +
                             std::to_string(_patch);
        if (_prerelease && _unknown) result += _unknown.value();
        if (_pre) result += _pre.value().toString();
        if (_build_info) result += _build_info.value();
        return result;
    }

private:
    class NumberOrString {
    private:
        std::string original;
        bool isNumeric;
        int number;

    public:
        explicit NumberOrString(const std::string& numberOrString);

        NumberOrString(const NumberOrString& other)
                : original(other.original), isNumeric(other.isNumeric), number(other.number) {}

        int compareTo(const NumberOrString& that) const;
        std::string toString() const;

        bool operator<(const NumberOrString& that) const;
        bool operator==(const NumberOrString& that) const;
        bool operator!=(const NumberOrString& that) const;
        bool operator>(const NumberOrString& that) const;
        bool operator<=(const NumberOrString& that) const;
        bool operator>=(const NumberOrString& that) const;
    };

    class Prerelease {
    private:
        std::string _original;
        std::vector<NumberOrString> _identifiers;

        static std::vector<std::string> split(const std::string& s, const std::regex& delimiter);

    public:
        explicit Prerelease(std::string original);

        int compareTo(const Prerelease& that) const;
        std::string toString() const;

        bool operator<(const Prerelease& that) const;
        bool operator==(const Prerelease& that) const;
        bool operator!=(const Prerelease& that) const;
        bool operator>(const Prerelease& that) const;
        bool operator<=(const Prerelease& that) const;
        bool operator>=(const Prerelease& that) const;

        const std::string& original() const { return _original; }
    };

    int _major;
    int _minor;
    int _patch;
    bool _prerelease;
    std::optional<std::string> _unknown;
    std::optional<Prerelease> _pre;
    std::optional<std::string> _build_info;

    static int compareIntegers(int x, int y) { return (x < y) ? -1 : ((x == y) ? 0 : 1); }

    static int compareBooleans(bool x, bool y) { return (x == y) ? 0 : (x ? 1 : -1); }
};

class CorruptStatistics {
public:
    static bool should_ignore_statistics(const std::string& created_by,
                                         tparquet::Type::type physical_type) {
        if (physical_type != tparquet::Type::BYTE_ARRAY &&
            physical_type != tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
            // The bug only applies to binary columns
            return false;
        }

        if (created_by.empty()) {
            // created_by is not populated
            warn_once(
                    "Ignoring statistics because created_by is null or empty! See PARQUET-251 and "
                    "PARQUET-297");
            return true;
        }

        Status status;
        std::unique_ptr<ParsedVersion> parsed_version;
        status = VersionParser::parse(created_by, &parsed_version);
        if (!status.ok()) {
            warn_parse_error_once(created_by, status.msg());
            return true;
        }

        if (parsed_version->application != "parquet-mr") {
            // Assume other applications don't have this bug
            return false;
        }

        if ((!parsed_version->version.has_value()) || parsed_version->version.value().empty()) {
            warn_once(
                    "Ignoring statistics because created_by did not contain a semver (see "
                    "PARQUET-251): " +
                    created_by);
            return true;
        }

        std::unique_ptr<SemanticVersion> semantic_version;
        status = SemanticVersion::parse(parsed_version->version.value(), &semantic_version);
        if (!status.ok()) {
            warn_parse_error_once(created_by, status.msg());
            return true;
        }
        if (semantic_version->compareTo(PARQUET_251_FIXED_VERSION) < 0 &&
            !(semantic_version->compareTo(CDH_5_PARQUET_251_FIXED_START) >= 0 &&
              semantic_version->compareTo(CDH_5_PARQUET_251_FIXED_END) < 0)) {
            warn_once(
                    "Ignoring statistics because this file was created prior to the fixed version, "
                    "see PARQUET-251");
            return true;
        }

        // This file was created after the fix
        return false;
    }

private:
    static void warn_parse_error_once(const std::string& createdBy, const std::string_view& msg) {
        //if (!already_logged.exchange(true)) {
        LOG(WARNING) << "Ignoring statistics because created_by could not be parsed (see "
                        "PARQUET-251)."
                        " CreatedBy: "
                     << createdBy << ", msg: " << msg;
        //}
    }

    static void warn_once(const std::string_view& msg) {
        //if (!already_logged.exchange(true)) {
        LOG(WARNING) << msg;
        //}
    }

    static std::atomic<bool> already_logged;

    static const SemanticVersion PARQUET_251_FIXED_VERSION;
    static const SemanticVersion CDH_5_PARQUET_251_FIXED_START;
    static const SemanticVersion CDH_5_PARQUET_251_FIXED_END;
};

} // namespace doris::vectorized
