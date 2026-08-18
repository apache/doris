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

// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep
#include <limits.h>
#include <stdint.h>
#include <sys/time.h>

#include <array>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <iterator>
#include <limits>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/olap_common.h"

namespace doris {
static const std::string DELETE_SIGN = "__DORIS_DELETE_SIGN__";
static const std::string WHERE_SIGN = "__DORIS_WHERE_SIGN__";
static const std::string VERSION_COL = "__DORIS_VERSION_COL__";
static const std::string SKIP_BITMAP_COL = "__DORIS_SKIP_BITMAP_COL__";
static const std::string SEQUENCE_COL = "__DORIS_SEQUENCE_COL__";

// 用来加速运算
const static int32_t g_power_table[] = {1,      10,      100,      1000,      10000,
                                        100000, 1000000, 10000000, 100000000, 1000000000};

// 计时工具，用于确定一段代码执行的时间，用于性能调优
class OlapStopWatch {
public:
    uint64_t get_elapse_time_us() const {
        struct timeval now;
        gettimeofday(&now, nullptr);
        return (uint64_t)((now.tv_sec - _begin_time.tv_sec) * 1e6 +
                          (now.tv_usec - _begin_time.tv_usec));
    }

    double get_elapse_second() const { return get_elapse_time_us() / 1000000.0; }

    void reset() { gettimeofday(&_begin_time, nullptr); }

    OlapStopWatch() { reset(); }

private:
    struct timeval _begin_time; // 起始时间戳
};

// @brief 切分字符串
// @param base 原串
// @param separator 分隔符
// @param result 切分结果
template <typename Str, typename T>
Status split_string(const Str& base, const T separator, std::vector<std::string>* result) {
    if (!result) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT>("split_string meet nullptr result input");
    }

    // 处理base为空的情况
    // 在删除功能中，当varchar类型列的过滤条件为空时，会出现这种情况
    if (base.size() == 0) {
        result->push_back("");
        return Status::OK();
    }

    size_t offset = 0;
    while (offset < base.length()) {
        size_t next = base.find(separator, offset);
        if (next == std::string::npos) {
            result->emplace_back(base.substr(offset));
            break;
        } else {
            result->emplace_back(base.substr(offset, next - offset));
            offset = next + 1;
        }
    }

    return Status::OK();
}

uint32_t olap_adler32_init();
uint32_t olap_adler32(uint32_t adler, const char* buf, size_t len);

// 获取系统当前时间，并将时间转换为字符串
Status gen_timestamp_string(std::string* out_string);

Status check_datapath_rw(const std::string& path);

Status read_write_test_file(const std::string& test_file_path);

// 打印Errno
class Errno {
public:
    // 返回Errno对应的错误信息,线程安全
    static const char* str();
    static const char* str(int no);
    static int no();

private:
    static const int BUF_SIZE = 256;
    static __thread char _buf[BUF_SIZE];
};

// 检查int8_t, int16_t, int32_t, int64_t的值是否溢出
template <typename T>
bool valid_signed_number(const std::string& value_str) {
    char* endptr = nullptr;
    errno = 0;
    int64_t value = strtol(value_str.c_str(), &endptr, 10);

    if ((errno == ERANGE && (value == LONG_MAX || value == LONG_MIN)) ||
        (errno != 0 && value == 0) || endptr == value_str || *endptr != '\0') {
        return false;
    }

    if (value < std::numeric_limits<T>::min() || value > std::numeric_limits<T>::max()) {
        return false;
    }

    return true;
}

template <>
bool valid_signed_number<int128_t>(const std::string& value_str);

// 检查uint8_t, uint16_t, uint32_t, uint64_t的值是否溢出
template <typename T>
bool valid_unsigned_number(const std::string& value_str) {
    if (value_str[0] == '-') {
        return false;
    }

    char* endptr = nullptr;
    errno = 0;
    uint64_t value = strtoul(value_str.c_str(), &endptr, 10);

    if ((errno == ERANGE && (value == ULONG_MAX)) || (errno != 0 && value == 0) ||
        endptr == value_str || *endptr != '\0') {
        return false;
    }

    if (value < std::numeric_limits<T>::min() || value > std::numeric_limits<T>::max()) {
        return false;
    }

    return true;
}

bool valid_decimal(const std::string& value_str, const uint32_t precision, const uint32_t frac);

// Validate for date/datetime roughly. The format is 'yyyy-MM-dd HH:mm:ss'
// TODO: support 'yyyy-MM-dd HH:mm:ss.SSS'
bool valid_datetime(const std::string& value_str, const uint32_t scale);

bool valid_bool(const std::string& value_str);

bool valid_ipv4(const std::string& value_str);

bool valid_ipv6(const std::string& value_str);

constexpr bool is_string_type(const FieldType& field_type) {
    return field_type == FieldType::OLAP_FIELD_TYPE_VARCHAR ||
           field_type == FieldType::OLAP_FIELD_TYPE_CHAR ||
           field_type == FieldType::OLAP_FIELD_TYPE_STRING;
}

// Util used to get string name of thrift enum item
#define EnumToString(enum_type, index, out)                   \
    do {                                                      \
        auto it = _##enum_type##_VALUES_TO_NAMES.find(index); \
        if (it == _##enum_type##_VALUES_TO_NAMES.end()) {     \
            out = "NULL";                                     \
        } else {                                              \
            out = it->second;                                 \
        }                                                     \
    } while (0)

struct RowLocation {
    RowLocation() : segment_id(0), row_id(0) {}
    RowLocation(uint32_t sid, uint32_t rid) : segment_id(sid), row_id(rid) {}
    RowLocation(RowsetId rsid, uint32_t sid, uint32_t rid)
            : rowset_id(rsid), segment_id(sid), row_id(rid) {}
    RowsetId rowset_id;
    uint32_t segment_id;
    uint32_t row_id;

    bool operator==(const RowLocation& rhs) const {
        return rowset_id == rhs.rowset_id && segment_id == rhs.segment_id && row_id == rhs.row_id;
    }

    bool operator<(const RowLocation& rhs) const {
        if (rowset_id != rhs.rowset_id) {
            return rowset_id < rhs.rowset_id;
        } else if (segment_id != rhs.segment_id) {
            return segment_id < rhs.segment_id;
        } else {
            return row_id < rhs.row_id;
        }
    }
};
using RowLocationSet = std::set<RowLocation>;
using RowLocationPairList = std::list<std::pair<RowLocation, RowLocation>>;

struct GlobalRowLoacation {
    GlobalRowLoacation(int64_t tid, RowsetId rsid, uint32_t sid, uint32_t rid)
            : tablet_id(tid), row_location(rsid, sid, rid) {}
    int64_t tablet_id;
    RowLocation row_location;

    bool operator==(const GlobalRowLoacation& rhs) const {
        return tablet_id == rhs.tablet_id && row_location == rhs.row_location;
    }

    bool operator<(const GlobalRowLoacation& rhs) const {
        if (tablet_id != rhs.tablet_id) {
            return tablet_id < rhs.tablet_id;
        } else {
            return row_location < rhs.row_location;
        }
    }
};

// Wire-protocol values: never reorder or reuse an existing value. A new value may use a new
// encoded structure and size, provided its decoder keeps supporting all older values.
enum class ROW_VERSION : uint8_t {
    // The row ID is a uint32 ordinal local to the FileMapping identified by file_id.
    FILE_LOCAL_ROW_ID = 0,
    // The row ID is an opaque uint64 ID in a fixed Lance dataset snapshot.
    LANCE_DATASET_ROW_ID = 1,
};

/*
 * A serialized global row location has a fixed size of 24 bytes. The version determines how the
 * bytes at offsets 4..7 and 16..23 must be interpreted:
 *
 * FILE_LOCAL_ROW_ID (version = 0), used by Doris, Parquet, and ORC:
 *
 *   byte offset   0        1..7             8..15          16..19      20..23
 *               +--------+----------------+---------------+-----------+-----------+
 *               | ver=0  | reserved       | backend_id    | file_id   | row_id    |
 *               +--------+----------------+---------------+-----------+-----------+
 *                 uint8      7 bytes          int64          uint32      uint32
 *
 *   row_id is an ordinal local to the FileMapping selected by file_id.
 *
 * LANCE_DATASET_ROW_ID (version = 1), used by Lance:
 *
 *   byte offset   0        1..3       4..7          8..15          16..23
 *               +--------+----------+-------------+---------------+----------------+
 *               | ver=1  | reserved | file_id     | backend_id    | lance_row_id   |
 *               +--------+----------+-------------+---------------+----------------+
 *                 uint8     3 bytes     uint32        int64           uint64
 *
 *   lance_row_id is an opaque row ID in the fixed dataset snapshot recorded by the FileMapping.
 *
 * The first union reuses four bytes that are padding in version 0 as Lance's file_id in version 1.
 * The second union reuses the original {uint32 file_id, uint32 row_id} payload as one uint64 Lance
 * row ID. Therefore, always check version before reading either union.
 */
struct GlobalRowLoacationV2 {
    static constexpr uint8_t VERSION = static_cast<uint8_t>(ROW_VERSION::FILE_LOCAL_ROW_ID);

    struct FileLocalRowId {
        uint32_t file_id;
        uint32_t row_id;
    };

    GlobalRowLoacationV2(uint8_t ver, uint64_t bid, uint32_t fid, uint32_t rid)
            : version(ver),
              reserved_for_file_local(0),
              backend_id(bid),
              file_local {.file_id = fid, .row_id = rid} {}
    GlobalRowLoacationV2(ROW_VERSION ver, uint64_t bid, uint32_t fid, uint64_t rid)
            : version(static_cast<uint8_t>(ver)),
              lance_file_id(fid),
              backend_id(bid),
              lance_row_id(rid) {}

    uint8_t version;
    std::array<uint8_t, 3> reserved_before_file_id {};
    union {
        // version 0: offsets 4..7 remain reserved, preserving the original V2 layout.
        uint32_t reserved_for_file_local;
        // version 1: offsets 4..7 identify the FileMapping for lance_row_id.
        uint32_t lance_file_id;
    };
    int64_t backend_id;
    union {
        // version 0: file_id is at offset 16 and its uint32 row ordinal is at offset 20.
        FileLocalRowId file_local;
        // version 1: offsets 16..23 are one opaque uint64 Lance row ID.
        uint64_t lance_row_id;
    };
};

static_assert(sizeof(GlobalRowLoacationV2) == 24);
static_assert(sizeof(GlobalRowLoacationV2::FileLocalRowId) == 8);
static_assert(offsetof(GlobalRowLoacationV2, version) == 0);
static_assert(offsetof(GlobalRowLoacationV2, reserved_before_file_id) == 1);
static_assert(offsetof(GlobalRowLoacationV2, reserved_for_file_local) == 4);
static_assert(offsetof(GlobalRowLoacationV2, lance_file_id) == 4);
static_assert(offsetof(GlobalRowLoacationV2, backend_id) == 8);
static_assert(offsetof(GlobalRowLoacationV2, file_local) == 16);
static_assert(offsetof(GlobalRowLoacationV2::FileLocalRowId, file_id) == 0);
static_assert(offsetof(GlobalRowLoacationV2::FileLocalRowId, row_id) == 4);
static_assert(offsetof(GlobalRowLoacationV2, lance_row_id) == 16);

} // namespace doris
