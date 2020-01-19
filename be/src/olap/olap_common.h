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

#ifndef DORIS_BE_SRC_OLAP_OLAP_COMMON_H
#define DORIS_BE_SRC_OLAP_OLAP_COMMON_H

#include <netinet/in.h>

#include <list>
#include <map>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <typeinfo>
#include <unordered_map>
#include <unordered_set>
#include <functional>

#include "gen_cpp/Types_types.h"
#include "olap/olap_define.h"
#include "util/hash_util.hpp"
#include "util/uid_util.h"

#define LOW_56_BITS 0x00ffffffffffffff

namespace doris {

static const int64_t MAX_ROWSET_ID = 1L << 56;

typedef int32_t SchemaHash;
typedef int64_t VersionHash;
typedef __int128 int128_t;
typedef unsigned __int128 uint128_t;

typedef UniqueId TabletUid;

enum CompactionType {
    BASE_COMPACTION = 1,
    CUMULATIVE_COMPACTION = 2
};

struct DataDirInfo {
    DataDirInfo():
            capacity(1),
            available(0),
            data_used_capacity(0),
            is_used(false) { }

    std::string path;
    size_t path_hash;
    int64_t capacity;                  // 总空间，单位字节
    int64_t available;                 // 可用空间，单位字节
    int64_t data_used_capacity;
    bool is_used;                       // 是否可用标识
    TStorageMedium::type storage_medium;  // 存储介质类型：SSD|HDD
};

struct TabletInfo {
    TabletInfo(TTabletId in_tablet_id, TSchemaHash in_schema_hash, UniqueId in_uid) :
            tablet_id(in_tablet_id),
            schema_hash(in_schema_hash),
            tablet_uid(in_uid) {}

    bool operator<(const TabletInfo& right) const {
        if (tablet_id != right.tablet_id) {
            return tablet_id < right.tablet_id;
        } else if (schema_hash != right.schema_hash) {
            return schema_hash < right.schema_hash;
        } else {
            return tablet_uid < right.tablet_uid;
        }
    }

    std::string to_string() const {
        std::stringstream ss;
        ss << tablet_id << "." << schema_hash << "." << tablet_uid.to_string();
        return ss.str();
    }

    TTabletId tablet_id;
    TSchemaHash schema_hash;
    UniqueId tablet_uid;
};

enum RangeCondition {
    GT = 0,     // greater than
    GE = 1,     // greater or equal
    LT = 2,     // less than
    LE = 3,     // less or equal
};

enum DelCondSatisfied {
    DEL_SATISFIED = 0, //satisfy delete condition
    DEL_NOT_SATISFIED = 1, //not satisfy delete condition
    DEL_PARTIAL_SATISFIED = 2, //partially satisfy delete condition
};

// 定义Field支持的所有数据类型
enum FieldType {
    OLAP_FIELD_TYPE_TINYINT = 1,           // MYSQL_TYPE_TINY
    OLAP_FIELD_TYPE_UNSIGNED_TINYINT = 2,
    OLAP_FIELD_TYPE_SMALLINT = 3,          // MYSQL_TYPE_SHORT
    OLAP_FIELD_TYPE_UNSIGNED_SMALLINT = 4,
    OLAP_FIELD_TYPE_INT = 5,            // MYSQL_TYPE_LONG
    OLAP_FIELD_TYPE_UNSIGNED_INT = 6,
    OLAP_FIELD_TYPE_BIGINT = 7,           // MYSQL_TYPE_LONGLONG
    OLAP_FIELD_TYPE_UNSIGNED_BIGINT = 8,
    OLAP_FIELD_TYPE_LARGEINT = 9,
    OLAP_FIELD_TYPE_FLOAT = 10,          // MYSQL_TYPE_FLOAT
    OLAP_FIELD_TYPE_DOUBLE = 11,        // MYSQL_TYPE_DOUBLE
    OLAP_FIELD_TYPE_DISCRETE_DOUBLE = 12,
    OLAP_FIELD_TYPE_CHAR = 13,        // MYSQL_TYPE_STRING
    OLAP_FIELD_TYPE_DATE = 14,          // MySQL_TYPE_NEWDATE
    OLAP_FIELD_TYPE_DATETIME = 15,      // MySQL_TYPE_DATETIME
    OLAP_FIELD_TYPE_DECIMAL = 16,       // DECIMAL, using different store format against MySQL
    OLAP_FIELD_TYPE_VARCHAR = 17,

    OLAP_FIELD_TYPE_STRUCT = 18,        // Struct
    OLAP_FIELD_TYPE_LIST = 19,          // LIST
    OLAP_FIELD_TYPE_MAP = 20,           // Map
    OLAP_FIELD_TYPE_UNKNOWN = 21,       // UNKNOW Type
    OLAP_FIELD_TYPE_NONE = 22,
    OLAP_FIELD_TYPE_HLL = 23,
    OLAP_FIELD_TYPE_BOOL = 24,
    OLAP_FIELD_TYPE_OBJECT = 25
};

// 定义Field支持的所有聚集方法
// 注意，实际中并非所有的类型都能使用以下所有的聚集方法
// 例如对于string类型使用SUM就是毫无意义的(但不会导致程序崩溃)
// Field类的实现并没有进行这类检查，应该在创建表的时候进行约束
enum FieldAggregationMethod {
    OLAP_FIELD_AGGREGATION_NONE = 0,
    OLAP_FIELD_AGGREGATION_SUM = 1,
    OLAP_FIELD_AGGREGATION_MIN = 2,
    OLAP_FIELD_AGGREGATION_MAX = 3,
    OLAP_FIELD_AGGREGATION_REPLACE = 4,
    OLAP_FIELD_AGGREGATION_HLL_UNION = 5,
    OLAP_FIELD_AGGREGATION_UNKNOWN = 6,
    OLAP_FIELD_AGGREGATION_BITMAP_UNION = 7,
    // Replace if and only if added value is not null
    OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL = 8,
};

// 压缩算法类型
enum OLAPCompressionType {
    OLAP_COMP_TRANSPORT = 1,    // 用于网络传输的压缩算法，压缩率低，cpu开销低
    OLAP_COMP_STORAGE = 2,      // 用于硬盘数据的压缩算法，压缩率高，cpu开销大
    OLAP_COMP_LZ4 = 3,          // 用于储存的压缩算法，压缩率低，cpu开销低
};

enum PushType {
    PUSH_NORMAL = 1,
    PUSH_FOR_DELETE = 2,
    PUSH_FOR_LOAD_DELETE = 3,
};

enum ReaderType {
    READER_QUERY = 0,
    READER_ALTER_TABLE = 1,
    READER_BASE_COMPACTION = 2,
    READER_CUMULATIVE_COMPACTION = 3,
    READER_CHECKSUM = 4,
};

// <start_version_id, end_version_id>, such as <100, 110>
//using Version = std::pair<TupleVersion, TupleVersion>;

struct Version {
    int64_t first;
    int64_t second;

    Version(int64_t first_, int64_t second_) : first(first_), second(second_) {}
    Version() : first(0), second(0) {}

    friend std::ostream& operator<<(std::ostream& os, const Version& version);

    bool operator!=(const Version& rhs) const {
        return first != rhs.first || second != rhs.second;
    }

    bool operator==(const Version& rhs) const {
        return first == rhs.first && second == rhs.second;
    }

    bool contains(const Version& other) const {
        return first <= other.first && second >= other.second;
    }
};

typedef std::vector<Version> Versions;

inline std::ostream& operator<<(std::ostream& os, const Version& version) {
    return os << "["<< version.first << "-" << version.second << "]";
}

// used for hash-struct of hash_map<Version, Rowset*>.
struct HashOfVersion {
    size_t operator()(const Version& version) const {
        size_t seed = 0;
        seed = HashUtil::hash64(&version.first, sizeof(version.first), seed);
        seed = HashUtil::hash64(&version.second, sizeof(version.second), seed);
        return seed;
    }
};

// It is used to represent Graph vertex.
struct Vertex {
    int64_t value;
    std::list<int64_t>* edges;
};

class Field;
class WrapperField;
using KeyRange = std::pair<WrapperField*, WrapperField*>;

// ReaderStatistics used to collect statistics when scan data from storage
struct OlapReaderStatistics {
    int64_t io_ns = 0;
    int64_t compressed_bytes_read = 0;

    int64_t decompress_ns = 0;
    int64_t uncompressed_bytes_read = 0;

    // total read bytes in memory
    int64_t bytes_read = 0;

    int64_t block_load_ns = 0;
    int64_t blocks_load = 0;
    int64_t block_fetch_ns = 0;
    int64_t block_seek_num = 0;
    int64_t block_seek_ns = 0;
    int64_t block_convert_ns = 0;

    int64_t raw_rows_read = 0;

    int64_t rows_vec_cond_filtered = 0;
    int64_t vec_cond_ns = 0;

    int64_t rows_stats_filtered = 0;
    int64_t rows_bf_filtered = 0;
    int64_t rows_del_filtered = 0;

    int64_t index_load_ns = 0;

    int64_t total_pages_num = 0;
    int64_t cached_pages_num = 0;

    int64_t bitmap_index_filter_count = 0;
    int64_t bitmap_index_filter_timer = 0;
};

typedef uint32_t ColumnId;
// Column unique id set
typedef std::set<uint32_t> UniqueIdSet;
// Column unique Id -> column id map
typedef std::map<ColumnId, ColumnId> UniqueIdToColumnIdMap;

// 8 bit rowset id version
// 56 bit, inc number from 0
// 128 bit backend uid, it is a uuid bit, id version
struct RowsetId {
    int8_t version = 0;
    int64_t hi = 0;
    int64_t mi = 0;
    int64_t lo = 0;

    void init(const std::string& rowset_id_str) {
        // for new rowsetid its a 48 hex string
        // if the len < 48, then it is an old format rowset id
        if (rowset_id_str.length() < 48) {
            int64_t high = std::stol(rowset_id_str, nullptr, 10);
            init(1, high, 0, 0);
        } else {
            int64_t high = 0;
            int64_t middle = 0;
            int64_t low = 0;
            from_hex(&high, rowset_id_str.substr(0, 16));
            from_hex(&middle, rowset_id_str.substr(16, 16));
            from_hex(&low, rowset_id_str.substr(32, 16));
            init(high >> 56, high & LOW_56_BITS, middle, low);
        }
    }

    // to compatiable with old version
    void init(int64_t rowset_id) {
        init(1, rowset_id, 0, 0);
    }

    void init(int64_t id_version, int64_t high, int64_t middle, int64_t low) {
        version = id_version;
        if (high >= MAX_ROWSET_ID) {
            LOG(FATAL) << "inc rowsetid is too large:" << high;
        }
        hi = (id_version << 56) + (high & LOW_56_BITS);
        mi = middle;
        lo = low;
    }

    std::string to_string() const {
        if (version < 2) {
            return std::to_string(hi & LOW_56_BITS);
        } else {
            char buf[48];
            to_hex(hi, buf);
            to_hex(mi, buf + 16);
            to_hex(lo, buf + 32);
            return {buf, 48};
        }
    }

    // std::unordered_map need this api
    bool operator==(const RowsetId& rhs) const {
        return hi == rhs.hi && mi == rhs.mi && lo == rhs.lo;
    }

    bool operator!=(const RowsetId& rhs) const {
        return hi != rhs.hi || mi != rhs.mi || lo != rhs.lo;
    }

    bool operator<(const RowsetId& rhs) const {
        if (hi != rhs.hi) {
            return hi < rhs.hi;
        } else if (mi != rhs.mi) {
            return mi < rhs.mi;
        } else {
            return lo < rhs.lo;
        }
    }

    friend std::ostream& operator<<(std::ostream& out, const RowsetId& rowset_id) {
        out << rowset_id.to_string();
        return out;
    }
};

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_OLAP_COMMON_H
