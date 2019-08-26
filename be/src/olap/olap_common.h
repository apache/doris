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

namespace doris {

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
    int64_t path_hash;
    int64_t capacity;                  // 总空间，单位字节
    int64_t available;                 // 可用空间，单位字节
    int64_t data_used_capacity;
    bool is_used;                       // 是否可用标识
    TStorageMedium::type storage_medium;  // 存储介质类型：SSD|HDD
};

struct TabletInfo {
    TabletInfo(
            TTabletId in_tablet_id,
            TSchemaHash in_schema_hash, 
            UniqueId in_uid) :
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
    OLAP_FIELD_TYPE_BOOL = 24
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
    OLAP_FIELD_AGGREGATION_UNKNOWN = 6
};

// 压缩算法类型
enum OLAPCompressionType {
    OLAP_COMP_TRANSPORT = 1,    // 用于网络传输的压缩算法，压缩率低，cpu开销低
    OLAP_COMP_STORAGE = 2,      // 用于硬盘数据的压缩算法，压缩率高，cpu开销大
    OLAP_COMP_LZ4 = 3,          // 用于储存的压缩算法，压缩率低，cpu开销低
};

// hll数据存储格式,优化存储结构减少多余空间的占用
enum HllDataType {
    HLL_DATA_EMPTY = 0,      // 用于记录空的hll集合
    HLL_DATA_EXPLICIT,    // 直接存储hash后结果的集合类型
    HLL_DATA_SPRASE,     // 记录register不为空的集合类型
    HLL_DATA_FULL,        // 记录完整的hll集合
    HLL_DATA_NONE
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
typedef std::pair<int64_t, int64_t> Version;
typedef std::vector<Version> Versions;


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

    int64_t bytes_read = 0;

    int64_t block_load_ns = 0;
    int64_t blocks_load = 0;
    int64_t block_fetch_ns = 0;
    int64_t block_seek_ns = 0;
    int64_t block_convert_ns = 0;

    int64_t raw_rows_read = 0;

    int64_t rows_vec_cond_filtered = 0;
    int64_t vec_cond_ns = 0;

    int64_t rows_stats_filtered = 0;
    int64_t rows_del_filtered = 0;

    int64_t index_load_ns = 0;
};

typedef uint32_t ColumnId;
// Column unique id set
typedef std::set<uint32_t> UniqueIdSet;
// Column unique Id -> column id map
typedef std::map<ColumnId, ColumnId> UniqueIdToColumnIdMap;

typedef int64_t RowsetId;

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_OLAP_COMMON_H
