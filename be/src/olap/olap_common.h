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

#include <netinet/in.h>

#include <functional>
#include <list>
#include <map>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <typeinfo>
#include <unordered_map>
#include <unordered_set>

#include "env/env.h"
#include "gen_cpp/Types_types.h"
#include "olap/olap_define.h"
#include "util/hash_util.hpp"
#include "util/uid_util.h"

#define LOW_56_BITS 0x00ffffffffffffff

namespace doris {

static const int64_t MAX_ROWSET_ID = 1L << 56;

typedef int32_t SchemaHash;
typedef __int128 int128_t;
typedef unsigned __int128 uint128_t;

typedef UniqueId TabletUid;

enum CompactionType { BASE_COMPACTION = 1, CUMULATIVE_COMPACTION = 2 };

struct DataDirInfo {
    FilePathDesc path_desc;
    size_t path_hash = 0;
    int64_t disk_capacity = 1; // actual disk capacity
    int64_t available = 0;     // available space, in bytes unit
    int64_t data_used_capacity = 0;
    bool is_used = false;                                      // whether available mark
    TStorageMedium::type storage_medium = TStorageMedium::HDD; // Storage medium type: SSD|HDD
};

// Sort DataDirInfo by available space.
struct DataDirInfoLessAvailability {
    bool operator()(const DataDirInfo& left, const DataDirInfo& right) const {
        return left.available < right.available;
    }
};

struct TabletInfo {
    TabletInfo(TTabletId in_tablet_id, TSchemaHash in_schema_hash, UniqueId in_uid)
            : tablet_id(in_tablet_id), schema_hash(in_schema_hash), tablet_uid(in_uid) {}

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

struct TabletSize {
    TabletSize(TTabletId in_tablet_id, TSchemaHash in_schema_hash, size_t in_tablet_size)
            : tablet_id(in_tablet_id), schema_hash(in_schema_hash), tablet_size(in_tablet_size) {}

    TTabletId tablet_id;
    TSchemaHash schema_hash;
    size_t tablet_size;
};

enum RangeCondition {
    GT = 0, // greater than
    GE = 1, // greater or equal
    LT = 2, // less than
    LE = 3, // less or equal
};

enum DelCondSatisfied {
    DEL_SATISFIED = 0,         //satisfy delete condition
    DEL_NOT_SATISFIED = 1,     //not satisfy delete condition
    DEL_PARTIAL_SATISFIED = 2, //partially satisfy delete condition
};
// Define all data types supported by Field.
// If new filed_type is defined, not only new TypeInfo may need be defined,
// but also some functions like get_type_info in types.cpp need to be changed.
enum FieldType {
    OLAP_FIELD_TYPE_TINYINT = 1, // MYSQL_TYPE_TINY
    OLAP_FIELD_TYPE_UNSIGNED_TINYINT = 2,
    OLAP_FIELD_TYPE_SMALLINT = 3, // MYSQL_TYPE_SHORT
    OLAP_FIELD_TYPE_UNSIGNED_SMALLINT = 4,
    OLAP_FIELD_TYPE_INT = 5, // MYSQL_TYPE_LONG
    OLAP_FIELD_TYPE_UNSIGNED_INT = 6,
    OLAP_FIELD_TYPE_BIGINT = 7, // MYSQL_TYPE_LONGLONG
    OLAP_FIELD_TYPE_UNSIGNED_BIGINT = 8,
    OLAP_FIELD_TYPE_LARGEINT = 9,
    OLAP_FIELD_TYPE_FLOAT = 10,  // MYSQL_TYPE_FLOAT
    OLAP_FIELD_TYPE_DOUBLE = 11, // MYSQL_TYPE_DOUBLE
    OLAP_FIELD_TYPE_DISCRETE_DOUBLE = 12,
    OLAP_FIELD_TYPE_CHAR = 13,     // MYSQL_TYPE_STRING
    OLAP_FIELD_TYPE_DATE = 14,     // MySQL_TYPE_NEWDATE
    OLAP_FIELD_TYPE_DATETIME = 15, // MySQL_TYPE_DATETIME
    OLAP_FIELD_TYPE_DECIMAL = 16,  // DECIMAL, using different store format against MySQL
    OLAP_FIELD_TYPE_VARCHAR = 17,

    OLAP_FIELD_TYPE_STRUCT = 18,  // Struct
    OLAP_FIELD_TYPE_ARRAY = 19,   // ARRAY
    OLAP_FIELD_TYPE_MAP = 20,     // Map
    OLAP_FIELD_TYPE_UNKNOWN = 21, // UNKNOW OLAP_FIELD_TYPE_STRING
    OLAP_FIELD_TYPE_NONE = 22,
    OLAP_FIELD_TYPE_HLL = 23,
    OLAP_FIELD_TYPE_BOOL = 24,
    OLAP_FIELD_TYPE_OBJECT = 25,
    OLAP_FIELD_TYPE_STRING = 26,
    OLAP_FIELD_TYPE_QUANTILE_STATE = 27
};

// Define all aggregation methods supported by Field
// Note that in practice, not all types can use all the following aggregation methods
// For example, it is meaningless to use SUM for the string type (but it will not cause the program to crash)
// The implementation of the Field class does not perform such checks, and should be constrained when creating the table
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
    OLAP_FIELD_AGGREGATION_QUANTILE_UNION = 9
};

// Compression algorithm type
enum OLAPCompressionType {
    // Compression algorithm used for network transmission, low compression rate, low cpu overhead
    OLAP_COMP_TRANSPORT = 1,
    // Compression algorithm used for hard disk data, with high compression rate and high CPU overhead
    OLAP_COMP_STORAGE = 2,
    // The compression algorithm used for storage, the compression rate is low, and the cpu overhead is low
    OLAP_COMP_LZ4 = 3,
};

enum PushType {
    PUSH_NORMAL = 1,          // for broker/hadoop load
    PUSH_FOR_DELETE = 2,      // for delete
    PUSH_FOR_LOAD_DELETE = 3, // not use
    PUSH_NORMAL_V2 = 4,       // for spark load
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

    bool operator!=(const Version& rhs) const { return first != rhs.first || second != rhs.second; }

    bool operator==(const Version& rhs) const { return first == rhs.first && second == rhs.second; }

    bool contains(const Version& other) const {
        return first <= other.first && second >= other.second;
    }
};

typedef std::vector<Version> Versions;

inline std::ostream& operator<<(std::ostream& os, const Version& version) {
    return os << "[" << version.first << "-" << version.second << "]";
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
    int64_t value = 0;
    std::list<int64_t> edges;

    Vertex(int64_t v) : value(v) {}
};

class Field;
class WrapperField;
using KeyRange = std::pair<WrapperField*, WrapperField*>;

static const int GENERAL_DEBUG_COUNT = 0;

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
    int64_t block_fetch_ns = 0; // time of rowset reader's `next_batch()` call
    int64_t block_seek_num = 0;
    int64_t block_seek_ns = 0;
    int64_t block_convert_ns = 0;

    int64_t raw_rows_read = 0;

    int64_t rows_vec_cond_filtered = 0;
    int64_t rows_vec_del_cond_filtered = 0;
    int64_t vec_cond_ns = 0;
    int64_t short_cond_ns = 0;
    int64_t pred_col_read_ns = 0;
    int64_t lazy_read_ns = 0;
    int64_t output_col_ns = 0;

    int64_t rows_key_range_filtered = 0;
    int64_t rows_stats_filtered = 0;
    int64_t rows_bf_filtered = 0;
    // Including the number of rows filtered out according to the Delete information in the Tablet,
    // and the number of rows filtered for marked deleted rows under the unique key model.
    // This metric is mainly used to record the number of rows filtered by the delete condition in Segment V1,
    // and it is also used to record the replaced rows in the Unique key model in the "Reader" class.
    // In segmentv2, if you want to get all filtered rows, you need the sum of "rows_del_filtered" and "rows_conditions_filtered".
    int64_t rows_del_filtered = 0;
    // the number of rows filtered by various column indexes.
    int64_t rows_conditions_filtered = 0;

    int64_t index_load_ns = 0;

    int64_t total_pages_num = 0;
    int64_t cached_pages_num = 0;

    int64_t rows_bitmap_index_filtered = 0;
    int64_t bitmap_index_filter_timer = 0;
    // number of segment filtered by column stat when creating seg iterator
    int64_t filtered_segment_number = 0;
    // total number of segment
    int64_t total_segment_number = 0;
    // general_debug_ns is designed for the purpose of DEBUG, to record any infomations of debugging or profiling.
    // different from specific meaningful timer such as index_load_ns, general_debug_ns can be used flexibly.
    // general_debug_ns has associated with OlapScanNode's _general_debug_timer already.
    // so general_debug_ns' values will update to _general_debug_timer automaticly,
    // the timer result can be checked through QueryProfile web page easily.
    // when search general_debug_ns, you can find that general_debug_ns has not been used,
    // this is because such codes added for debug purpose should not commit, it's just for debuging.
    // so, please do not delete general_debug_ns defined here
    // usage example:
    //               SCOPED_RAW_TIMER(&_stats->general_debug_ns[1]);
    int64_t general_debug_ns[GENERAL_DEBUG_COUNT] = {};
};

typedef uint32_t ColumnId;
// Column unique id set
typedef std::set<uint32_t> UniqueIdSet;
// Column unique Id -> column id map
typedef std::map<ColumnId, ColumnId> UniqueIdToColumnIdMap;

// 8 bit rowset id version
// 56 bit, inc number from 1
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

    // to compatible with old version
    void init(int64_t rowset_id) { init(1, rowset_id, 0, 0); }

    void init(int64_t id_version, int64_t high, int64_t middle, int64_t low) {
        version = id_version;
        if (UNLIKELY(high >= MAX_ROWSET_ID)) {
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

} // namespace doris
