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

#ifndef DORIS_BE_RUNTIME_DPP_SINK_INTERNAL_H
#define DORIS_BE_RUNTIME_DPP_SINK_INTERNAL_H

#include <string>
#include <vector>

#include "common/status.h"
#include "gen_cpp/Types_types.h"
#include "runtime/primitive_type.h"
#include "runtime/raw_value.h"
#include "util/hash_util.hpp"

namespace doris {

class ExprContext;
class MemTracker;
class ObjectPool;
class RuntimeState;
class RowDescriptor;
class TPartitionKey;
class TPartitionRange;
class TRangePartition;

class PartRangeKey {
public:
    PartRangeKey() {}

    ~PartRangeKey() {}

    static const PartRangeKey& pos_infinite() { return _s_pos_infinite; }

    static const PartRangeKey& neg_infinite() { return _s_neg_infinite; }

    static Status from_thrift(ObjectPool* pool, const TPartitionKey& t_key, PartRangeKey* key);

    static Status from_value(PrimitiveType type, void* value, PartRangeKey* key);

    bool operator==(const PartRangeKey& other) const {
        if (_sign != other._sign) {
            return false;
        }
        if (_sign != 0) {
            return true;
        }

        return RawValue::compare(_key, other._key, TypeDescriptor(_type)) == 0;
    }

    bool operator!=(const PartRangeKey& other) const { return !(*this == other); }

    bool operator<(const PartRangeKey& other) const {
        if (_sign < other._sign) {
            return true;
        } else if (_sign > other._sign) {
            return false;
        }
        if (_sign != 0) {
            return false;
        }
        return RawValue::compare(_key, other._key, TypeDescriptor(_type)) < 0;
    }

    bool operator<=(const PartRangeKey& other) const { return *this < other || *this == other; }

    bool operator>(const PartRangeKey& other) const { return !(*this <= other); }

    bool operator>=(const PartRangeKey& other) const { return !(*this < other); }

    std::string debug_string() const {
        std::stringstream debug_str;
        debug_str << "sign: " << _sign << "; "
                  << "type: " << type_to_string(_type) << "; "
                  << "key: " << _str_key;
        return debug_str.str();
    }

private:
    // Used for static variables
    PartRangeKey(int sign) : _sign(sign) {}
    // When sign is less than 0, that key represent negative infinite
    // When sign is greater than 0, that key represent positive infinite
    // When sign is equal 0, that key represent normal key
    int _sign;
    PrimitiveType _type;
    void* _key;
    std::string _str_key;

    static PartRangeKey _s_pos_infinite;
    static PartRangeKey _s_neg_infinite;
};

class PartRange {
public:
    PartRange() {}

    ~PartRange() {}

    static const PartRange& all() { return _s_all_range; }

    static Status from_thrift(ObjectPool* pool, const TPartitionRange& t_part_range,
                              PartRange* range);

    // return -1 : 'this' range is on the left of 'key'
    // return 0 : 'this' range contains 'key'
    // return 1 : 'this' range is on the right of 'key'
    int compare_key(const PartRangeKey& key) const {
        if (_start_key > key) {
            return 1;
        } else if (_start_key == key) {
            if (!_include_start_key) {
                return 1;
            } else {
                return 0;
            }
        } else if (_end_key < key) {
            return -1;
        } else if (_end_key == key) {
            if (!_include_end_key) {
                return -1;
            }
        }
        return 0;
    }

    // NOTE: operator "<" and ">" is reciprocal for PartRange, but is not always exist.
    // e.g. [1, 3) is neither '<' nor '=' nor '>' [2, 4)
    bool operator<(const PartRange& other) const {
        VLOG_ROW << "in PartRange::operator<: " << std::endl
                 << "this: " << debug_string() << std::endl
                 << "other: " << other.debug_string() << std::endl;
        if (_end_key < other._start_key) {
            VLOG_ROW << "in PartRange::operator<: result: " << true << std::endl;
            return true;
        } else if (_end_key == other._start_key) {
            if ((_include_end_key && other._include_start_key) == false) {
                VLOG_ROW << "in PartRange::operator<: result: " << true << std::endl;
                return true;
            }
            VLOG_ROW << "in PartRange::operator<. "
                     << "first.end=other.end. but include judge failed." << std::endl;
        }

        VLOG_ROW << "in PartRange::operator<: result: " << false << std::endl;
        return false;
    }

    std::string debug_string() const {
        std::stringstream debug_str;
        debug_str << "start_key: [" << _start_key.debug_string() << "]; "
                  << "end_key: [" << _end_key.debug_string() << "]; "
                  << "include_start: " << _include_start_key << "; "
                  << "include_end: " << _include_end_key << ";";
        return debug_str.str();
    }

private:
    PartRange(const PartRangeKey& start_key, const PartRangeKey& end_key, bool include_start,
              bool include_end)
            : _start_key(start_key),
              _end_key(end_key),
              _include_start_key(include_start),
              _include_end_key(include_end) {}

    PartRangeKey _start_key;
    PartRangeKey _end_key;
    bool _include_start_key;
    bool _include_end_key;

    static PartRange _s_all_range;
};

class PartitionInfo {
public:
    PartitionInfo() : _id(-1), _distributed_bucket(0) {}

    static Status from_thrift(ObjectPool* pool, const TRangePartition& t_partition,
                              PartitionInfo* partition);

    Status prepare(RuntimeState* state, const RowDescriptor& row_desc,
                   const std::shared_ptr<MemTracker>& mem_tracker);

    Status open(RuntimeState* state);

    void close(RuntimeState* state);

    int64_t id() const { return _id; }

    const std::vector<ExprContext*>& distributed_expr_ctxs() const {
        return _distributed_expr_ctxs;
    }

    int distributed_bucket() const { return _distributed_bucket; }

    const PartRange& range() const { return _range; }

private:
    int64_t _id;
    PartRange _range;
    // Information used to distribute data
    // distribute exprs
    std::vector<ExprContext*> _distributed_expr_ctxs;
    int32_t _distributed_bucket;
};

} // namespace doris

#endif
