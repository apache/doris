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

#include <roaring/roaring.hh>
#include <string>
#include <vector>

#include "common/logging.h"
#include "gutil/strings/substitute.h"
#include "olap/rowset/segment_v2/common.h"

namespace doris {
namespace segment_v2 {

// RowRange stands for range[From, To), From is inclusive,
// To is exclusive. It is used for row id range calculation.
class RowRange {
public:
    // Returns true if two ranges are overlapped or false.
    // The union range will be returned through range.
    static bool range_union(const RowRange& left, const RowRange& right, RowRange* range) {
        if (left._from <= right._from) {
            if (left._to >= right._from) {
                range->_from = left._from;
                range->_to = std::max(left._to, right._to);
                return true;
            }
        } else if (right._to >= left._from) {
            range->_from = right._from;
            range->_to = std::max(left._to, right._to);
            return true;
        }
        // return a invalid range
        range->_from = 0;
        range->_to = 0;
        return false;
    }

    // Returns true if the two ranges are intersected or false.
    // The intersection of the two ranges is returned through range.
    static bool range_intersection(const RowRange& left, const RowRange& right, RowRange* range) {
        if (left._from <= right._from) {
            if (left._to > right._from) {
                range->_from = right._from;
                range->_to = std::min(left._to, right._to);
                return true;
            }
        } else if (right._to > left._from) {
            range->_from = left._from;
            range->_to = std::min(left._to, right._to);
            return true;
        }
        // return a invalid range
        range->_from = 0;
        range->_to = 0;
        return false;
    }

    RowRange() : _from(0), _to(0) {}

    // Creates a range of [from, to) (from inclusive and to exclusive; empty ranges are invalid)
    RowRange(int64_t from, int64_t to) : _from(from), _to(to) {}

    bool is_valid() const { return _from < _to; }

    size_t count() const { return _to - _from; }

    bool is_before(const RowRange& other) const { return _to <= other._from; }

    bool is_after(const RowRange& other) const { return _from >= other._to; }

    int64_t from() const { return _from; }

    int64_t to() const { return _to; }

    std::string to_string() const { return strings::Substitute("[$0-$1)", _from, _to); }

private:
    int64_t _from;
    int64_t _to;
};

class RowRanges {
public:
    RowRanges() : _count(0) {}

    void clear() {
        _ranges.clear();
        _count = 0;
    }

    // Creates a new RowRanges object with the single range [0, row_count).
    static RowRanges create_single(uint64_t row_count) {
        RowRanges ranges;
        ranges.add(RowRange(0, row_count));
        return ranges;
    }

    // Creates a new RowRanges object with the single range [from, to).
    static RowRanges create_single(int64_t from, int64_t to) {
        DCHECK(from <= to);
        RowRanges ranges;
        ranges.add(RowRange(from, to));
        return ranges;
    }

    // Calculates the union of the two specified RowRanges object. The union of two range is calculated if there are
    // elements between them. Otherwise, the two disjunct ranges are stored separately.
    // For example:
    // [113, 241) ∪ [221, 340) = [113, 340)
    // [113, 230) ∪ [230, 340) = [113, 340]
    // while
    // [113, 230) ∪ [231, 340) = [113, 230), [231, 340)
    static void ranges_union(const RowRanges& left, const RowRanges& right, RowRanges* result) {
        RowRanges tmp_range;
        auto it1 = left._ranges.begin();
        auto it2 = right._ranges.begin();
        // merge and add
        while (it1 != left._ranges.end() && it2 != right._ranges.end()) {
            if (it1->is_after(*it2)) {
                tmp_range.add(*it2);
                ++it2;
            } else {
                tmp_range.add(*it1);
                ++it1;
            }
        }
        while (it1 != left._ranges.end()) {
            tmp_range.add(*it1);
            ++it1;
        }
        while (it2 != right._ranges.end()) {
            tmp_range.add(*it2);
            ++it2;
        }
        *result = std::move(tmp_range);
    }

    // Calculates the intersection of the two specified RowRanges object. Two ranges intersect if they have common
    // elements otherwise the result is empty.
    // For example:
    // [113, 241) ∩ [221, 340) = [221, 241)
    // while
    // [113, 230) ∩ [230, 340) = <EMPTY>
    //
    // The result RowRanges object will contain all the row indexes there were contained in both of the specified objects
    static void ranges_intersection(const RowRanges& left, const RowRanges& right,
                                    RowRanges* result) {
        RowRanges tmp_range;
        int right_index = 0;
        for (auto it1 = left._ranges.begin(); it1 != left._ranges.end(); ++it1) {
            const RowRange& range1 = *it1;
            for (int i = right_index; i < right._ranges.size(); ++i) {
                const RowRange& range2 = right._ranges[i];
                if (range1.is_before(range2)) {
                    break;
                } else if (range1.is_after(range2)) {
                    right_index = i + 1;
                    continue;
                }
                RowRange merge_range;
                bool ret = RowRange::range_intersection(range1, range2, &merge_range);
                DCHECK(ret);
                tmp_range.add(merge_range);
            }
        }
        *result = std::move(tmp_range);
    }

    static roaring::Roaring ranges_to_roaring(const RowRanges& ranges) {
        roaring::Roaring result;
        for (auto it = ranges._ranges.begin(); it != ranges._ranges.end(); ++it) {
            result.addRange(it->from(), it->to());
        }
        return result;
    }

    size_t count() { return _count; }

    bool is_empty() { return _count == 0; }

    bool contain(rowid_t from, rowid_t to) {
        // binary search
        RowRange tmp_range = RowRange(from, to);
        int32_t start = 0;
        int32_t end = _ranges.size();
        while (start <= end) {
            int32_t mid = (start + end) / 2;
            if (_ranges[mid].is_before(tmp_range)) {
                start = mid;
            } else if (_ranges[mid].is_after(tmp_range)) {
                end = mid - 1;
            } else {
                return true;
            }
        }
        return false;
    }

    int64_t from() {
        DCHECK(!is_empty());
        return _ranges[0].from();
    }

    int64_t to() {
        DCHECK(!is_empty());
        return _ranges[_ranges.size() - 1].to();
    }

    size_t range_size() { return _ranges.size(); }

    int64_t get_range_from(size_t range_index) { return _ranges[range_index].from(); }

    int64_t get_range_to(size_t range_index) { return _ranges[range_index].to(); }

    size_t get_range_count(size_t range_index) { return _ranges[range_index].count(); }

    std::string to_string() {
        std::string result;
        for (auto range : _ranges) {
            result += range.to_string() + " ";
        }
        return result;
    }

    // Adds a range to the end of the list of ranges. It maintains the disjunct ascending order(*) of the ranges by
    // trying to union the specified range to the last ranges in the list. The specified range shall be larger(*) than
    // the last one or might be overlapped with some of the last ones.
    void add(const RowRange& range) {
        if (range.count() == 0) {
            return;
        }
        RowRange range_to_add = range;
        for (int i = _ranges.size() - 1; i >= 0; --i) {
            const RowRange last = _ranges[i];
            DCHECK(!last.is_after(range));
            RowRange u;
            bool ret = RowRange::range_union(last, range_to_add, &u);
            if (!ret) {
                // range do not intersect with the last
                break;
            }
            range_to_add = u;
            _ranges.erase(_ranges.begin() + i);
            _count -= last.count();
        }
        _ranges.emplace_back(range_to_add);
        _count += range_to_add.count();
    }

private:
    std::vector<RowRange> _ranges;
    size_t _count;
};

} // namespace segment_v2
} // namespace doris
