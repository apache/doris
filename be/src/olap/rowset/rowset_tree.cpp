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
//
// This file is copied from
// https://github.com/apache/kudu/blob/master/src/kudu/tablet/rowset_tree.cc
// and modified by Doris

#include "olap/rowset/rowset_tree.h"

#include <glog/logging.h>

#include <cstddef>
#include <functional>
#include <iterator>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include "gutil/stl_util.h"
#include "gutil/strings/substitute.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_meta.h"
#include "util/interval_tree-inl.h"
#include "util/slice.h"

using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

namespace doris {

namespace {

// Lexicographic, first by slice, then by rowset pointer, then by segment id, then by start/stop
bool RSEndpointBySliceCompare(const RowsetTree::RSEndpoint& a, const RowsetTree::RSEndpoint& b) {
    int slice_cmp = a.slice_.compare(b.slice_);
    if (slice_cmp) return slice_cmp < 0;
    ptrdiff_t rs_cmp = a.rowset_.get() - b.rowset_.get();
    if (rs_cmp) return rs_cmp < 0;
    int seg_cmp = a.segment_id_ < b.segment_id_;
    if (seg_cmp) return seg_cmp < 0;
    if (a.endpoint_ != b.endpoint_) return a.endpoint_ == RowsetTree::START;
    return false;
}

// Wrapper used when making batch queries into the interval tree.
struct QueryStruct {
    // The slice of the operation performing the query.
    Slice slice;
    // The original index of this slice in the incoming batch.
    int idx;
};

} // anonymous namespace

// Entry for use in the interval tree.
struct RowsetWithBounds {
    string min_key;
    string max_key;

    // NOTE: the ordering of struct fields here is purposeful: we access
    // min_key and max_key frequently, so putting them first in the struct
    // ensures they fill a single 64-byte cache line (each is 32 bytes).
    // The 'rowset' pointer is accessed comparitively rarely.
    RowsetSharedPtr rowset;
    int32_t segment_id;
};

// Traits struct for IntervalTree.
struct RowsetIntervalTraits {
    typedef Slice point_type;
    typedef RowsetWithBounds* interval_type;

    static Slice get_left(const RowsetWithBounds* rs) { return Slice(rs->min_key); }

    static Slice get_right(const RowsetWithBounds* rs) { return Slice(rs->max_key); }

    static int compare(const Slice& a, const Slice& b) { return a.compare(b); }

    static int compare(const Slice& a, const QueryStruct& b) { return a.compare(b.slice); }

    static int compare(const QueryStruct& a, const Slice& b) { return -compare(b, a); }

    // When 'a' is std::nullopt:
    //  (1)'a' is +OO when 'positive_direction' is true;
    //  (2)'a' is -OO when 'positive_direction' is false.
    static int compare(const std::optional<Slice>& a, const Slice& b, const EndpointIfNone& type) {
        if (a == std::nullopt) {
            return ((POSITIVE_INFINITY == type) ? 1 : -1);
        }

        return compare(*a, b);
    }

    static int compare(const Slice& a, const std::optional<Slice>& b, const EndpointIfNone& type) {
        return -compare(b, a, type);
    }
};

RowsetTree::RowsetTree() : initted_(false) {}

Status RowsetTree::Init(const RowsetVector& rowsets) {
    if (initted_) {
        return Status::InternalError("Call Init method on a RowsetTree that's already inited!");
    }
    std::vector<RowsetWithBounds*> entries;
    ElementDeleter deleter(&entries);
    entries.reserve(rowsets.size());
    std::vector<RSEndpoint> endpoints;
    endpoints.reserve(rowsets.size() * 2);

    // Iterate over each of the provided Rowsets, fetching their
    // bounds and adding them to the local vectors.
    for (const RowsetSharedPtr& rs : rowsets) {
        std::vector<KeyBoundsPB> segments_key_bounds;
        Status s = rs->get_segments_key_bounds(&segments_key_bounds);
        if (!s.ok()) {
            LOG(WARNING) << "Unable to construct RowsetTree: " << rs->rowset_id()
                         << " unable to determine its bounds: " << s.to_string();
            return s;
        }
        DCHECK_EQ(segments_key_bounds.size(), rs->num_segments());

        for (auto i = 0; i < rs->num_segments(); i++) {
            unique_ptr<RowsetWithBounds> rsit(new RowsetWithBounds());
            rsit->rowset = rs;
            rsit->segment_id = i;
            string min_key = segments_key_bounds[i].min_key();
            string max_key = segments_key_bounds[i].max_key();
            DCHECK_LE(min_key.compare(max_key), 0)
                    << "Rowset min: " << min_key << " must be <= max: " << max_key;

            // Load bounds and save entry
            rsit->min_key = std::move(min_key);
            rsit->max_key = std::move(max_key);

            // Load into key endpoints.
            endpoints.emplace_back(rsit->rowset, i, START, rsit->min_key);
            endpoints.emplace_back(rsit->rowset, i, STOP, rsit->max_key);

            entries.push_back(rsit.release());
        }
    }

    // Sort endpoints
    std::sort(endpoints.begin(), endpoints.end(), RSEndpointBySliceCompare);

    // Install the vectors into the object.
    entries_.swap(entries);
    tree_.reset(new IntervalTree<RowsetIntervalTraits>(entries_));
    key_endpoints_.swap(endpoints);
    all_rowsets_.assign(rowsets.begin(), rowsets.end());

    // Build the mapping from RS_ID to RS.
    rs_by_id_.clear();
    for (auto& rs : all_rowsets_) {
        if (!rs_by_id_.insert({rs->rowset_id(), rs}).second) {
            return Status::InternalError(strings::Substitute(
                    "Add rowset with $0 to rowset tree of tablet $1 failed",
                    rs->rowset_id().to_string(), rs->rowset_meta()->tablet_uid().to_string()));
        }
    }

    initted_ = true;

    return Status::OK();
}

void RowsetTree::FindRowsetsIntersectingInterval(
        const std::optional<Slice>& lower_bound, const std::optional<Slice>& upper_bound,
        vector<std::pair<RowsetSharedPtr, int32_t>>* rowsets) const {
    DCHECK(initted_);

    vector<RowsetWithBounds*> from_tree;
    from_tree.reserve(all_rowsets_.size());
    tree_->FindIntersectingInterval(lower_bound, upper_bound, &from_tree);
    rowsets->reserve(rowsets->size() + from_tree.size());
    for (RowsetWithBounds* rs : from_tree) {
        rowsets->emplace_back(rs->rowset, rs->segment_id);
    }
}

void RowsetTree::FindRowsetsWithKeyInRange(
        const Slice& encoded_key, const RowsetIdUnorderedSet* rowset_ids,
        vector<std::pair<RowsetSharedPtr, int32_t>>* rowsets) const {
    DCHECK(initted_);

    // Query the interval tree to efficiently find rowsets with known bounds
    // whose ranges overlap the probe key.
    vector<RowsetWithBounds*> from_tree;
    from_tree.reserve(all_rowsets_.size());
    tree_->FindContainingPoint(encoded_key, &from_tree);
    rowsets->reserve(rowsets->size() + from_tree.size());
    for (RowsetWithBounds* rs : from_tree) {
        if (!rowset_ids || rowset_ids->find(rs->rowset->rowset_id()) != rowset_ids->end()) {
            rowsets->emplace_back(rs->rowset, rs->segment_id);
        }
    }
}

void RowsetTree::ForEachRowsetContainingKeys(
        const std::vector<Slice>& encoded_keys,
        const std::function<void(RowsetSharedPtr, int)>& cb) const {
    DCHECK(std::is_sorted(encoded_keys.cbegin(), encoded_keys.cend(), Slice::Comparator()));
    // The interval tree batch query callback would naturally just give us back
    // the matching Slices, but that won't allow us to easily tell the caller
    // which specific operation _index_ matched the Rowset. So, we make a vector
    // of QueryStructs to pair the Slice with its original index.
    vector<QueryStruct> queries;
    queries.resize(encoded_keys.size());
    for (int i = 0; i < encoded_keys.size(); i++) {
        queries[i] = {encoded_keys[i], i};
    }

    tree_->ForEachIntervalContainingPoints(
            queries, [&](const QueryStruct& qs, RowsetWithBounds* rs) { cb(rs->rowset, qs.idx); });
}

RowsetTree::~RowsetTree() {
    for (RowsetWithBounds* e : entries_) {
        delete e;
    }
    entries_.clear();
}

void ModifyRowSetTree(const RowsetTree& old_tree, const RowsetVector& rowsets_to_remove,
                      const RowsetVector& rowsets_to_add, RowsetTree* new_tree) {
    RowsetVector post_swap;

    // O(n^2) diff algorithm to collect the set of rowsets excluding
    // the rowsets that were included in the compaction
    int num_removed = 0;

    for (const RowsetSharedPtr& rs : old_tree.all_rowsets()) {
        // Determine if it should be removed
        bool should_remove = false;
        for (const RowsetSharedPtr& to_remove : rowsets_to_remove) {
            if (to_remove->rowset_id() == rs->rowset_id()) {
                should_remove = true;
                num_removed++;
                break;
            }
        }
        if (!should_remove) {
            post_swap.push_back(rs);
        }
    }

    CHECK_EQ(num_removed, rowsets_to_remove.size());

    // Then push the new rowsets on the end of the new list
    std::copy(rowsets_to_add.begin(), rowsets_to_add.end(), std::back_inserter(post_swap));

    CHECK(new_tree->Init(post_swap).ok());
}

} // namespace doris
