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
// https://github.com/apache/kudu/blob/master/src/kudu/util/interval_tree-inl.h
// and modified by Doris
//

#pragma once

#include <algorithm>
#include <vector>

#include "util/interval_tree.h"

namespace doris {

template <class Traits>
IntervalTree<Traits>::IntervalTree(const IntervalVector& intervals) : root_(NULL) {
    if (!intervals.empty()) {
        root_ = CreateNode(intervals);
    }
}

template <class Traits>
IntervalTree<Traits>::~IntervalTree() {
    delete root_;
}

template <class Traits>
template <class QueryPointType>
void IntervalTree<Traits>::FindContainingPoint(const QueryPointType& query,
                                               IntervalVector* results) const {
    if (root_) {
        root_->FindContainingPoint(query, results);
    }
}

template <class Traits>
template <class Callback, class QueryContainer>
void IntervalTree<Traits>::ForEachIntervalContainingPoints(const QueryContainer& queries,
                                                           const Callback& cb) const {
    if (root_) {
        root_->ForEachIntervalContainingPoints(queries.begin(), queries.end(), cb);
    }
}

template <class Traits>
template <class QueryPointType>
void IntervalTree<Traits>::FindIntersectingInterval(const QueryPointType& lower_bound,
                                                    const QueryPointType& upper_bound,
                                                    IntervalVector* results) const {
    if (root_) {
        root_->FindIntersectingInterval(lower_bound, upper_bound, results);
    }
}

template <class Traits>
bool LessThan(const typename Traits::point_type& a, const typename Traits::point_type& b) {
    return Traits::compare(a, b) < 0;
}

// Select a split point which attempts to evenly divide 'in' into three groups:
//  (a) those that are fully left of the split point
//  (b) those that overlap the split point.
//  (c) those that are fully right of the split point
// These three groups are stored in the output parameters '*left', '*overlapping',
// and '*right', respectively. The selected split point is stored in *split_point.
//
// For example, the input interval set:
//
//   |------1-------|         |-----2-----|
//       |--3--|    |---4--|    |----5----|
//                     |
// Resulting split:    | Partition point
//                     |
//
// *left: intervals 1 and 3
// *overlapping: interval 4
// *right: intervals 2 and 5
template <class Traits>
void IntervalTree<Traits>::Partition(const IntervalVector& in, point_type* split_point,
                                     IntervalVector* left, IntervalVector* overlapping,
                                     IntervalVector* right) {
    CHECK(!in.empty());

    // Pick a split point which is the median of all of the interval boundaries.
    std::vector<point_type> endpoints;
    endpoints.reserve(in.size() * 2);
    for (const interval_type& interval : in) {
        endpoints.push_back(Traits::get_left(interval));
        endpoints.push_back(Traits::get_right(interval));
    }
    std::sort(endpoints.begin(), endpoints.end(), LessThan<Traits>);
    *split_point = endpoints[endpoints.size() / 2];

    // Partition into the groups based on the determined split point.
    for (const interval_type& interval : in) {
        if (Traits::compare(Traits::get_right(interval), *split_point) < 0) {
            //                 | split point
            // |------------|  |
            //    interval
            left->push_back(interval);
        } else if (Traits::compare(Traits::get_left(interval), *split_point) > 0) {
            //                 | split point
            //                 |    |------------|
            //                         interval
            right->push_back(interval);
        } else {
            //                 | split point
            //                 |
            //          |------------|
            //             interval
            overlapping->push_back(interval);
        }
    }
}

template <class Traits>
typename IntervalTree<Traits>::node_type* IntervalTree<Traits>::CreateNode(
        const IntervalVector& intervals) {
    IntervalVector left, right, overlap;
    point_type split_point;

    // First partition the input intervals and select a split point
    Partition(intervals, &split_point, &left, &overlap, &right);

    // Recursively subdivide the intervals which are fully left or fully
    // right of the split point into subtree nodes.
    node_type* left_node = !left.empty() ? CreateNode(left) : NULL;
    node_type* right_node = !right.empty() ? CreateNode(right) : NULL;

    return new node_type(split_point, left_node, overlap, right_node);
}

namespace interval_tree_internal {

// Node in the interval tree.
template <typename Traits>
class ITNode {
private:
    // Import types.
    typedef std::vector<typename Traits::interval_type> IntervalVector;
    typedef typename Traits::interval_type interval_type;
    typedef typename Traits::point_type point_type;

public:
    ITNode(point_type split_point, ITNode<Traits>* left, const IntervalVector& overlap,
           ITNode<Traits>* right);
    ~ITNode();

    // See IntervalTree::FindContainingPoint(...)
    template <class QueryPointType>
    void FindContainingPoint(const QueryPointType& query, IntervalVector* results) const;

    // See IntervalTree::ForEachIntervalContainingPoints().
    // We use iterators here since as recursion progresses down the tree, we
    // process sub-sequences of the original set of query points.
    template <class Callback, class ItType>
    void ForEachIntervalContainingPoints(ItType begin_queries, ItType end_queries,
                                         const Callback& cb) const;

    // See IntervalTree::FindIntersectingInterval(...)
    template <class QueryPointType>
    void FindIntersectingInterval(const QueryPointType& lower_bound,
                                  const QueryPointType& upper_bound, IntervalVector* results) const;

private:
    // Comparators for sorting lists of intervals.
    static bool SortByAscLeft(const interval_type& a, const interval_type& b);
    static bool SortByDescRight(const interval_type& a, const interval_type& b);

    // Partition point of this node.
    point_type split_point_;

    // Those nodes that overlap with split_point_, in ascending order by their left side.
    IntervalVector overlapping_by_asc_left_;

    // Those nodes that overlap with split_point_, in descending order by their right side.
    IntervalVector overlapping_by_desc_right_;

    // Tree node for intervals fully left of split_point_, or NULL.
    ITNode* left_ = nullptr;

    // Tree node for intervals fully right of split_point_, or NULL.
    ITNode* right_ = nullptr;

    DISALLOW_COPY_AND_ASSIGN(ITNode);
};

template <class Traits>
bool ITNode<Traits>::SortByAscLeft(const interval_type& a, const interval_type& b) {
    return Traits::compare(Traits::get_left(a), Traits::get_left(b)) < 0;
}

template <class Traits>
bool ITNode<Traits>::SortByDescRight(const interval_type& a, const interval_type& b) {
    return Traits::compare(Traits::get_right(a), Traits::get_right(b)) > 0;
}

template <class Traits>
ITNode<Traits>::ITNode(typename Traits::point_type split_point, ITNode<Traits>* left,
                       const IntervalVector& overlap, ITNode<Traits>* right)
        : split_point_(std::move(split_point)), left_(left), right_(right) {
    // Store two copies of the set of intervals which overlap the split point:
    // 1) Sorted by ascending left boundary
    overlapping_by_asc_left_.assign(overlap.begin(), overlap.end());
    std::sort(overlapping_by_asc_left_.begin(), overlapping_by_asc_left_.end(), SortByAscLeft);
    // 2) Sorted by descending right boundary
    overlapping_by_desc_right_.assign(overlap.begin(), overlap.end());
    std::sort(overlapping_by_desc_right_.begin(), overlapping_by_desc_right_.end(),
              SortByDescRight);
}

template <class Traits>
ITNode<Traits>::~ITNode() {
    if (left_) delete left_;
    if (right_) delete right_;
}

template <class Traits>
template <class Callback, class ItType>
void ITNode<Traits>::ForEachIntervalContainingPoints(ItType begin_queries, ItType end_queries,
                                                     const Callback& cb) const {
    if (begin_queries == end_queries) return;

    typedef decltype(*begin_queries) QueryPointType;
    const auto& partitioner = [&](const QueryPointType& query_point) {
        return Traits::compare(query_point, split_point_) < 0;
    };

    // Partition the query points into those less than the split_point_ and those greater
    // than or equal to the split_point_. Because the input queries are already sorted, we
    // can use 'std::partition_point' instead of 'std::partition'.
    //
    // The resulting 'partition_point' is the first query point in the second group.
    //
    // Complexity: O(log(number of query points))
    DCHECK(std::is_partitioned(begin_queries, end_queries, partitioner));
    auto partition_point = std::partition_point(begin_queries, end_queries, partitioner);

    // Recurse left: any query points left of the split point may intersect
    // with non-overlapping intervals fully-left of our split point.
    if (left_ != NULL) {
        left_->ForEachIntervalContainingPoints(begin_queries, partition_point, cb);
    }

    // Handle the query points < split_point                  /
    //                                                        /
    //      split_point_                                      /
    //         |                                              /
    //   [------]         \                                   /
    //     [-------]       | overlapping_by_asc_left_         /
    //       [--------]   /                                   /
    // Q   Q      Q                                           /
    // ^   ^      \___ not handled (right of split_point_)    /
    // |   |                                                  /
    // \___\___ these points will be handled here             /
    //

    // Lower bound of query points still relevant.
    auto rem_queries = begin_queries;
    for (const interval_type& interval : overlapping_by_asc_left_) {
        const auto& interval_left = Traits::get_left(interval);
        // Find those query points which are right of the left side of the interval.
        // 'first_match' here is the first query point >= interval_left.
        // Complexity: O(log(num_queries))
        //
        // TODO(todd): The non-batched implementation is O(log(num_intervals) * num_queries)
        // whereas this loop ends up O(num_intervals * log(num_queries)). So, for
        // small numbers of queries this is not the fastest way to structure these loops.
        auto first_match = std::partition_point(
                rem_queries, partition_point, [&](const QueryPointType& query_point) {
                    return Traits::compare(query_point, interval_left) < 0;
                });
        for (auto it = first_match; it != partition_point; ++it) {
            cb(*it, interval);
        }
        // Since the intervals are sorted in ascending-left order, we can start
        // the search for the next interval at the first match in this interval.
        // (any query point which was left of the current interval will also be left
        // of all future intervals).
        rem_queries = std::move(first_match);
    }

    // Handle the query points >= split_point                        /
    //                                                               /
    //    split_point_                                               /
    //       |                                                       /
    //     [--------]   \                                            /
    //   [-------]       | overlapping_by_desc_right_                /
    // [------]         /                                            /
    //   Q   Q      Q                                                /
    //   |    \______\___ these points will be handled here          /
    //   |                                                           /
    //   \___ not handled (left of split_point_)                     /

    // Upper bound of query points still relevant.
    rem_queries = end_queries;
    for (const interval_type& interval : overlapping_by_desc_right_) {
        const auto& interval_right = Traits::get_right(interval);
        // Find the first query point which is > the right side of the interval.
        auto first_non_match = std::partition_point(
                partition_point, rem_queries, [&](const QueryPointType& query_point) {
                    return Traits::compare(query_point, interval_right) <= 0;
                });
        for (auto it = partition_point; it != first_non_match; ++it) {
            cb(*it, interval);
        }
        // Same logic as above: if a query point was fully right of 'interval',
        // then it will be fully right of all following intervals because they are
        // sorted by descending-right.
        rem_queries = std::move(first_non_match);
    }

    if (right_ != NULL) {
        while (partition_point != end_queries &&
               Traits::compare(*partition_point, split_point_) == 0) {
            ++partition_point;
        }
        right_->ForEachIntervalContainingPoints(partition_point, end_queries, cb);
    }
}

template <class Traits>
template <class QueryPointType>
void ITNode<Traits>::FindContainingPoint(const QueryPointType& query,
                                         IntervalVector* results) const {
    int cmp = Traits::compare(query, split_point_);
    if (cmp < 0) {
        // None of the intervals in right_ may intersect this.
        if (left_ != NULL) {
            left_->FindContainingPoint(query, results);
        }

        // Any intervals which start before the query point and overlap the split point
        // must therefore contain the query point.
        auto p = std::partition_point(
                overlapping_by_asc_left_.cbegin(), overlapping_by_asc_left_.cend(),
                [&](const interval_type& interval) {
                    return Traits::compare(Traits::get_left(interval), query) <= 0;
                });
        results->insert(results->end(), overlapping_by_asc_left_.cbegin(), p);
    } else if (cmp > 0) {
        // None of the intervals in left_ may intersect this.
        if (right_ != NULL) {
            right_->FindContainingPoint(query, results);
        }

        // Any intervals which end after the query point and overlap the split point
        // must therefore contain the query point.
        auto p = std::partition_point(
                overlapping_by_desc_right_.cbegin(), overlapping_by_desc_right_.cend(),
                [&](const interval_type& interval) {
                    return Traits::compare(Traits::get_right(interval), query) >= 0;
                });
        results->insert(results->end(), overlapping_by_desc_right_.cbegin(), p);
    } else {
        DCHECK_EQ(cmp, 0);
        // The query is exactly our split point -- in this case we've already got
        // the computed list of overlapping intervals.
        results->insert(results->end(), overlapping_by_asc_left_.begin(),
                        overlapping_by_asc_left_.end());
    }
}

template <class Traits>
template <class QueryPointType>
void ITNode<Traits>::FindIntersectingInterval(const QueryPointType& lower_bound,
                                              const QueryPointType& upper_bound,
                                              IntervalVector* results) const {
    if (Traits::compare(upper_bound, split_point_, POSITIVE_INFINITY) <= 0) {
        // The interval is fully left of the split point and with split point.
        // So, it may not overlap with any in 'right_'
        if (left_ != NULL) {
            left_->FindIntersectingInterval(lower_bound, upper_bound, results);
        }

        // Any interval whose left edge is < the query interval's right edge
        // intersect the query interval. 'std::partition_point' returns the first
        // such interval which does not meet that criterion, so we insert all
        // up to that point.
        auto first_greater = std::partition_point(
                overlapping_by_asc_left_.cbegin(), overlapping_by_asc_left_.cend(),
                [&](const interval_type& interval) {
                    return Traits::compare(Traits::get_left(interval), upper_bound,
                                           POSITIVE_INFINITY) < 0;
                });
        results->insert(results->end(), overlapping_by_asc_left_.cbegin(), first_greater);
    } else if (Traits::compare(lower_bound, split_point_, NEGATIVE_INFINITY) > 0) {
        // The interval is fully right of the split point. So, it may not overlap
        // with any in 'left_'.
        if (right_ != NULL) {
            right_->FindIntersectingInterval(lower_bound, upper_bound, results);
        }

        // Any interval whose right edge is >= the query interval's left edge
        // intersect the query interval. 'std::partition_point' returns the first
        // such interval which does not meet that criterion, so we insert all
        // up to that point.
        auto first_lesser = std::partition_point(
                overlapping_by_desc_right_.cbegin(), overlapping_by_desc_right_.cend(),
                [&](const interval_type& interval) {
                    return Traits::compare(Traits::get_right(interval), lower_bound,
                                           NEGATIVE_INFINITY) >= 0;
                });
        results->insert(results->end(), overlapping_by_desc_right_.cbegin(), first_lesser);
    } else {
        // The query interval contains the split point. Therefore all other intervals
        // which also contain the split point are intersecting.
        results->insert(results->end(), overlapping_by_asc_left_.begin(),
                        overlapping_by_asc_left_.end());

        // The query interval may _also_ intersect some in either child.
        if (left_ != NULL) {
            left_->FindIntersectingInterval(lower_bound, upper_bound, results);
        }
        if (right_ != NULL) {
            right_->FindIntersectingInterval(lower_bound, upper_bound, results);
        }
    }
}

} // namespace interval_tree_internal

} // namespace doris
