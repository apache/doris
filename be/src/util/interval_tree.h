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
// Implements an Interval Tree. See http://en.wikipedia.org/wiki/Interval_tree
// or CLRS for a full description of the data structure.
//
// This file is copied from
// https://github.com/apache/kudu/blob/master/src/kudu/util/interval_tree.h
// and modified by Doris
//
// Callers of this class should also include interval_tree-inl.h for function
// definitions.

#pragma once

#include <glog/logging.h>

#include <vector>

#include "gutil/macros.h"

namespace doris {

namespace interval_tree_internal {
template <class Traits>
class ITNode;
}

// End point type when boost::none.
enum EndpointIfNone { POSITIVE_INFINITY, NEGATIVE_INFINITY };

// Implements an Interval Tree.
//
// An Interval Tree is a data structure which stores a set of intervals and supports
// efficient searches to determine which intervals in that set overlap a query
// point or interval. These operations are O(lg n + k) where 'n' is the number of
// intervals in the tree and 'k' is the number of results returned for a given query.
//
// This particular implementation is a static tree -- intervals may not be added or
// removed once the tree is instantiated.
//
// This class also assumes that all intervals are "closed" intervals -- the intervals
// are inclusive of their start and end points.
//
// The Traits class should have the following members:
//   Traits::point_type
//     a typedef for what a "point" in the range is
//
//   Traits::interval_type
//     a typedef for an interval
//
//   static point_type get_left(const interval_type &)
//   static point_type get_right(const interval_type &)
//     accessors which fetch the left and right bound of the interval, respectively.
//
//   static int compare(const point_type &a, const point_type &b)
//     return < 0 if a < b, 0 if a == b, > 0 if a > b
//
// See interval_tree-test.cc for an example Traits class for 'int' ranges.
template <class Traits>
class IntervalTree {
private:
    // Import types from the traits class to make code more readable.
    typedef typename Traits::interval_type interval_type;
    typedef typename Traits::point_type point_type;

    // And some convenience types.
    typedef std::vector<interval_type> IntervalVector;
    typedef interval_tree_internal::ITNode<Traits> node_type;

public:
    // Construct an Interval Tree containing the given set of intervals.
    explicit IntervalTree(const IntervalVector& intervals);

    ~IntervalTree();

    // Find all intervals in the tree which contain the query point.
    // The resulting intervals are added to the 'results' vector.
    // The vector is not cleared first.
    //
    // NOTE: 'QueryPointType' is usually point_type, but can be any other
    // type for which there exists the appropriate Traits::Compare(...) method.
    template <class QueryPointType>
    void FindContainingPoint(const QueryPointType& query, IntervalVector* results) const;

    // For each of the query points in the STL container 'queries', find all
    // intervals in the tree which may contain those points. Calls 'cb(point, interval)'
    // for each such interval.
    //
    // The points in the query container must be comparable to 'point_type'
    // using Traits::Compare().
    //
    // The implementation sequences the calls to 'cb' with the following guarantees:
    // 1) all of the results corresponding to a given interval will be yielded in at
    //    most two "groups" of calls (i.e. sub-sequences of calls with the same interval).
    // 2) within each "group" of calls, the query points will be in ascending order.
    //
    // For example, the callback sequence may be:
    //
    //  cb(q1, interval_1) -
    //  cb(q2, interval_1)  | first group of interval_1
    //  cb(q6, interval_1)  |
    //  cb(q7, interval_1) -
    //
    //  cb(q2, interval_2) -
    //  cb(q3, interval_2)  | first group of interval_2
    //  cb(q4, interval_2) -
    //
    //  cb(q3, interval_1) -
    //  cb(q4, interval_1)  | second group of interval_1
    //  cb(q5, interval_1) -
    //
    //  cb(q2, interval_3) -
    //  cb(q3, interval_3)  | first group of interval_3
    //  cb(q4, interval_3) -
    //
    //  cb(q5, interval_2) -
    //  cb(q6, interval_2)  | second group of interval_2
    //  cb(q7, interval_2) -
    //
    // REQUIRES: The input points must be pre-sorted or else this will return invalid
    // results.
    template <class Callback, class QueryContainer>
    void ForEachIntervalContainingPoints(const QueryContainer& queries, const Callback& cb) const;

    // Find all intervals in the tree which intersect the given interval.
    // The resulting intervals are added to the 'results' vector.
    // The vector is not cleared first.
    template <class QueryPointType>
    void FindIntersectingInterval(const QueryPointType& lower_bound,
                                  const QueryPointType& upper_bound, IntervalVector* results) const;

private:
    static void Partition(const IntervalVector& in, point_type* split_point, IntervalVector* left,
                          IntervalVector* overlapping, IntervalVector* right);

    // Create a node containing the given intervals, recursively splitting down the tree.
    static node_type* CreateNode(const IntervalVector& intervals);

    node_type* root_ = nullptr;

    DISALLOW_COPY_AND_ASSIGN(IntervalTree);
};

} // namespace doris
