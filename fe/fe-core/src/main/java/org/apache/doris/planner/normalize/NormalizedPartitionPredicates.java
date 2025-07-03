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

package org.apache.doris.planner.normalize;

import org.apache.doris.analysis.Expr;

import java.util.List;
import java.util.Map;

/** NormalizedPartitionPredicates */
public class NormalizedPartitionPredicates {
    // the predicates which can not compute intersect range with the partitions
    // case 1: no partition columns, say `non_partition_column = 1`
    // case 2: partition column compute with non partition column, say `non_partition_column + partition_column = 1`
    // case 3: the partition predicate contains some function which currently not supported convert to range,
    //         say `date(partition_column) = '2024-08-14'`
    public final List<Expr> remainedPredicates;

    // the partitionId to partition range string,
    // for example:
    //   partitions is:
    //     P1 values [('2024-08-01'), ('2024-08-10')), the partition id is 10001
    //     P2 values [('2024-08-10'), ('2024-08-20')), the partition id is 10002
    //
    //   predicate is: part_column between '2024-08-08' and '2024-08-12'
    //
    //   the intersectPartitionRanges like is:
    //   {
    //     10001: "[('2024-08-08'), ('2024-08-10'))",
    //     10002: "[('2024-08-10'), ('2024-08-13'))"
    //   }
    //
    //   we should normalize the intersect range to closeOpened range to maintain the same format
    public final Map<Long, String> intersectPartitionRanges;

    public NormalizedPartitionPredicates(
            List<Expr> remainedPredicates, Map<Long, String> intersectPartitionRanges) {
        this.remainedPredicates = remainedPredicates;
        this.intersectPartitionRanges = intersectPartitionRanges;
    }
}
