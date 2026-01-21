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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.thrift.TExprOpcode;
import org.apache.doris.thrift.TScoreRangeInfo;

import java.util.Objects;

/**
 * ScoreRangeInfo represents the score range filter parameters
 * for BM25 range queries like "score() > 0.5".
 * This is used to push down score range predicates to the storage engine.
 */
public class ScoreRangeInfo {
    private final TExprOpcode op;
    private final double threshold;

    public ScoreRangeInfo(TExprOpcode op, double threshold) {
        this.op = Objects.requireNonNull(op, "op cannot be null");
        this.threshold = threshold;
    }

    public TExprOpcode getOp() {
        return op;
    }

    public double getThreshold() {
        return threshold;
    }

    /**
     * Convert to Thrift representation for sending to BE.
     */
    public TScoreRangeInfo toThrift() {
        TScoreRangeInfo tScoreRangeInfo = new TScoreRangeInfo();
        tScoreRangeInfo.setOp(op);
        tScoreRangeInfo.setThreshold(threshold);
        return tScoreRangeInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ScoreRangeInfo that = (ScoreRangeInfo) o;
        return Double.compare(that.threshold, threshold) == 0
                && op == that.op;
    }

    @Override
    public int hashCode() {
        return Objects.hash(op, threshold);
    }

    @Override
    public String toString() {
        return "ScoreRangeInfo{op=" + op + ", threshold=" + threshold + '}';
    }
}
