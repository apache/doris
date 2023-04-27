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

package org.apache.doris.planner;

import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class PartitionColumnFilter {
    private static final Logger LOG = LogManager.getLogger(PartitionColumnFilter.class);
    public LiteralExpr lowerBound;
    public boolean     lowerBoundInclusive;
    public LiteralExpr upperBound;
    public boolean     upperBoundInclusive;
    // InPredicate
    private InPredicate inPredicate;

    public InPredicate getInPredicate() {
        return inPredicate;
    }

    public void setInPredicate(InPredicate inPredicate) {
        this.inPredicate = inPredicate;
    }

    // select the bigger bound
    public void setLowerBound(LiteralExpr newLowerBound, boolean newLowerBoundInclusive) {
        if (null == lowerBound) {
            lowerBound = newLowerBound;
            lowerBoundInclusive = newLowerBoundInclusive;
        } else {
            int ret = lowerBound.compareLiteral(newLowerBound);
            if (ret < 0) {
                lowerBound = newLowerBound;
                lowerBoundInclusive = newLowerBoundInclusive;
            } else if (ret == 0) {
                if (newLowerBoundInclusive == false) {
                    lowerBoundInclusive = newLowerBoundInclusive;
                }
            } else {
                // pass
            }
        }
    }

    // select the smaller bound
    public void setUpperBound(LiteralExpr newUpperBound, boolean newUpperBoundInclusive) {
        if (null == upperBound) {
            upperBound = newUpperBound;
            upperBoundInclusive = newUpperBoundInclusive;
        } else {
            int ret = upperBound.compareLiteral(newUpperBound);
            if (ret < 0) {
                // pass
            } else if (ret == 0) {
                if (newUpperBoundInclusive == false) {
                    upperBoundInclusive = newUpperBoundInclusive;
                }
            } else {
                upperBound = newUpperBound;
                upperBoundInclusive = newUpperBoundInclusive;
            }
        }
    }

    public Range<PartitionKey> getRange(List<Column> columns) {
        LOG.info("range is " + toString());
        BoundType lowerType = lowerBoundInclusive ? BoundType.CLOSED : BoundType.OPEN;
        BoundType upperType = upperBoundInclusive ? BoundType.CLOSED : BoundType.OPEN;
        if (null != lowerBound && null != upperBound) {
            PartitionKey lowerKey = null;
            PartitionKey upperKey = null;
            // cmy mod, catch AnalysisException
            try {
                lowerKey = PartitionKey.createPartitionKey(
                        Lists.newArrayList(new PartitionValue(lowerBound.getStringValue())), columns);
                upperKey = PartitionKey.createPartitionKey(
                        Lists.newArrayList(new PartitionValue(upperBound.getStringValue())), columns);
            } catch (AnalysisException e) {
                LOG.warn(e.getMessage());
                return null;
            }
            return Range.range(lowerKey, lowerType, upperKey, upperType);
        }
        return null;
    }

    @Override
    public String toString() {
        String str = "";
        if (null == lowerBound) {
            str += "lowerBound is UNSET";
        } else {
            str += "lowerBound is " + lowerBound.getStringValue() + " and lowerBoundInclusive is "
                    + lowerBoundInclusive;
        }
        if (null == upperBound) {
            str += "\nupperBound is UNSET";
        } else {
            str += "\nupperBound is " + upperBound.getStringValue() + " and upperBoundInclusive is "
                    + upperBoundInclusive;
        }
        if (null == inPredicate) {
            str += "\ninPredicate is UNSET";
        } else {
            str += "\ninPredicate is " + inPredicate;
        }
        return str;
    }
}
