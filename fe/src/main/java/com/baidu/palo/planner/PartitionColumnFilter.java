// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.planner;

import com.baidu.palo.analysis.InPredicate;
import com.baidu.palo.analysis.LiteralExpr;
import com.baidu.palo.catalog.Column;
import com.baidu.palo.catalog.PartitionKey;
import com.baidu.palo.common.AnalysisException;
import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

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

    // selete the bigger bound
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

    // selete the smaller bound
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
                lowerKey = PartitionKey.createPartitionKey(Lists.newArrayList(lowerBound.getStringValue()), columns);
                upperKey = PartitionKey.createPartitionKey(Lists.newArrayList(upperBound.getStringValue()), columns);
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
            str += "lowerBound is NULL";
        } else {
            str += "lowerBound is " + lowerBound.getStringValue() + " and lowerBoundInclusive is " +
              lowerBoundInclusive;
        }
        if (null == upperBound) {
            str += "\nupperBound is NULL";
        } else {
            str +=
              "\nupperBound is " + upperBound.getStringValue() + " and upperBoundInclusive is " +
                upperBoundInclusive;
        }
        if (null == inPredicate) {
            str += "\ninPredicate is NULL";
        } else {
            str += "\ninPredicate is " + inPredicate;
        }
        return str;
    }
};
/* vim: set ts=4 sw=4 sts=4 tw=100 noet: */
