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

package org.apache.doris.mtmv;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;

import cfjd.com.google.gson.annotations.SerializedName;
import com.google.common.collect.Range;

import java.util.List;

public class MTMVRefreshPartitionRange {

    private List<Expression> lower;
    private List<Expression> upper;
    @SerializedName(value = "lower")
    private String lowerValue;
    @SerializedName(value = "upper")
    private String upperValue;

    public MTMVRefreshPartitionRange(List<Expression> lower, List<Expression> upper) {
        this.lower = lower;
        this.upper = upper;
    }

    public List<Expression> getLower() {
        return lower;
    }

    public List<Expression> getUpper() {
        return upper;
    }

    public void analyze() {
        String lowerStringValue = getStringValue(lower);
        String upperStringValue = getStringValue(upper);
        DateV2Literal lowerDate;
        DateV2Literal upperDate;
        try {
            // check if legal Date
            lowerDate = new DateV2Literal(lowerStringValue);
        } catch (Exception e) {
            throw new AnalysisException("cannot convert to date type: " + lowerStringValue);
        }
        try {
            // check if legal Date
            upperDate = new DateV2Literal(upperStringValue);
        } catch (Exception e) {
            throw new AnalysisException("cannot convert to date type: " + upperStringValue);
        }
        if (upperDate.compareTo(lowerDate) <= 0) {
            throw new AnalysisException("Upper should be greater than Lower");
        }
        this.lowerValue = lowerStringValue;
        this.upperValue = upperStringValue;
    }

    private String getStringValue(List<Expression> expressions) {
        if (expressions.size() != 1) {
            throw new AnalysisException("only support a single value");
        }
        Expression lowerExpression = expressions.get(0);
        if (!(lowerExpression instanceof StringLiteral)) {
            throw new AnalysisException("only support a String value");
        }
        return ((StringLiteral) lowerExpression).getValue();
    }

    public Range<DateV2Literal> getRange() {
        return Range.closedOpen(new DateV2Literal(lowerValue), new DateV2Literal(upperValue));
    }
}
