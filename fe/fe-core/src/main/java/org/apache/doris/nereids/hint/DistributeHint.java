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

package org.apache.doris.nereids.hint;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.DistributeType;

import java.util.List;
import java.util.Objects;

/**
 * Hints for join.
 * <p>
 * Hints for the right child of join are supported currently.
 * Left input and right input of join could have different hints for further extension.
 */
public class DistributeHint extends Hint {
    public DistributeType distributeType;

    private boolean isSuccessInLeading = false;

    private JoinSkewInfo skewInfo;

    public DistributeHint(DistributeType distributeType) {
        super("Distribute");
        this.distributeType = distributeType;
        this.skewInfo = null;
    }

    public DistributeHint(DistributeType distributeType, JoinSkewInfo skewInfo) {
        super("Distribute");
        this.distributeType = distributeType;
        this.skewInfo = skewInfo;
    }

    public void setSkewInfo(JoinSkewInfo skewInfo) {
        this.skewInfo = skewInfo;
    }

    public void setSuccessInLeading(boolean successInLeading) {
        isSuccessInLeading = successInLeading;
    }

    public Expression getSkewExpr() {
        return skewInfo.getSkewExpr();
    }

    public List<Expression> getSkewValues() {
        return skewInfo.getSkewValues();
    }

    public JoinSkewInfo getSkewInfo() {
        return skewInfo;
    }

    public boolean isSuccessInSkewRewrite() {
        return skewInfo.isSuccessInSkewRewrite();
    }

    /**
     * get explain string of distribute hint, when distribute hint success in leading, it would not show
     * @return explain string of distribute hint
     */
    public String getExplainString() {
        if (this.isSuccessInLeading) {
            return "";
        }
        StringBuilder out = new StringBuilder();
        switch (this.distributeType) {
            case NONE:
                break;
            case SHUFFLE_RIGHT:
                out.append("[shuffle");
                if (skewInfo != null && skewInfo.isSuccessInSkewRewrite()) {
                    out.append("_skew]");
                } else {
                    out.append("]");
                }
                break;
            case BROADCAST_RIGHT:
                out.append("[broadcast]");
                break;
            default:
                break;
        }
        return out.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DistributeHint that = (DistributeHint) o;
        return this.distributeType == that.distributeType && Objects.equals(this.skewInfo, that.skewInfo);
    }
}
