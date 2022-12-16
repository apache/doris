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

package org.apache.doris.analysis;


import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.VectorizedUtil;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Only used to plan the in bitmap syntax into join + bitmap filter.
 * This predicate not need to be sent to BE.
 */
public class BitmapFilterPredicate extends Predicate {

    private static final Logger LOG = LogManager.getLogger(BitmapFilterPredicate.class);

    private boolean notIn = false;

    BitmapFilterPredicate(Expr targetExpr, Expr srcExpr, boolean notIn) {
        super();
        this.notIn = notIn;
        Preconditions.checkNotNull(targetExpr);
        children.add(targetExpr);
        Preconditions.checkNotNull(srcExpr);
        children.add(srcExpr);
    }

    BitmapFilterPredicate(BitmapFilterPredicate other) {
        super(other);
        this.notIn = other.notIn;
    }

    public boolean isNotIn() {
        return notIn;
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        super.analyzeImpl(analyzer);

        Expr targetExpr = children.get(0);
        if (!targetExpr.getType().isIntegerType()) {
            throw new AnalysisException("Unsupported targetExpr type: " + targetExpr.getType().toSql()
                    + ". Target expr type must be integer.");
        }

        Expr srcExpr = children.get(1);
        if (!srcExpr.getType().isBitmapType()) {
            throw new AnalysisException("The srcExpr type must be bitmap, not " + srcExpr.getType().toSql() + ".");
        }

        if (ConnectContext.get() == null || (ConnectContext.get().getSessionVariable().getRuntimeFilterType()
                & TRuntimeFilterType.BITMAP.getValue()) == 0) {
            throw new AnalysisException("In bitmap syntax requires runtime filter of bitmap_filter to be enabled. "
                    + "Please `set runtime_filter_type = 'xxx, bitmap_filter'` first.");
        }

        if (!VectorizedUtil.isVectorized()) {
            throw new AnalysisException("In bitmap syntax is currently only supported in the vectorization engine.");
        }
    }

    @Override
    protected String toSqlImpl() {
        return (notIn ? "not " : "") + "BitmapFilterPredicate(" + children.get(0).toSql() + ", " + children.get(1)
                .toSql() + ")";
    }

    @Override
    protected void toThrift(TExprNode msg) {
        Preconditions.checkArgument(false, "`toThrift` in BitmapFilterPredicate should not be reached!");
    }

    @Override
    public Expr clone() {
        return new BitmapFilterPredicate(this);
    }
}
