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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/ColumnDef.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * This def used for column which defaultValue is an expression.
 */
public class DefaultValueExprDef implements Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(DefaultValueExprDef.class);
    @SerializedName("exprName")
    private String exprName;
    @SerializedName("precision")
    private Long precision;


    public DefaultValueExprDef(String exprName) {
        this.exprName = exprName;
    }

    public DefaultValueExprDef(String exprName, Long precision) {
        this.exprName = exprName;
        this.precision = precision;
    }

    /**
     * generate a FunctionCallExpr
     * @return FunctionCallExpr of exprName
     */
    public FunctionCallExpr getExpr(Type type) {
        List<Expr> exprs = null;
        if (precision != null) {
            exprs = Lists.newArrayList();
            exprs.add(new IntLiteral(precision));
        }
        FunctionCallExpr expr = new FunctionCallExpr(exprName, new FunctionParams(exprs));
        try {
            expr.analyzeImplForDefaultValue(type);
        } catch (AnalysisException e) {
            if (ConnectContext.get() != null) {
                ConnectContext.get().getState().reset();
            }
            LOG.warn("analyzeImplForDefaultValue fail: {}", e);
        }
        return expr;
    }

    public String getExprName() {
        return exprName;
    }

    public Long getPrecision() {
        return precision;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static DefaultValueExprDef read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, DefaultValueExprDef.class);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        if (precision == null) {
            precision = 0L;
        }
    }
}
