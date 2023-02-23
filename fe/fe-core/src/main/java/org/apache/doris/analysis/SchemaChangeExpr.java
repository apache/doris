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

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.VariantType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TSchemaChangeInfo;
import org.apache.doris.thrift.TTypeDesc;
import org.apache.doris.thrift.TTypeNode;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;

public class SchemaChangeExpr extends Expr {
    private static final Logger LOG = LogManager.getLogger(SchemaChangeExpr.class);
    // Target table id
    private int tableId;
    private SlotRef variantSlot;

    public SchemaChangeExpr(SlotRef sourceSlot, int tableId) {
        super();
        Preconditions.checkNotNull(sourceSlot);
        variantSlot = sourceSlot;
        this.tableId = tableId;
    }

    @Override
    protected void treeToThriftHelper(TExpr container) {
        super.treeToThriftHelper(container);
    }

    @Override
    protected void toThrift(TExprNode msg) {
        TSchemaChangeInfo schemaInfo = new TSchemaChangeInfo();
        schemaInfo.setTableId(tableId);
        msg.setSchemaChangeInfo(schemaInfo);
        // set src variant slot
        variantSlot.toThrift(msg);
        msg.node_type = TExprNodeType.SCHEMA_CHANGE_EXPR;
        // set type info
        TTypeDesc desc = new TTypeDesc();
        desc.setTypes(new ArrayList<TTypeNode>());
        VariantType variant = new VariantType();
        variant.toThrift(desc);
        msg.setType(desc);
    }

    @Override
    public Expr clone() {
        return new SchemaChangeExpr((SlotRef) getChild(0), tableId);
    }

    @Override
    public String toSqlImpl() {
        return "SCHEMA_CHANGE(" + getChild(0).toSql() + ")";
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        Type childType = getChild(0).getType();
        if (childType.getPrimitiveType() != PrimitiveType.VARIANT) {
            throw new AnalysisException("Invalid column " + getChild(0).toSql());
        }
    }
}
