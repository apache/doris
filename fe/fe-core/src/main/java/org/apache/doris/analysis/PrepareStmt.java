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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TDescriptorTable;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprList;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class PrepareStmt extends StatementBase {
    private static final Logger LOG = LogManager.getLogger(PrepareStmt.class);
    private StatementBase inner;
    private String stmtName;
    // select * from tbl where a = ? and b = ?
    // `?` is the placeholder
    protected List<PlaceHolderExpr> placeholders = new ArrayList<>();

    // Cached for better CPU performance, since serialize DescriptorTable and
    // outputExprs are heavy work
    private ByteString serializedDescTable;
    private ByteString serializedOutputExpr;
    private TDescriptorTable descTable;

    private UUID id;
    // whether return binary protocol mysql row or not
    private boolean binaryRowFormat;
    int schemaVersion = -1;
    OlapTable tbl;
    ConnectContext context;

    public PrepareStmt(StatementBase stmt, String name, boolean binaryRowFormat) {
        this.inner = stmt;
        this.stmtName = name;
        this.id = UUID.randomUUID();
        this.binaryRowFormat = binaryRowFormat;
    }

    public void setContext(ConnectContext ctx) {
        this.context = ctx;
    }

    public boolean reAnalyze() {
        if (schemaVersion == tbl.getBaseSchemaVersion()) {
            return false;
        }
        reset();
        return true;
    }

    public TDescriptorTable getDescTable() {
        return descTable;
    }

    public UUID getID() {
        return id;
    }

    public List<PlaceHolderExpr> placeholders() {
        return this.placeholders;
    }

    public boolean isBinaryProtocol() {
        return binaryRowFormat;
    }

    public void cacheSerializedDescriptorTable(DescriptorTable desctbl) {
        try {
            descTable = desctbl.toThrift();
            serializedDescTable = ByteString.copyFrom(
                    new TSerializer().serialize(descTable));
        } catch (TException e) {
            LOG.warn("failed to serilize DescriptorTable, {}", e.getMessage());
            Preconditions.checkState(false, e.getMessage());
        }
    }

    public void cacheSerializedOutputExprs(List<Expr> outExprs) {
        List<TExpr> exprs = new ArrayList<>();
        for (Expr expr : outExprs) {
            exprs.add(expr.treeToThrift());
        }
        TExprList exprList = new TExprList(exprs);
        try {
            serializedOutputExpr = ByteString.copyFrom(
                        new TSerializer().serialize(exprList));
        } catch (TException e) {
            LOG.warn("failed to serilize TExprList, {}", e.getMessage());
            Preconditions.checkState(false, e.getMessage());
        }
    }

    public ByteString getSerializedDescTable() {
        return serializedDescTable;
    }

    public ByteString getSerializedOutputExprs() {
        return serializedOutputExpr;
    }

    public int getParmCount() {
        return placeholders.size();
    }

    public List<Expr> getSlotRefOfPlaceHolders() {
        ArrayList<Expr> slots = new ArrayList<>();
        if (inner instanceof SelectStmt) {
            SelectStmt select = (SelectStmt) inner;
            for (PlaceHolderExpr pexpr : placeholders) {
                // Only point query support
                for (Map.Entry<SlotRef, Expr> entry :
                            select.getPointQueryEQPredicates().entrySet()) {
                    // same instance
                    if (entry.getValue() == pexpr) {
                        slots.add(entry.getKey());
                    }
                }
            }
            return slots;
        }
        return null;
    }

    public List<String> getColLabelsOfPlaceHolders() {
        ArrayList<String> lables = new ArrayList<>();
        if (inner instanceof SelectStmt) {
            for (Expr slotExpr : getSlotRefOfPlaceHolders()) {
                SlotRef slot = (SlotRef) slotExpr;
                Column c = slot.getColumn();
                if (c != null) {
                    lables.add(c.getName());
                    continue;
                }
                lables.add("");
            }
            return lables;
        }
        return null;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        if (!(inner instanceof SelectStmt)) {
            throw new UserException("Only support prepare SelectStmt now");
        }
        // Use tmpAnalyzer since selectStmt will be reAnalyzed
        Analyzer tmpAnalyzer = new Analyzer(context.getEnv(), context);
        // collect placeholders from stmt exprs tree
        SelectStmt selectStmt = (SelectStmt) inner;
        // TODO(lhy) support more clauses
        if (selectStmt.getWhereClause() != null) {
            selectStmt.getWhereClause().collect(PlaceHolderExpr.class, placeholders);
        }
        inner.analyze(tmpAnalyzer);
        if (!selectStmt.checkAndSetPointQuery()) {
            throw new UserException("Only support prepare SelectStmt point query now");
        }
        tbl = (OlapTable) selectStmt.getTableRefs().get(0).getTable();
        schemaVersion = tbl.getBaseSchemaVersion();
        // reset will be reAnalyzed
        selectStmt.reset();
        analyzer.setPrepareStmt(this);
        // tmpAnalyzer.setPrepareStmt(this);
    }

    public String getName() {
        return stmtName;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }

    public StatementBase getInnerStmt() {
        return inner;
    }

    public int argsSize() {
        return placeholders.size();
    }

    public void asignValues(List<LiteralExpr> values) throws UserException {
        if (values.size() != placeholders.size()) {
            throw new UserException("Invalid arguments size "
                                + values.size() + ", expected " + placeholders.size());
        }
        for (int i = 0; i < values.size(); ++i) {
            placeholders.get(i).setLiteral(values.get(i));
        }
    }

    @Override
    public void reset() {
        serializedDescTable = null;
        serializedOutputExpr = null;
        descTable = null;
        this.id = UUID.randomUUID();
        placeholders.clear();
        inner.reset();
    }
}
