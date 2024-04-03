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

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TDescriptorTable;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprList;
import org.apache.doris.thrift.TQueryOptions;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
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

    // Cached for better CPU performance, since serialize DescriptorTable and
    // outputExprs are heavy work
    private ByteString serializedDescTable;
    private ByteString serializedOutputExpr;
    private ByteString serializedQueryOptions;
    private TDescriptorTable descTable;

    private UUID id;
    // whether return binary protocol mysql row or not
    private boolean binaryRowFormat;
    int schemaVersion = -1;
    OlapTable tbl;
    ConnectContext context;
    // Serialized mysql Field, this could avoid serialize mysql field each time sendFields.
    // Since, serialize fields is too heavy when table is wide
    Map<String, byte[]> serializedFields =  Maps.newHashMap();

    // We provide bellow types of prepared statement:
    // NONE, which is not prepared
    // FULL_PREPARED, which is really prepared, which will cache analyzed statement and planner
    // STATEMENT, which only cache statement itself, but need to analyze each time executed
    public enum PreparedType {
        NONE, FULL_PREPARED, STATEMENT
    }

    private PreparedType preparedType = PreparedType.STATEMENT;

    public PrepareStmt(StatementBase stmt, String name, boolean binaryRowFormat) {
        this.inner = stmt;
        this.stmtName = name;
        this.id = UUID.randomUUID();
        this.binaryRowFormat = binaryRowFormat;
    }

    public void setContext(ConnectContext ctx) {
        this.context = ctx;
    }

    public boolean needReAnalyze() {
        if (preparedType == PreparedType.FULL_PREPARED && schemaVersion == tbl.getBaseSchemaVersion()) {
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

    public boolean isBinaryProtocol() {
        return binaryRowFormat;
    }

    public byte[] getSerializedField(String colName) {
        return serializedFields.getOrDefault(colName, null);
    }

    public void setSerializedField(String colName, byte[] serializedField) {
        serializedFields.put(colName, serializedField);
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

    public void cacheSerializedQueryOptions(TQueryOptions queryOptions) {
        try {
            serializedQueryOptions = ByteString.copyFrom(
                    new TSerializer().serialize(queryOptions));
        } catch (TException e) {
            LOG.warn("failed to serilize queryOptions , {}", e.getMessage());
            Preconditions.checkState(false, e.getMessage());
        }
    }

    public ByteString getSerializedDescTable() {
        return serializedDescTable;
    }

    public ByteString getSerializedOutputExprs() {
        return serializedOutputExpr;
    }

    public ByteString getSerializedQueryOptions() {
        return serializedQueryOptions;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        if (inner instanceof SelectStmt) {
            // Use tmpAnalyzer since selectStmt will be reAnalyzed
            Analyzer tmpAnalyzer = new Analyzer(context.getEnv(), context);
            SelectStmt selectStmt = (SelectStmt) inner;
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
            preparedType = PreparedType.FULL_PREPARED;
        } else if (inner instanceof NativeInsertStmt) {
            LabelName label = ((NativeInsertStmt) inner).getLoadLabel();
            if (label == null || Strings.isNullOrEmpty(label.getLabelName())) {
                analyzer.setPrepareStmt(this);
                preparedType = PreparedType.STATEMENT;
            } else {
                throw new UserException("Only support prepare InsertStmt without label now");
            }
        } else {
            throw new UserException("Only support prepare SelectStmt or InsertStmt now");
        }
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

    public List<PlaceHolderExpr> placeholders() {
        return inner.getPlaceHolders();
    }

    public int getParmCount() {
        return inner.getPlaceHolders().size();
    }

    public List<Expr> getPlaceHolderExprList() {
        ArrayList<Expr> slots = new ArrayList<>();
        for (PlaceHolderExpr pexpr : inner.getPlaceHolders()) {
            slots.add(pexpr);
        }
        return slots;
    }

    public List<String> getColLabelsOfPlaceHolders() {
        ArrayList<String> lables = new ArrayList<>();
        for (int i = 0; i < inner.getPlaceHolders().size(); ++i) {
            lables.add("lable " + i);
        }
        return lables;
    }

    public void asignValues(List<LiteralExpr> values) throws UserException {
        if (values.size() != inner.getPlaceHolders().size()) {
            throw new UserException("Invalid arguments size "
                                + values.size() + ", expected " + inner.getPlaceHolders().size());
        }
        for (int i = 0; i < values.size(); ++i) {
            inner.getPlaceHolders().get(i).setLiteral(values.get(i));
        }
        if (!values.isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("assign values {}", values.get(0).toSql());
            }
        }
    }

    public PreparedType getPreparedType() {
        return preparedType;
    }

    @Override
    public void reset() {
        serializedDescTable = null;
        serializedOutputExpr = null;
        descTable = null;
        this.id = UUID.randomUUID();
        inner.reset();
        if (inner instanceof NativeInsertStmt) {
            ((NativeInsertStmt) inner).resetPrepare();
        }
        serializedFields.clear();
    }
}
