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

// import org.apache.doris.catalog.Column;
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
    // We provide bellow types of prepared statement:
    // NONE, which is not prepared
    // FULL_PREPARED, which is real prepared, which will cache analyzed statement and planner
    // STATEMENT, which only cache statement it self, but need to analyze each time executed.
    public enum PreparedType {
        NONE, FULL_PREPARED, STATEMENT
    }

    private static final Logger LOG = LogManager.getLogger(PrepareStmt.class);
    private StatementBase inner;
    private String stmtName;
    // Cached for better CPU performance, since serialize DescriptorTable and
    // outputExprs are heavy work
    private ByteString serializedDescTable;
    private ByteString serializedOutputExpr;
    private ByteString serializedQueryOptions;


    private UUID id;
    private int schemaVersion = -1;
    private OlapTable tbl;
    private ConnectContext context;
    private PreparedType preparedType = PreparedType.STATEMENT;
    boolean isPointQueryShortCircuit = false;

    private TDescriptorTable descTable;
    // Serialized mysql Field, this could avoid serialize mysql field each time sendFields.
    // Since, serialize fields is too heavy when table is wide
    Map<String, byte[]> serializedFields =  Maps.newHashMap();

    public PrepareStmt(StatementBase stmt, String name) {
        this.inner = stmt;
        this.stmtName = name;
        this.id = UUID.randomUUID();
    }

    public void setContext(ConnectContext ctx) {
        this.context = ctx;
    }

    public boolean needReAnalyze() {
        if (preparedType == PreparedType.FULL_PREPARED
                    && schemaVersion == tbl.getBaseSchemaVersion()) {
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

    public boolean isPointQueryShortCircuit() {
        return isPointQueryShortCircuit;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        // TODO support more Statement
        if (!(inner instanceof SelectStmt) && !(inner instanceof NativeInsertStmt)) {
            throw new UserException("Only support prepare SelectStmt or NativeInsertStmt");
        }
        analyzer.setPrepareStmt(this);
        if (inner instanceof SelectStmt) {
            // Try to use FULL_PREPARED to increase performance
            SelectStmt selectStmt = (SelectStmt) inner;
            try {
                // Use tmpAnalyzer since selectStmt will be reAnalyzed
                Analyzer tmpAnalyzer = new Analyzer(context.getEnv(), context);
                inner.analyze(tmpAnalyzer);
                // Case 1 short circuit point query
                if (selectStmt.checkAndSetPointQuery()) {
                    tbl = (OlapTable) selectStmt.getTableRefs().get(0).getTable();
                    schemaVersion = tbl.getBaseSchemaVersion();
                    preparedType = PreparedType.FULL_PREPARED;
                    isPointQueryShortCircuit = true;
                    LOG.debug("using FULL_PREPARED prepared");
                    return;
                }
            } catch (UserException e) {
                LOG.debug("fallback to STATEMENT prepared, {}", e);
            } finally {
                // will be reanalyzed
                selectStmt.reset();
            }
            // use session var to decide whether to use full prepared or let user client handle to do fail over
            if (preparedType != PreparedType.FULL_PREPARED
                    && !ConnectContext.get().getSessionVariable().enableServeSidePreparedStatement) {
                throw new UserException("Failed to prepare statement"
                                + "try to set enable_server_side_prepared_statement = true");
            }
        } else if (inner instanceof NativeInsertStmt) {
            LabelName label = ((NativeInsertStmt) inner).getLoadLabel();
            if (label == null || Strings.isNullOrEmpty(label.getLabelName())) {
                analyzer.setPrepareStmt(this);
                preparedType = PreparedType.STATEMENT;
            } else {
                throw new UserException("Only support prepare InsertStmt without label now");
            }
        }
        preparedType = PreparedType.STATEMENT;
        LOG.debug("using STATEMENT prepared");
    }

    public String getName() {
        return stmtName;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }

    public List<PlaceHolderExpr> placeholders() {
        return inner.getPlaceHolders();
    }

    public int getParmCount() {
        return inner.getPlaceHolders().size();
    }

    public PreparedType getPreparedType() {
        return preparedType;
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

    public StatementBase getInnerStmt() {
        if (preparedType == PreparedType.FULL_PREPARED) {
            // For performance reason we could reuse the inner statement when FULL_PREPARED
            return inner;
        }
        // Make a copy of Statement, since anlyze will modify the structure of Statement.
        // But we should keep the original statement
        if (inner instanceof SelectStmt) {
            return new SelectStmt((SelectStmt) inner);
        }
        // Other statement could reuse the inner statement
        return inner;
    }

    public int argsSize() {
        return inner.getPlaceHolders().size();
    }

    public void asignValues(List<LiteralExpr> values) throws UserException {
        if (values.size() != inner.getPlaceHolders().size()) {
            throw new UserException("Invalid arguments size "
                                + values.size() + ", expected " + inner.getPlaceHolders().size());
        }
        for (int i = 0; i < values.size(); ++i) {
            inner.getPlaceHolders().get(i).setLiteral(values.get(i));
            inner.getPlaceHolders().get(i).analysisDone();
        }
        if (!values.isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("assign values {}", values.get(0).toSql());
            }
        }
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
