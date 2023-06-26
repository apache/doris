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

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.DataPartition;
import org.apache.doris.planner.DataSink;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This is the unified abstract stmt for all load kinds of load in {@link LoadType}
 * All contents of native InsertStmt is moved to {@link NativeInsertStmt}
 * Currently this abstract class keep the native insert methods for compatibility, and will eventually be moved
 * to {@link NativeInsertStmt}
 */
public abstract class InsertStmt extends DdlStmt {

    protected LabelName label;

    protected LoadProperties loadProperties;

    protected List<Table> targetTables;

    protected String comments;

    public InsertStmt(LabelName label, Map<String, String> properties, String comment) {
        this.label = label;
        this.loadProperties = new LoadProperties(properties);
        this.comments = comment != null ? comment : "";
    }

    // ---------------------------- for old insert stmt ----------------------------

    public boolean isValuesOrConstantSelect() {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    public Table getTargetTable() {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    public void setTargetTable(Table targetTable) {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    public long getTransactionId() {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    public Boolean isRepartition() {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    public String getDbName() {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    public String getTbl() {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    public void getTables(Analyzer analyzer, Map<Long, TableIf> tableMap, Set<String> parentViewNameSet)
            throws AnalysisException {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    public QueryStmt getQueryStmt() {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    public void setQueryStmt(QueryStmt queryStmt) {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    public String getLabel() {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    public DataSink getDataSink() {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    public DatabaseIf getDbObj() {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    public boolean isTransactionBegin() {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    public void prepareExpressions() throws UserException {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    public void complete() throws UserException {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    public DataPartition getDataPartition() {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    // ---------------------------------------------------------------------------

    // ------------------------- for unified insert stmt -------------------------

    public boolean needLoadManager() {
        return getLoadType() != LoadType.NATIVE_INSERT && getLoadType() != LoadType.UNKNOWN;
    }

    public LabelName getLoadLabel() {
        return label;
    }

    /**
     * for multi-tables load, we need have several target tbl
     *
     * @return all target table names
     */
    public List<Table> getTargetTableList() {
        return targetTables;
    }

    public abstract List<? extends DataDesc> getDataDescList();

    public abstract ResourceDesc getResourceDesc();

    public abstract LoadType getLoadType();

    public LoadProperties getLoadProperties() {
        return loadProperties;
    }

    public String getComments() {
        return comments;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        analyzeProperties();
    }

    protected void analyzeProperties() throws DdlException {
        loadProperties.analyze();
    }

    public NativeInsertStmt getNativeInsertStmt() {
        throw new UnsupportedOperationException("only invoked in NativeInsertStmt");
    }

    /**
     * TODO: unify the data_desc
     * the unique entrance for data_desc
     */
    interface DataDesc {

        String toSql();

    }
}
