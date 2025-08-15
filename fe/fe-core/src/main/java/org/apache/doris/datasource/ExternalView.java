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

package org.apache.doris.datasource;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.ViewIf;
import org.apache.doris.common.Pair;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.BaseAnalysisTask;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.TableStatsMeta;
import org.apache.doris.thrift.TTableDescriptor;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class ExternalView implements ViewIf {
    private String viewText;
    private ExternalTable externalTable;

    public ExternalView(ExternalTable externalTable, String viewText) {
        this.viewText = viewText;
        this.externalTable = externalTable;
    }

    @Override
    public String getViewText() {
        return viewText;
    }

    public ExternalTable getExternalTable() {
        return externalTable;
    }


    @Override
    public long getId() {
        return externalTable.getId();
    }

    public String getName() {
        return externalTable.getName();
    }

    @Override
    public TableType getType() {
        return externalTable.getType();
    }

    @Override
    public List<Column> getFullSchema() {
        return externalTable.getFullSchema();
    }

    @Override
    public List<Column> getBaseSchema() {
        return externalTable.getBaseSchema();
    }

    @Override
    public List<Column> getSchemaAllIndexes(boolean full) {
        return externalTable.getSchemaAllIndexes(full);
    }

    @Override
    public List<Column> getBaseSchema(boolean full) {
        return externalTable.getBaseSchema();
    }

    @Override
    public void setNewFullSchema(List<Column> newSchema) {
        externalTable.setNewFullSchema(newSchema);
    }

    @Override
    public Column getColumn(String name) {
        return externalTable.getColumn(name);
    }

    @Override
    public String getMysqlType() {
        return externalTable.getMysqlType();
    }

    @Override
    public String getEngine() {
        return externalTable.getEngine();
    }

    @Override
    public String getComment() {
        return externalTable.getComment();
    }

    @Override
    public long getCreateTime() {
        return externalTable.getCreateTime();
    }

    @Override
    public long getUpdateTime() {
        return externalTable.getUpdateTime();
    }

    @Override
    public long getRowCount() {
        return externalTable.getRowCount();
    }

    @Override
    public long getCachedRowCount() {
        return externalTable.getCachedRowCount();
    }

    @Override
    public long fetchRowCount() {
        return externalTable.fetchRowCount();
    }

    @Override
    public long getDataLength() {
        return externalTable.getDataLength();
    }

    @Override
    public long getAvgRowLength() {
        return externalTable.getAvgRowLength();
    }

    @Override
    public long getLastCheckTime() {
        return externalTable.getLastCheckTime();
    }

    @Override
    public String getComment(boolean escapeQuota) {
        return externalTable.getComment();
    }

    @Override
    public TTableDescriptor toThrift() {
        return externalTable.toThrift();
    }

    @Override
    public BaseAnalysisTask createAnalysisTask(AnalysisInfo info) {
        return externalTable.createAnalysisTask(info);
    }

    @Override
    public DatabaseIf getDatabase() {
        return externalTable.getDatabase();
    }

    @Override
    public Optional<ColumnStatistic> getColumnStatistic(String colName) {
        return externalTable.getColumnStatistic(colName);
    }

    @Override
    public boolean needReAnalyzeTable(TableStatsMeta tblStats) {
        return false;
    }

    @Override
    public List<Pair<String, String>> getColumnIndexPairs(Set<String> columns) {
        return externalTable.getColumnIndexPairs(columns);
    }

    @Override
    public List<Long> getChunkSizes() {
        return externalTable.getChunkSizes();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        externalTable.write(out);
    }

    @Override
    public boolean autoAnalyzeEnabled() {
        return externalTable.autoAnalyzeEnabled();
    }

}
