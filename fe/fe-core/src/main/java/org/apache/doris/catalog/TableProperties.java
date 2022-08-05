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

package org.apache.doris.catalog;

import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DataSortInfo;
import org.apache.doris.analysis.KeysDesc;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.thrift.TCompressionType;
import org.apache.doris.thrift.TStorageFormat;
import org.apache.doris.thrift.TStorageType;
import org.apache.doris.thrift.TTabletType;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableProperties {
    public Boolean useLightSchemaChange;
    public TStorageFormat storageFormat;
    public TCompressionType compressionType;
    public DataSortInfo dataSortInfo;
    public boolean enableUniqueKeyMergeOnWrite;
    public Set<String> bfColumns;
    public double bfFpp;
    public boolean isInMemory;
    public String remoteStoragePolicy;
    public String storagePolicy;
    public TTabletType tabletType;
    public DataProperty dataProperty;
    public String colocateGroup;
    public TStorageType storageType;
    public int schemaVersion;
    public Type sequenceColType;
    public Long versionInfo;

    public static class Builder {

        private class Context {
            List<Column> schema;
            KeysDesc keysDesc;
        }

        Context context;
        TableProperties tableProperties = new TableProperties();

        public Builder prepare(CreateTableStmt stmt) {
            context = new Context();
            context.schema = stmt.getColumns();
            context.keysDesc = stmt.getKeysDesc();
            return this;
        }

        public TableProperties build() {
            return tableProperties;
        }

        public Builder analyze(Map<String, String> properties) throws AnalysisException {
            return withUseLightSchemaChange(properties)
                    .withStorageFormat(properties)
                    .withCompressionType(properties)
                    .withDataSortInfo(properties)
                    .withEnableUniqueKeyMergeOnWrite(properties)
                    .withBloomFilterInfo(properties)
                    .withIsInMemory(properties)
                    .withRemoteStoragePolicy(properties)
                    .withStoragePolicy(properties)
                    .withTabletType(properties)
                    .withDataProperty(properties)
                    .withColocateGroup(properties)
                    .withStorageType(properties)
                    .withSchemaVersion(properties)
                    .withSequenceColType(properties)
                    .withVersionInfo(properties);
        }

        public Builder withUseLightSchemaChange(Map<String, String> properties) throws AnalysisException {
            tableProperties.useLightSchemaChange = PropertyAnalyzer.analyzeUseLightSchemaChange(properties);
            return this;
        }

        public Builder withStorageFormat(Map<String, String> properties) throws AnalysisException {
            tableProperties.storageFormat = PropertyAnalyzer.analyzeStorageFormat(properties);
            return this;
        }

        public Builder withCompressionType(Map<String, String> properties) throws AnalysisException {
            tableProperties.compressionType = PropertyAnalyzer.analyzeCompressionType(properties);
            return this;
        }

        public Builder withDataSortInfo(Map<String, String> properties) throws AnalysisException {
            tableProperties.dataSortInfo = PropertyAnalyzer.analyzeDataSortInfo(
                    properties, context.keysDesc.getKeysType(),
                    context.keysDesc.keysColumnSize(), tableProperties.storageFormat);
            return this;
        }

        public Builder withEnableUniqueKeyMergeOnWrite(Map<String, String> properties) throws AnalysisException {
            tableProperties.enableUniqueKeyMergeOnWrite = PropertyAnalyzer.analyzeUniqueKeyMergeOnWrite(properties);
            return this;
        }

        public Builder withBloomFilterInfo(Map<String, String> properties) throws AnalysisException {
            Set<String> bfColumns = null;
            double bfFpp = 0;
            bfColumns = PropertyAnalyzer.analyzeBloomFilterColumns(
                    properties, context.schema, context.keysDesc.getKeysType());
            if (bfColumns != null && bfColumns.isEmpty()) {
                bfColumns = null;
            }

            bfFpp = PropertyAnalyzer.analyzeBloomFilterFpp(properties);
            if (bfColumns != null && bfFpp == 0) {
                bfFpp = FeConstants.default_bloom_filter_fpp;
            } else if (bfColumns == null) {
                bfFpp = 0;
            }

            tableProperties.bfColumns = bfColumns;
            tableProperties.bfFpp = bfFpp;
            return this;
        }

        public Builder withIsInMemory(Map<String, String> properties) {
            tableProperties.isInMemory = PropertyAnalyzer.analyzeBooleanProp(
                    properties, PropertyAnalyzer.PROPERTIES_INMEMORY, false);
            return this;
        }

        public Builder withRemoteStoragePolicy(Map<String, String> properties) throws AnalysisException {
            tableProperties.remoteStoragePolicy = PropertyAnalyzer.analyzeRemoteStoragePolicy(properties);
            return this;
        }

        public Builder withStoragePolicy(Map<String, String> properties) throws AnalysisException {
            tableProperties.storagePolicy = PropertyAnalyzer.analyzeStoragePolicy(properties);
            return this;
        }

        public Builder withTabletType(Map<String, String> properties) throws AnalysisException {
            tableProperties.tabletType = PropertyAnalyzer.analyzeTabletType(properties);
            return this;
        }

        public Builder withDataProperty(Map<String, String> properties) throws AnalysisException {
            tableProperties.dataProperty = PropertyAnalyzer.analyzeDataProperty(
                    properties, DataProperty.DEFAULT_DATA_PROPERTY);
            return this;
        }

        public Builder withColocateGroup(Map<String, String> properties) throws AnalysisException {
            tableProperties.colocateGroup = PropertyAnalyzer.analyzeColocate(properties);
            return this;
        }

        public Builder withStorageType(Map<String, String> properties) throws AnalysisException {
            tableProperties.storageType = PropertyAnalyzer.analyzeStorageType(properties);
            return this;
        }

        public Builder withSchemaVersion(Map<String, String> properties) throws AnalysisException {
            tableProperties.schemaVersion = PropertyAnalyzer.analyzeSchemaVersion(properties);
            return this;
        }

        public Builder withSequenceColType(Map<String, String> properties) throws AnalysisException {
            tableProperties.sequenceColType = PropertyAnalyzer.analyzeSequenceType(
                    properties, context.keysDesc.getKeysType());
            return this;
        }

        public Builder withVersionInfo(Map<String, String> properties) throws AnalysisException {
            tableProperties.versionInfo = PropertyAnalyzer.analyzeVersionInfo(properties);
            return this;
        }
    }
}
