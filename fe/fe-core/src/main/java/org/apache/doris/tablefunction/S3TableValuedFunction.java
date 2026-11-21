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

package org.apache.doris.tablefunction;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.catalog.Column;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.lance.metadata.LanceMetadataLoader;
import org.apache.doris.datasource.lance.metadata.LanceMultiSnapshotLoader;
import org.apache.doris.datasource.lance.metadata.LancePlannedDataset;
import org.apache.doris.datasource.lance.metadata.LanceTableMetadata;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.datasource.property.storage.Uris;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TFileType;

import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * The Implement of table valued function
 * S3("uri" = "xxx", "access_key" = "xx", "SECRET_KEY" = "qqq", "FORMAT" = "csv").
 * <p>
 * For AWS S3, uri should be:
 * s3://bucket.s3.us-east-1.amazonaws.com/csv/taxi.csv
 * or
 * https://us-east-1.amazonaws.com/bucket/csv/taxi.csv with "use_path_style"="true"
 * or
 * https://bucket.us-east-1.amazonaws.com/csv/taxi.csv with "use_path_style"="false"
 */
public class S3TableValuedFunction extends ExternalFileTableValuedFunction {
    public static final String NAME = "s3";

    public S3TableValuedFunction(Map<String, String> properties) throws AnalysisException {
        this(properties, null);
    }

    public S3TableValuedFunction(Map<String, String> properties, List<TBrokerFileStatus> files)
            throws AnalysisException {
        // 1. analyze common properties
        Map<String, String> props = super.parseCommonProperties(properties);
        boolean hasUris = props.keySet().stream().anyMatch(key -> key.equalsIgnoreCase(Uris.KEY));
        List<String> locations = Collections.emptyList();
        try {
            this.storageProperties = StorageProperties.createPrimary(props);
            this.backendConnectProperties.putAll(storageProperties.getBackendConfigProperties());
            if (hasUris) {
                if (!isLanceFormat()) {
                    throw new AnalysisException(
                            "'" + Uris.KEY + "' is supported only for the Lance format");
                }
                locations = storageProperties.validateAndNormalizeUris(props);
            } else {
                String uri = storageProperties.validateAndGetUri(props);
                filePath = storageProperties.validateAndNormalizeUri(uri);
                this.backendConnectProperties.put(URI_KEY, filePath);
            }
        } catch (UserException e) {
            throw new RuntimeException(e);
        }

        if (FeConstants.runningUnitTest) {
            // Just check
            // Fixme wait to be done  #50320
            // FileSystemFactory.get(storageProperties);
            if (files != null) {
                this.fileStatuses.addAll(files);
            }
            return;
        }

        if (!isLanceFormat()) {
            parseFile(files);
            return;
        }
        List<LancePlannedDataset> datasets;
        Schema referenceSchema;
        if (hasUris) {
            LanceMultiSnapshotLoader.Loaded loaded =
                    LanceMultiSnapshotLoader.load(locations, storageProperties);
            datasets = loaded.datasets;
            referenceSchema = loaded.referenceSchema;
        } else {
            LanceTableMetadata metadata;
            try {
                metadata = LanceMetadataLoader.loadLatestForTvf(
                        filePath, Collections.singletonList(storageProperties));
            } catch (Exception e) {
                throw new AnalysisException(
                        "Failed to load Lance metadata from S3 dataset '" + filePath + "': "
                                + e.getMessage(),
                                e);
            }
            datasets = Collections.singletonList(new LancePlannedDataset(
                    metadata.getDatasetUri(), metadata.getVersion(), metadata.getFragments()));
            referenceSchema = metadata.getSchema();
        }
        setLancePlannedDatasets(datasets, referenceSchema);
    }



    // =========== implement abstract methods of ExternalFileTableValuedFunction =================
    @Override
    public TFileType getTFileType() {
        return TFileType.FILE_S3;
    }

    @Override
    public List<Column> getTableColumns() throws AnalysisException {
        if (isLanceFormat()) {
            if (columns == null) {
                throw new AnalysisException("S3 Lance metadata was not initialized in FE");
            }
            return columns;
        }
        return super.getTableColumns();
    }

    @Override
    public String getFilePath() {
        // must be "s3://..."
        return filePath;
    }

    @Override
    public BrokerDesc getBrokerDesc() {
        return new BrokerDesc("S3TvfBroker", processedParams);
    }

    // =========== implement abstract methods of TableValuedFunctionIf =================
    @Override
    public String getTableName() {
        return "S3TableValuedFunction";
    }
}
