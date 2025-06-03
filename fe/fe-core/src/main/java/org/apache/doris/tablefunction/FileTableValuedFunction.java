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
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.storage.AbstractS3CompatibleProperties;
import org.apache.doris.datasource.property.storage.AzureProperties;
import org.apache.doris.datasource.property.storage.HdfsCompatibleProperties;
import org.apache.doris.datasource.property.storage.LocalProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;

import java.util.List;
import java.util.Map;

public class FileTableValuedFunction extends ExternalFileTableValuedFunction {
    public static final String NAME = "file";

    private ExternalFileTableValuedFunction delegateTvf;

    public FileTableValuedFunction(Map<String, String> properties) throws AnalysisException {
        // We don't need to parseCommonProperties because the corresponding Storage will do it
        // Map<String, String> props = super.parseCommonProperties(properties);
        try {
            this.storageProperties = StorageProperties.createPrimary(properties);
            if (this.storageProperties instanceof AbstractS3CompatibleProperties
                    || this.storageProperties instanceof AzureProperties) {
                delegateTvf = new S3TableValuedFunction(properties);
            } else if (this.storageProperties instanceof HdfsCompatibleProperties) {
                delegateTvf = new HdfsTableValuedFunction(properties);
            } else if (this.storageProperties instanceof LocalProperties) {
                delegateTvf = new LocalTableValuedFunction(properties);
            } else {
                throw new AnalysisException("Could not find storage_type: " + storageProperties);
            }
        } catch (UserException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Column> getTableColumns() throws AnalysisException {
        return delegateTvf.getTableColumns();
    }

    @Override
    public ScanNode getScanNode(PlanNodeId id, TupleDescriptor desc, SessionVariable sv) {
        return delegateTvf.getScanNode(id, desc, sv);
    }

    @Override
    public TFileFormatType getTFileFormatType() {
        return delegateTvf.getTFileFormatType();
    }

    @Override
    public TFileCompressType getTFileCompressType() {
        return delegateTvf.getTFileCompressType();
    }

    @Override
    public Map<String, String> getBackendConnectProperties() {
        return delegateTvf.getBackendConnectProperties();
    }

    @Override
    public List<String> getPathPartitionKeys() {
        return delegateTvf.getPathPartitionKeys();
    }

    @Override
    public List<TBrokerFileStatus> getFileStatuses() {
        return delegateTvf.getFileStatuses();
    }

    @Override
    public TFileAttributes getFileAttributes() {
        return delegateTvf.getFileAttributes();
    }

    @Override
    public void checkAuth(ConnectContext ctx) {
        delegateTvf.checkAuth(ctx);
    }

    @Override
    public TFileType getTFileType() {
        return delegateTvf.getTFileType();
    }

    @Override
    public String getFilePath() {
        return delegateTvf.getFilePath();
    }

    @Override
    public BrokerDesc getBrokerDesc() {
        return delegateTvf.getBrokerDesc();
    }

    @Override
    public String getTableName() {
        return delegateTvf.getTableName();
    }

    @Override
    protected Backend getBackend() {
        return delegateTvf.getBackend();
    }
}
