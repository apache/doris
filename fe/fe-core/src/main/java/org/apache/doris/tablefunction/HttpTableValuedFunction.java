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


import com.amazonaws.services.dynamodbv2.xspec.S;
import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.StorageBackend.StorageType;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.property.storage.HttpProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.plsql.Column;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HttpTableValuedFunction extends ExternalFileTableValuedFunction {
    public static final String NAME = "http";

    private HttpProperties httpProperties;
    private String uri;

    public HttpTableValuedFunction(Map<String, String> properties) throws AnalysisException {
        Map<String, String> props = super.parseCommonProperties(properties);

        try {
            this.storageProperties = StorageProperties.createPrimary(props);
            if (!(storageProperties instanceof HttpProperties)) {
                throw new AnalysisException("HttpTableValuedFunction only support http storage properties");
            }

            this.httpProperties = (HttpProperties)storageProperties;
            this.uri = this.httpProperties.validateAndGetUri(props);

            this.backendConnectProperties.putAll(storageProperties.getBackendConfigProperties());

            this.fileStatuses.clear();
            this.fileStatuses.add(new TBrokerFileStatus(this.uri, false, -1, false));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        TFileFormatType t = fileFormatProperties.getFileFormatType();
        switch (t) {
            case FORMAT_CSV_PLAIN:
            case FORMAT_CSV_GZ:
            case FORMAT_CSV_BZ2:
            case FORMAT_CSV_LZOP:
            case FORMAT_CSV_LZ4FRAME:
                break;
            default:
                throw new AnalysisException(
                    "http() only supports format='csv' (plain/gz/bz2/lzop/lz4frame)" /* + " or json" */
                );
        }
    }

    @Override
    public TFileType getTFileType() {
        return TFileType.FILE_HTTP;
    }

    @Override
    public String getFilePath() {
       if(uri == null) {
           throw new IllegalArgumentException("HttpTableValuedFunction uri is null");
       }
       return uri;
    }

    @Override
    public BrokerDesc getBrokerDesc() {
        return new BrokerDesc("HttpTvfBroker", StorageType.HTTP, processedParams);
    }

    @Override
    public String getTableName() {
        return "HttpTableValuedFunction";
    }


}
