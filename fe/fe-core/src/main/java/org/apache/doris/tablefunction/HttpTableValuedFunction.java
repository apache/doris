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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.property.storage.HttpProperties;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class HttpTableValuedFunction extends ExternalFileTableValuedFunction {
    private static final Logger LOG = LogManager.getLogger(HttpTableValuedFunction.class);
    public static final String NAME = "http";

    private HttpProperties httpProperties;

    public HttpTableValuedFunction(Map<String, String> properties) throws AnalysisException {
        super.parseCommonProperties(properties);

        this.httpProperties = new HttpProperties(properties);

        if (fileFormatProperties.getFileFormatType() == TFileFormatType.FORMAT_PARQUET
            || fileFormatProperties.getFileFormatType() == TFileFormatType.FORMAT_AVRO
            || fileFormatProperties.getFileFormatType() == TFileFormatType.FORMAT_ORC) {
            throw new AnalysisException("http does not yet support parquet, avro and orc");
        }

        String uri = httpProperties.getUri();
        if (uri == null || (!uri.startsWith("http://") && !uri.startsWith("https://"))) {
            throw new AnalysisException("http table function requires a valid http(s) uri");
        }
    }

    @Override
    public TFileType getTFileType() {
        return TFileType.FILE_HTTP;
    }

    @Override
    public String getFilePath() {
        return httpProperties.getUri();
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
