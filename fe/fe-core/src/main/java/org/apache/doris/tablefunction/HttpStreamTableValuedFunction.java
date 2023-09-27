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
import org.apache.doris.analysis.StorageBackend.StorageType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TFileType;

import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * The Implement of table valued function
 * http_stream("FORMAT" = "csv").
 */
public class HttpStreamTableValuedFunction extends ExternalFileTableValuedFunction {
    private static final Logger LOG = LogManager.getLogger(HttpStreamTableValuedFunction.class);
    public static final String NAME = "http_stream";

    public HttpStreamTableValuedFunction(Map<String, String> params) throws AnalysisException {
        Map<String, String> fileParams = new CaseInsensitiveMap();
        for (String key : params.keySet()) {
            String lowerKey = key.toLowerCase();
            if (!FILE_FORMAT_PROPERTIES.contains(lowerKey)) {
                throw new AnalysisException(key + " is invalid property");
            }
            fileParams.put(lowerKey, params.get(key).toLowerCase());
        }

        String formatString = fileParams.getOrDefault(FORMAT, "").toLowerCase();
        if (formatString.equals("parquet")
                || formatString.equals("avro")
                || formatString.equals("orc")) {
            throw new AnalysisException("current http_stream does not yet support parquet, avro and orc");
        }

        super.parseProperties(fileParams);
    }

    // =========== implement abstract methods of ExternalFileTableValuedFunction =================
    @Override
    public TFileType getTFileType() {
        return TFileType.FILE_STREAM;
    }

    @Override
    public String getFilePath() {
        return null;
    }

    @Override
    public BrokerDesc getBrokerDesc() {
        return new BrokerDesc("HttpStreamTvfBroker", StorageType.STREAM, locationProperties);
    }

    // =========== implement abstract methods of TableValuedFunctionIf =================
    @Override
    public String getTableName() {
        return "HttpStreamTableValuedFunction";
    }
}
