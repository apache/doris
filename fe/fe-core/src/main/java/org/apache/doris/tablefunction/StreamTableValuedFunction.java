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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TFileType;

import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * The Implement of table valued function
 * stream("FORMAT" = "csv").
 */
public class StreamTableValuedFunction extends ExternalFileTableValuedFunction {
    private static final Logger LOG = LogManager.getLogger(StreamTableValuedFunction.class);
    public static final String NAME = "stream";

    public StreamTableValuedFunction(Map<String, String> params) throws AnalysisException {
        Map<String, String> validParams = new CaseInsensitiveMap();
        for (String key : params.keySet()) {
            if (!FILE_FORMAT_PROPERTIES.contains(key.toLowerCase())) {
                throw new AnalysisException(key + " is invalid property");
            }
            validParams.put(key, params.get(key));
        }
        parseProperties(validParams);
    }

    // =========== implement abstract methods of ExternalFileTableValuedFunction =================
    @Override
    public TFileType getTFileType() {
        switch (getTFileFormatType()) {
            case FORMAT_PARQUET:
            case FORMAT_ORC:
                return TFileType.FILE_LOCAL;
            default:
                return TFileType.FILE_STREAM;
        }
    }

    @Override
    public String getFilePath() {
        return null;
    }

    @Override
    public BrokerDesc getBrokerDesc() {
        return null;
    }

    // =========== implement abstract methods of TableValuedFunctionIf =================
    @Override
    public String getTableName() {
        return "StreamTableValuedFunction";
    }
}
