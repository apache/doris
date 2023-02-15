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
import org.apache.doris.common.util.S3URI;
import org.apache.doris.thrift.TFileType;

import com.google.common.collect.ImmutableSet;
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
    public static final String S3_URI = "uri";
    public static final String S3_AK = "AWS_ACCESS_KEY";
    public static final String S3_SK = "AWS_SECRET_KEY";
    public static final String S3_ENDPOINT = "AWS_ENDPOINT";
    public static final String S3_REGION = "AWS_REGION";
    private static final String AK = "access_key";
    private static final String SK = "secret_key";

    private static final String USE_PATH_STYLE = "use_path_style";
    private static final String REGION = "region";

    private static final ImmutableSet<String> PROPERTIES_SET = new ImmutableSet.Builder<String>()
                        .add(S3_URI)
                        .add(AK)
                        .add(SK)
                        .add(USE_PATH_STYLE)
                        .add(REGION)
                        .build();
    private S3URI s3uri;
    private String s3AK;
    private String s3SK;
    private String endPoint;
    private String virtualBucket;
    private boolean forceVirtualHosted;

    public StreamTableValuedFunction(Map<String, String> params) throws AnalysisException {
        Map<String, String> validParams = new CaseInsensitiveMap();
        for (String key : params.keySet()) {
            if (!PROPERTIES_SET.contains(key.toLowerCase()) && !FILE_FORMAT_PROPERTIES.contains(key.toLowerCase())) {
                throw new AnalysisException(key + " is invalid property");
            }
            validParams.put(key, params.get(key));
        }
        parseProperties(validParams);
        parseFile();
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
        return null;
    }

    // =========== implement abstract methods of TableValuedFunctionIf =================
    @Override
    public String getTableName() {
        return "StreamTableValuedFunction";
    }
}
