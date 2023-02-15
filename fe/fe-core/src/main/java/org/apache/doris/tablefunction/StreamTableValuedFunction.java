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
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.S3URI;
import org.apache.doris.thrift.TFileType;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
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

        String originUri = "s3://my_bucket.cos.ap-beijing.myqcloud.com/file.txt";
        if (originUri.toLowerCase().startsWith("s3")) {
            // s3 protocol, default virtual-hosted style
            forceVirtualHosted = true;
        } else {
            // not s3 protocol, forceVirtualHosted is determined by USE_PATH_STYLE.
            forceVirtualHosted = !Boolean.valueOf(validParams.get(USE_PATH_STYLE)).booleanValue();
        }

        try {
            s3uri = S3URI.create(originUri, forceVirtualHosted);
        } catch (UserException e) {
            throw new AnalysisException("parse s3 uri failed, uri = " + originUri, e);
        }
        if (forceVirtualHosted) {
            // s3uri.getVirtualBucket() is: virtualBucket.endpoint, Eg:
            //          uri: http://my_bucket.cos.ap-beijing.myqcloud.com/file.txt
            // s3uri.getVirtualBucket() = my_bucket.cos.ap-beijing.myqcloud.com,
            // so we need separate virtualBucket and endpoint.
            String[] fileds = s3uri.getVirtualBucket().split("\\.", 2);
            virtualBucket = fileds[0];
            if (fileds.length > 1) {
                endPoint = fileds[1];
            } else {
                throw new AnalysisException("can not parse endpoint, please check uri.");
            }
        } else {
            endPoint = s3uri.getBucketScheme();
        }
        s3AK = validParams.getOrDefault(AK, "");
        s3SK = validParams.getOrDefault(SK, "");
        String usePathStyle = validParams.getOrDefault(USE_PATH_STYLE, "false");

        parseProperties(validParams);

        // set S3 location properties
        // these five properties is necessary, no one can be lost.
        locationProperties = Maps.newHashMap();
        locationProperties.put(S3_ENDPOINT, endPoint);
        locationProperties.put(S3_AK, s3AK);
        locationProperties.put(S3_SK, s3SK);
        locationProperties.put(S3_REGION, validParams.getOrDefault(REGION, ""));
        locationProperties.put(USE_PATH_STYLE, usePathStyle);

        parseFile();
    }

    // =========== implement abstract methods of ExternalFileTableValuedFunction =================
    @Override
    public TFileType getTFileType() {
        return TFileType.FILE_STREAM;
    }

    @Override
    public String getFilePath() {
        return "s3://my_bucket.cos.ap-beijing.myqcloud.com/file.txt";
    }

    @Override
    public BrokerDesc getBrokerDesc() {
        return new BrokerDesc("StreamTvfBroker", StorageType.STREAM, locationProperties);
    }

    // =========== implement abstract methods of TableValuedFunctionIf =================
    @Override
    public String getTableName() {
        return "StreamTableValuedFunction";
    }
}
