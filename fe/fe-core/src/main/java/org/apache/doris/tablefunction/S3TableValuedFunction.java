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
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.thrift.TFileType;

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
        // 1. analyze common properties
        Map<String, String> props = super.parseCommonProperties(properties);
        try {
            this.storageProperties = StorageProperties.createPrimary(props);
            this.backendConnectProperties.putAll(storageProperties.getBackendConfigProperties());
            String uri = storageProperties.validateAndGetUri(props);
            filePath = storageProperties.validateAndNormalizeUri(uri);
            this.backendConnectProperties.put(URI_KEY, filePath);
        } catch (UserException e) {
            throw new RuntimeException(e);
        }
        if (FeConstants.runningUnitTest) {
            // Just check
            // Fixme wait to be done  #50320
            // FileSystemFactory.get(storageProperties);
        } else {
            try {
                parseFile();
            } catch (AnalysisException e) {
                if (BrokerDesc.isS3AccessDeniedWithoutExplicitCredentials(storageProperties, e)) {
                    LOG.info("S3 TVF got 403 with no explicit credentials, retrying with anonymous access");
                    try {
                        retryWithAnonymousCredentials(props);
                    } catch (Exception retryException) {
                        LOG.warn("S3 TVF anonymous access retry also failed: {}",
                                retryException.getMessage());
                        throw e;
                    }
                } else {
                    throw e;
                }
            }
        }
    }

    /**
     * Switches all property maps to use anonymous credentials and retries parseFile().
     */
    private void retryWithAnonymousCredentials(Map<String, String> props) throws AnalysisException {
        props.put("s3.credentials_provider_type", "ANONYMOUS");

        try {
            this.storageProperties = StorageProperties.createPrimary(props);
        } catch (Exception e) {
            throw new AnalysisException("Failed to create anonymous storage properties: " + e.getMessage(), e);
        }

        this.backendConnectProperties.clear();
        this.backendConnectProperties.putAll(storageProperties.getBackendConfigProperties());
        this.backendConnectProperties.put(URI_KEY, filePath);

        this.processedParams.put("s3.credentials_provider_type", "ANONYMOUS");

        this.fileStatuses.clear();

        parseFile();
    }


    // =========== implement abstract methods of ExternalFileTableValuedFunction =================
    @Override
    public TFileType getTFileType() {
        return TFileType.FILE_S3;
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

