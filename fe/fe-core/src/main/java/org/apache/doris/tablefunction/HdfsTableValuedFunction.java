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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 * The Implement of table valued function
 * hdfs("uri" = "xxx", "hadoop.username" = "xx", "FORMAT" = "csv").
 */
public class HdfsTableValuedFunction extends ExternalFileTableValuedFunction {
    public static final Logger LOG = LogManager.getLogger(HdfsTableValuedFunction.class);
    public static final String NAME = "hdfs";
    private static final String PROP_URI = "uri";

    public HdfsTableValuedFunction(Map<String, String> properties) throws AnalysisException {
        init(properties);
    }

    private void init(Map<String, String> properties) throws AnalysisException {
        // 1. analyze common properties
        Map<String, String> props = super.parseCommonProperties(properties);

        this.storageProperties = StorageProperties.createStorageProperties(props);
        locationProperties.putAll(storageProperties.getBackendConfigProperties());
        // 2. analyze uri
        try {
            String    uri = storageProperties.checkLoadPropsAndReturnUri(props);
            filePath = storageProperties.convertUrlToFilePath(uri);
        } catch (UserException e) {
            throw new AnalysisException("Failed check storage props", e);
        }

        if (!FeConstants.runningUnitTest) {
            // 4. parse file
            parseFile();
        }
    }

    // =========== implement abstract methods of ExternalFileTableValuedFunction =================
    @Override
    public TFileType getTFileType() {
        return TFileType.FILE_HDFS;
    }

    @Override
    public String getFilePath() {
        // must be "hdfs://namenode/filepath"
        return filePath;
    }

    @Override
    public BrokerDesc getBrokerDesc() {
        return new BrokerDesc("HdfsTvfBroker", locationProperties, storageProperties);
    }

    // =========== implement abstract methods of TableValuedFunctionIf =================
    @Override
    public String getTableName() {
        return "HDFSTableValuedFunction";
    }
}
