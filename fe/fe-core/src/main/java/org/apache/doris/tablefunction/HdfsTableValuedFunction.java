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
import org.apache.doris.analysis.StorageBackend;
import org.apache.doris.analysis.StorageBackend.StorageType;
import org.apache.doris.catalog.HdfsResource;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.URI;
import org.apache.doris.thrift.TFileType;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.commons.collections.map.CaseInsensitiveMap;
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
    public static final String HDFS_URI = "uri";
    // simple or kerberos

    private static final ImmutableSet<String> LOCATION_PROPERTIES = new ImmutableSet.Builder<String>()
            .add(HDFS_URI)
            .add(HdfsResource.HADOOP_SECURITY_AUTHENTICATION)
            .add(HdfsResource.HADOOP_FS_NAME)
            .add(HdfsResource.HADOOP_USER_NAME)
            .add(HdfsResource.HADOOP_KERBEROS_PRINCIPAL)
            .add(HdfsResource.HADOOP_KERBEROS_KEYTAB)
            .add(HdfsResource.HADOOP_SHORT_CIRCUIT)
            .add(HdfsResource.HADOOP_SOCKET_PATH)
            .build();

    private URI hdfsUri;

    public HdfsTableValuedFunction(Map<String, String> params) throws AnalysisException {
        Map<String, String> fileParams = new CaseInsensitiveMap();
        locationProperties = Maps.newHashMap();
        for (String key : params.keySet()) {
            String lowerKey = key.toLowerCase();
            if (FILE_FORMAT_PROPERTIES.contains(lowerKey)) {
                fileParams.put(lowerKey, params.get(key));
            } else if (LOCATION_PROPERTIES.contains(lowerKey)) {
                locationProperties.put(lowerKey, params.get(key));
            } else if (HdfsResource.HADOOP_FS_NAME.equalsIgnoreCase(key)) {
                // because HADOOP_FS_NAME contains upper and lower case
                locationProperties.put(HdfsResource.HADOOP_FS_NAME, params.get(key));
            } else {
                locationProperties.put(key, params.get(key));
            }
        }

        if (!locationProperties.containsKey(HDFS_URI)) {
            throw new AnalysisException(String.format("Configuration '%s' is required.", HDFS_URI));
        }
        StorageBackend.checkPath(locationProperties.get(HDFS_URI), StorageType.HDFS);
        hdfsUri = URI.create(locationProperties.get(HDFS_URI));
        filePath = locationProperties.get(HdfsResource.HADOOP_FS_NAME) + hdfsUri.getPath();

        super.parseProperties(fileParams);

        parseFile();
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
        return new BrokerDesc("HdfsTvfBroker", StorageType.HDFS, locationProperties);
    }

    // =========== implement abstract methods of TableValuedFunctionIf =================
    @Override
    public String getTableName() {
        return "HDFSTableValuedFunction";
    }
}
