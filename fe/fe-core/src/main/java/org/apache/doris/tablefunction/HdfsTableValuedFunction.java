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
import org.apache.doris.analysis.ExportStmt;
import org.apache.doris.analysis.StorageBackend.StorageType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
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
    public static String HADOOP_FS_NAME = "fs.defaultFS";
    // simple or kerberos
    public static String HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";
    public static String HADOOP_USER_NAME = "hadoop.username";
    public static String HADOOP_KERBEROS_PRINCIPAL = "hadoop.kerberos.principal";
    public static String HADOOP_KERBEROS_KEYTAB = "hadoop.kerberos.keytab";
    public static String HADOOP_SHORT_CIRCUIT = "dfs.client.read.shortcircuit";
    public static String HADOOP_SOCKET_PATH = "dfs.domain.socket.path";

    private static final ImmutableSet<String> LOCATION_PROPERTIES = new ImmutableSet.Builder<String>()
            .add(HDFS_URI)
            .add(HADOOP_SECURITY_AUTHENTICATION)
            .add(HADOOP_FS_NAME)
            .add(HADOOP_USER_NAME)
            .add(HADOOP_KERBEROS_PRINCIPAL)
            .add(HADOOP_KERBEROS_KEYTAB)
            .add(HADOOP_SHORT_CIRCUIT)
            .add(HADOOP_SOCKET_PATH)
            .build();

    private URI hdfsUri;
    private String filePath;

    public HdfsTableValuedFunction(Map<String, String> params) throws UserException {
        Map<String, String> fileFormatParams = new CaseInsensitiveMap();
        locationProperties = Maps.newHashMap();
        for (String key : params.keySet()) {
            if (FILE_FORMAT_PROPERTIES.contains(key.toLowerCase())) {
                fileFormatParams.put(key, params.get(key));
            } else if (LOCATION_PROPERTIES.contains(key.toLowerCase()) || HADOOP_FS_NAME.equalsIgnoreCase(key)) {
                // because HADOOP_FS_NAME contains upper and lower case
                if (HADOOP_FS_NAME.equalsIgnoreCase(key)) {
                    locationProperties.put(HADOOP_FS_NAME, params.get(key));
                } else {
                    locationProperties.put(key.toLowerCase(), params.get(key));
                }
            } else {
                throw new AnalysisException(key + " is invalid property");
            }
        }

        ExportStmt.checkPath(locationProperties.get(HDFS_URI), StorageType.HDFS);
        hdfsUri = URI.create(locationProperties.get(HDFS_URI));
        filePath = locationProperties.get(HADOOP_FS_NAME) + hdfsUri.getPath();

        parseProperties(fileFormatParams);
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
        return new BrokerDesc("S3TvfBroker", StorageType.HDFS, locationProperties);
    }

    // =========== implement abstract methods of TableValuedFunctionIf =================
    @Override
    public String getTableName() {
        return "HDFSTableValuedFunction";
    }
}
