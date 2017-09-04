// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

package com.baidu.palo.common;

import com.baidu.palo.analysis.ClusterName;
import com.baidu.palo.cluster.ClusterNamespace;
import com.baidu.palo.system.SystemInfoService;

import com.google.common.base.Strings;

public class FeNameFormat {
    private static final String CLUSTER_REGEX = "^[a-zA-Z][a-zA-Z0-9_]{0,63}$";
    private static final String DB_REGEX = "^[a-zA-Z][a-zA-Z0-9_]{0,63}$";
    private static final String TABLE_REGEX = "^[a-zA-Z][a-zA-Z0-9_]{0,63}$";
    private static final String PARTITION_REGEX = "^[a-zA-Z][a-zA-Z0-9_]{0,63}$";
    private static final String COLUMN_REGEX = "^[a-zA-Z][a-zA-Z0-9_]{0,63}$";
    private static final String USER_REGEX = "^[a-zA-Z][a-zA-Z0-9_]{0,63}$";
    private static final String LABEL_REGEX = "^[-_A-Za-z0-9]{1,128}$";

    public static final String FORBIDDEN_PARTITION_NAME = "placeholder_";

    public static void checkClusterName(String clusterName) throws AnalysisException {
        if (Strings.isNullOrEmpty(clusterName) || !clusterName.matches(CLUSTER_REGEX)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_CLUSTER_NAME, clusterName);
        }
        if (clusterName.equalsIgnoreCase(SystemInfoService.DEFAULT_CLUSTER)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_CLUSTER_NAME, clusterName);
        }
    }

    public static void checkDbName(String dbName) throws AnalysisException {
        if (Strings.isNullOrEmpty(dbName) || !dbName.matches(DB_REGEX)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_DB_NAME, dbName);
        }
    }

    public static void checkTableName(String tableName) throws AnalysisException {
        if (Strings.isNullOrEmpty(tableName) || !tableName.matches(TABLE_REGEX)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_TABLE_NAME, tableName);
        }
    }

    public static void checkPartitionName(String partitionName) throws AnalysisException {
        if (Strings.isNullOrEmpty(partitionName) || !partitionName.matches(PARTITION_REGEX)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_PARTITION_NAME, partitionName);
        }

        if (partitionName.startsWith(FORBIDDEN_PARTITION_NAME)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_PARTITION_NAME, partitionName);
        }
    }

    public static void checkColumnName(String columnName) throws AnalysisException {
        if (Strings.isNullOrEmpty(columnName) || !columnName.matches(COLUMN_REGEX)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_COLUMN_NAME, columnName);
        }
    }

    public static void checkLabel(String label) throws AnalysisException {
        if (Strings.isNullOrEmpty(label) || !label.matches(LABEL_REGEX)) {
            throw new AnalysisException("Label format error. regex: " + LABEL_REGEX + ", label: " + label);
        }
    }
    
    public static void checkUserName(String userName) throws AnalysisException {
        if (Strings.isNullOrEmpty(userName) || !userName.matches(USER_REGEX)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_CANNOT_USER, "CREATE USER", userName);
        }
    }
}
