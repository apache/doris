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

package com.baidu.palo.analysis;

import com.baidu.palo.catalog.AccessPrivilege;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.common.InternalException;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

public class AbstractBackupStmt extends DdlStmt {
    private static final String SERVER_TYPE = "server_type";
    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static final String OPT_PROPERTIES = "opt_properties";

    protected LabelName labelName;
    protected List<PartitionName> objNames;
    protected String remotePath;
    protected Map<String, String> properties;

    public AbstractBackupStmt(LabelName labelName, List<PartitionName> objNames,
                              String remotePath, Map<String, String> properties) {
        this.labelName = labelName;
        this.objNames = objNames;
        if (this.objNames == null) {
            this.objNames = Lists.newArrayList();
        }

        this.remotePath = remotePath;
        this.properties = properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, InternalException {
        labelName.analyze(analyzer);

        // check authenticate
        if (!analyzer.getCatalog().getUserMgr()
                .checkAccess(analyzer.getUser(), labelName.getDbName(), AccessPrivilege.READ_WRITE)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DB_ACCESS_DENIED, analyzer.getUser(),
                                                labelName.getDbName());
        }

        for (PartitionName partitionName : objNames) {
            partitionName.analyze(analyzer);
        }

        if (Strings.isNullOrEmpty(remotePath)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_MISSING_PARAM, "restore path is not specified");
        }

        analyzeProperties();

        // check if partition names has intersection
        PartitionName.checkIntersect(objNames);
    }

    private void analyzeProperties() throws AnalysisException {
        if (properties == null || properties.isEmpty()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_MISSING_PARAM, "romote source info is not specified");
        }

        Map<String, String> tmpProp = Maps.newHashMap();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            tmpProp.put(entry.getKey().toLowerCase(), entry.getValue());
        }
        properties = tmpProp;

        if (!properties.containsKey(SERVER_TYPE)
                || !properties.containsKey(HOST)
                || !properties.containsKey(PORT)
                || !properties.containsKey(USER)
                || !properties.containsKey(PASSWORD)) {
            throw new AnalysisException("Properties should contains required params.");
        }

        if (!properties.containsKey(OPT_PROPERTIES)) {
            properties.put(OPT_PROPERTIES, "");
        }
    }

    public String getDbName() {
        return labelName.getDbName();
    }

    public String getLabel() {
        return labelName.getLabelName();
    }

    public LabelName getLabelName() {
        return labelName;
    }

    public List<PartitionName> getObjNames() {
        return objNames;
    }

    public String getRemotePath() {
        return remotePath;
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}
