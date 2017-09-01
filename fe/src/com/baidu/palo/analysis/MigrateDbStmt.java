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

import com.baidu.palo.cluster.ClusterNamespace;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.common.InternalException;

public class MigrateDbStmt extends DdlStmt {

    private ClusterName src;
    private ClusterName dest;
    private String srcCluster;
    private String destCluster;
    private String srcDb;
    private String destDb;

    MigrateDbStmt(ClusterName src, ClusterName dest) {
        this.src = src;
        this.dest = dest;
    }

    public String getSrcCluster() {
        return srcCluster;
    }

    public String getDestCluster() {
        return destCluster;
    }

    public String getSrcDb() {
        return srcDb;
    }

    public String getDestDb() {
        return destDb;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, InternalException {

        src.analyze(analyzer);
        dest.analyze(analyzer);

        if (!analyzer.getCatalog().getUserMgr().isAdmin(analyzer.getUser())) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_CLUSTER_NO_PERMISSIONS);
        }

        srcCluster = src.getCluster();
        srcDb = ClusterNamespace.getFullName(srcCluster, src.getDb());
        destCluster = dest.getCluster();
        destDb = ClusterNamespace.getFullName(destCluster, dest.getDb());
    }

    @Override
    public String toSql() {
        return "MIGRATE DATABASE " + srcCluster + "." + srcDb + " " + destCluster + "." + destDb;
    }

    @Override
    public String toString() {
        return toSql();
    }
}
