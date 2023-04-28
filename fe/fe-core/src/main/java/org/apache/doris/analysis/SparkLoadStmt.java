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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/StatementBase.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SparkLoadStmt extends InsertStmt {

    private final DataDescription dataDescription;

    private final ResourceDesc resourceDesc;

    public SparkLoadStmt(LabelName label, List<DataDescription> dataDescList, ResourceDesc resourceDesc,
            Map<String, String> properties, String comments) {
        this.label = label;
        Preconditions.checkState(dataDescList.size() == 1,
                "spark load could only have one desc");
        this.dataDescription = dataDescList.get(0);
        this.resourceDesc = resourceDesc;
        this.properties = properties;
        this.comments = comments;
    }

    @Override
    public List<? extends DataDesc> getDataDescList() {
        return Collections.singletonList(dataDescription);
    }

    @Override
    public ResourceDesc getResourceDesc() {
        return resourceDesc;
    }

    @Override
    public LoadType getLoadType() {
        return LoadType.SPARK_LOAD;
    }

    @Override
    public void analyzeProperties() throws DdlException {

    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        label.analyze(analyzer);
        Preconditions.checkNotNull(dataDescription, new AnalysisException("No data file in load statement."));
        Preconditions.checkNotNull(resourceDesc, new AnalysisException("Resource desc not found"));
        String fullDbName = dataDescription.analyzeFullDbName(label.getDbName(), analyzer);
        dataDescription.analyze(fullDbName);
        resourceDesc.analyze();
        Database db = analyzer.getEnv().getInternalCatalog().getDbOrAnalysisException(fullDbName);
        OlapTable table = db.getOlapTableOrAnalysisException(dataDescription.getTableName());
        dataDescription.checkKeyTypeForLoad(table);
        // check resource usage privilege
        if (!Env.getCurrentEnv().getAccessManager().checkResourcePriv(ConnectContext.get(),
                resourceDesc.getName(),
                PrivPredicate.USAGE)) {
            throw new AnalysisException("USAGE denied to user '" + ConnectContext.get().getQualifiedUser()
                    + "'@'" + ConnectContext.get().getRemoteIP()
                    + "' for resource '" + resourceDesc.getName() + "'");
        }
    }

    @Override
    public String toSql() {
        return super.toSql();
    }
}
