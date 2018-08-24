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

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.common.util.PrintableMap;
import com.baidu.palo.mysql.privilege.PrivPredicate;
import com.baidu.palo.qe.ConnectContext;

import java.util.Map;

// clause which is used to modify table properties
public class ModifyTablePropertiesClause extends AlterClause {

    private static final String KEY_STORAGE_TYPE = "storage_type";

    private Map<String, String> properties;

    public ModifyTablePropertiesClause(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (properties == null || properties.isEmpty()) {
            throw new AnalysisException("Properties is not set");
        }

        if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ALTER)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                                                "ALTER");
        }

        if (properties.containsKey(KEY_STORAGE_TYPE)) {
            // if set storage type, we need ADMIN privs.
            if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                                                    "ADMIN");
            }

            if (!properties.get(KEY_STORAGE_TYPE).equals("column")) {
                throw new AnalysisException("Can only change storage type to COLUMN");
            }
        }
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("PROPERTIES (");
        sb.append(new PrintableMap<String, String>(properties, "=", true, false));
        sb.append(")");
        
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
