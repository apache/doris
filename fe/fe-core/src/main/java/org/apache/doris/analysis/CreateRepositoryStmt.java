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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import java.util.Map;

public class CreateRepositoryStmt extends DdlStmt {
    private boolean isReadOnly;
    private String name;
    private StorageBackend storage;

    public CreateRepositoryStmt(boolean isReadOnly, String name, StorageBackend storage) {
        this.isReadOnly = isReadOnly;
        this.name = name;
        this.storage = storage;
    }

    public boolean isReadOnly() {
        return isReadOnly;
    }

    public String getName() {
        return name;
    }

    public String getBrokerName() {
        return storage.getStorageName();
    }

    public StorageBackend.StorageType getStorageType() {
        return storage.getStorageType();
    }

    public String getLocation() {
        return storage.getLocation();
    }

    public Map<String, String> getProperties() {
        return storage.getProperties();
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        storage.analyze(analyzer);
        // check auth
        if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE ");
        if (isReadOnly) {
            sb.append("READ_ONLY ");
        }
        sb.append("REPOSITORY `").append(name).append("` WITH ").append(storage.toSql());
        return sb.toString();
    }
}
