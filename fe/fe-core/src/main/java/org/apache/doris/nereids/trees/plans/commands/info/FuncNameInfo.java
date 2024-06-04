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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * procedure, function, package name info
 */
public class FuncNameInfo {
    private final List<String> nameParts;
    private String ctl = "";
    private long ctlId = -1;
    private String db = "";
    private long dbId = -1;
    private final String name;
    private boolean isAnalyzed = false;

    /**
     * FuncNameInfo
     *
     * @param parts like [ctl1,db1,name1] or [db1,name1] or [name1]
     */
    public FuncNameInfo(List<String> parts) {
        nameParts = parts;
        Objects.requireNonNull(parts, "require parts object");
        int size = parts.size();
        Preconditions.checkArgument(size > 0, "procedure/function/package name can't be empty");
        name = parts.get(size - 1).toUpperCase();
        if (size >= 2) {
            db = parts.get(size - 2);
        }
        if (size >= 3) {
            ctl = parts.get(size - 3);
        }
    }

    /**
     * FuncNameInfo
     *
     * @param ctl catalogName
     * @param db dbName
     * @param name funcName
     */
    public FuncNameInfo(String ctl, String db, String name) {
        Objects.requireNonNull(ctl, "require tbl object");
        Objects.requireNonNull(db, "require db object");
        Objects.requireNonNull(name, "require name object");
        this.ctl = ctl;
        this.db = db;
        this.name = name.toUpperCase();
        this.nameParts = Lists.newArrayList(ctl, db, name);
    }

    /**
     * FuncNameInfo
     *
     * @param name funcName
     */
    public FuncNameInfo(String name) {
        Objects.requireNonNull(name, "require name object");
        this.name = name.toUpperCase();
        this.nameParts = Lists.newArrayList(name);
    }

    /**
     * analyze procedureNameInfo
     *
     * @param ctx ctx
     */
    public void analyze(ConnectContext ctx) {
        if (isAnalyzed) {
            return;
        }
        try {
            if (Strings.isNullOrEmpty(ctl)) {
                ctl = ctx.getDefaultCatalog();
                if (Strings.isNullOrEmpty(ctl)) {
                    ctl = InternalCatalog.INTERNAL_CATALOG_NAME;
                }
            }
            ctlId = ctx.getCatalog(ctl).getId();
            if (Strings.isNullOrEmpty(db)) {
                db = ctx.getDatabase();
                if (Strings.isNullOrEmpty(db)) {
                    db = FeConstants.INTERNAL_DB_NAME;
                }
            }
            Optional<DatabaseIf> dbInstance = ctx.getCatalog(ctl).getDb(db);
            dbId = dbInstance.map(DatabaseIf::getId).orElse(-1L);
            if (Strings.isNullOrEmpty(name)) {
                throw new AnalysisException("procedure/function/package name is null");
            }
        } catch (Exception e) {
            throw new AnalysisException("failed to analyze procedure name", e);
        }
        isAnalyzed = true;
    }

    /**
     * get catalog name
     *
     * @return ctlName
     */
    public String getCtl() {
        analyze(ConnectContext.get());
        return ctl == null ? "" : ctl;
    }

    /**
     * get catalog id
     *
     * @return ctlId
     */
    public long getCtlId() {
        analyze(ConnectContext.get());
        return ctlId;
    }

    /**
     * get db name
     *
     * @return dbName
     */
    public String getDbName() {
        analyze(ConnectContext.get());
        return db == null ? "" : db;
    }

    /**
     * get db id
     *
     * @return dbId
     */
    public long getDbId() {
        analyze(ConnectContext.get());
        return dbId;
    }

    /**
     * get table name
     *
     * @return tableName
     */
    public String getName() {
        analyze(ConnectContext.get());
        return name == null ? "" : name;
    }

    public String toString() {
        return nameParts.stream().map(Utils::quoteIfNeeded).reduce((left, right) -> left + "." + right).orElse("");
    }
}
