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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.Objects;

/**
 * Command to create a catalog in the Nereids planner.
 */
public class CreateCatalogCommand extends Command implements ForwardWithSync, NeedAuditEncryption {
    private final String catalogName;
    private final boolean ifNotExists;
    private final String resourceName;
    private final String comment;
    private final Map<String, String> properties;

    /**
    * Constructor
    */
    public CreateCatalogCommand(String catalogName, boolean ifNotExists, String resourceName, String comment,
                                Map<String, String> properties) {
        super(PlanType.CREATE_CATALOG_COMMAND);
        this.catalogName = Objects.requireNonNull(catalogName, "Catalog name cannot be null");
        this.ifNotExists = ifNotExists;
        this.resourceName = resourceName;
        this.comment = comment;
        this.properties = properties == null ? Maps.newHashMap() : properties;
    }

    private void validate(ConnectContext ctx) throws AnalysisException {
        Util.checkCatalogAllRules(catalogName);
        if (catalogName.equals(InternalCatalog.INTERNAL_CATALOG_NAME)) {
            throw new AnalysisException("Internal catalog name can't be create.");
        }

        if (!Env.getCurrentEnv().getAccessManager().checkCtlPriv(
                ConnectContext.get(), catalogName, PrivPredicate.CREATE)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_CATALOG_ACCESS_DENIED,
                    ctx.getQualifiedUser(), catalogName);
        }

        if (Config.disallow_create_catalog_with_resource && !Strings.isNullOrEmpty(resourceName)) {
            throw new AnalysisException("Create catalog with resource is deprecated and is not allowed."
                    + " You can set `disallow_create_catalog_with_resource=false` in fe.conf"
                    + " to enable it temporarily.");
        }

        String currentDateTime = LocalDateTime.now(ZoneId.systemDefault()).toString().replace("T", " ");
        properties.put(ExternalCatalog.CREATE_TIME, currentDateTime);
        PropertyAnalyzer.checkCatalogProperties(properties, false);
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        Env.getCurrentEnv().getCatalogMgr().createCatalog(this);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCreateCatalogCommand(this, context);
    }

    public String getCatalogName() {
        return catalogName;
    }

    public boolean isSetIfNotExists() {
        return ifNotExists;
    }

    public String getResource() {
        return resourceName;
    }

    public String getComment() {
        return comment;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public boolean needAuditEncryption() {
        return true;
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("CREATE CATALOG ").append("`").append(catalogName).append("`");
        if (!Strings.isNullOrEmpty(resourceName)) {
            stringBuilder.append(" WITH RESOURCE `").append(resourceName).append("`");
        }
        if (!Strings.isNullOrEmpty(comment)) {
            stringBuilder.append("\nCOMMENT \"").append(comment).append("\"");
        }
        if (properties.size() > 0) {
            stringBuilder.append("\nPROPERTIES (\n");
            stringBuilder.append(new PrintableMap<>(properties, "=", true, true, true));
            stringBuilder.append("\n)");
        }
        return stringBuilder.toString();
    }
}

