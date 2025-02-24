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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;

/**
 * Statement for create a new catalog.
 */
public class CreateCatalogStmt extends DdlStmt implements NotFallbackInParser {
    private final boolean ifNotExists;
    private final String catalogName;
    private final String resource;
    private final String comment;
    private final Map<String, String> properties;

    /**
     * Statement for create a new catalog.
     */
    public CreateCatalogStmt(boolean ifNotExists, String catalogName, String resource, Map<String, String> properties,
            String comment) {
        this.ifNotExists = ifNotExists;
        this.catalogName = catalogName;
        this.resource = Strings.nullToEmpty(resource);
        this.comment = Strings.nullToEmpty(comment);
        this.properties = properties == null ? Maps.newHashMap() : properties;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getResource() {
        return resource;
    }

    public String getComment() {
        return comment;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public boolean isSetIfNotExists() {
        return ifNotExists;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        Util.checkCatalogAllRules(catalogName);
        if (catalogName.equals(InternalCatalog.INTERNAL_CATALOG_NAME)) {
            throw new AnalysisException("Internal catalog name can't be create.");
        }

        if (!Env.getCurrentEnv().getAccessManager().checkCtlPriv(
                ConnectContext.get(), catalogName, PrivPredicate.CREATE)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_CATALOG_ACCESS_DENIED,
                    analyzer.getQualifiedUser(), catalogName);
        }

        if (Config.disallow_create_catalog_with_resource && !Strings.isNullOrEmpty(resource)) {
            throw new AnalysisException("Create catalog with resource is deprecated and is not allowed."
                    + " You can set `disallow_create_catalog_with_resource=false` in fe.conf"
                    + " to enable it temporarily.");
        }

        String currentDateTime = LocalDateTime.now(ZoneId.systemDefault()).toString().replace("T", " ");
        properties.put(ExternalCatalog.CREATE_TIME, currentDateTime);
        PropertyAnalyzer.checkCatalogProperties(properties, false);
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("CREATE CATALOG ").append("`").append(catalogName).append("`");
        if (!Strings.isNullOrEmpty(resource)) {
            stringBuilder.append(" WITH RESOURCE `").append(resource).append("`");
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

    @Override
    public boolean needAuditEncryption() {
        return true;
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CREATE;
    }
}
