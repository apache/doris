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
import org.apache.doris.catalog.Resource.ResourceType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import java.util.Map;

// CREATE [EXTERNAL] RESOURCE resource_name
// PROPERTIES (key1 = value1, ...)
public class CreateResourceStmt extends DdlStmt {
    private static final String TYPE = "type";

    private final boolean isExternal;
    private final String resourceName;
    private final Map<String, String> properties;
    private ResourceType resourceType;

    public CreateResourceStmt(boolean isExternal, String resourceName, Map<String, String> properties) {
        this.isExternal = isExternal;
        this.resourceName = resourceName;
        this.properties = properties;
        this.resourceType = ResourceType.UNKNOWN;
    }

    public String getResourceName() {
        return resourceName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public ResourceType getResourceType() {
        return resourceType;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        // check auth
        if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        // check name
        FeNameFormat.checkResourceName(resourceName);

        // check type in properties
        if (properties == null || properties.isEmpty()) {
            throw new AnalysisException("Resource properties can't be null");
        }
        String type = properties.get(TYPE);
        if (type == null) {
            throw new AnalysisException("Resource type can't be null");
        }
        resourceType = ResourceType.fromString(type);
        if (resourceType == ResourceType.UNKNOWN) {
            throw new AnalysisException("Unsupported resource type: " + type);
        }
        if (resourceType == ResourceType.SPARK && !isExternal) {
            throw new AnalysisException("Spark is external resource");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE ");
        if (isExternal) {
            sb.append("EXTERNAL ");
        }
        sb.append("RESOURCE '").append(resourceName).append("' ");
        sb.append("PROPERTIES(").append(new PrintableMap<>(properties, " = ", true, false)).append(")");
        return sb.toString();
    }
}

