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

package org.apache.doris.datasource;

import org.apache.doris.analysis.AlterCatalogNameStmt;
import org.apache.doris.analysis.AlterCatalogPropertyStmt;
import org.apache.doris.analysis.CreateCatalogStmt;
import org.apache.doris.analysis.DropCatalogStmt;
import org.apache.doris.analysis.RefreshCatalogStmt;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.common.DdlException;
import org.apache.doris.external.elasticsearch.EsUtil;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * A factory to create catalog instance of log or covert catalog into log.
 */
public class CatalogFactory {

    /**
     * Convert the sql statement into catalog log.
     */
    public static CatalogLog constructorCatalogLog(long catalogId, StatementBase stmt) {
        CatalogLog log = new CatalogLog();
        if (stmt instanceof CreateCatalogStmt) {
            log.setCatalogId(catalogId);
            log.setCatalogName(((CreateCatalogStmt) stmt).getCatalogName());
            log.setProps(((CreateCatalogStmt) stmt).getProperties());
        } else if (stmt instanceof DropCatalogStmt) {
            log.setCatalogId(catalogId);
        } else if (stmt instanceof AlterCatalogPropertyStmt) {
            log.setCatalogId(catalogId);
            log.setNewProps(((AlterCatalogPropertyStmt) stmt).getNewProperties());
        } else if (stmt instanceof AlterCatalogNameStmt) {
            log.setCatalogId(catalogId);
            log.setNewCatalogName(((AlterCatalogNameStmt) stmt).getNewCatalogName());
        } else if (stmt instanceof RefreshCatalogStmt) {
            log.setCatalogId(catalogId);
        } else {
            throw new RuntimeException("Unknown stmt for datasource manager " + stmt.getClass().getSimpleName());
        }
        return log;
    }

    /**
     * create the catalog instance from catalog log.
     */
    public static CatalogIf constructorFromLog(CatalogLog log) throws DdlException {
        return constructorCatalog(log.getCatalogId(), log.getCatalogName(), log.getProps());
    }

    private static CatalogIf constructorCatalog(long catalogId, String name, Map<String, String> props)
            throws DdlException {
        String type = props.get("type");
        CatalogIf catalog;
        switch (type) {
            case "hms":
                catalog = new HMSExternalCatalog(catalogId, name, props);
                break;
            case "es":
                validateEsCatalogProperties(props);
                catalog = new EsExternalCatalog(catalogId, name, props);
                break;
            default:
                throw new RuntimeException("Unknown catalog type: " + type);
        }
        return catalog;
    }

    private static void validateEsCatalogProperties(Map<String, String> properties) throws DdlException {
        if (properties == null) {
            throw new DdlException(
                    "Please set properties of elasticsearch table, " + "they are: hosts, user, password, index");
        }

        if (StringUtils.isBlank(properties.get(EsExternalCatalog.PROP_HOSTS))) {
            throw new DdlException("Hosts of ES table is null.");
        }
        String[] nodes = properties.get(EsExternalCatalog.PROP_HOSTS).trim().split(",");
        // check protocol
        for (String seed : nodes) {
            if (!seed.startsWith("http")) {
                throw new DdlException("the protocol must be used");
            }
            if (properties.containsKey(EsExternalCatalog.PROP_SSL)) {
                boolean enableSsl = EsUtil.getBoolean(properties, EsExternalCatalog.PROP_SSL);
                if (enableSsl && seed.startsWith("http://")) {
                    throw new DdlException("if ssl_enabled is true, the https protocol must be used");
                }
                if (!enableSsl && seed.startsWith("https://")) {
                    throw new DdlException("if ssl_enabled is false, the http protocol must be used");
                }
            }
        }
    }
}
