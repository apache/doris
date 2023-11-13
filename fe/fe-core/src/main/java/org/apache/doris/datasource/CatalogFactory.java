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

import org.apache.doris.analysis.AlterCatalogCommentStmt;
import org.apache.doris.analysis.AlterCatalogNameStmt;
import org.apache.doris.analysis.AlterCatalogPropertyStmt;
import org.apache.doris.analysis.CreateCatalogStmt;
import org.apache.doris.analysis.DropCatalogStmt;
import org.apache.doris.analysis.RefreshCatalogStmt;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Resource;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.deltalake.DeltaLakeExternalCatalog;
import org.apache.doris.datasource.iceberg.IcebergExternalCatalogFactory;
import org.apache.doris.datasource.jdbc.JdbcExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonExternalCatalogFactory;
import org.apache.doris.datasource.test.TestExternalCatalog;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * A factory to create catalog instance of log or covert catalog into log.
 */
public class CatalogFactory {
    private static final Logger LOG = LogManager.getLogger(CatalogFactory.class);

    /**
     * Convert the sql statement into catalog log.
     */
    public static CatalogLog createCatalogLog(long catalogId, StatementBase stmt) {
        CatalogLog log = new CatalogLog();
        if (stmt instanceof DropCatalogStmt) {
            log.setCatalogId(catalogId);
        } else if (stmt instanceof AlterCatalogPropertyStmt) {
            log.setCatalogId(catalogId);
            log.setNewProps(((AlterCatalogPropertyStmt) stmt).getNewProperties());
        } else if (stmt instanceof AlterCatalogNameStmt) {
            log.setCatalogId(catalogId);
            log.setNewCatalogName(((AlterCatalogNameStmt) stmt).getNewCatalogName());
        } else if (stmt instanceof RefreshCatalogStmt) {
            log.setCatalogId(catalogId);
            log.setInvalidCache(((RefreshCatalogStmt) stmt).isInvalidCache());
        } else if (stmt instanceof AlterCatalogCommentStmt) {
            log.setCatalogId(catalogId);
            log.setComment(((AlterCatalogCommentStmt) stmt).getComment());
        } else {
            throw new RuntimeException("Unknown stmt for catalog manager " + stmt.getClass().getSimpleName());
        }
        return log;
    }

    /**
     * create the catalog instance from catalog log.
     */
    public static CatalogIf createFromLog(CatalogLog log) throws DdlException {
        return createCatalog(log.getCatalogId(), log.getCatalogName(), log.getResource(),
                log.getComment(), log.getProps(), true);
    }

    /**
     * create the catalog instance from creating statement.
     */
    public static CatalogIf createFromStmt(long catalogId, CreateCatalogStmt stmt)
            throws DdlException {
        return createCatalog(catalogId, stmt.getCatalogName(), stmt.getResource(),
                stmt.getComment(), stmt.getProperties(), false);
    }

    private static CatalogIf createCatalog(long catalogId, String name, String resource, String comment,
            Map<String, String> props, boolean isReplay) throws DdlException {
        // get catalog type from resource or properties
        String catalogType;
        if (!Strings.isNullOrEmpty(resource)) {
            Resource catalogResource = Env.getCurrentEnv().getResourceMgr().getResource(resource);
            if (catalogResource == null) {
                // This is temp bug fix to continue replaying edit log even if resource doesn't exist.
                // In new version, create catalog with resource is not allowed by default.
                LOG.warn("Resource doesn't exist: {} when create catalog {}", resource, name);
                catalogType = "hms";
            } else {
                catalogType = catalogResource.getType().name().toLowerCase();
            }
        } else {
            String type = props.get(CatalogMgr.CATALOG_TYPE_PROP);
            if (Strings.isNullOrEmpty(type)) {
                throw new DdlException("Missing property 'type' in properties");
            }
            catalogType = type.toLowerCase();
        }

        // create catalog
        ExternalCatalog catalog;
        switch (catalogType) {
            case "hms":
                catalog = new HMSExternalCatalog(catalogId, name, resource, props, comment);
                break;
            case "es":
                catalog = new EsExternalCatalog(catalogId, name, resource, props, comment);
                break;
            case "jdbc":
                catalog = new JdbcExternalCatalog(catalogId, name, resource, props, comment);
                break;
            case "iceberg":
                catalog = IcebergExternalCatalogFactory.createCatalog(catalogId, name, resource, props, comment);
                break;
            case "paimon":
                catalog = PaimonExternalCatalogFactory.createCatalog(catalogId, name, resource, props, comment);
                break;
            case "max_compute":
                catalog = new MaxComputeExternalCatalog(catalogId, name, resource, props, comment);
                break;
            case "deltalake":
                catalog = new DeltaLakeExternalCatalog(catalogId, name, resource, props, comment);
                break;
            case "test":
                if (!FeConstants.runningUnitTest) {
                    throw new DdlException("test catalog is only for FE unit test");
                }
                catalog = new TestExternalCatalog(catalogId, name, resource, props, comment);
                break;
            default:
                throw new DdlException("Unknown catalog type: " + catalogType);
        }
        if (!isReplay) {
            // set some default properties when creating catalog.
            // do not call this method when replaying edit log. Because we need to keey the original properties.
            catalog.setDefaultPropsWhenCreating(isReplay);
            // This will check if the customized access controller can be created successfully.
            // If failed, it will throw exception and the catalog will not be created.
            try {
                catalog.initAccessController(true);
            } catch (Exception e) {
                LOG.warn("Failed to init access controller", e);
                throw new DdlException("Failed to init access controller: " + e.getMessage());
            }
        }
        return catalog;
    }
}

