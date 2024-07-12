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

package org.apache.doris.mysql.privilege;

import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.AuthorizationInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.authorizer.ranger.doris.RangerCacheDorisAccessController;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * AccessControllerManager is the entry point of privilege authentication.
 * There are 2 kinds of access controller:
 * SystemAccessController: for global level priv, resource priv and other Doris internal priv checking
 * CatalogAccessController: for specified catalog's priv checking, can be customized.
 * And using InternalCatalogAccessController as default.
 */
public class AccessControllerManager {
    private static final Logger LOG = LogManager.getLogger(AccessControllerManager.class);

    private Auth auth;
    private CatalogAccessController defaultAccessController;
    private Map<String, CatalogAccessController> ctlToCtlAccessController = Maps.newConcurrentMap();

    public AccessControllerManager(Auth auth) {
        this.auth = auth;
        if (Config.access_controller_type.equalsIgnoreCase("ranger-doris")) {
            defaultAccessController = new RangerCacheDorisAccessController("doris");
        } else {
            defaultAccessController = new InternalAccessController(auth);
        }
        ctlToCtlAccessController.put(InternalCatalog.INTERNAL_CATALOG_NAME, defaultAccessController);
    }

    public CatalogAccessController getAccessControllerOrDefault(String ctl) {
        CatalogAccessController catalogAccessController = ctlToCtlAccessController.get(ctl);
        if (catalogAccessController != null) {
            return catalogAccessController;
        }
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(ctl);
        if (catalog != null && catalog instanceof ExternalCatalog) {
            lazyLoadCtlAccessController((ExternalCatalog) catalog);
            return ctlToCtlAccessController.get(ctl);
        }

        return defaultAccessController;
    }

    private synchronized void lazyLoadCtlAccessController(ExternalCatalog catalog) {
        if (ctlToCtlAccessController.containsKey(catalog.getName())) {
            return;
        }
        catalog.initAccessController(false);
        if (!ctlToCtlAccessController.containsKey(catalog.getName())) {
            ctlToCtlAccessController.put(catalog.getName(), defaultAccessController);
        }
    }

    public boolean checkIfAccessControllerExist(String ctl) {
        return ctlToCtlAccessController.containsKey(ctl);
    }

    public void createAccessController(String ctl, String acFactoryClassName, Map<String, String> prop,
            boolean isDryRun) {
        Class<?> factoryClazz = null;
        try {
            factoryClazz = Class.forName(acFactoryClassName);
            AccessControllerFactory factory = (AccessControllerFactory) factoryClazz.newInstance();
            CatalogAccessController accessController = factory.createAccessController(prop);
            if (!isDryRun) {
                ctlToCtlAccessController.put(ctl, accessController);
                LOG.info("create access controller {} for catalog {}", ctl, acFactoryClassName);
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public void removeAccessController(String ctl) {
        ctlToCtlAccessController.remove(ctl);
        LOG.info("remove access controller for catalog {}", ctl);
    }

    public Auth getAuth() {
        return this.auth;
    }

    // ==== Global ====
    public boolean checkGlobalPriv(ConnectContext ctx, PrivPredicate wanted) {
        return checkGlobalPriv(ctx.getCurrentUserIdentity(), wanted);
    }

    public boolean checkGlobalPriv(UserIdentity currentUser, PrivPredicate wanted) {
        return defaultAccessController.checkGlobalPriv(currentUser, wanted);
    }

    // ==== Catalog ====
    public boolean checkCtlPriv(ConnectContext ctx, String ctl, PrivPredicate wanted) {
        return checkCtlPriv(ctx.getCurrentUserIdentity(), ctl, wanted);
    }

    public boolean checkCtlPriv(UserIdentity currentUser, String ctl, PrivPredicate wanted) {
        boolean hasGlobal = checkGlobalPriv(currentUser, wanted);
        // for checking catalog priv, always use InternalAccessController.
        // because catalog priv is only saved in InternalAccessController.
        return defaultAccessController.checkCtlPriv(hasGlobal, currentUser, ctl, wanted);
    }

    // ==== Database ====
    public boolean checkDbPriv(ConnectContext ctx, String ctl, String db, PrivPredicate wanted) {
        return checkDbPriv(ctx.getCurrentUserIdentity(), ctl, db, wanted);
    }

    public boolean checkDbPriv(UserIdentity currentUser, String ctl, String db, PrivPredicate wanted) {
        boolean hasGlobal = checkGlobalPriv(currentUser, wanted);
        return getAccessControllerOrDefault(ctl).checkDbPriv(hasGlobal, currentUser, ctl, db, wanted);
    }

    // ==== Table ====
    public boolean checkTblPriv(ConnectContext ctx, TableName tableName, PrivPredicate wanted) {
        Preconditions.checkState(tableName.isFullyQualified());
        return checkTblPriv(ctx, tableName.getCtl(), tableName.getDb(), tableName.getTbl(), wanted);
    }

    public boolean checkTblPriv(ConnectContext ctx, String qualifiedCtl,
            String qualifiedDb, String tbl, PrivPredicate wanted) {
        if (ctx.isSkipAuth()) {
            return true;
        }
        return checkTblPriv(ctx.getCurrentUserIdentity(), qualifiedCtl, qualifiedDb, tbl, wanted);
    }

    public boolean checkTblPriv(UserIdentity currentUser, String ctl, String db, String tbl, PrivPredicate wanted) {
        boolean hasGlobal = checkGlobalPriv(currentUser, wanted);
        return getAccessControllerOrDefault(ctl).checkTblPriv(hasGlobal, currentUser, ctl, db, tbl, wanted);
    }

    // ==== Column ====
    public void checkColumnsPriv(UserIdentity currentUser, String
            ctl, String qualifiedDb, String tbl, Set<String> cols,
            PrivPredicate wanted) throws UserException {
        boolean hasGlobal = checkGlobalPriv(currentUser, wanted);
        CatalogAccessController accessController = getAccessControllerOrDefault(ctl);
        accessController.checkColsPriv(hasGlobal, currentUser, ctl, qualifiedDb,
                tbl, cols, wanted);

    }

    // ==== Resource ====
    public boolean checkResourcePriv(ConnectContext ctx, String resourceName, PrivPredicate wanted) {
        return checkResourcePriv(ctx.getCurrentUserIdentity(), resourceName, wanted);
    }

    public boolean checkResourcePriv(UserIdentity currentUser, String resourceName, PrivPredicate wanted) {
        return defaultAccessController.checkResourcePriv(currentUser, resourceName, wanted);
    }

    // ==== Cloud ====
    public boolean checkCloudPriv(ConnectContext ctx, String cloudName, PrivPredicate wanted, ResourceTypeEnum type) {
        return checkCloudPriv(ctx.getCurrentUserIdentity(), cloudName, wanted, type);
    }

    public boolean checkCloudPriv(UserIdentity currentUser, String cloudName,
            PrivPredicate wanted, ResourceTypeEnum type) {
        return defaultAccessController.checkCloudPriv(currentUser, cloudName, wanted, type);
    }


    public boolean checkWorkloadGroupPriv(ConnectContext ctx, String workloadGroupName, PrivPredicate wanted) {
        return checkWorkloadGroupPriv(ctx.getCurrentUserIdentity(), workloadGroupName, wanted);
    }

    public boolean checkWorkloadGroupPriv(UserIdentity currentUser, String workloadGroupName, PrivPredicate wanted) {
        return defaultAccessController.checkWorkloadGroupPriv(currentUser, workloadGroupName, wanted);
    }

    // ==== Other ====
    public boolean checkPrivByAuthInfo(ConnectContext ctx, AuthorizationInfo authInfo, PrivPredicate wanted) {
        if (authInfo == null) {
            return false;
        }
        if (authInfo.getDbName() == null) {
            return false;
        }
        if (authInfo.getTableNameList() == null || authInfo.getTableNameList().isEmpty()) {
            return checkDbPriv(ctx, InternalCatalog.INTERNAL_CATALOG_NAME, authInfo.getDbName(), wanted);
        }
        for (String tblName : authInfo.getTableNameList()) {
            if (!checkTblPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, authInfo.getDbName(),
                    tblName, wanted)) {
                return false;
            }
        }
        return true;
    }

    public Map<String, Optional<DataMaskPolicy>> evalDataMaskPolicies(UserIdentity currentUser, String
            ctl, String db, String tbl, Set<String> cols) {
        Map<String, Optional<DataMaskPolicy>> res = Maps.newHashMap();
        for (String col : cols) {
            res.put(col, evalDataMaskPolicy(currentUser, ctl, db, tbl, col));
        }
        return res;
    }

    public Optional<DataMaskPolicy> evalDataMaskPolicy(UserIdentity currentUser, String
            ctl, String db, String tbl, String col) {
        Objects.requireNonNull(currentUser, "require currentUser object");
        Objects.requireNonNull(ctl, "require ctl object");
        Objects.requireNonNull(db, "require db object");
        Objects.requireNonNull(tbl, "require tbl object");
        Objects.requireNonNull(col, "require col object");
        return getAccessControllerOrDefault(ctl).evalDataMaskPolicy(currentUser, ctl, db, tbl, col.toLowerCase());
    }

    public List<? extends RowFilterPolicy> evalRowFilterPolicies(UserIdentity currentUser, String
            ctl, String db, String tbl) {
        Objects.requireNonNull(currentUser, "require currentUser object");
        Objects.requireNonNull(ctl, "require ctl object");
        Objects.requireNonNull(db, "require db object");
        Objects.requireNonNull(tbl, "require tbl object");
        return getAccessControllerOrDefault(ctl).evalRowFilterPolicies(currentUser, ctl, db, tbl);
    }
}
