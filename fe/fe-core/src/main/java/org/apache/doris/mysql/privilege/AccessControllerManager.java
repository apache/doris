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

import org.apache.doris.analysis.TableName;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.AuthorizationInfo;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.Auth.PrivLevel;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * AccessControllerManager is the entry point of privilege authentication.
 * There are 2 kinds of access controller:
 * SystemAccessController: for global level priv, resource priv and other Doris internal priv checking
 * CatalogAccessController: for specified catalog's priv checking, can be customized.
 * And using InternalCatalogAccessController as default.
 */
public class AccessControllerManager {
    private SystemAccessController sysAccessController;
    private CatalogAccessController internalAccessController;
    private Map<String, CatalogAccessController> ctlToCtlAccessController = Maps.newConcurrentMap();

    public AccessControllerManager(Auth auth) {
        sysAccessController = new SystemAccessController(auth);
        internalAccessController = new InternalCatalogAccessController(auth);
        ctlToCtlAccessController.put(InternalCatalog.INTERNAL_CATALOG_NAME, internalAccessController);
    }

    private CatalogAccessController getAccessControllerOrDefault(String ctl) {
        return ctlToCtlAccessController.getOrDefault(ctl, internalAccessController);
    }

    public void addCatalogAccessControl(String ctl, CatalogAccessController controller) {
        ctlToCtlAccessController.put(ctl, controller);
    }

    public Auth getAuth() {
        return sysAccessController.getAuth();
    }

    // ==== Global ====
    public boolean checkGlobalPriv(ConnectContext ctx, PrivPredicate wanted) {
        return checkGlobalPriv(ctx.getCurrentUserIdentity(), wanted);
    }

    public boolean checkGlobalPriv(UserIdentity currentUser, PrivPredicate wanted) {
        return sysAccessController.checkGlobalPriv(currentUser, wanted);
    }

    // ==== Catalog ====
    public boolean checkCtlPriv(ConnectContext ctx, String ctl, PrivPredicate wanted) {
        return checkCtlPriv(ctx.getCurrentUserIdentity(), ctl, wanted);
    }

    public boolean checkCtlPriv(UserIdentity currentUser, String ctl, PrivPredicate wanted) {
        boolean hasGlobal = sysAccessController.checkGlobalPriv(currentUser, wanted);
        boolean hasCtl = getAccessControllerOrDefault(ctl).checkCtlPriv(hasGlobal, currentUser, ctl, wanted);
        return hasGlobal || hasCtl;
    }

    // ==== Database ====
    public boolean checkDbPriv(ConnectContext ctx, String qualifiedDb, PrivPredicate wanted) {
        return checkDbPriv(ctx.getCurrentUserIdentity(), qualifiedDb, wanted);
    }

    public boolean checkDbPriv(UserIdentity currentUser, String db, PrivPredicate wanted) {
        return checkDbPriv(currentUser, Auth.DEFAULT_CATALOG, db, wanted);
    }

    public boolean checkDbPriv(ConnectContext ctx, String ctl, String db, PrivPredicate wanted) {
        return checkDbPriv(ctx.getCurrentUserIdentity(), ctl, db, wanted);
    }

    public boolean checkDbPriv(UserIdentity currentUser, String ctl, String db, PrivPredicate wanted) {
        boolean hasGlobal = sysAccessController.checkGlobalPriv(currentUser, wanted);
        boolean hasDb = getAccessControllerOrDefault(ctl).checkDbPriv(hasGlobal, currentUser, ctl, db, wanted);
        return hasGlobal || hasDb;
    }

    // ==== Table ====
    public boolean checkTblPriv(ConnectContext ctx, String qualifiedCtl,
            String qualifiedDb, String tbl, PrivPredicate wanted) {
        return checkTblPriv(ctx.getCurrentUserIdentity(), qualifiedCtl, qualifiedDb, tbl, wanted);
    }

    public boolean checkTblPriv(ConnectContext ctx, String qualifiedDb, String tbl, PrivPredicate wanted) {
        return checkTblPriv(ctx, Auth.DEFAULT_CATALOG, qualifiedDb, tbl, wanted);
    }

    public boolean checkTblPriv(ConnectContext ctx, TableName tableName, PrivPredicate wanted) {
        Preconditions.checkState(tableName.isFullyQualified());
        return checkTblPriv(ctx, tableName.getCtl(), tableName.getDb(), tableName.getTbl(), wanted);
    }

    public boolean checkTblPriv(UserIdentity currentUser, String db, String tbl, PrivPredicate wanted) {
        return checkTblPriv(currentUser, Auth.DEFAULT_CATALOG, db, tbl, wanted);
    }

    public boolean checkTblPriv(UserIdentity currentUser, String ctl, String db, String tbl, PrivPredicate wanted) {
        boolean hasGlobal = sysAccessController.checkGlobalPriv(currentUser, wanted);
        boolean hasTbl = getAccessControllerOrDefault(ctl).checkTblPriv(hasGlobal, currentUser, ctl, db, tbl, wanted);
        return hasGlobal || hasTbl;
    }

    // ==== Resource ====
    public boolean checkResourcePriv(ConnectContext ctx, String resourceName, PrivPredicate wanted) {
        return checkResourcePriv(ctx.getCurrentUserIdentity(), resourceName, wanted);
    }

    public boolean checkResourcePriv(UserIdentity currentUser, String resourceName, PrivPredicate wanted) {
        return sysAccessController.checkResourcePriv(currentUser, resourceName, wanted);
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
            return checkDbPriv(ctx, authInfo.getDbName(), wanted);
        }
        for (String tblName : authInfo.getTableNameList()) {
            if (!checkTblPriv(ConnectContext.get(), authInfo.getDbName(), tblName, wanted)) {
                return false;
            }
        }
        return true;
    }

    /*
     * Check if current user has certain privilege.
     * This method will check the given privilege levels
     */
    public boolean checkHasPriv(ConnectContext ctx, PrivPredicate priv, PrivLevel... levels) {
        return sysAccessController.checkHasPriv(ctx, priv, levels);
    }
}
