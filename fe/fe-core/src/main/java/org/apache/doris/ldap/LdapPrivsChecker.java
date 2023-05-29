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

package org.apache.doris.ldap;

import org.apache.doris.analysis.ResourcePattern;
import org.apache.doris.analysis.TablePattern;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.LdapConfig;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.Auth.PrivLevel;
import org.apache.doris.mysql.privilege.PrivBitSet;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.Privilege;
import org.apache.doris.mysql.privilege.Role;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * This class is used for checking current user LDAP group privileges.
 */
public class LdapPrivsChecker {
    private static final Logger LOG = LogManager.getLogger(LdapPrivsChecker.class);

    public static boolean hasGlobalPrivFromLdap(UserIdentity currentUser, PrivPredicate wanted) {
        return hasLdapPrivs(currentUser) && getUserLdapPrivs(currentUser.getQualifiedUser()).checkGlobalPriv(wanted);
    }

    public static boolean hasCatalogPrivFromLdap(UserIdentity currentUser, String ctl, PrivPredicate wanted) {
        return hasLdapPrivs(currentUser) && getUserLdapPrivs(currentUser.getQualifiedUser()).checkCtlPriv(ctl, wanted);
    }

    public static boolean hasDbPrivFromLdap(UserIdentity currentUser, String ctl, String db, PrivPredicate wanted) {
        return hasLdapPrivs(currentUser) && getUserLdapPrivs(currentUser.getQualifiedUser()).checkDbPriv(ctl, db,
                wanted);
    }

    public static boolean hasTblPrivFromLdap(UserIdentity currentUser, String ctl, String db, String tbl,
            PrivPredicate wanted) {
        return hasLdapPrivs(currentUser) && getUserLdapPrivs(currentUser.getQualifiedUser()).checkTblPriv(ctl, db, tbl,
                wanted);
    }

    public static boolean checkHasPriv(UserIdentity currentUser, PrivPredicate priv, PrivLevel[] levels) {
        return hasLdapPrivs(currentUser) && getUserLdapPrivs(currentUser.getQualifiedUser()).checkHasPriv(priv, levels);
    }

    public static boolean hasResourcePrivFromLdap(UserIdentity currentUser, String resourceName, PrivPredicate wanted) {
        return hasLdapPrivs(currentUser) && getUserLdapPrivs(currentUser.getQualifiedUser()).checkResourcePriv(
                resourceName, wanted);
    }

    public static PrivBitSet getResourcePrivFromLdap(UserIdentity currentUser, String resourceName) {
        PrivBitSet savedPrivs = PrivBitSet.of();
        if (hasLdapPrivs(currentUser)) {
            getUserLdapPrivs(currentUser.getQualifiedUser()).getResourcePrivTable().getPrivs(resourceName, savedPrivs);
        }
        return savedPrivs;
    }

    public static boolean hasLdapPrivs(UserIdentity userIdent) {
        return LdapConfig.ldap_authentication_enabled && Env.getCurrentEnv().getAuth().getLdapManager()
                .doesUserExist(userIdent.getQualifiedUser());
    }

    public static Map<TablePattern, PrivBitSet> getLdapAllDbPrivs(UserIdentity userIdentity) {
        Map<TablePattern, PrivBitSet> ldapDbPrivs = Maps.newConcurrentMap();
        if (!hasLdapPrivs(userIdentity)) {
            return ldapDbPrivs;
        }
        for (Map.Entry<TablePattern, PrivBitSet> entry : getUserLdapPrivs(userIdentity.getQualifiedUser())
                .getTblPatternToPrivs().entrySet()) {
            if (entry.getKey().getPrivLevel().equals(Auth.PrivLevel.DATABASE)) {
                ldapDbPrivs.put(entry.getKey(), entry.getValue());
            }
        }
        return ldapDbPrivs;
    }

    public static Map<TablePattern, PrivBitSet> getLdapAllTblPrivs(UserIdentity userIdentity) {
        Map<TablePattern, PrivBitSet> ldapTblPrivs = Maps.newConcurrentMap();
        if (!hasLdapPrivs(userIdentity)) {
            return ldapTblPrivs;
        }
        for (Map.Entry<TablePattern, PrivBitSet> entry : getUserLdapPrivs(userIdentity.getQualifiedUser())
                .getTblPatternToPrivs().entrySet()) {
            if (entry.getKey().getPrivLevel().equals(Auth.PrivLevel.TABLE)) {
                ldapTblPrivs.put(entry.getKey(), entry.getValue());
            }
        }
        return ldapTblPrivs;
    }

    public static Map<ResourcePattern, PrivBitSet> getLdapAllResourcePrivs(UserIdentity userIdentity) {
        Map<ResourcePattern, PrivBitSet> ldapResourcePrivs = Maps.newConcurrentMap();
        if (!hasLdapPrivs(userIdentity)) {
            return ldapResourcePrivs;
        }
        for (Map.Entry<ResourcePattern, PrivBitSet> entry : getUserLdapPrivs(userIdentity.getQualifiedUser())
                .getResourcePatternToPrivs().entrySet()) {
            if (entry.getKey().getPrivLevel().equals(Auth.PrivLevel.RESOURCE)) {
                ldapResourcePrivs.put(entry.getKey(), entry.getValue());
            }
        }
        return ldapResourcePrivs;
    }

    private static Role getUserLdapPrivs(String fullName) {
        return Env.getCurrentEnv().getAuth().getLdapManager().getUserInfo(fullName).getPaloRole();
    }

    // Temporary user has information_schema 'Select_priv' priv by default.
    public static void grantDefaultPrivToTempUser(Role role, String clusterName) throws DdlException {
        TablePattern tblPattern = new TablePattern(InfoSchemaDb.DATABASE_NAME, "*");
        try {
            tblPattern.analyze(clusterName);
        } catch (AnalysisException e) {
            LOG.warn("should not happen.", e);
        }
        Role newRole = new Role(role.getRoleName(), tblPattern, PrivBitSet.of(Privilege.SELECT_PRIV));
        role.merge(newRole);
    }
}
