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

import com.google.common.collect.Maps;
import org.apache.doris.analysis.ResourcePattern;
import org.apache.doris.analysis.TablePattern;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.LdapConfig;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.mysql.privilege.PaloPrivilege;
import org.apache.doris.mysql.privilege.PaloRole;
import org.apache.doris.mysql.privilege.PrivBitSet;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * If the user logs in with LDAP authentication, the user LDAP group privileges will be saved in 'ldapGroupsPrivs' of ConnectContext.
 * When checking user privileges, Doris need to check both the privileges granted by Doris and LDAP group privileges.
 * This class is used for checking current user LDAP group privileges.
 */
public class LdapPrivsChecker {
    private static final Logger LOG = LogManager.getLogger(LdapPrivsChecker.class);

    public static boolean hasGlobalPrivFromLdap(UserIdentity currentUser, PrivPredicate wanted) {
        return hasTblPatternPrivs(currentUser, wanted, null, null, PaloAuth.PrivLevel.GLOBAL)
                || hasResourcePatternPrivs(currentUser, wanted, null, PaloAuth.PrivLevel.GLOBAL);
    }

    public static boolean hasDbPrivFromLdap(UserIdentity currentUser, String db, PrivPredicate wanted) {
        return hasTblPatternPrivs(currentUser, wanted, db, null, PaloAuth.PrivLevel.DATABASE);
    }

    // Any database has wanted priv return true.
    public static boolean hasDbPrivFromLdap(UserIdentity currentUser, PrivPredicate wanted) {
        return hasPrivs(currentUser, wanted, PaloAuth.PrivLevel.DATABASE);
    }

    public static boolean hasTblPrivFromLdap(UserIdentity currentUser, String db, String tbl, PrivPredicate wanted) {
        return hasTblPatternPrivs(currentUser, wanted, db, tbl, PaloAuth.PrivLevel.TABLE);
    }

    // Any table has wanted priv return true.
    public static boolean hasTblPrivFromLdap(UserIdentity currentUser, PrivPredicate wanted) {
        return hasPrivs(currentUser, wanted, PaloAuth.PrivLevel.TABLE);
    }

    public static boolean hasResourcePrivFromLdap(UserIdentity currentUser, String resourceName, PrivPredicate wanted) {
        return hasResourcePatternPrivs(currentUser, wanted, resourceName, PaloAuth.PrivLevel.RESOURCE);
    }

    private static boolean hasTblPatternPrivs(UserIdentity currentUser, PrivPredicate wanted, String db, String tbl,
                                              PaloAuth.PrivLevel level) {
        PrivBitSet savedPrivs = PrivBitSet.of();
        getCurrentUserTblPrivs(currentUser, db, tbl, savedPrivs, level);
        return PaloPrivilege.satisfy(savedPrivs, wanted);
    }

    private static boolean hasResourcePatternPrivs(UserIdentity currentUser, PrivPredicate wanted, String resourceName,
                                                   PaloAuth.PrivLevel level) {
        PrivBitSet savedPrivs = PrivBitSet.of();
        getCurrentUserResourcePrivs(currentUser, resourceName, savedPrivs, level);
        return PaloPrivilege.satisfy(savedPrivs, wanted);
    }

    public static PrivBitSet getGlobalPrivFromLdap(UserIdentity currentUser) {
        PrivBitSet savedPrivs = PrivBitSet.of();
        getCurrentUserTblPrivs(currentUser, null, null, savedPrivs, PaloAuth.PrivLevel.GLOBAL);
        getCurrentUserResourcePrivs(currentUser, null, savedPrivs, PaloAuth.PrivLevel.GLOBAL);
        return savedPrivs;
    }

    public static PrivBitSet getDbPrivFromLdap(UserIdentity currentUser, String db) {
        PrivBitSet savedPrivs = PrivBitSet.of();
        getCurrentUserTblPrivs(currentUser, db, null, savedPrivs, PaloAuth.PrivLevel.DATABASE);
        return savedPrivs;
    }

    public static PrivBitSet getTblPrivFromLdap(UserIdentity currentUser, String db, String tbl) {
        PrivBitSet savedPrivs = PrivBitSet.of();
        getCurrentUserTblPrivs(currentUser, db, tbl, savedPrivs, PaloAuth.PrivLevel.TABLE);
        return savedPrivs;
    }

    public static PrivBitSet getResourcePrivFromLdap(UserIdentity currentUser, String resourceName) {
        PrivBitSet savedPrivs = PrivBitSet.of();
        getCurrentUserResourcePrivs(currentUser, resourceName, savedPrivs, PaloAuth.PrivLevel.RESOURCE);
        return savedPrivs;
    }

    private static void getCurrentUserTblPrivs(UserIdentity currentUser, String db, String tbl, PrivBitSet savedPrivs,
                                               PaloAuth.PrivLevel level) {
        if (!hasLdapPrivs(currentUser)) {
            return;
        }
        PaloRole currentUserLdapPrivs = ConnectContext.get().getLdapGroupsPrivs();
        for (Map.Entry<TablePattern, PrivBitSet> entry : currentUserLdapPrivs.getTblPatternToPrivs().entrySet()) {
            switch (entry.getKey().getPrivLevel()) {
                case GLOBAL:
                    if (level.equals(PaloAuth.PrivLevel.GLOBAL)) {
                        savedPrivs.or(entry.getValue());
                        return;
                    }
                    break;
                case DATABASE:
                    if (level.equals(PaloAuth.PrivLevel.DATABASE) && db != null
                            && entry.getKey().getQualifiedDb().equals(db)) {
                        savedPrivs.or(entry.getValue());
                        return;
                    }
                    break;
                case TABLE:
                    if (level.equals(PaloAuth.PrivLevel.TABLE) && db != null && tbl != null
                            && entry.getKey().getQualifiedDb().equals(db) && entry.getKey().getTbl().equals(tbl)) {
                        savedPrivs.or(entry.getValue());
                        return;
                    }
                    break;
                default:
                    Preconditions.checkNotNull(null, entry.getKey().getPrivLevel());
            }
        }
    }

    private static void getCurrentUserResourcePrivs(UserIdentity currentUser, String resourceName, PrivBitSet savedPrivs,
                                                    PaloAuth.PrivLevel level) {
        if (!hasLdapPrivs(currentUser)) {
            return;
        }
        PaloRole currentUserLdapPrivs = ConnectContext.get().getLdapGroupsPrivs();
        for (Map.Entry<ResourcePattern, PrivBitSet> entry : currentUserLdapPrivs.getResourcePatternToPrivs().entrySet()) {
            switch (entry.getKey().getPrivLevel()) {
                case GLOBAL:
                    if (level.equals(PaloAuth.PrivLevel.GLOBAL)) {
                        savedPrivs.or(entry.getValue());
                        return;
                    }
                    break;
                case RESOURCE:
                    if (level.equals(PaloAuth.PrivLevel.RESOURCE) && resourceName != null
                            && entry.getKey().getResourceName().equals(resourceName)) {
                        savedPrivs.or(entry.getValue());
                        return;
                    }
                    break;
                default:
                    Preconditions.checkNotNull(null, entry.getKey().getPrivLevel());
            }
        }
    }

    private static boolean hasPrivs(UserIdentity currentUser, PrivPredicate wanted, PaloAuth.PrivLevel level) {
        if (!hasLdapPrivs(currentUser)) {
            return false;
        }
        PaloRole currentUserLdapPrivs = ConnectContext.get().getLdapGroupsPrivs();
        for (Map.Entry<TablePattern, PrivBitSet> entry : currentUserLdapPrivs.getTblPatternToPrivs().entrySet()) {
            if (entry.getKey().getPrivLevel().equals(level) && PaloPrivilege.satisfy(entry.getValue(), wanted)) {
                return true;
            }
        }
        return false;
    }

    // Check if user has any privs of tables in this database.
    public static boolean hasPrivsOfDb(UserIdentity currentUser, String db) {
        if (!hasLdapPrivs(currentUser)) {
            return false;
        }
        PaloRole currentUserLdapPrivs = ConnectContext.get().getLdapGroupsPrivs();
        for (Map.Entry<TablePattern, PrivBitSet> entry : currentUserLdapPrivs.getTblPatternToPrivs().entrySet()) {
            if (entry.getKey().getPrivLevel().equals(PaloAuth.PrivLevel.TABLE) && entry.getKey().getQualifiedDb().equals(db)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isCurrentUser(UserIdentity userIdent) {
        ConnectContext context = ConnectContext.get();
        if (context == null) {
            return false;
        }
        UserIdentity currentUser = context.getCurrentUserIdentity();
        return currentUser.getQualifiedUser().equals(userIdent.getQualifiedUser())
                && currentUser.getHost().equals(userIdent.getHost());
    }

    public static boolean hasLdapPrivs(UserIdentity userIdent) {
        return LdapConfig.ldap_authentication_enabled && isCurrentUser(userIdent)
                && ConnectContext.get().getLdapGroupsPrivs() != null;
    }

    public static Map<TablePattern, PrivBitSet> getLdapAllDbPrivs(UserIdentity userIdentity) {
        Map<TablePattern, PrivBitSet> ldapDbPrivs = Maps.newConcurrentMap();
        if (!hasLdapPrivs(userIdentity)) return ldapDbPrivs;
        for (Map.Entry<TablePattern, PrivBitSet> entry : ConnectContext.get().getLdapGroupsPrivs()
                .getTblPatternToPrivs().entrySet()) {
            if (entry.getKey().getPrivLevel().equals(PaloAuth.PrivLevel.DATABASE)) {
                ldapDbPrivs.put(entry.getKey(), entry.getValue());
            }
        }
        return ldapDbPrivs;
    }

    public static Map<TablePattern, PrivBitSet> getLdapAllTblPrivs(UserIdentity userIdentity) {
        Map<TablePattern, PrivBitSet> ldapTblPrivs = Maps.newConcurrentMap();
        if (!hasLdapPrivs(userIdentity)) return ldapTblPrivs;
        for (Map.Entry<TablePattern, PrivBitSet> entry : ConnectContext.get().getLdapGroupsPrivs()
                .getTblPatternToPrivs().entrySet()) {
            if (entry.getKey().getPrivLevel().equals(PaloAuth.PrivLevel.TABLE)) {
                ldapTblPrivs.put(entry.getKey(), entry.getValue());
            }
        }
        return ldapTblPrivs;
    }

    public static Map<ResourcePattern, PrivBitSet> getLdapAllResourcePrivs(UserIdentity userIdentity) {
        Map<ResourcePattern, PrivBitSet> ldapResourcePrivs = Maps.newConcurrentMap();
        if (!hasLdapPrivs(userIdentity)) return ldapResourcePrivs;
        for (Map.Entry<ResourcePattern, PrivBitSet> entry : ConnectContext.get().getLdapGroupsPrivs()
                .getResourcePatternToPrivs().entrySet()) {
            if (entry.getKey().getPrivLevel().equals(PaloAuth.PrivLevel.RESOURCE)) {
                ldapResourcePrivs.put(entry.getKey(), entry.getValue());
            }
        }
        return ldapResourcePrivs;
    }

    // Temporary user has information_schema 'Select_priv' priv by default.
    public static void grantDefaultPrivToTempUser(PaloRole role, String clusterName) {
        TablePattern tblPattern = new TablePattern(InfoSchemaDb.DATABASE_NAME, "*");
        try {
            tblPattern.analyze(clusterName);
        } catch (AnalysisException e) {
            LOG.warn("should not happen.", e);
        }
        PaloRole newRole = new PaloRole(role.getRoleName(), tblPattern, PrivBitSet.of(PaloPrivilege.SELECT_PRIV));
        role.merge(newRole);
    }
}
