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

import org.apache.doris.analysis.ResourcePattern;
import org.apache.doris.analysis.TablePattern;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.PatternMatcherException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.mysql.privilege.Auth.PrivLevel;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class Role implements Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(Role.class);

    // operator is responsible for operating cluster, such as add/drop node
    public static String OPERATOR_ROLE = "operator";
    // admin is like DBA, who has all privileges except for NODE privilege held by operator
    public static String ADMIN_ROLE = "admin";

    public static Role OPERATOR = new Role(OPERATOR_ROLE,
            TablePattern.ALL, PrivBitSet.of(Privilege.NODE_PRIV, Privilege.ADMIN_PRIV),
            ResourcePattern.ALL, PrivBitSet.of(Privilege.NODE_PRIV, Privilege.ADMIN_PRIV));
    public static Role ADMIN = new Role(ADMIN_ROLE,
            TablePattern.ALL, PrivBitSet.of(Privilege.ADMIN_PRIV),
            ResourcePattern.ALL, PrivBitSet.of(Privilege.ADMIN_PRIV));

    @SerializedName(value = "roleName")
    private String roleName;
    // Will be persisted
    @SerializedName(value = "tblPatternToPrivs")
    private Map<TablePattern, PrivBitSet> tblPatternToPrivs = Maps.newConcurrentMap();
    @SerializedName(value = "resourcePatternToPrivs")
    private Map<ResourcePattern, PrivBitSet> resourcePatternToPrivs = Maps.newConcurrentMap();

    // Will not be persisted,generage by tblPatternToPrivs and resourcePatternToPrivs
    private GlobalPrivTable globalPrivTable = new GlobalPrivTable();
    private CatalogPrivTable catalogPrivTable = new CatalogPrivTable();
    private DbPrivTable dbPrivTable = new DbPrivTable();
    private TablePrivTable tablePrivTable = new TablePrivTable();
    private ResourcePrivTable resourcePrivTable = new ResourcePrivTable();

    @Deprecated
    private Set<UserIdentity> users = Sets.newConcurrentHashSet();

    @Deprecated
    public Set<UserIdentity> getUsers() {
        return users;
    }

    private Role() {

    }

    public Role(String roleName) {
        this.roleName = roleName;
    }

    public Role(String roleName, TablePattern tablePattern, PrivBitSet privs) throws DdlException {
        this.roleName = roleName;
        this.tblPatternToPrivs.put(tablePattern, privs);
        grantPrivs(tablePattern, privs.copy());
    }

    public Role(String roleName, ResourcePattern resourcePattern, PrivBitSet privs) throws DdlException {
        this.roleName = roleName;
        this.resourcePatternToPrivs.put(resourcePattern, privs);
        grantPrivs(resourcePattern, privs.copy());
    }

    public Role(String roleName, TablePattern tablePattern, PrivBitSet tablePrivs,
            ResourcePattern resourcePattern, PrivBitSet resourcePrivs) {
        this.roleName = roleName;
        this.tblPatternToPrivs.put(tablePattern, tablePrivs);
        this.resourcePatternToPrivs.put(resourcePattern, resourcePrivs);
        //for init admin role,will not generate exception
        try {
            grantPrivs(tablePattern, tablePrivs.copy());
            grantPrivs(resourcePattern, resourcePrivs.copy());
        } catch (DdlException e) {
            LOG.warn("grant failed,", e);
        }

    }

    public String getRoleName() {
        return roleName;
    }

    public Map<TablePattern, PrivBitSet> getTblPatternToPrivs() {
        return tblPatternToPrivs;
    }

    public Map<ResourcePattern, PrivBitSet> getResourcePatternToPrivs() {
        return resourcePatternToPrivs;
    }

    // merge role not check role name.
    public void mergeNotCheck(Role other) throws DdlException {
        for (Map.Entry<TablePattern, PrivBitSet> entry : other.getTblPatternToPrivs().entrySet()) {
            if (tblPatternToPrivs.containsKey(entry.getKey())) {
                PrivBitSet existPrivs = tblPatternToPrivs.get(entry.getKey());
                existPrivs.or(entry.getValue());
            } else {
                tblPatternToPrivs.put(entry.getKey(), entry.getValue());
            }
            grantPrivs(entry.getKey(), entry.getValue().copy());
        }
        for (Map.Entry<ResourcePattern, PrivBitSet> entry : other.resourcePatternToPrivs.entrySet()) {
            if (resourcePatternToPrivs.containsKey(entry.getKey())) {
                PrivBitSet existPrivs = resourcePatternToPrivs.get(entry.getKey());
                existPrivs.or(entry.getValue());
            } else {
                resourcePatternToPrivs.put(entry.getKey(), entry.getValue());
            }
            grantPrivs(entry.getKey(), entry.getValue().copy());
        }
    }

    public void merge(Role other) throws DdlException {
        Preconditions.checkState(roleName.equalsIgnoreCase(other.getRoleName()));
        mergeNotCheck(other);
    }

    public boolean checkGlobalPriv(PrivPredicate wanted) {
        PrivBitSet savedPrivs = PrivBitSet.of();
        return checkGlobalInternal(wanted, savedPrivs);
    }

    public boolean checkCtlPriv(String ctl, PrivPredicate wanted) {
        PrivBitSet savedPrivs = PrivBitSet.of();
        if (checkGlobalInternal(wanted, savedPrivs)
                || checkCatalogInternal(ctl, wanted, savedPrivs)) {
            return true;
        }
        // if user has any privs of databases or tables in this catalog, and the wanted priv is SHOW, return true
        if (ctl != null && wanted == PrivPredicate.SHOW && checkAnyPrivWithinCatalog(ctl)) {
            return true;
        }
        LOG.debug("failed to get wanted privs: {}, granted: {}", wanted, savedPrivs);
        return false;
    }

    private boolean checkGlobalInternal(PrivPredicate wanted, PrivBitSet savedPrivs) {
        globalPrivTable.getPrivs(savedPrivs);
        if (Privilege.satisfy(savedPrivs, wanted)) {
            return true;
        }
        return false;
    }

    /*
     * User may not have privs on a catalog, but have privs of databases or tables in this catalog.
     * So we have to check if user has any privs of databases or tables in this catalog.
     * if so, the catalog should be visible to this user.
     */
    private boolean checkAnyPrivWithinCatalog(String ctl) {
        return dbPrivTable.hasPrivsOfCatalog(ctl)
                || tablePrivTable.hasPrivsOfCatalog(ctl);

    }

    private boolean checkCatalogInternal(String ctl, PrivPredicate wanted, PrivBitSet savedPrivs) {
        catalogPrivTable.getPrivs(ctl, savedPrivs);
        if (Privilege.satisfy(savedPrivs, wanted)) {
            return true;
        }

        return false;
    }

    public boolean checkDbPriv(String ctl, String db, PrivPredicate wanted) {
        PrivBitSet savedPrivs = PrivBitSet.of();
        if (checkGlobalInternal(wanted, savedPrivs)
                || checkCatalogInternal(ctl, wanted, savedPrivs)
                || checkDbInternal(ctl, db, wanted, savedPrivs)) {
            return true;
        }

        // if user has any privs of table in this db, and the wanted priv is SHOW, return true
        if (ctl != null && db != null && wanted == PrivPredicate.SHOW && checkAnyPrivWithinDb(ctl, db)) {
            return true;
        }

        LOG.debug("failed to get wanted privs: {}, granted: {}", wanted, savedPrivs);
        return false;
    }

    /*
     * User may not have privs on a database, but have privs of tables in this database.
     * So we have to check if user has any privs of tables in this database.
     * if so, the database should be visible to this user.
     */
    private boolean checkAnyPrivWithinDb(String ctl, String db) {
        return tablePrivTable.hasPrivsOfDb(ctl, db);

    }

    private boolean checkDbInternal(String ctl, String db, PrivPredicate wanted,
            PrivBitSet savedPrivs) {
        dbPrivTable.getPrivs(ctl, db, savedPrivs);
        if (Privilege.satisfy(savedPrivs, wanted)) {
            return true;
        }
        return false;
    }

    public boolean checkTblPriv(String ctl, String db, String tbl, PrivPredicate wanted) {
        PrivBitSet savedPrivs = PrivBitSet.of();
        if (checkGlobalInternal(wanted, savedPrivs)
                || checkCatalogInternal(ctl, wanted, savedPrivs)
                || checkDbInternal(ctl, db, wanted, savedPrivs)
                || checkTblInternal(ctl, db, tbl, wanted, savedPrivs)) {
            return true;
        }
        LOG.debug("failed to get wanted privs: {}, granted: {}", wanted, savedPrivs);
        return false;
    }

    private boolean checkTblInternal(String ctl, String db, String tbl, PrivPredicate wanted, PrivBitSet savedPrivs) {
        tablePrivTable.getPrivs(ctl, db, tbl, savedPrivs);
        return Privilege.satisfy(savedPrivs, wanted);
    }

    public boolean checkResourcePriv(String resourceName, PrivPredicate wanted) {
        PrivBitSet savedPrivs = PrivBitSet.of();
        if (checkGlobalInternal(wanted, savedPrivs)
                || checkResourceInternal(resourceName, wanted, savedPrivs)) {
            return true;
        }

        LOG.debug("failed to get wanted privs: {}, granted: {}", wanted, savedPrivs);
        return false;
    }

    private boolean checkResourceInternal(String resourceName, PrivPredicate wanted, PrivBitSet savedPrivs) {
        resourcePrivTable.getPrivs(resourceName, savedPrivs);
        return Privilege.satisfy(savedPrivs, wanted);
    }

    public boolean checkHasPriv(PrivPredicate priv, PrivLevel[] levels) {
        for (PrivLevel privLevel : levels) {
            switch (privLevel) {
                case GLOBAL:
                    if (globalPrivTable.hasPriv(priv)) {
                        return true;
                    }
                    break;
                case DATABASE:
                    if (dbPrivTable.hasPriv(priv)) {
                        return true;
                    }
                    break;
                case TABLE:
                    if (tablePrivTable.hasPriv(priv)) {
                        return true;
                    }
                    break;
                default:
                    break;
            }
        }
        return false;
    }

    public GlobalPrivTable getGlobalPrivTable() {
        return globalPrivTable;
    }

    public CatalogPrivTable getCatalogPrivTable() {
        return catalogPrivTable;
    }

    public DbPrivTable getDbPrivTable() {
        return dbPrivTable;
    }

    public TablePrivTable getTablePrivTable() {
        return tablePrivTable;
    }

    public ResourcePrivTable getResourcePrivTable() {
        return resourcePrivTable;
    }

    public boolean checkCanEnterCluster(String clusterName) {
        if (checkGlobalPriv(PrivPredicate.ALL)) {
            return true;
        }

        if (dbPrivTable.hasClusterPriv(clusterName)) {
            return true;
        }

        if (tablePrivTable.hasClusterPriv(clusterName)) {
            return true;
        }
        return false;
    }


    private void grantPrivs(ResourcePattern resourcePattern, PrivBitSet privs) throws DdlException {

        // grant privs to user
        switch (resourcePattern.getPrivLevel()) {
            case GLOBAL:
                grantGlobalPrivs(privs);
                break;
            case RESOURCE:
                grantResourcePrivs(resourcePattern.getResourceName(), privs);
                break;
            default:
                Preconditions.checkNotNull(null, resourcePattern.getPrivLevel());
        }

    }

    private void grantPrivs(TablePattern tblPattern, PrivBitSet privs) throws DdlException {
        // grant privs to user
        switch (tblPattern.getPrivLevel()) {
            case GLOBAL:
                grantGlobalPrivs(privs);
                break;
            case CATALOG:
                grantCatalogPrivs(tblPattern.getQualifiedCtl(),
                        privs);
                break;
            case DATABASE:
                grantDbPrivs(tblPattern.getQualifiedCtl(),
                        tblPattern.getQualifiedDb(),
                        privs);
                break;
            case TABLE:
                grantTblPrivs(tblPattern.getQualifiedCtl(),
                        tblPattern.getQualifiedDb(),
                        tblPattern.getTbl(),
                        privs);
                break;
            default:
                Preconditions.checkNotNull(null, tblPattern.getPrivLevel());
        }

    }

    private GlobalPrivEntry grantGlobalPrivs(PrivBitSet privs) throws DdlException {
        GlobalPrivEntry entry = GlobalPrivEntry.create(privs);
        globalPrivTable.addEntry(entry, false, false);
        return entry;
    }

    private void grantResourcePrivs(String resourceName, PrivBitSet privs) throws DdlException {
        ResourcePrivEntry entry;
        try {
            entry = ResourcePrivEntry.create(resourceName, privs);
        } catch (AnalysisException | PatternMatcherException e) {
            throw new DdlException(e.getMessage());
        }
        resourcePrivTable.addEntry(entry, false, false);
    }

    private void grantCatalogPrivs(String ctl, PrivBitSet privs) throws DdlException {
        CatalogPrivEntry entry;
        try {
            entry = CatalogPrivEntry.create(ctl, privs);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        catalogPrivTable.addEntry(entry, false, false);
    }

    private void grantDbPrivs(String ctl, String db,
            PrivBitSet privs) throws DdlException {
        DbPrivEntry entry;
        try {
            entry = DbPrivEntry.create(ctl, db, privs);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        dbPrivTable.addEntry(entry, false, false);
    }

    private void grantTblPrivs(String ctl, String db, String tbl, PrivBitSet privs) throws DdlException {
        TablePrivEntry entry;
        try {
            entry = TablePrivEntry.create(ctl, db, tbl, privs);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        tablePrivTable.addEntry(entry, false, false);
    }

    public void revokePrivs(TablePattern tblPattern, PrivBitSet privs, boolean errOnNonExist) throws DdlException {
        PrivBitSet existingPriv = tblPatternToPrivs.get(tblPattern);
        if (existingPriv == null) {
            if (errOnNonExist) {
                throw new DdlException(tblPattern + " does not exist in role " + roleName);
            }
            return;
        }
        existingPriv.remove(privs);
        revokePrivs(tblPattern, privs);
    }

    public void revokePrivs(ResourcePattern resourcePattern, PrivBitSet privs, boolean errOnNonExist)
            throws DdlException {
        PrivBitSet existingPriv = resourcePatternToPrivs.get(resourcePattern);
        if (existingPriv == null) {
            if (errOnNonExist) {
                throw new DdlException(resourcePattern + " does not exist in role " + roleName);
            }
            return;
        }
        existingPriv.remove(privs);
        revokePrivs(resourcePattern, privs);
    }

    private void revokePrivs(ResourcePattern resourcePattern, PrivBitSet privs) throws DdlException {
        switch (resourcePattern.getPrivLevel()) { // CHECKSTYLE IGNORE THIS LINE: missing switch default
            case GLOBAL:
                revokeGlobalPrivs(privs);
                break;
            case RESOURCE:
                revokeResourcePrivs(resourcePattern.getResourceName(), privs);
                break;
        }
    }

    private void revokeResourcePrivs(String resourceName, PrivBitSet privs) throws DdlException {
        ResourcePrivEntry entry;
        try {
            entry = ResourcePrivEntry.create(resourceName, privs);
        } catch (AnalysisException | PatternMatcherException e) {
            throw new DdlException(e.getMessage());
        }
        resourcePrivTable.revoke(entry, false, true);
    }

    private void revokePrivs(TablePattern tblPattern, PrivBitSet privs) throws DdlException {
        switch (tblPattern.getPrivLevel()) {
            case GLOBAL:
                revokeGlobalPrivs(privs);
                break;
            case CATALOG:
                revokeCatalogPrivs(tblPattern.getQualifiedCtl(), privs);
                break;
            case DATABASE:
                revokeDbPrivs(tblPattern.getQualifiedCtl(),
                        tblPattern.getQualifiedDb(), privs);
                break;
            case TABLE:
                revokeTblPrivs(tblPattern.getQualifiedCtl(), tblPattern.getQualifiedDb(),
                        tblPattern.getTbl(), privs);
                break;
            default:
                Preconditions.checkNotNull(null, tblPattern.getPrivLevel());
        }

    }

    private void revokeGlobalPrivs(PrivBitSet privs)
            throws DdlException {
        GlobalPrivEntry entry = GlobalPrivEntry.create(privs);
        globalPrivTable.revoke(entry, false, false);
    }

    private void revokeCatalogPrivs(String ctl,
            PrivBitSet privs) throws DdlException {
        CatalogPrivEntry entry;
        try {
            entry = CatalogPrivEntry.create(
                    ctl, privs);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        catalogPrivTable.revoke(entry, false, true);
    }

    private void revokeDbPrivs(String ctl, String db, PrivBitSet privs) throws DdlException {
        DbPrivEntry entry;
        try {
            entry = DbPrivEntry.create(ctl, db, privs);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }

        dbPrivTable.revoke(entry, false, true);
    }

    private void revokeTblPrivs(String ctl, String db, String tbl,
            PrivBitSet privs) throws DdlException {
        TablePrivEntry entry;
        try {
            entry = TablePrivEntry.create(ctl, db, tbl, privs);
        } catch (AnalysisException e) {
            throw new DdlException(e.getMessage());
        }
        tablePrivTable.revoke(entry, false, true);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("role: ").append(roleName).append(", db table privs: ").append(tblPatternToPrivs);
        sb.append(", resource privs: ").append(resourcePatternToPrivs);
        return sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static Role read(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_116) {
            Role role = new Role();
            try {
                role.readFields(in);
            } catch (DdlException e) {
                LOG.warn("grant failed,", e);
            }
            return role;
        } else {
            String json = Text.readString(in);
            return GsonUtils.GSON.fromJson(json, Role.class);
        }
    }

    @Deprecated
    private void readFields(DataInput in) throws IOException, DdlException {
        roleName = Text.readString(in);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            TablePattern tblPattern = TablePattern.read(in);
            PrivBitSet privs = PrivBitSet.read(in);
            tblPatternToPrivs.put(tblPattern, privs);
            grantPrivs(tblPattern, privs.copy());
        }
        size = in.readInt();
        for (int i = 0; i < size; i++) {
            ResourcePattern resourcePattern = ResourcePattern.read(in);
            PrivBitSet privs = PrivBitSet.read(in);
            resourcePatternToPrivs.put(resourcePattern, privs);
            grantPrivs(resourcePattern, privs.copy());
        }
        size = in.readInt();
        for (int i = 0; i < size; i++) {
            UserIdentity userIdentity = UserIdentity.read(in);
            users.add(userIdentity);
        }

    }

    @Override
    public void gsonPostProcess() throws IOException {
        for (Entry<TablePattern, PrivBitSet> entry : tblPatternToPrivs.entrySet()) {
            try {
                grantPrivs(entry.getKey(), entry.getValue().copy());
            } catch (DdlException e) {
                LOG.warn("grant failed,", e);
            }
        }
        for (Entry<ResourcePattern, PrivBitSet> entry : resourcePatternToPrivs.entrySet()) {
            try {
                grantPrivs(entry.getKey(), entry.getValue().copy());
            } catch (DdlException e) {
                LOG.warn("grant failed,", e);
            }
        }
    }
}
