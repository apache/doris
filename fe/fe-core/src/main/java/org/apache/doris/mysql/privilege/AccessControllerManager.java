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
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.authorization.AccessDeniedException;
import org.apache.doris.authorization.AccessRequirement;
import org.apache.doris.authorization.AuthorizedResource;
import org.apache.doris.authorization.DataMaskSpec;
import org.apache.doris.authorization.ResourceKind;
import org.apache.doris.authorization.RowFilterSpec;
import org.apache.doris.authorization.spi.AuthorizationPlugin;
import org.apache.doris.catalog.AuthorizationInfo;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AuthorizationException;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.ClassLoaderUtils;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.plugin.PropertiesUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * AccessControllerManager is the entry point of privilege authentication.
 *
 * <p>Access is decided by authorization sources - the built-in privilege model, or a plugin standing for an
 * external system - and this class decides only which one to ask: system-wide objects and catalog level
 * grants go to the source {@code access_controller_type} installs, everything inside a catalog goes to the
 * source that catalog is bound to. Whatever that source answers is the answer. The manager establishes no
 * privilege of its own beforehand and never combines two sources' verdicts, so which policies apply to a
 * resource is readable from which source the catalog is bound to.
 */
public class AccessControllerManager {
    private static final Logger LOG = LogManager.getLogger(AccessControllerManager.class);

    private Auth auth;
    // Governs everything no catalog-bound source governs; the built-in model unless configured otherwise
    private AuthorizationPlugin defaultAccessController;
    // A catalog name can be reused after DROP. Keep the catalog id next to the source so cleanup from
    // an old catalog generation can never remove or close the replacement generation's source.
    private Map<String, CatalogAccessControllerEntry> ctlToCtlAccessController = Maps.newConcurrentMap();
    // Cache of loaded access controller factories for quick creation of new access controllers
    private ConcurrentHashMap<String, AccessControllerFactory> accessControllerFactoriesCache
            = new ConcurrentHashMap<>();
    // Mapping between access controller class names and their identifiers for easy lookup of factory identifiers
    private ConcurrentHashMap<String, String> accessControllerClassNameMapping = new ConcurrentHashMap<>();

    public AccessControllerManager(Auth auth) {
        this.auth = auth;
        loadAccessControllerPlugins();
        String accessControllerName = Config.access_controller_type;
        this.defaultAccessController = loadAccessControllerOrThrow(accessControllerName);
        ctlToCtlAccessController.put(InternalCatalog.INTERNAL_CATALOG_NAME,
                new CatalogAccessControllerEntry(
                        InternalCatalog.INTERNAL_CATALOG_ID, defaultAccessController, false));
    }

    private static final class CatalogAccessControllerEntry {
        private final long catalogId;
        private final AuthorizationPlugin accessController;
        // The default source is shared with the internal catalog. Catalog aliases must detach it but never
        // close it when an external catalog is reset or dropped.
        private final boolean owned;

        private CatalogAccessControllerEntry(
                long catalogId, AuthorizationPlugin accessController, boolean owned) {
            this.catalogId = catalogId;
            this.accessController = accessController;
            this.owned = owned;
        }
    }

    private AuthorizationPlugin loadAccessControllerOrThrow(String accessControllerName) {
        if (accessControllerName.equalsIgnoreCase(InternalAuthorizationPlugin.NAME)) {
            return new InternalAuthorizationPlugin(auth);
        }
        if (accessControllerFactoriesCache.containsKey(accessControllerName)) {
            Map<String, String> prop;
            try {
                prop = PropertiesUtils.loadAccessControllerPropertiesOrNull();
            } catch (IOException e) {
                throw new RuntimeException("Failed to load authorization properties."
                        + "Please check the configuration file, authorization name is " + accessControllerName, e);
            }
            return adapt(accessControllerName,
                    accessControllerFactoriesCache.get(accessControllerName).createAccessController(prop));
        }
        throw new RuntimeException("No authorization plugin factory found for " + accessControllerName
                + ". Please confirm that your plugin is placed in the correct location.");
    }

    /** Presents a controller written against the older per-scope interface as an authorization source. */
    private AuthorizationPlugin adapt(String name, CatalogAccessController controller) {
        return new LegacyAccessControllerPlugin(name, controller);
    }

    private void loadAccessControllerPlugins() {
        ServiceLoader<AccessControllerFactory> loaderFromClasspath = ServiceLoader.load(AccessControllerFactory.class);
        for (AccessControllerFactory factory : loaderFromClasspath) {
            LOG.info("Found Authentication Plugin Factories: {} from class path.", factory.factoryIdentifier());
            accessControllerFactoriesCache.put(factory.factoryIdentifier(), factory);
            accessControllerClassNameMapping.put(factory.getClass().getName(), factory.factoryIdentifier());
        }
        List<AccessControllerFactory> loader = null;
        try {
            loader = ClassLoaderUtils.loadServicesFromDirectory(AccessControllerFactory.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load Authentication Plugin Factories", e);
        }
        for (AccessControllerFactory factory : loader) {
            LOG.info("Found Access Controller Plugin Factory: {} from directory.", factory.factoryIdentifier());
            accessControllerFactoriesCache.put(factory.factoryIdentifier(), factory);
            accessControllerClassNameMapping.put(factory.getClass().getName(), factory.factoryIdentifier());
        }
    }

    /** The authorization source governing the objects inside {@code ctl}. */
    public AuthorizationPlugin getAccessControllerOrDefault(String ctl) {
        if (InternalCatalog.INTERNAL_CATALOG_NAME.equals(ctl)) {
            return defaultAccessController;
        }
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(ctl);
        if (catalog != null && catalog instanceof ExternalCatalog) {
            CatalogAccessControllerEntry entry = ctlToCtlAccessController.get(ctl);
            if (entry != null && entry.catalogId == catalog.getId()) {
                return entry.accessController;
            }
            lazyLoadCtlAccessController((ExternalCatalog) catalog);
            entry = ctlToCtlAccessController.get(ctl);
            if (entry != null && entry.catalogId == catalog.getId()) {
                return entry.accessController;
            }
        }

        return defaultAccessController;
    }

    private void lazyLoadCtlAccessController(ExternalCatalog catalog) {
        CatalogAccessControllerEntry staleEntry = null;
        synchronized (this) {
            if (!isCurrentCatalog(catalog)) {
                return;
            }
            CatalogAccessControllerEntry entry = ctlToCtlAccessController.get(catalog.getName());
            if (entry != null && entry.catalogId == catalog.getId()) {
                return;
            }
            if (entry != null && ctlToCtlAccessController.remove(catalog.getName(), entry)) {
                staleEntry = entry;
            }
        }
        closeEntry(catalog.getName(), staleEntry);

        catalog.initAccessController(false);

        CatalogAccessControllerEntry displaced = null;
        boolean stillCurrent;
        synchronized (this) {
            stillCurrent = isCurrentCatalog(catalog);
            if (stillCurrent) {
                CatalogAccessControllerEntry entry = ctlToCtlAccessController.get(catalog.getName());
                if (entry == null || entry.catalogId != catalog.getId()) {
                    displaced = ctlToCtlAccessController.put(catalog.getName(),
                            new CatalogAccessControllerEntry(catalog.getId(), defaultAccessController, false));
                }
            }
        }
        closeEntry(catalog.getName(), displaced);
        if (!stillCurrent) {
            // A DROP can complete while initAccessController() is constructing the plugin. The custom
            // publication path performs the same post-publication check; this also covers the fallback path.
            removeAccessController(catalog.getName(), catalog.getId());
            return;
        }
        // If DROP won immediately after the synchronized publication, its onClose() removes this id. If DROP
        // already completed before publication, this final identity check removes the orphan ourselves.
        if (!isCurrentCatalog(catalog)) {
            removeAccessController(catalog.getName(), catalog.getId());
        }
    }

    public boolean checkIfAccessControllerExist(String ctl) {
        return ctlToCtlAccessController.containsKey(ctl);
    }

    public void createAccessController(ExternalCatalog catalog, String acFactoryClassName, Map<String, String> prop,
                                       boolean isDryRun) {
        String pluginIdentifier = getPluginIdentifierForAccessController(acFactoryClassName);
        AuthorizationPlugin accessController = adapt(pluginIdentifier,
                accessControllerFactoriesCache.get(pluginIdentifier).createAccessController(prop));
        if (isDryRun) {
            closeAccessController(catalog.getName(), accessController);
            return;
        }

        CatalogAccessControllerEntry displaced = null;
        boolean installed = false;
        synchronized (this) {
            if (isCurrentCatalog(catalog)) {
                CatalogAccessControllerEntry current = ctlToCtlAccessController.get(catalog.getName());
                if (current == null || current.catalogId != catalog.getId()) {
                    displaced = ctlToCtlAccessController.put(catalog.getName(),
                            new CatalogAccessControllerEntry(catalog.getId(), accessController, true));
                    installed = true;
                }
            }
        }
        closeEntry(catalog.getName(), displaced);
        if (!installed) {
            closeAccessController(catalog.getName(), accessController);
            return;
        }
        LOG.info("create access controller {} for catalog {}:{}",
                acFactoryClassName, catalog.getName(), catalog.getId());
        if (!isCurrentCatalog(catalog)) {
            removeAccessController(catalog.getName(), catalog.getId());
        }
    }

    private boolean isCurrentCatalog(ExternalCatalog catalog) {
        CatalogIf currentCatalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalog.getName());
        return currentCatalog == catalog && currentCatalog.getId() == catalog.getId();
    }

    private String getPluginIdentifierForAccessController(String acClassName) {
        String pluginIdentifier = null;
        if (accessControllerClassNameMapping.containsKey(acClassName)) {
            pluginIdentifier = accessControllerClassNameMapping.get(acClassName);
        }
        if (accessControllerFactoriesCache.containsKey(acClassName)) {
            pluginIdentifier = acClassName;
        }
        if (null == pluginIdentifier || !accessControllerFactoriesCache.containsKey(pluginIdentifier)) {
            throw new RuntimeException("Access Controller Plugin Factory not found for " + acClassName);
        }
        return pluginIdentifier;
    }

    public void removeAccessController(String ctl, long catalogId) {
        detachAccessController(ctl, catalogId).run();
    }

    /**
     * Atomically detach the controller owned by one catalog generation. The returned cleanup can be executed
     * after the caller releases CatalogMgr's global lock, so a slow plugin close never blocks unrelated DDL.
     */
    public Runnable detachAccessController(String ctl, long catalogId) {
        if (StringUtils.isBlank(ctl)) {
            return () -> { };
        }
        CatalogAccessControllerEntry entry = ctlToCtlAccessController.get(ctl);
        if (entry == null || entry.catalogId != catalogId || !ctlToCtlAccessController.remove(ctl, entry)) {
            return () -> { };
        }
        LOG.info("detach access controller for catalog {}:{}", ctl, catalogId);
        return () -> closeEntry(ctl, entry);
    }

    private void closeEntry(String ctl, CatalogAccessControllerEntry entry) {
        if (entry == null || !entry.owned || entry.accessController == defaultAccessController) {
            return;
        }
        closeAccessController(ctl, entry.accessController);
    }

    private void closeAccessController(String ctl, AuthorizationPlugin accessController) {
        try {
            accessController.close();
        } catch (Throwable e) {
            // Access-controller plugins are external code. A faulty cleanup must not prevent the catalog
            // lifecycle from releasing its own resources.
            LOG.warn("Failed to close access controller for catalog {}", ctl, e);
        }
    }

    public Auth getAuth() {
        return this.auth;
    }

    /**
     * Answers whether {@code subject} may act on {@code resource} as {@code requirement} demands.
     *
     * <p>This is the one place a check is routed. Which source is asked follows from the resource alone -
     * system-wide objects and catalog-level grants go to the source {@code access_controller_type}
     * installs, everything inside a catalog goes to the source that catalog is bound to - and whatever it
     * answers is the answer. Combining two sources, or granting anything before asking, would have to
     * happen here, and deliberately does not.
     *
     * <p>Columns are not decided here: see {@link #decideColumns}.
     */
    public boolean decide(UserIdentity subject, AuthorizedResource resource, AccessRequirement requirement) {
        if (resource.getKind() == ResourceKind.COLUMNS) {
            throw new IllegalArgumentException("column access is decided by decideColumns(), which"
                    + " reports which column was refused instead of a yes or no");
        }
        try {
            ask(subject, resource, requirement);
            return true;
        } catch (AccessDeniedException e) {
            // The reason travels no further for now: every caller of the boolean facades phrases its own
            // error message. It is carried this far so that the day one of them stops doing so, there is
            // something to phrase it from.
            return false;
        }
    }

    /**
     * Checks access to named columns, reporting the column that was refused rather than a yes or no.
     *
     * <p>Kept apart from {@link #decide} because the answer has a different shape, not because the routing
     * differs: it is the same source the table itself would be asked about.
     */
    public void decideColumns(UserIdentity subject, AuthorizedResource.Columns columns,
            AccessRequirement requirement) throws AuthorizationException {
        try {
            ask(subject, columns, requirement);
        } catch (AccessDeniedException e) {
            throw new AuthorizationException(e.getMessage());
        }
    }

    private void ask(UserIdentity subject, AuthorizedResource resource, AccessRequirement requirement)
            throws AccessDeniedException {
        controllerOf(resource).checkPrivilege(AccessTranslation.subjectOf(subject), resource, requirement,
                ConnectionAccessContext.current());
    }

    /**
     * The authorization source that answers for {@code resource}. This is the whole of the routing: system
     * wide objects and catalog level grants belong to the source installed for the instance, everything
     * inside a catalog to the source that catalog is bound to.
     */
    private AuthorizationPlugin controllerOf(AuthorizedResource resource) {
        switch (resource.getKind()) {
            case GLOBAL:
            case RESOURCE:
            case WORKLOAD_GROUP:
            case STORAGE_VAULT:
            case CLOUD_GENERAL:
            case CLOUD_COMPUTE_GROUP:
            case CLOUD_STAGE:
            case CLOUD_STORAGE_VAULT:
                return systemScopeController();
            case CATALOG:
                // Catalog level grants are only ever stored by the system scope source, so it answers for
                // every catalog, including those bound to a source of their own.
                return systemScopeController();
            case DATABASE:
                return getAccessControllerOrDefault(((AuthorizedResource.Database) resource).getCatalog());
            case TABLE:
                return getAccessControllerOrDefault(((AuthorizedResource.Table) resource).getCatalog());
            case COLUMNS:
                return getAccessControllerOrDefault(((AuthorizedResource.Columns) resource).getCatalog());
            default:
                throw new IllegalStateException("no route for resource kind " + resource.getKind());
        }
    }

    /**
     * The source governing everything that is not inside a catalog: global privileges, resources,
     * workload groups, cloud objects, storage vaults - and catalog level grants, which only it stores.
     */
    private AuthorizationPlugin systemScopeController() {
        return defaultAccessController;
    }

    /**
     * Whether {@code candidate} is itself the source governing instance scope.
     *
     * <p>Asked by a source that would otherwise defer to that authority, so that it does not ask itself a
     * question it is about to answer - the two would agree, at the price of evaluating the same policies
     * twice. Identity is the question, so a controller reached through an adapter is compared against the
     * controller, not against its wrapper.
     */
    public boolean isGlobalScopeAuthority(Object candidate) {
        AuthorizationPlugin authority = systemScopeController();
        if (authority == candidate) {
            return true;
        }
        return authority instanceof LegacyAccessControllerPlugin
                && ((LegacyAccessControllerPlugin) authority).getController() == candidate;
    }

    // ==== Global ====
    public boolean checkGlobalPriv(ConnectContext ctx, PrivPredicate wanted) {
        return checkGlobalPriv(ctx.getCurrentUserIdentity(), wanted);
    }

    public boolean checkGlobalPriv(UserIdentity currentUser, PrivPredicate wanted) {
        return decide(currentUser, AuthorizedResource.global(), AccessTranslation.requirementOf(wanted));
    }

    // ==== Catalog ====
    public boolean checkCtlPriv(ConnectContext ctx, String ctl, PrivPredicate wanted) {
        return checkCtlPriv(ctx.getCurrentUserIdentity(), ctl, wanted);
    }

    private boolean canSkipCatalogPrivCheck(PrivPredicate wanted) {
        return wanted == PrivPredicate.SHOW || wanted == PrivPredicate.SELECT;
    }

    private boolean shouldSkipCatalogPrivCheck(PrivPredicate wanted) {
        return Config.skip_catalog_priv_check && canSkipCatalogPrivCheck(wanted);
    }

    public boolean checkCtlPriv(UserIdentity currentUser, String ctl, PrivPredicate wanted) {
        if (shouldSkipCatalogPrivCheck(wanted)) {
            CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(ctl);
            if (catalog == null) {
                return false;
            }
            // An external catalog bound to a controller of its own keeps no catalog level grants anywhere,
            // so with the check switched off there is nobody left to ask. Every other catalog still goes
            // through the normal route below.
            String className = catalog.isInternalCatalog() ? ""
                    : (String) catalog.getProperties().getOrDefault(CatalogMgr.ACCESS_CONTROLLER_CLASS_PROP, "");
            if (!Strings.isNullOrEmpty(className)) {
                return true;
            }
        }
        return decide(currentUser, AuthorizedResource.catalog(ctl), AccessTranslation.requirementOf(wanted));
    }

    // ==== Database ====
    public boolean checkDbPriv(ConnectContext ctx, String ctl, String db, PrivPredicate wanted) {
        return checkDbPriv(ctx.getCurrentUserIdentity(), ctl, db, wanted);
    }

    public boolean checkDbPriv(UserIdentity currentUser, String ctl, String db, PrivPredicate wanted) {
        return decide(currentUser, AuthorizedResource.database(ctl, db), AccessTranslation.requirementOf(wanted));
    }

    // ==== Table ====
    public boolean checkTblPriv(ConnectContext ctx, TableNameInfo tableName, PrivPredicate wanted) {
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
        return decide(currentUser, AuthorizedResource.table(ctl, db, tbl), AccessTranslation.requirementOf(wanted));
    }

    // ==== Column ====
    // If param has ctx, we can skip auth by isSkipAuth field in ctx
    public void checkColumnsPriv(ConnectContext ctx, String ctl, String qualifiedDb, String tbl, Set<String> cols,
                                 PrivPredicate wanted) throws UserException {
        if (ctx.isSkipAuth()) {
            return;
        }
        checkColumnsPriv(ctx.getCurrentUserIdentity(), ctl, qualifiedDb, tbl, cols, wanted);
    }

    public void checkColumnsPriv(UserIdentity currentUser, String
            ctl, String qualifiedDb, String tbl, Set<String> cols,
                                 PrivPredicate wanted) throws UserException {
        long start = System.currentTimeMillis();
        decideColumns(currentUser, AuthorizedResource.columns(ctl, qualifiedDb, tbl, cols),
                AccessTranslation.requirementOf(wanted));
        if (LOG.isDebugEnabled()) {
            LOG.debug("checkColumnsPriv use {} mills, user: {}, ctl: {}, db: {}, table: {}, cols: {}",
                    System.currentTimeMillis() - start, currentUser, ctl, qualifiedDb, tbl, cols);
        }
    }

    // ==== Resource ====
    public boolean checkResourcePriv(ConnectContext ctx, String resourceName, PrivPredicate wanted) {
        return checkResourcePriv(ctx.getCurrentUserIdentity(), resourceName, wanted);
    }

    public boolean checkResourcePriv(UserIdentity currentUser, String resourceName, PrivPredicate wanted) {
        return decide(currentUser, AuthorizedResource.resource(resourceName),
                AccessTranslation.requirementOf(wanted));
    }

    // ==== Cloud ====
    public boolean checkCloudPriv(ConnectContext ctx, String cloudName, PrivPredicate wanted, ResourceTypeEnum type) {
        return checkCloudPriv(ctx.getCurrentUserIdentity(), cloudName, wanted, type);
    }

    public boolean checkCloudPriv(UserIdentity currentUser, String cloudName,
                                  PrivPredicate wanted, ResourceTypeEnum type) {
        return decide(currentUser, AuthorizedResource.cloud(AccessTranslation.cloudKindOf(type), cloudName),
                AccessTranslation.requirementOf(wanted));
    }

    public boolean checkStorageVaultPriv(ConnectContext ctx, String storageVaultName, PrivPredicate wanted) {
        return checkStorageVaultPriv(ctx.getCurrentUserIdentity(), storageVaultName, wanted);
    }

    public boolean checkStorageVaultPriv(UserIdentity currentUser, String storageVaultName, PrivPredicate wanted) {
        return decide(currentUser, AuthorizedResource.storageVault(storageVaultName),
                AccessTranslation.requirementOf(wanted));
    }

    public boolean checkWorkloadGroupPriv(ConnectContext ctx, String workloadGroupName, PrivPredicate wanted) {
        return checkWorkloadGroupPriv(ctx.getCurrentUserIdentity(), workloadGroupName, wanted);
    }

    public boolean checkWorkloadGroupPriv(UserIdentity currentUser, String workloadGroupName, PrivPredicate wanted) {
        return decide(currentUser, AuthorizedResource.workloadGroup(workloadGroupName),
                AccessTranslation.requirementOf(wanted));
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

    public Optional<DataMaskSpec> evalDataMaskPolicy(UserIdentity currentUser, String
            ctl, String db, String tbl, String col) {
        Objects.requireNonNull(currentUser, "require currentUser object");
        Objects.requireNonNull(ctl, "require ctl object");
        Objects.requireNonNull(db, "require db object");
        Objects.requireNonNull(tbl, "require tbl object");
        Objects.requireNonNull(col, "require col object");
        // Sources are asked about columns in lower case, which is how the ones that store policies per
        // column have them written.
        String column = col.toLowerCase();
        AuthorizedResource.Table table = AuthorizedResource.table(ctl, db, tbl);
        return Optional.ofNullable(controllerOf(table)
                .getDataMasks(AccessTranslation.subjectOf(currentUser), table,
                        Collections.singleton(column), ConnectionAccessContext.current())
                .get(column));
    }

    public List<RowFilterSpec> evalRowFilterPolicies(UserIdentity currentUser, String
            ctl, String db, String tbl) {
        Objects.requireNonNull(currentUser, "require currentUser object");
        Objects.requireNonNull(ctl, "require ctl object");
        Objects.requireNonNull(db, "require db object");
        Objects.requireNonNull(tbl, "require tbl object");
        AuthorizedResource.Table table = AuthorizedResource.table(ctl, db, tbl);
        return controllerOf(table).getRowFilters(AccessTranslation.subjectOf(currentUser), table,
                ConnectionAccessContext.current());
    }
}
