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

package org.apache.doris.catalog.authorizer.ranger.doris;

import org.apache.doris.authorization.AccessAction;
import org.apache.doris.authorization.AccessContext;
import org.apache.doris.authorization.AccessDeniedException;
import org.apache.doris.authorization.AccessRequirement;
import org.apache.doris.authorization.AccessRequirements;
import org.apache.doris.authorization.AuthorizedResource;
import org.apache.doris.authorization.AuthorizedSubject;
import org.apache.doris.authorization.spi.AuthorizationContext;
import org.apache.doris.catalog.authorizer.ranger.RangerAccessController;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest.ResourceMatchingScope;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.apache.ranger.plugin.service.RangerAuthContextListener;
import org.apache.ranger.plugin.service.RangerBasePlugin;

import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;

/**
 * A Doris Ranger service: it answers about every kind of object Doris has, which is why it is also the one
 * source that can be installed for the whole instance.
 */
public class RangerDorisAccessController extends RangerAccessController {
    private static final Logger LOG = LogManager.getLogger(RangerDorisAccessController.class);

    /** The name this source is selected by, in {@code access_controller_type} and in catalog properties. */
    public static final String NAME = "ranger-doris";

    // ranger must set name, we agreed that this name must be used
    private static final String GLOBAL_PRIV_FIXED_NAME = "*";

    /**
     * The workload group every user may use without holding a privilege on it, spelled out here rather than
     * read from the engine so that this plugin compiles against the authorization contract alone.
     *
     * <p>It has to stay equal to {@code WorkloadGroupMgr.DEFAULT_GROUP_NAME}. Nothing enforces that across
     * the two, so if the engine ever renames its default group, users of a Ranger-governed instance start
     * needing an explicit Ranger policy for the group they were silently allowed before.
     */
    private static final String ENGINE_DEFAULT_WORKLOAD_GROUP = "normal";

    /**
     * The Ranger plugin this controller reads policies out of, shared with every other controller of this
     * source; never cleared, because a query can still be holding this controller when the manager removes it
     * and the lifecycle fence in {@link RangerAccessController} - not a null field - is what stops it from
     * reaching a plugin that has been cleaned up.
     */
    private final RangerBasePlugin dorisPlugin;
    // private static ScheduledThreadPoolExecutor logFlushTimer = ThreadPoolManager.newDaemonScheduledThreadPool(1,
    //        "ranger-doris-audit-log-flusher-timer", true);
    // private RangerHiveAuditHandler auditHandler;

    public RangerDorisAccessController(String serviceName, Map<String, String> properties,
            AuthorizationContext context) {
        this(serviceName, null, properties, context);
    }

    public RangerDorisAccessController(String serviceName, RangerAuthContextListener rangerAuthContextListener,
            Map<String, String> properties, AuthorizationContext context) {
        this(new RangerDorisPlugin(serviceName, rangerAuthContextListener), properties, context);
        // auditHandler = new RangerHiveAuditHandler(dorisPlugin.getConfig());
        // start a timed log flusher
        // logFlushTimer.scheduleAtFixedRate(new RangerHiveAuditLogFlusher(auditHandler), 10, 20L, TimeUnit.SECONDS);
    }

    public RangerDorisAccessController(RangerBasePlugin plugin, AuthorizationContext context) {
        this(plugin, Collections.emptyMap(), context);
    }

    /**
     * A controller over a Ranger plugin somebody else owns, which is how the factory builds every one it hands
     * out: the plugin is what polling the Ranger service costs, so it is shared, while everything a binding
     * configures - {@link #DEFER_TO_GLOBAL_SCOPE_AUTHORITY} - belongs to the controller and so to the binding.
     */
    public RangerDorisAccessController(RangerBasePlugin plugin, Map<String, String> properties,
            AuthorizationContext context) {
        super(properties, context);
        dorisPlugin = Objects.requireNonNull(plugin, "ranger plugin is required");
    }

    @Override
    public String name() {
        return NAME;
    }

    /**
     * Lets go of this controller, stopping the Ranger plugin when this controller is the one that owns it.
     *
     * <p>The factory hands one controller to every binding configured alike, and one Ranger plugin to all of
     * them, so a binding going away is on its own no reason to stop polling: the factory counts the holders,
     * fences the controller when the last one lets go, and leaves the shared plugin running - there is at
     * most one of it, and stopping it would make an ordinary {@code ALTER CATALOG} tear down and rebuild the
     * thing it just detached. See the shared plugin
     * {@link RangerDorisAccessControllerFactory} keeps.
     *
     * <p>What this does do immediately is fence this controller off, before the factory decides anything: from
     * here on it refuses every check instead of answering out of a plugin that may be on its way down.
     *
     * <p>A controller built directly - a test, or an embedding handing in a plugin of its own - is the owner
     * of what it was handed, and that one is stopped here.
     */
    @Override
    public void close() {
        if (RangerDorisAccessControllerFactory.release(this)) {
            return;
        }
        // Built directly rather than through the factory - a test, or an embedding that owns its own plugin -
        // so there is nobody else to account for and the plugin is ours to stop.
        if (markClosed()) {
            stopPlugin();
        }
    }

    /**
     * Fences this controller off so nothing reaches the Ranger plugin through it again. Idempotent, and the
     * only thing the factory needs from a controller it is releasing - stopping the plugin is the factory's
     * own call, because the plugin outlives any single controller.
     */
    void fenceOff() {
        markClosed();
    }

    /** Stops the Ranger plugin's threads; called with no lock held, see {@link #markClosed()}. */
    private void stopPlugin() {
        try {
            dorisPlugin.cleanup();
        } catch (Throwable e) {
            LOG.warn("Failed to clean up the Ranger Doris plugin", e);
        }
    }

    @Override
    protected void checkPrivilegeInternal(AuthorizedSubject subject, AuthorizedResource resource,
            AccessRequirement requirement, AccessContext context) throws AccessDeniedException {
        switch (resource.getKind()) {
            case GLOBAL:
                refuseUnless(checkGlobal(subject, requirement, context), subject, resource, requirement);
                return;
            case CATALOG:
                refuseUnless(checkCatalog(subject, ((AuthorizedResource.Catalog) resource).getCatalog(),
                        requirement, context), subject, resource, requirement);
                return;
            case DATABASE: {
                AuthorizedResource.Database database = (AuthorizedResource.Database) resource;
                refuseUnless(checkDatabase(subject, database.getCatalog(), database.getDatabase(), requirement,
                        context), subject, resource, requirement);
                return;
            }
            case TABLE: {
                AuthorizedResource.Table table = (AuthorizedResource.Table) resource;
                refuseUnless(checkTable(subject, table.getCatalog(), table.getDatabase(), table.getTable(),
                        requirement, context), subject, resource, requirement);
                return;
            }
            case COLUMNS:
                checkColumns(subject, (AuthorizedResource.Columns) resource, requirement, context);
                return;
            case RESOURCE: {
                EnumSet<AccessAction> granted = EnumSet.noneOf(AccessAction.class);
                refuseUnless(checkGlobalInternal(subject, requirement, granted, context)
                                || ask(subject, requirement, granted, new RangerDorisResource(
                                        DorisObjectType.RESOURCE, named(resource)), context),
                        subject, resource, requirement);
                return;
            }
            case WORKLOAD_GROUP: {
                // For compatibility with older versions, it is not needed to check the privileges of the
                // default group.
                if (ENGINE_DEFAULT_WORKLOAD_GROUP.equals(named(resource))) {
                    return;
                }
                EnumSet<AccessAction> granted = EnumSet.noneOf(AccessAction.class);
                refuseUnless(checkGlobalInternal(subject, requirement, granted, context)
                                || ask(subject, requirement, granted, new RangerDorisResource(
                                        DorisObjectType.WORKLOAD_GROUP, named(resource)), context),
                        subject, resource, requirement);
                return;
            }
            case STORAGE_VAULT: {
                EnumSet<AccessAction> granted = EnumSet.noneOf(AccessAction.class);
                refuseUnless(checkGlobalInternal(subject, requirement, granted, context)
                                || ask(subject, requirement, granted, new RangerDorisResource(
                                        DorisObjectType.STORAGE_VAULT, named(resource)), context),
                        subject, resource, requirement);
                return;
            }
            case CLOUD_COMPUTE_GROUP: {
                EnumSet<AccessAction> granted = EnumSet.noneOf(AccessAction.class);
                refuseUnless(checkGlobalInternal(subject, requirement, granted, context)
                                || ask(subject, requirement, granted, new RangerDorisResource(
                                        DorisObjectType.COMPUTE_GROUP, named(resource)), context),
                        subject, resource, requirement);
                return;
            }
            case CLOUD_GENERAL:
            case CLOUD_STAGE:
            case CLOUD_STORAGE_VAULT:
                // A general cloud resource is asked about as a resource and a vault as a vault; a stage is
                // reached through `copy into`, which is on its way out, so Ranger never governed it.
                throw AccessDeniedException.of(subject, resource, requirement, NAME);
            default:
                throw new IllegalStateException("the Ranger Doris service has no answer for resource kind "
                        + resource.getKind());
        }
    }

    private static String named(AuthorizedResource resource) {
        return ((AuthorizedResource.Named) resource).getName();
    }

    private void refuseUnless(boolean allowed, AuthorizedSubject subject, AuthorizedResource resource,
            AccessRequirement requirement) throws AccessDeniedException {
        if (!allowed) {
            throw AccessDeniedException.of(subject, resource, requirement, NAME);
        }
    }

    private RangerAccessRequestImpl createRequest(AuthorizedSubject subject, DorisAccessType accessType,
            AccessContext context) {
        RangerAccessRequestImpl request = createRequest(subject, context);
        request.setAction(accessType.name());
        request.setAccessType(accessType.name());
        return request;
    }

    @Override
    protected String readAccessTypeName() {
        return DorisAccessType.SELECT.name();
    }

    @Override
    protected RangerAccessRequestImpl createRequest(AuthorizedSubject subject, AccessContext context) {
        RangerAccessRequestImpl request = new RangerAccessRequestImpl();
        request.setUser(subject.getUser());
        // No user roles, unlike ranger-hive, which does send them. Not an oversight and not free to change:
        // a request carrying roles matches policy items written against a role, so sending them would start
        // granting - and denying - on policies this source has never matched, in every deployment that has
        // any. That is a change to what an existing Ranger service decides and belongs with a release note
        // of its own, not here.
        request.setClientIPAddress(clientAddressOf(subject, context));
        request.setClusterType(CLIENT_TYPE_DORIS);
        request.setClientType(CLIENT_TYPE_DORIS);
        request.setAccessTime(new Date());

        return request;
    }

    // Both of the two places this source reaches its Ranger plugin go through the lifecycle fence, rather than
    // only the public entry points: the manager can close this controller while a query is part way down the
    // global-then-catalog-then-database-then-table cascade, and each step of that cascade arrives here.

    private boolean checkPrivilegeByPlugin(AuthorizedSubject subject, DorisAccessType accessType,
            RangerDorisResource resource, AccessContext context) {
        return decideWhileOpen(() -> {
            RangerAccessRequestImpl request = createRequest(subject, accessType, context);
            request.setResource(resource);
            if (LOG.isDebugEnabled()) {
                LOG.debug("ranger request: {}", request);
            }
            RangerAccessResult result = dorisPlugin.isAccessAllowed(request);
            return checkRequestResult(request, result, accessType.name());
        });
    }

    private boolean checkShowPrivilegeByPlugin(AuthorizedSubject subject, RangerDorisResource resource,
            AccessContext context) {
        return decideWhileOpen(() -> {
            RangerAccessRequestImpl request = createRequest(subject, context);
            request.setResource(resource);
            request.setResourceMatchingScope(ResourceMatchingScope.SELF_OR_DESCENDANTS);
            if (LOG.isDebugEnabled()) {
                LOG.debug("ranger request: {}", request);
            }
            RangerAccessResult result = dorisPlugin.isAccessAllowed(request);
            return checkRequestResult(request, result, DorisAccessType.NONE.name());
        });
    }

    /**
     * Asks Ranger about the actions still outstanding, one request each, until the requirement is met.
     *
     * <p>{@code granted} carries what the levels already walked have granted, so a privilege answered on the
     * catalog is not asked about again on the database and on the table: the same requirement checked down a
     * three-level path costs one evaluation per action, not three.
     */
    private boolean ask(AuthorizedSubject subject, AccessRequirement requirement,
            EnumSet<AccessAction> granted, RangerDorisResource resource, AccessContext context) {
        EnumSet<AccessAction> outstanding = EnumSet.copyOf(requirement.getActions());
        outstanding.removeAll(granted);
        for (AccessAction action : outstanding) {
            if (checkPrivilegeByPlugin(subject, DorisAccessType.of(action), resource, context)) {
                granted.add(action);
            }
            if (requirement.isSatisfiedBy(granted)) {
                return true;
            }
        }
        return false;
    }

    private boolean checkGlobal(AuthorizedSubject subject, AccessRequirement requirement,
            AccessContext context) {
        return checkGlobalInternal(subject, requirement, EnumSet.noneOf(AccessAction.class), context);
    }

    private boolean checkGlobalInternal(AuthorizedSubject subject, AccessRequirement requirement,
            EnumSet<AccessAction> granted, AccessContext context) {
        return ask(subject, requirement, granted,
                new RangerDorisResource(DorisObjectType.GLOBAL, GLOBAL_PRIV_FIXED_NAME), context);
    }

    private boolean checkCatalog(AuthorizedSubject subject, String ctl, AccessRequirement requirement,
            AccessContext context) {
        EnumSet<AccessAction> granted = EnumSet.noneOf(AccessAction.class);
        if (checkGlobalInternal(subject, requirement, granted, context)
                || checkCatalogInternal(subject, ctl, requirement, granted, context)) {
            return true;
        }
        return isVisibility(requirement) && anyPrivilegeWithin(subject,
                new RangerDorisResource(DorisObjectType.CATALOG, ctl), context);
    }

    private boolean checkCatalogInternal(AuthorizedSubject subject, String ctl, AccessRequirement requirement,
            EnumSet<AccessAction> granted, AccessContext context) {
        return ask(subject, requirement, granted, new RangerDorisResource(DorisObjectType.CATALOG, ctl),
                context);
    }

    private boolean checkDatabase(AuthorizedSubject subject, String ctl, String db,
            AccessRequirement requirement, AccessContext context) {
        if (grantedByGlobalScopeAuthority(subject, requirement)) {
            return true;
        }
        EnumSet<AccessAction> granted = EnumSet.noneOf(AccessAction.class);
        if (checkGlobalInternal(subject, requirement, granted, context)
                || checkCatalogInternal(subject, ctl, requirement, granted, context)
                || checkDatabaseInternal(subject, ctl, db, requirement, granted, context)) {
            return true;
        }
        return isVisibility(requirement) && anyPrivilegeWithin(subject,
                new RangerDorisResource(DorisObjectType.DATABASE, ctl, db), context);
    }

    private boolean checkDatabaseInternal(AuthorizedSubject subject, String ctl, String db,
            AccessRequirement requirement, EnumSet<AccessAction> granted, AccessContext context) {
        return ask(subject, requirement, granted, new RangerDorisResource(DorisObjectType.DATABASE, ctl, db),
                context);
    }

    private boolean checkTable(AuthorizedSubject subject, String ctl, String db, String tbl,
            AccessRequirement requirement, AccessContext context) {
        if (grantedByGlobalScopeAuthority(subject, requirement)) {
            return true;
        }
        EnumSet<AccessAction> granted = EnumSet.noneOf(AccessAction.class);
        if (checkGlobalInternal(subject, requirement, granted, context)
                || checkCatalogInternal(subject, ctl, requirement, granted, context)
                || checkDatabaseInternal(subject, ctl, db, requirement, granted, context)
                || checkTableInternal(subject, ctl, db, tbl, requirement, granted, context)) {
            return true;
        }
        return isVisibility(requirement) && anyPrivilegeWithin(subject,
                new RangerDorisResource(DorisObjectType.TABLE, ctl, db, tbl), context);
    }

    private boolean checkTableInternal(AuthorizedSubject subject, String ctl, String db, String tbl,
            AccessRequirement requirement, EnumSet<AccessAction> granted, AccessContext context) {
        return ask(subject, requirement, granted, new RangerDorisResource(DorisObjectType.TABLE, ctl, db, tbl),
                context);
    }

    private void checkColumns(AuthorizedSubject subject, AuthorizedResource.Columns columns,
            AccessRequirement requirement, AccessContext context) throws AccessDeniedException {
        if (grantedByGlobalScopeAuthority(subject, requirement)) {
            return;
        }
        String ctl = columns.getCatalog();
        String db = columns.getDatabase();
        String tbl = columns.getTable();
        EnumSet<AccessAction> granted = EnumSet.noneOf(AccessAction.class);
        boolean hasTablePriv = checkGlobalInternal(subject, requirement, granted, context)
                || checkCatalogInternal(subject, ctl, requirement, granted, context)
                || checkDatabaseInternal(subject, ctl, db, requirement, granted, context)
                || checkTableInternal(subject, ctl, db, tbl, requirement, granted, context);
        if (hasTablePriv) {
            return;
        }

        for (String col : columns.getColumns()) {
            // Each column starts from what the levels above granted, and none of them may add to it: a
            // privilege held on one column says nothing about the next one.
            if (!ask(subject, requirement, EnumSet.copyOf(granted),
                    new RangerDorisResource(DorisObjectType.COLUMN, ctl, db, tbl, col), context)) {
                throw AccessDeniedException.withMessage(String.format(
                        "Permission denied: user [%s] does not have privilege for [%s] command on [%s].[%s].[%s].[%s]",
                        subject, requirement, ctl, db, tbl, col), columns, NAME);
            }
        }
    }

    /**
     * Whether this is the question "may the subject see the object at all", which Ranger answers by looking
     * for any policy anywhere under it rather than for a privilege on it.
     */
    private static boolean isVisibility(AccessRequirement requirement) {
        return AccessRequirements.VISIBILITY.equals(requirement);
    }

    private boolean anyPrivilegeWithin(AuthorizedSubject subject, RangerDorisResource resource,
            AccessContext context) {
        return checkShowPrivilegeByPlugin(subject, resource, context);
    }

    @Override
    protected RangerDorisResource createResource(String ctl, String db, String tbl) {
        return new RangerDorisResource(DorisObjectType.TABLE,
                ctl, db, tbl);
    }

    @Override
    protected RangerDorisResource createResource(String ctl, String db, String tbl, String col) {
        return new RangerDorisResource(DorisObjectType.COLUMN,
                ctl, db, tbl, col);
    }

    @Override
    protected RangerBasePlugin getPlugin() {
        return dorisPlugin;
    }

    @Override
    protected RangerAccessResultProcessor getAccessResultProcessor() {
        return null;
    }
}
