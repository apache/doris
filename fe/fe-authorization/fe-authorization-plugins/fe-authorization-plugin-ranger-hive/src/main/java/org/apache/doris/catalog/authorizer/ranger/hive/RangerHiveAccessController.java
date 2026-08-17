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

package org.apache.doris.catalog.authorizer.ranger.hive;

import org.apache.doris.authorization.AccessContext;
import org.apache.doris.authorization.AccessDeniedException;
import org.apache.doris.authorization.AccessRequirement;
import org.apache.doris.authorization.AccessRequirements;
import org.apache.doris.authorization.AuthorizedResource;
import org.apache.doris.authorization.AuthorizedSubject;
import org.apache.doris.authorization.DataMaskSpec;
import org.apache.doris.authorization.RowFilterSpec;
import org.apache.doris.authorization.spi.AuthorizationContext;
import org.apache.doris.catalog.authorizer.ranger.RangerAccessController;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.service.RangerAuthContextListener;
import org.apache.ranger.plugin.service.RangerBasePlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A Hive Ranger service governing one catalog: it knows databases, tables and columns, and nothing else Doris
 * has. What it is not asked about it refuses, except for the two kinds it has to let through for a catalog
 * bound to it to be usable at all - the catalog itself, and workload groups, neither of which a Hive service
 * has policies for.
 */
public class RangerHiveAccessController extends RangerAccessController {
    private static final Logger LOG = LogManager.getLogger(RangerHiveAccessController.class);

    /**
     * Drains the audit buffers. One thread for the process: every catalog bound to a Hive Ranger service
     * schedules its own task on this timer.
     *
     * <p>Built here rather than through the engine's thread pool manager, which a plugin outside fe-core
     * cannot reach. What that costs is the pool's entry in the FE thread-pool metrics
     * ({@code doris_fe_thread_pool} with name {@code ranger-hive-audit-log-flusher-timer}), which no plugin
     * loaded from its own directory can register into; for a fixed single-thread timer those gauges never
     * moved.
     */
    private static final ScheduledThreadPoolExecutor LOG_FLUSH_TIMER = new ScheduledThreadPoolExecutor(1,
            new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("ranger-hive-audit-log-flusher-timer-%d")
                    .build());

    /** The name this source is selected by in catalog properties. */
    public static final String NAME = "ranger-hive";

    private RangerHivePlugin hivePlugin;
    private RangerHiveAuditHandler auditHandler;
    private ScheduledFuture<?> logFlushFuture;
    // The manager can remove a controller while a query still holds its reference. Keep the plugin alive
    // until that query completes, then prevent any later authorization from using the cleaned plugin.
    private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock();
    private boolean closed;

    public RangerHiveAccessController(Map<String, String> properties, AuthorizationContext context) {
        this(properties, null, context);
    }

    public RangerHiveAccessController(Map<String, String> properties,
            RangerAuthContextListener rangerAuthContextListener, AuthorizationContext context) {
        super(properties, context);
        String serviceName = properties.get("ranger.service.name");
        hivePlugin = new RangerHivePlugin(serviceName, rangerAuthContextListener);
        auditHandler = new RangerHiveAuditHandler(hivePlugin.getConfig());
        // start a timed log flusher
        logFlushFuture = LOG_FLUSH_TIMER.scheduleAtFixedRate(
                new RangerHiveAuditLogFlusher(auditHandler), 10, 20L, TimeUnit.SECONDS);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void close() {
        lifecycleLock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            if (logFlushFuture != null) {
                logFlushFuture.cancel(false);
                logFlushFuture = null;
            }
            // flushAudit atomically drains the handler. This preserves events produced before close without
            // racing the periodic flusher or re-emitting events it has already sent.
            try {
                auditHandler.flushAudit();
            } catch (Throwable e) {
                LOG.warn("Failed to flush Ranger Hive audit events while closing the access controller", e);
            }
            if (hivePlugin != null) {
                try {
                    hivePlugin.cleanup();
                } catch (Throwable e) {
                    LOG.warn("Failed to clean up Ranger Hive plugin", e);
                } finally {
                    hivePlugin = null;
                }
            }
        } finally {
            lifecycleLock.writeLock().unlock();
        }
    }

    @Override
    public void checkPrivilege(AuthorizedSubject subject, AuthorizedResource resource,
            AccessRequirement requirement, AccessContext context) throws AccessDeniedException {
        switch (resource.getKind()) {
            case GLOBAL:
                // A Hive service has no notion of an instance-wide privilege, so this is entirely whoever
                // governs instance scope. Installed as that authority itself, it grants nothing: it would
                // otherwise be asking itself, and this configuration is not one a Hive service can serve.
                refuseUnless(grantedByGlobalScopeAuthority(subject, requirement), subject, resource, requirement);
                return;
            case CATALOG:
                // The catalog is the thing bound to this service; the policies are about what is inside it.
                return;
            case DATABASE: {
                AuthorizedResource.Database database = (AuthorizedResource.Database) resource;
                refuseUnless(checkResource(subject, requirement,
                        new RangerHiveResource(HiveObjectType.DATABASE, database.getDatabase())),
                        subject, resource, requirement);
                return;
            }
            case TABLE: {
                AuthorizedResource.Table table = (AuthorizedResource.Table) resource;
                refuseUnless(checkResource(subject, requirement,
                        new RangerHiveResource(HiveObjectType.TABLE, table.getDatabase(), table.getTable())),
                        subject, resource, requirement);
                return;
            }
            case COLUMNS:
                checkColumns(subject, (AuthorizedResource.Columns) resource, requirement);
                return;
            case WORKLOAD_GROUP:
                // Not support workload group privilege in ranger hive plugin.
                // So always allow to pass the check
                return;
            case RESOURCE:
            case STORAGE_VAULT:
            case CLOUD_GENERAL:
            case CLOUD_COMPUTE_GROUP:
            case CLOUD_STAGE:
            case CLOUD_STORAGE_VAULT:
                throw AccessDeniedException.of(subject, resource, requirement, NAME);
            default:
                throw new IllegalStateException("the Ranger Hive service has no answer for resource kind "
                        + resource.getKind());
        }
    }

    private void refuseUnless(boolean allowed, AuthorizedSubject subject, AuthorizedResource resource,
            AccessRequirement requirement) throws AccessDeniedException {
        if (!allowed) {
            throw AccessDeniedException.of(subject, resource, requirement, NAME);
        }
    }

    private boolean checkResource(AuthorizedSubject subject, AccessRequirement requirement,
            RangerHiveResource resource) {
        if (grantedByGlobalScopeAuthority(subject, requirement)) {
            return true;
        }
        return checkPrivilege(subject, accessTypeOf(requirement), resource);
    }

    private void checkColumns(AuthorizedSubject subject, AuthorizedResource.Columns columns,
            AccessRequirement requirement) throws AccessDeniedException {
        if (grantedByGlobalScopeAuthority(subject, requirement)) {
            return;
        }
        List<RangerHiveResource> resources = new ArrayList<>();
        for (String col : columns.getColumns()) {
            RangerHiveResource resource = new RangerHiveResource(HiveObjectType.COLUMN,
                    columns.getDatabase(), columns.getTable(), col);
            resources.add(resource);
        }

        checkPrivileges(subject, accessTypeOf(requirement), resources, columns);
    }

    private RangerAccessRequestImpl createRequest(AuthorizedSubject subject, HiveAccessType accessType) {
        RangerAccessRequestImpl request = createRequest(subject);
        if (accessType == HiveAccessType.USE) {
            request.setAccessType(RangerPolicyEngine.ANY_ACCESS);
        } else {
            request.setAccessType(accessType.name().toLowerCase());
        }
        return request;
    }

    /**
     * Lower case, the same spelling the privilege checks above ask with - because that is the spelling the
     * Hive service definition declares.
     *
     * <p>A Ranger service definition names its access types, and the policies of a service can only be
     * written with the names it declares: Ranger's stock Hive definition declares {@code select}, and its
     * {@code rowFilterDef} declares {@code select} as well, so a row filter or a column mask on a Hive
     * service can only carry that spelling. Doris used to ask for these with a single hard-coded
     * {@code "SELECT"} shared with the Doris service type - correct there, where the definition really does
     * declare upper case, and never matching anything here. The effect was silent and one-directional: a
     * row filter written in the Ranger UI against a Hive service simply never reached the query, which is
     * the failure mode a data policy must not have.
     */
    @Override
    protected String readAccessTypeName() {
        return HiveAccessType.SELECT.name().toLowerCase();
    }

    @Override
    protected RangerAccessRequestImpl createRequest(AuthorizedSubject subject) {
        RangerAccessRequestImpl request = new RangerAccessRequestImpl();
        request.setUser(subject.getUser());
        // Policies in Ranger may be written against a role rather than a user, and the roles a Doris account
        // holds are the engine's to know, not this service's.
        request.setUserRoles(getContext().rolesOf(subject));
        request.setClientIPAddress(subject.getHost());
        request.setClusterType(CLIENT_TYPE_DORIS);
        request.setClientType(CLIENT_TYPE_DORIS);
        request.setAccessTime(new Date());

        return request;
    }

    private void checkPrivileges(AuthorizedSubject subject, HiveAccessType accessType,
            List<RangerHiveResource> hiveResources, AuthorizedResource asked) throws AccessDeniedException {
        lifecycleLock.readLock().lock();
        try {
            if (closed) {
                throw AccessDeniedException.withMessage("Ranger Hive access controller has been closed",
                        asked, NAME);
            }
            List<RangerAccessRequest> requests = new ArrayList<>();
            for (RangerHiveResource resource : hiveResources) {
                RangerAccessRequestImpl request = createRequest(subject, accessType);
                request.setResource(resource);
                requests.add(request);
            }

            Collection<RangerAccessResult> results = hivePlugin.isAccessAllowed(requests, auditHandler);
            checkRequestResults(results, accessType.name(), asked);
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    private boolean checkPrivilege(AuthorizedSubject subject, HiveAccessType accessType,
            RangerHiveResource resource) {
        lifecycleLock.readLock().lock();
        try {
            if (closed) {
                return false;
            }
            RangerAccessRequestImpl request = createRequest(subject, accessType);
            request.setResource(resource);

            RangerAccessResult result = hivePlugin.isAccessAllowed(request, auditHandler);
            return checkRequestResult(request, result, accessType.name());
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    /**
     * The Hive access type standing for the question being asked.
     *
     * <p>Only the questions the engine asks by name map onto one; anything else - a requirement assembled for
     * one statement, say - is deliberately {@link HiveAccessType#NONE}, which no Hive policy grants. Guessing
     * an access type from an unrecognised set of actions would grant on a policy written for something else.
     */
    @VisibleForTesting
    static HiveAccessType accessTypeOf(AccessRequirement requirement) {
        if (AccessRequirements.VISIBILITY.equals(requirement)) {
            return HiveAccessType.USE;
        } else if (AccessRequirements.SELECT.equals(requirement)) {
            return HiveAccessType.SELECT;
        } else if (AccessRequirements.ADMINISTRATION.equals(requirement)
                || AccessRequirements.ANY_PRIVILEGE.equals(requirement)) {
            return HiveAccessType.ALL;
        } else if (AccessRequirements.LOAD.equals(requirement)) {
            return HiveAccessType.UPDATE;
        } else if (AccessRequirements.ALTER.equals(requirement)) {
            return HiveAccessType.ALTER;
        } else if (AccessRequirements.CREATE.equals(requirement)) {
            return HiveAccessType.CREATE;
        } else if (AccessRequirements.DROP.equals(requirement)) {
            return HiveAccessType.DROP;
        } else {
            return HiveAccessType.NONE;
        }
    }

    @Override
    public List<RowFilterSpec> getRowFilters(AuthorizedSubject subject, AuthorizedResource.Table table,
            AccessContext context) {
        lifecycleLock.readLock().lock();
        try {
            return closed ? new ArrayList<>() : super.getRowFilters(subject, table, context);
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    @Override
    public Map<String, DataMaskSpec> getDataMasks(AuthorizedSubject subject, AuthorizedResource.Table table,
            Set<String> columns, AccessContext context) {
        lifecycleLock.readLock().lock();
        try {
            return closed ? Collections.<String, DataMaskSpec>emptyMap()
                    : super.getDataMasks(subject, table, columns, context);
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    @Override
    protected RangerHiveResource createResource(String ctl, String db, String tbl) {
        return new RangerHiveResource(HiveObjectType.TABLE,
                db, tbl);
    }

    @Override
    protected RangerHiveResource createResource(String ctl, String db, String tbl, String col) {
        return new RangerHiveResource(HiveObjectType.COLUMN,
                db, tbl, col);
    }

    @Override
    protected RangerBasePlugin getPlugin() {
        return hivePlugin;
    }

    @Override
    protected RangerAccessResultProcessor getAccessResultProcessor() {
        return auditHandler;
    }
}
