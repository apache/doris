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
import org.apache.doris.authorization.spi.AuthorizationContext;
import org.apache.doris.catalog.authorizer.ranger.RangerAccessController;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
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
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;

/**
 * A Hive Ranger service governing one catalog: it knows databases, tables and columns, and nothing else Doris
 * has. What it is not asked about it refuses, except for the two kinds it has to let through for a catalog
 * bound to it to be usable at all - the catalog itself, and workload groups, neither of which a Hive service
 * has policies for.
 *
 * <p><b>Its data policies are written for another engine.</b> A row filter on a Hive service is free text an
 * administrator typed into a UI that says "Hive", and a mask type carries an expression the Hive definition
 * declares - so both arrive in Hive dialect, while {@code RowFilterSpec} and {@code DataMaskSpec} are Doris
 * dialect. The mask types are translated here, see {@link #DORIS_MASK_EXPRESSIONS}. A row filter cannot be:
 * it is arbitrary text, and translating SQL is not something this source does. So a row filter on a Hive
 * service has to be written in SQL Doris reads the same way - {@code ||} above all, which is string
 * concatenation in Hive and {@code OR} here, so {@code concat()} is the portable spelling. This is in the
 * release note, because the alternative to saying it is a filter that silently admits rows it was not meant to.
 */
public class RangerHiveAccessController extends RangerAccessController {
    private static final Logger LOG = LogManager.getLogger(RangerHiveAccessController.class);

    /** The name this source is selected by in catalog properties. */
    public static final String NAME = "ranger-hive";

    /** The property naming the Ranger service a binding reads its policies out of. */
    static final String SERVICE_NAME_PROPERTY = "ranger.service.name";

    /**
     * What each mask type of the stock Hive service definition means, written in Doris dialect.
     *
     * <p>Needed because the expression a Ranger service definition carries is written for the engine the
     * definition belongs to: the stock Hive definition's transformers are Hive UDFs
     * ({@code mask_show_last_n}, {@code mask_hash}, and a nine-argument {@code mask}), and Doris has none of
     * them. Handing one to the planner fails the statement on an unknown function, with nothing in the error
     * pointing at Ranger - which is what the access type fix in {@link #readAccessTypeName()} would otherwise
     * have turned a silently unmasked read into.
     *
     * <p>The expressions are the ones {@code ranger-servicedef-doris.json} declares for the same mask types,
     * deliberately: a "Partial mask: show last 4" written in the Ranger UI has to mean the same thing whether
     * the catalog it governs is bound to this source or to {@code ranger-doris}. Keep the two in step.
     *
     * <p>{@code MASK_NULL}, {@code MASK_NONE} and {@code CUSTOM} are absent because Doris writes their
     * payloads itself, before a transformer is ever consulted. Those three plus these five are every mask
     * type the stock Hive definition declares, so anything reaching the miss branch of
     * {@link #dataMaskExpressionOf} is a definition Doris has never seen.
     */
    private static final Map<String, String> DORIS_MASK_EXPRESSIONS = ImmutableMap.of(
            "MASK", "regexp_replace(regexp_replace(regexp_replace({col},'([A-Z])', 'x'),'([a-z])','x'),"
                    + "'([0-9])','n')",
            "MASK_SHOW_LAST_4", "LPAD(RIGHT({col}, 4), CHAR_LENGTH({col}), 'X')",
            "MASK_SHOW_FIRST_4", "RPAD(LEFT({col}, 4), CHAR_LENGTH({col}), 'X')",
            "MASK_HASH", "hex(sha2({col}, 256))",
            "MASK_DATE_SHOW_YEAR", "date_trunc({col}, 'year')");

    // Never cleared once set: the manager can remove a controller while a query still holds its reference,
    // and the lifecycle fence in RangerAccessController - not a null field - is what keeps that query from
    // reaching a cleaned plugin.
    private final RangerHivePlugin hivePlugin;
    private final RangerHiveAuditHandler auditHandler;
    // Package-private so the case about this binding's audit lifecycle can see whether a task was scheduled
    // and whether closing cancelled it; nothing else here says so.
    @VisibleForTesting
    ScheduledFuture<?> logFlushFuture;

    public RangerHiveAccessController(Map<String, String> properties, AuthorizationContext context) {
        this(properties, null, context);
    }

    /**
     * A controller owning the audit stack it reads through, which is how a test or an embedding builds one.
     * Bindings do not come this way - the factory shares one stack per Ranger service, for the reason
     * the audit stacks {@link RangerHiveAccessControllerFactory} shares gives.
     */
    public RangerHiveAccessController(Map<String, String> properties,
            RangerAuthContextListener rangerAuthContextListener, AuthorizationContext context) {
        super(properties, context);
        RangerHiveAuditStack stack = RangerHiveAuditStack.startFor(
                properties.get(SERVICE_NAME_PROPERTY), rangerAuthContextListener);
        hivePlugin = stack.getPlugin();
        auditHandler = stack.getAuditHandler();
        logFlushFuture = stack.getFlushFuture();
    }

    /**
     * A controller over an audit stack somebody else owns, which is how the factory builds every one it hands
     * out: polling a Ranger service is what the stack costs, so it is shared, while everything a binding
     * configures - {@link #DEFER_TO_GLOBAL_SCOPE_AUTHORITY} - belongs to the controller and so to the binding.
     */
    RangerHiveAccessController(RangerHiveAuditStack stack, Map<String, String> properties,
            AuthorizationContext context) {
        super(properties, context);
        hivePlugin = stack.getPlugin();
        auditHandler = stack.getAuditHandler();
        // No flush task of its own: the timer draining this stack belongs to whoever owns the stack, and
        // cancelling it here would stop auditing for every other binding reading the same service.
        logFlushFuture = null;
    }

    @Override
    public String name() {
        return NAME;
    }

    /**
     * Lets go of this controller, stopping the audit stack once nothing holds it any more.
     *
     * <p>The factory hands one controller to every binding configured alike, and one audit stack to every
     * binding reading the same Ranger service, so a binding going away is on its own no reason to stop
     * polling: the factory counts the holders and fences the controller when the last one lets go. What it
     * deliberately does not do is stop the stack - see
     * the audit stacks {@link RangerHiveAccessControllerFactory} shares.
     */
    @Override
    public void close() {
        if (RangerHiveAccessControllerFactory.release(this)) {
            return;
        }
        // Built directly rather than through the factory - a test, or an embedding that owns its own stack -
        // so there is nobody else to account for and the stack is ours to stop.
        if (!markClosed()) {
            return;
        }
        // Everything below runs with no lock held. cleanup() stops the policy refresher by interrupting it
        // and joining without a timeout, so against an unreachable Ranger admin it takes the whole REST
        // timeout; holding the fence across it would queue every check on this source behind it instead of
        // refusing them, which is what the fence is for.
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
        try {
            hivePlugin.cleanup();
        } catch (Throwable e) {
            LOG.warn("Failed to clean up Ranger Hive plugin", e);
        }
    }

    /**
     * Fences this controller off so nothing reaches the Ranger plugin through it again. Idempotent, and the
     * only thing the factory needs from a controller it is releasing - stopping the audit stack is not the
     * factory's call either, because the stack outlives any single controller.
     */
    void fenceOff() {
        markClosed();
    }

    @Override
    protected void checkPrivilegeInternal(AuthorizedSubject subject, AuthorizedResource resource,
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
        return createRequest(subject, accessType, getContext().rolesOf(subject));
    }

    private RangerAccessRequestImpl createRequest(AuthorizedSubject subject, HiveAccessType accessType,
            Set<String> roles) {
        RangerAccessRequestImpl request = createRequest(subject, roles);
        if (accessType == HiveAccessType.USE) {
            request.setAccessType(RangerPolicyEngine.ANY_ACCESS);
        } else {
            // Locale.ROOT because this spelling has to be the one the Hive service definition declares, and
            // that is not a property of whoever started the FE: with a Turkish locale the default rules fold
            // the "I" of INDEX to "ı", an access type no service definition declares.
            request.setAccessType(accessType.name().toLowerCase(Locale.ROOT));
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
        return HiveAccessType.SELECT.name().toLowerCase(Locale.ROOT);
    }

    /** Translated rather than passed through, for the reason {@link #DORIS_MASK_EXPRESSIONS} gives. */
    @Override
    protected String dataMaskExpressionOf(RangerAccessResult policy, String maskType) {
        String expression = DORIS_MASK_EXPRESSIONS.get(maskType);
        if (expression == null) {
            // Refused rather than passed through: the definition's own transformer is Hive dialect, so
            // reaching here means either a mask type added to the definition after this map was written, or
            // one Doris cannot express. Both are better as a statement that fails naming the mask type than
            // as an unknown-function error, and far better than a column returned in the clear.
            throw new IllegalStateException("mask type " + maskType + " has no Doris equivalent, so the"
                    + " expression the hive service definition carries for it cannot be applied");
        }
        return expression;
    }

    @Override
    protected RangerAccessRequestImpl createRequest(AuthorizedSubject subject) {
        // Policies in Ranger may be written against a role rather than a user, and the roles a Doris account
        // holds are the engine's to know, not this service's.
        return createRequest(subject, getContext().rolesOf(subject));
    }

    private RangerAccessRequestImpl createRequest(AuthorizedSubject subject, Set<String> roles) {
        RangerAccessRequestImpl request = new RangerAccessRequestImpl();
        request.setUser(subject.getUser());
        request.setUserRoles(roles);
        request.setClientIPAddress(subject.getHost());
        request.setClusterType(CLIENT_TYPE_DORIS);
        request.setClientType(CLIENT_TYPE_DORIS);
        request.setAccessTime(new Date());

        return request;
    }

    private void checkPrivileges(AuthorizedSubject subject, HiveAccessType accessType,
            List<RangerHiveResource> hiveResources, AuthorizedResource asked) throws AccessDeniedException {
        checkWhileOpen(asked, () -> {
            // Asked once for the whole batch. Every request in it is about the same subject, and reading the
            // roles takes a read lock on the engine's privilege tables - a 200 column table would take 200 of
            // them to build 200 identical sets.
            Set<String> roles = getContext().rolesOf(subject);
            List<RangerAccessRequest> requests = new ArrayList<>();
            for (RangerHiveResource resource : hiveResources) {
                RangerAccessRequestImpl request = createRequest(subject, accessType, roles);
                request.setResource(resource);
                requests.add(request);
            }

            Collection<RangerAccessResult> results = hivePlugin.isAccessAllowed(requests, auditHandler);
            checkRequestResults(results, accessType.name(), asked);
        });
    }

    private boolean checkPrivilege(AuthorizedSubject subject, HiveAccessType accessType,
            RangerHiveResource resource) {
        return decideWhileOpen(() -> {
            RangerAccessRequestImpl request = createRequest(subject, accessType);
            request.setResource(resource);

            RangerAccessResult result = hivePlugin.isAccessAllowed(request, auditHandler);
            return checkRequestResult(request, result, accessType.name());
        });
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

    // getRowFilters and getDataMasks are fenced by RangerAccessController itself, which refuses rather than
    // answering "no policy" once this controller is closed. Overriding them to return an empty answer is what
    // this class used to do, and it made a closed controller indistinguishable from an unrestricted table.

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
