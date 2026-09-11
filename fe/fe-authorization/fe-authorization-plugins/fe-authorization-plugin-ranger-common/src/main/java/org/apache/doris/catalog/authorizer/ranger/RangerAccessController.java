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

package org.apache.doris.catalog.authorizer.ranger;

import org.apache.doris.authorization.AccessContext;
import org.apache.doris.authorization.AccessDeniedException;
import org.apache.doris.authorization.AccessRequirement;
import org.apache.doris.authorization.AuthorizedResource;
import org.apache.doris.authorization.AuthorizedSubject;
import org.apache.doris.authorization.DataMaskSpec;
import org.apache.doris.authorization.RowFilterSpec;
import org.apache.doris.authorization.spi.AuthorizationContext;
import org.apache.doris.authorization.spi.AuthorizationPlugin;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;
import org.apache.ranger.plugin.service.RangerBasePlugin;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * What the Ranger-backed authorization sources have in common: they answer out of a Ranger service's
 * policies, they honour whoever governs instance scope, and they stop answering the moment they are closed.
 *
 * <p>Both halves of that last part live here rather than in each source, because both are the same rule read
 * twice: an answer this source cannot stand behind is never given. A closed controller refuses instead of
 * answering ({@link #markClosed}, {@link #whileOpen} and friends), and a Ranger plugin that has no answer to
 * give - its policy engine not yet initialized, or being cleaned up - is a refusal too, not an empty policy
 * set. Getting the second one wrong is silent: the engine reads "no row filter, no column mask" as licence to
 * read the table whole and in the clear.
 */
public abstract class RangerAccessController implements AuthorizationPlugin {
    private static final Logger LOG = LogManager.getLogger(RangerAccessController.class);

    protected static final String CLIENT_TYPE_DORIS = "doris";

    /**
     * Property switching off deference to whoever governs instance scope, so that a deployment can decide
     * that inside what Ranger governs, only Ranger's policies grant anything - not even to an administrator
     * of the instance. Defaults to deferring, which is what Doris did before this was a source's own choice.
     */
    public static final String DEFER_TO_GLOBAL_SCOPE_AUTHORITY = "ranger.defer_to_global_scope_authority";

    private final AuthorizationContext context;
    private final boolean deferToGlobalScopeAuthority;

    /**
     * Guards the Ranger plugin against a check that is still in flight when this controller is taken away.
     *
     * <p>The manager hands a controller out without holding a lock and closes it outside every lock -
     * deliberately, so that a slow plugin close never blocks unrelated DDL - so a query can be holding this
     * controller while another thread closes it. Under the read lock the plugin stays alive until that check
     * finishes; once the write lock has been taken nothing reaches the plugin from here at all, and every
     * path refuses rather than answers - including the two data policy paths, where refusing means throwing.
     */
    private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock();
    private volatile boolean closed;

    protected RangerAccessController(Map<String, String> properties, AuthorizationContext context) {
        this.context = Objects.requireNonNull(context, "authorization context is required");
        this.deferToGlobalScopeAuthority = deferenceFrom(properties);
    }

    /**
     * Fences this controller off, so that nothing reaches the Ranger plugin through it from here on.
     *
     * <p>Whatever stops the plugin has to run after this returns rather than inside it: {@code cleanup()}
     * stops the policy refresher by interrupting it and joining without a timeout, so with the write lock held
     * over an unreachable Ranger admin every check on this source would queue behind the whole REST timeout -
     * a {@link ReentrantReadWriteLock} is not fair, and a reader arriving after a waiting writer waits too.
     *
     * @return whether this call is the one that closed it; false when it was closed already
     */
    protected final boolean markClosed() {
        lifecycleLock.writeLock().lock();
        try {
            if (closed) {
                return false;
            }
            closed = true;
            return true;
        } finally {
            lifecycleLock.writeLock().unlock();
        }
    }

    /**
     * Answers {@code question} with the Ranger plugin held alive, and refuses once this controller is closed.
     *
     * <p>Refusing is throwing, because the callers are the two data policy methods and their empty answers -
     * no row filter, no column mask - mean "this source defines no policy", which the engine acts on by
     * reading the table unfiltered and unmasked. A source that cannot answer must not say that.
     */
    protected final <T> T whileOpen(Supplier<T> question) {
        lifecycleLock.readLock().lock();
        try {
            if (closed) {
                throw new IllegalStateException("authorization source " + name() + " has been closed; it"
                        + " cannot say which row filters or column masks apply");
            }
            return question.get();
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    /** As {@link #whileOpen}, for a check answered with a verdict: a closed controller grants nothing. */
    protected final boolean decideWhileOpen(BooleanSupplier check) {
        lifecycleLock.readLock().lock();
        try {
            if (closed) {
                LOG.warn("Refusing a check against authorization source {}: it has been closed.", name());
                return false;
            }
            return check.getAsBoolean();
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    /** As {@link #decideWhileOpen}, for a check that refuses by throwing and names what it refused. */
    protected final void checkWhileOpen(AuthorizedResource resource, PluginCheck check)
            throws AccessDeniedException {
        lifecycleLock.readLock().lock();
        try {
            if (closed) {
                throw AccessDeniedException.withMessage("authorization source " + name()
                        + " has been closed", resource, name());
            }
            check.run();
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    /** One check inside this source; refusing is the only thing it may fail with. */
    @FunctionalInterface
    protected interface PluginCheck {
        void run() throws AccessDeniedException;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Final so that the fence is on every path in, not only on the ones that reach the Ranger plugin: a
     * check can be granted without the plugin being asked at all - by deference to whoever governs instance
     * scope - and a closed source must not grant that either. The places inside that do reach the plugin are
     * fenced as well, because they are also reached from a cascade that may be part way down when this
     * controller is closed; the read lock is reentrant, so paying for it twice costs nothing.
     */
    @Override
    public final void checkPrivilege(AuthorizedSubject subject, AuthorizedResource resource,
            AccessRequirement requirement, AccessContext context) throws AccessDeniedException {
        checkWhileOpen(resource, () -> checkPrivilegeInternal(subject, resource, requirement, context));
    }

    /** What this Ranger service decides about {@code resource}, asked only while this source is open. */
    protected abstract void checkPrivilegeInternal(AuthorizedSubject subject, AuthorizedResource resource,
            AccessRequirement requirement, AccessContext context) throws AccessDeniedException;

    /** What this source may ask the engine. */
    protected AuthorizationContext getContext() {
        return context;
    }

    /**
     * Whether the requirement is already held at instance scope, which these sources honour as a grant on
     * everything they govern.
     *
     * <p>Instance scope is not a Ranger service: it belongs to whichever source {@code access_controller_type}
     * installs, so that is who the engine asks on our behalf. With the built-in model there this reproduces
     * "an administrator of the cluster can reach a Ranger-governed catalog"; with Ranger installed for the
     * instance, Ranger decides its own exemptions and the built-in grants stay out of it. Deciding it here
     * rather than in the engine is what lets a source refuse the exemption outright - as this one does when
     * configured to.
     */
    protected boolean grantedByGlobalScopeAuthority(AuthorizedSubject subject, AccessRequirement requirement) {
        return deferToGlobalScopeAuthority && context.grantedByGlobalScopeAuthority(subject, requirement);
    }

    /**
     * Settles what {@code properties} configures without building anything, throwing what the constructor
     * would have thrown.
     *
     * <p>For a factory that has to start a Ranger plugin before it can construct a controller: the plugin
     * starts a policy refresher and a policy download timer that only {@code cleanup()} stops, so a value
     * this class refuses must be refused before that, or the rejected binding leaves a refresher behind that
     * no controller - and therefore no release - can reach.
     */
    public static void validateProperties(Map<String, String> properties) {
        deferenceFrom(properties);
    }

    /**
     * The address to give Ranger as the client's, which is the connection's and not the account's.
     *
     * <p>{@link AuthorizedSubject#getHost()} is the host pattern of the account - the {@code '%'} of
     * {@code CREATE USER foo}, a pattern and not an address - so a Ranger policy conditioned on the client's
     * IP was being matched against something that is not one. The address the client actually connected from
     * is what {@link AccessContext} carries, and it is absent for the checks that do not come from a client
     * statement at all: a background job, a replay. The pattern is the fallback there rather than nothing,
     * because that is what this source sent before and a policy written against it keeps working.
     */
    protected static String clientAddressOf(AuthorizedSubject subject, AccessContext context) {
        if (context == null) {
            return subject.getHost();
        }
        return context.getClientIp().filter(StringUtils::isNotBlank).orElseGet(subject::getHost);
    }

    private static boolean deferenceFrom(Map<String, String> properties) {
        String configured = properties == null ? null : properties.get(DEFER_TO_GLOBAL_SCOPE_AUTHORITY);
        if (configured == null) {
            return true;
        }
        String value = configured.trim();
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        // Parsing this leniently would read a typo as "false" and silently take away an administrator's
        // access to every Ranger-governed object.
        throw new IllegalArgumentException(DEFER_TO_GLOBAL_SCOPE_AUTHORITY + " must be true or false, but is \""
                + configured + "\"");
    }

    protected static boolean checkRequestResult(RangerAccessRequestImpl request,
            RangerAccessResult result, String name) {
        if (result == null) {
            LOG.warn("Error getting authorizer result, please check your ranger config. Make sure "
                    + "ranger policy engine is initialized. Request: {}", request);
            return false;
        }

        if (result.getIsAllowed()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("request {} match policy {}", request, result.getPolicyId());
            }
            return true;
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format(
                        "Permission denied: user [%s] does not have privilege for [%s] command on [%s]",
                        result.getAccessRequest().getUser(), name,
                        result.getAccessRequest().getResource().getAsString()));
            }
            return false;
        }
    }

    /**
     * Refuses on the first request Ranger denied, naming the resource that request was about - which is the
     * answer when a batch of requests stands for the columns of one table.
     *
     * <p>A null result set is the batch form of the null {@link #checkRequestResult} reads as a refusal:
     * {@code RangerBasePlugin} answers null when its policy engine cannot answer at all. Reading it as "every
     * column is allowed" would be the one place this source answers a question it cannot answer.
     */
    protected void checkRequestResults(Collection<RangerAccessResult> results, String name,
            AuthorizedResource resource) throws AccessDeniedException {
        if (results == null) {
            throw AccessDeniedException.withMessage("the Ranger policy engine of authorization source "
                    + name() + " has no answer for " + resource + "; please check your ranger config and make"
                    + " sure the policy engine is initialized", resource, name());
        }
        for (RangerAccessResult result : results) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("request {} match policy {}", result.getAccessRequest(), result.getPolicyId());
            }
            if (!result.getIsAllowed()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(result.getReason());
                }
                throw AccessDeniedException.withMessage(String.format(
                        "Permission denied: user [%s] does not have privilege for [%s] command on [%s]",
                        result.getAccessRequest().getUser(), name,
                        Optional.ofNullable(result.getAccessRequest().getResource().getAsString())
                                .orElse("unknown resource").replaceAll("/", ".")),
                        resource, name());
            }
        }
    }

    /**
     * Identifies a Ranger policy for auditing and for the SQL cache's change detection. The version is part of
     * it on purpose: an administrator editing a policy in place keeps its id, and without the version an
     * updated policy would compare equal to the one the cached result was planned with.
     */
    private static String policyIdent(RangerAccessResult policy) {
        return policy.getPolicyId() + ":" + policy.getPolicyVersion();
    }

    @Override
    public List<RowFilterSpec> getRowFilters(AuthorizedSubject subject, AuthorizedResource.Table table,
            AccessContext context) {
        return whileOpen(() -> evalRowFilterPolicies(subject, table, context));
    }

    private List<RowFilterSpec> evalRowFilterPolicies(AuthorizedSubject subject,
            AuthorizedResource.Table table, AccessContext context) {
        RangerAccessResourceImpl resource = createResource(table.getCatalog(), table.getDatabase(),
                table.getTable());
        RangerAccessRequestImpl request = createRequest(subject, context);
        request.setAccessType(readAccessTypeName());
        request.setResource(resource);

        if (LOG.isDebugEnabled()) {
            LOG.debug("ranger request: {}", request);
        }
        List<RowFilterSpec> res = Lists.newArrayList();
        RangerAccessResult policy = getPlugin().evalRowFilterPolicies(request, getAccessResultProcessor());
        if (LOG.isDebugEnabled()) {
            LOG.debug("ranger response: {}", policy);
        }
        if (policy == null) {
            throw noAnswerFrom(request);
        }
        String filterExpr = policy.getFilterExpr();
        // Blank and not merely empty: whitespace is what a Ranger row filter policy holds when an
        // administrator typed a space into it, and a space is not a restriction anybody wrote. Carried
        // further it reaches RowFilterSpec, which refuses it saying "filterSql must not be blank" and
        // naming neither this source, nor the policy, nor the table.
        if (StringUtils.isBlank(filterExpr)) {
            return res;
        }
        try {
            // Ranger row filters are always restrictive: it returns at most one expression and the row must
            // match it.
            res.add(RowFilterSpec.restrictive(policyIdent(policy), filterExpr));
        } catch (RuntimeException e) {
            // Reported from this frame for the reason evalDataMaskPolicy gives: it is the one that knows
            // which policy on which table was asked about, and every refusal below is an administrator's
            // mistake. The mask half of this class already says so; a row filter refused without any of it
            // is the harder of the two to trace back.
            throw new IllegalStateException("authorization source " + name() + " cannot apply the row filter"
                    + " of ranger policy " + policyIdent(policy) + " to " + table.getCatalog() + "."
                    + table.getDatabase() + "." + table.getTable() + ": " + e.getMessage(), e);
        }
        return res;
    }

    @Override
    public Map<String, DataMaskSpec> getDataMasks(AuthorizedSubject subject, AuthorizedResource.Table table,
            Set<String> columns, AccessContext context) {
        return whileOpen(() -> {
            Map<String, DataMaskSpec> masks = new HashMap<>();
            for (String column : columns) {
                // One request per column: a masking policy in Ranger is written against a column, and the
                // plugin evaluates them one resource at a time.
                evalDataMaskPolicy(subject, table, column, context).ifPresent(
                        mask -> masks.put(column, mask));
            }
            return masks;
        });
    }

    /**
     * The Ranger plugin had no answer at all, which is not the same as "no policy applies here".
     *
     * <p>{@code RangerBasePlugin} answers null when its policy engine cannot answer - it has not downloaded
     * the service's policies yet, or it is being cleaned up. Reading that as "no row filter, no column mask"
     * would hand back an unfiltered, unmasked read of a table Ranger governs, and nothing anywhere would say
     * so; the privilege path in {@link #checkRequestResult} reads the same null the same way and refuses.
     */
    private IllegalStateException noAnswerFrom(RangerAccessRequestImpl request) {
        return new IllegalStateException("the Ranger policy engine of authorization source " + name()
                + " has no answer for " + request.getResource().getAsString() + "; please check your ranger"
                + " config and make sure the policy engine is initialized");
    }

    private Optional<DataMaskSpec> evalDataMaskPolicy(AuthorizedSubject subject, AuthorizedResource.Table table,
            String col, AccessContext context) {
        RangerAccessResourceImpl resource = createResource(table.getCatalog(), table.getDatabase(),
                table.getTable(), col);
        RangerAccessRequestImpl request = createRequest(subject, context);
        request.setAccessType(readAccessTypeName());
        request.setResource(resource);

        if (LOG.isDebugEnabled()) {
            LOG.debug("ranger request: {}", request);
        }
        RangerAccessResult policy = getPlugin().evalDataMaskPolicies(request, getAccessResultProcessor());
        if (LOG.isDebugEnabled()) {
            LOG.debug("ranger response: {}", policy);
        }
        if (policy == null) {
            throw noAnswerFrom(request);
        }
        String maskType = policy.getMaskType();
        if (StringUtils.isEmpty(maskType)) {
            return Optional.empty();
        }
        try {
            return dataMaskOf(policy, maskType, col);
        } catch (RuntimeException e) {
            // Reported from this frame because it is the one that knows which column of which table was
            // asked about, and every refusal below is an administrator's mistake or a service definition
            // Doris cannot read - a mask type with no Doris equivalent, a payload left blank. Thrown from
            // where it happens instead, the statement fails on a message naming neither the source, nor the
            // policy, nor the column, which is the one kind of refusal here a typo can produce.
            throw new IllegalStateException("authorization source " + name() + " cannot apply the data mask of"
                    + " ranger policy " + policyIdent(policy) + " to column " + col + " of "
                    + table.getCatalog() + "." + table.getDatabase() + "." + table.getTable() + ": "
                    + e.getMessage(), e);
        }
    }

    private Optional<DataMaskSpec> dataMaskOf(RangerAccessResult policy, String maskType, String col) {
        String expression;
        switch (maskType) {
            case "MASK_NULL":
                expression = "NULL";
                break;
            case "MASK_NONE":
                return Optional.empty();
            case "CUSTOM":
                expression = policy.getMaskedValue();
                break;
            default:
                expression = dataMaskExpressionOf(policy, maskType);
                break;
        }
        if (StringUtils.isEmpty(expression)) {
            // A mask type carrying no expression at all masks nothing, which is how Ranger's own plugins
            // read it. Blank but not empty is not the same thing and is not read as that: it reaches
            // DataMaskSpec, which refuses it, and the caller turns that refusal into a message that names
            // the policy and the column - the alternative is handing the column back in the clear because
            // an administrator typed a space.
            return Optional.empty();
        }
        return Optional.of(new DataMaskSpec(policyIdent(policy), expression.replace("{col}", col)));
    }

    /**
     * The Doris expression applying a mask type whose payload this class does not write itself.
     *
     * <p>Read out of the service definition the plugin downloaded, because that is where a mask type's
     * expression lives: an operator who edits the transformer of {@code MASK_HASH} in the Doris service
     * definition has changed what that mask type does, and this has to honour it.
     *
     * <p>Overridden by a source whose service definition Doris does not ship, because such a definition
     * writes those expressions in its own engine's dialect - see {@code RangerHiveAccessController}, where
     * the stock definition's transformers are Hive UDFs Doris has no function for.
     */
    protected String dataMaskExpressionOf(RangerAccessResult policy, String maskType) {
        if (policy.getMaskTypeDef() == null) {
            // Ranger answered, and its answer names a mask type the downloaded service definition does not
            // declare - a definition rolled back, or edited by hand. There is no expression to apply and no
            // way to know what was meant, so this is a refusal for the same reason a null result is.
            throw new IllegalStateException("mask type " + maskType + " is not declared by the service"
                    + " definition this plugin downloaded");
        }
        return policy.getMaskTypeDef().getTransformer();
    }

    /**
     * How this Ranger service type spells the access type a read is asked with.
     *
     * <p>Row-filter and data-mask lookups are made with it rather than left unset: unset means "any access",
     * which makes Ranger walk every permission item, and the Ranger UI writes these policies against the read
     * access type anyway, so asking with anything else costs time without changing the answer.
     *
     * <p>Every service type Doris talks to happens to spell it {@code SELECT}, which is why this used to be
     * one hard-coded value for both. It is asked of the subclass because the spelling belongs to the Ranger
     * service definition, not to Doris.
     */
    protected abstract String readAccessTypeName();

    /**
     * A Ranger request about {@code subject}, under the circumstances {@code context} describes.
     *
     * <p>The context is carried this far because Ranger conditions a policy on it: the client's address is
     * part of what a policy may be written against, and it belongs to the connection rather than to the
     * account. See {@link #clientAddressOf}.
     */
    protected abstract RangerAccessRequestImpl createRequest(AuthorizedSubject subject, AccessContext context);

    protected abstract RangerAccessResourceImpl createResource(String ctl, String db, String tbl);

    protected abstract RangerAccessResourceImpl createResource(String ctl, String db, String tbl, String col);

    protected abstract RangerBasePlugin getPlugin();

    protected abstract RangerAccessResultProcessor getAccessResultProcessor();
}
