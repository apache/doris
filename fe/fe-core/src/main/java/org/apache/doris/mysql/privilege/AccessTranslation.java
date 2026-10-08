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

import org.apache.doris.analysis.CompoundPredicate.Operator;
import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.authorization.AccessAction;
import org.apache.doris.authorization.AccessRequirement;
import org.apache.doris.authorization.ActionMatch;
import org.apache.doris.authorization.AuthorizedSubject;
import org.apache.doris.authorization.ResourceKind;

import com.google.common.annotations.VisibleForTesting;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Translates between the vocabulary Doris uses internally and the neutral one authorization sources speak.
 *
 * <p>The translation is lossless in both directions and has to stay that way, because the built-in model is
 * itself one of those sources: a requirement that reaches it must name exactly the privileges the caller
 * asked about. That is why {@link AccessAction} keeps cluster usage and stage usage apart from plain usage
 * even though a given plugin may well treat all three alike - the folding is the plugin's to do, not the
 * engine's.</p>
 *
 * <p>Object identity is preserved as well, and that is not a nicety: parts of the engine ask <em>which</em>
 * question is being asked by comparing the predicate against a constant with {@code ==} - {@code Role} grants
 * "may see this catalog" when any object under it is reachable, the Ranger controllers translate the
 * predicate into their own access types the same way. Handing those a freshly built predicate that merely
 * equals {@code PrivPredicate.SHOW} loses every one of those branches, silently and only for the users who
 * depended on them. So a requirement that came from a constant translates back to that same constant.</p>
 */
public final class AccessTranslation {

    private static final Map<Privilege, AccessAction> ACTION_OF_PRIVILEGE = new EnumMap<>(Privilege.class);
    private static final Map<AccessAction, Privilege> PRIVILEGE_OF_ACTION = new EnumMap<>(AccessAction.class);
    /**
     * The predicate constant each requirement came from, discovered reflectively so that a constant added
     * later is covered the day it is added rather than the day someone remembers this map.
     *
     * <p>Keyed by value, so it answers for any requirement naming those actions - including the
     * {@link org.apache.doris.authorization.AccessRequirements} constants, which are built in the plugin API
     * and never pass through {@link #requirementOf}. Where two predicate constants name the same actions it
     * can only answer with one of them, which is what {@link #PREDICATE_OF_REQUIREMENT} is for.
     */
    private static final Map<AccessRequirement, PrivPredicate> CANONICAL_PREDICATES = new HashMap<>();
    /**
     * The predicate constant each requirement <em>object</em> was built from, so that a requirement the engine
     * derived from a predicate translates back to that very predicate.
     *
     * <p>{@code SHOW_RESOURCES} and {@code SHOW_WORKLOAD_GROUP} name the same privileges with the same
     * operator, so they are one value and {@link #CANONICAL_PREDICATES} cannot tell them apart - the only such
     * pair among the seventeen constants. But {@link #REQUIREMENT_OF_PREDICATE} gave each constant a
     * requirement object of its own, and the engine hands that object on unchanged: through {@code decide},
     * through {@code ask}, into {@code checkPrivilege}. Keying on identity here is therefore what carries the
     * caller's predicate all the way to a controller written against the older interface, where comparing a
     * predicate against a constant with {@code ==} is established practice and being handed the other half of
     * the pair silently takes a branch away.
     */
    private static final Map<AccessRequirement, PrivPredicate> PREDICATE_OF_REQUIREMENT = new IdentityHashMap<>();
    /**
     * The requirement each predicate constant translates to, so that the common path allocates nothing.
     *
     * <p>Every check goes through {@link #requirementOf}, and all but a handful of them pass one of these
     * constants; building the answer means a list, two {@link EnumSet}s and two wrappers, per check, on a
     * path that runs once per object a statement touches - and once per column of a {@code DESCRIBE}.
     *
     * <p>Keyed by identity, which is what {@link PrivPredicate} has: it declares no {@code equals}. That is
     * the right key anyway, since a predicate built at runtime is exactly the case this table cannot answer
     * for and {@link #requirementOf} falls through for.
     */
    private static final Map<PrivPredicate, AccessRequirement> REQUIREMENT_OF_PREDICATE = new IdentityHashMap<>();

    static {
        ACTION_OF_PRIVILEGE.put(Privilege.NODE_PRIV, AccessAction.NODE);
        ACTION_OF_PRIVILEGE.put(Privilege.ADMIN_PRIV, AccessAction.ADMIN);
        ACTION_OF_PRIVILEGE.put(Privilege.GRANT_PRIV, AccessAction.GRANT);
        ACTION_OF_PRIVILEGE.put(Privilege.SELECT_PRIV, AccessAction.SELECT);
        ACTION_OF_PRIVILEGE.put(Privilege.LOAD_PRIV, AccessAction.LOAD);
        ACTION_OF_PRIVILEGE.put(Privilege.ALTER_PRIV, AccessAction.ALTER);
        ACTION_OF_PRIVILEGE.put(Privilege.CREATE_PRIV, AccessAction.CREATE);
        ACTION_OF_PRIVILEGE.put(Privilege.DROP_PRIV, AccessAction.DROP);
        ACTION_OF_PRIVILEGE.put(Privilege.USAGE_PRIV, AccessAction.USAGE);
        ACTION_OF_PRIVILEGE.put(Privilege.CLUSTER_USAGE_PRIV, AccessAction.CLUSTER_USAGE);
        ACTION_OF_PRIVILEGE.put(Privilege.STAGE_USAGE_PRIV, AccessAction.STAGE_USAGE);
        ACTION_OF_PRIVILEGE.put(Privilege.SHOW_VIEW_PRIV, AccessAction.SHOW_VIEW);
        // The retired bit indices carry the same meaning as the ones that replaced them, so they translate
        // to the same action - the same normalization Role.compatibilityAuthIndexChange() applies to stored
        // grants.
        ACTION_OF_PRIVILEGE.put(Privilege.SHOW_VIEW_PRIV_DEPRECATED, AccessAction.SHOW_VIEW);
        ACTION_OF_PRIVILEGE.put(Privilege.SHOW_VIEW_PRIV_CLOUD_DEPRECATED, AccessAction.SHOW_VIEW);
        ACTION_OF_PRIVILEGE.put(Privilege.CLUSTER_USAGE_PRIV_DEPRECATED, AccessAction.CLUSTER_USAGE);
        ACTION_OF_PRIVILEGE.put(Privilege.STAGE_USAGE_PRIV_DEPRECATED, AccessAction.STAGE_USAGE);

        PRIVILEGE_OF_ACTION.put(AccessAction.NODE, Privilege.NODE_PRIV);
        PRIVILEGE_OF_ACTION.put(AccessAction.ADMIN, Privilege.ADMIN_PRIV);
        PRIVILEGE_OF_ACTION.put(AccessAction.GRANT, Privilege.GRANT_PRIV);
        PRIVILEGE_OF_ACTION.put(AccessAction.SELECT, Privilege.SELECT_PRIV);
        PRIVILEGE_OF_ACTION.put(AccessAction.LOAD, Privilege.LOAD_PRIV);
        PRIVILEGE_OF_ACTION.put(AccessAction.ALTER, Privilege.ALTER_PRIV);
        PRIVILEGE_OF_ACTION.put(AccessAction.CREATE, Privilege.CREATE_PRIV);
        PRIVILEGE_OF_ACTION.put(AccessAction.DROP, Privilege.DROP_PRIV);
        PRIVILEGE_OF_ACTION.put(AccessAction.USAGE, Privilege.USAGE_PRIV);
        PRIVILEGE_OF_ACTION.put(AccessAction.CLUSTER_USAGE, Privilege.CLUSTER_USAGE_PRIV);
        PRIVILEGE_OF_ACTION.put(AccessAction.STAGE_USAGE, Privilege.STAGE_USAGE_PRIV);
        PRIVILEGE_OF_ACTION.put(AccessAction.SHOW_VIEW, Privilege.SHOW_VIEW_PRIV);

        // Sorted by name so that two constants naming the same privileges with the same match - today
        // SHOW_RESOURCES and SHOW_WORKLOAD_GROUP - always resolve to the same one of the pair. They ask the
        // same question, so either answers it, but which one it is must not depend on reflection order.
        for (Map.Entry<String, PrivPredicate> constant : new TreeMap<>(declaredPredicates()).entrySet()) {
            AccessRequirement requirement = buildRequirementOf(constant.getValue());
            REQUIREMENT_OF_PREDICATE.put(constant.getValue(), requirement);
            PREDICATE_OF_REQUIREMENT.put(requirement, constant.getValue());
            CANONICAL_PREDICATES.putIfAbsent(requirement, constant.getValue());
        }
    }

    private AccessTranslation() {
    }

    private static Map<String, PrivPredicate> declaredPredicates() {
        Map<String, PrivPredicate> constants = new HashMap<>();
        for (Field field : PrivPredicate.class.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers()) && field.getType() == PrivPredicate.class) {
                try {
                    // Made readable rather than assumed to be: a constant added as anything but public would
                    // otherwise fail this class's initialization, and this class initializing is what every
                    // privilege check in the FE goes through.
                    field.setAccessible(true);
                    constants.put(field.getName(), (PrivPredicate) field.get(null));
                } catch (IllegalAccessException | RuntimeException e) {
                    throw new IllegalStateException("cannot read PrivPredicate." + field.getName(), e);
                }
            }
        }
        return constants;
    }

    /** The action {@code privilege} stands for. */
    @VisibleForTesting
    static AccessAction actionOf(Privilege privilege) {
        AccessAction action = ACTION_OF_PRIVILEGE.get(privilege);
        if (action == null) {
            throw new IllegalStateException("privilege " + privilege + " has no access action; a new"
                    + " privilege must be given one before it can be checked");
        }
        return action;
    }

    /** The privilege {@code action} stands for. */
    @VisibleForTesting
    static Privilege privilegeOf(AccessAction action) {
        Privilege privilege = PRIVILEGE_OF_ACTION.get(action);
        if (privilege == null) {
            throw new IllegalStateException("access action " + action + " has no privilege");
        }
        return privilege;
    }

    /**
     * The neutral form of what {@code wanted} asks for.
     *
     * @throws IllegalArgumentException if the predicate names no privilege at all. Such a predicate is
     *         satisfied by everyone when combined with AND and by no one when combined with OR, so passing
     *         one on would decide access by accident; no caller builds one today.
     */
    public static AccessRequirement requirementOf(PrivPredicate wanted) {
        AccessRequirement known = REQUIREMENT_OF_PREDICATE.get(wanted);
        return known != null ? known : buildRequirementOf(wanted);
    }

    private static AccessRequirement buildRequirementOf(PrivPredicate wanted) {
        List<Privilege> privileges = wanted.getPrivs().toPrivilegeList();
        EnumSet<AccessAction> actions = EnumSet.noneOf(AccessAction.class);
        for (Privilege privilege : privileges) {
            actions.add(actionOf(privilege));
        }
        if (actions.isEmpty()) {
            throw new IllegalArgumentException("privilege predicate names no privilege: " + wanted);
        }
        return AccessRequirement.of(actions, wanted.getOp() == Operator.AND ? ActionMatch.ALL : ActionMatch.ANY);
    }

    /**
     * The privilege predicate {@code requirement} stands for; the constant it came from when there is one,
     * so that the {@code ==} comparisons the engine still makes against those constants keep working.
     *
     * <p>By identity first and by value second. The first answers with the predicate this very requirement
     * object was derived from, which is the only way to tell {@code SHOW_RESOURCES} and
     * {@code SHOW_WORKLOAD_GROUP} apart - they are the same value. The second answers for a requirement built
     * anywhere else, where either half of that pair asks the same question and either may be given.
     */
    public static PrivPredicate privPredicateOf(AccessRequirement requirement) {
        PrivPredicate origin = PREDICATE_OF_REQUIREMENT.get(requirement);
        if (origin != null) {
            return origin;
        }
        PrivPredicate canonical = CANONICAL_PREDICATES.get(requirement);
        if (canonical != null) {
            return canonical;
        }
        PrivBitSet privileges = PrivBitSet.of();
        for (AccessAction action : requirement.getActions()) {
            privileges.set(privilegeOf(action).getIdx());
        }
        return PrivPredicate.of(privileges,
                requirement.getMatch() == ActionMatch.ALL ? Operator.AND : Operator.OR);
    }

    /**
     * The neutral form of the account {@code user} names.
     *
     * <p>Only the three parts that identify an account are carried over, which is exactly what makes the
     * translation reversible: {@link UserIdentity#equals} compares those three and nothing else, and every
     * lookup an authorization decision performs - the privilege tables, the row policies - matches on them.
     * The certificate fields a connection may also carry take part in authentication, never in a decision
     * about what an account may do, so leaving them behind loses nothing.
     *
     * <p>Read with {@code getUser()} rather than {@code getQualifiedUser()}. The two return the same field;
     * the second additionally insists the identity has been through analysis, and callers exist that check
     * access with one that has not - {@code checkCloudPriv} is reached that way today. Translating is not the
     * place to start enforcing that, since it happens on every check and would turn callers that work into
     * callers that throw.
     */
    public static AuthorizedSubject subjectOf(UserIdentity user) {
        return AuthorizedSubject.of(user.getUser(), user.getHost(), user.isDomain());
    }

    /**
     * The account {@code subject} names, in the form the built-in privilege model looks accounts up by.
     * Equal to the identity it was translated from; see {@link #subjectOf}.
     */
    public static UserIdentity userIdentityOf(AuthorizedSubject subject) {
        return subject.isDomain()
                ? UserIdentity.createAnalyzedUserIdentWithDomain(subject.getUser(), subject.getHost())
                : UserIdentity.createAnalyzedUserIdentWithIp(subject.getUser(), subject.getHost());
    }

    /** The resource kind standing for a cloud object of {@code type}. */
    public static ResourceKind cloudKindOf(ResourceTypeEnum type) {
        switch (type) {
            case GENERAL:
                return ResourceKind.CLOUD_GENERAL;
            case CLUSTER:
                return ResourceKind.CLOUD_COMPUTE_GROUP;
            case STAGE:
                return ResourceKind.CLOUD_STAGE;
            case STORAGE_VAULT:
                return ResourceKind.CLOUD_STORAGE_VAULT;
            default:
                throw new IllegalStateException("no resource kind for cloud resource type " + type);
        }
    }

    /** The cloud resource type {@code kind} stands for; only the cloud kinds have one. */
    public static ResourceTypeEnum cloudTypeOf(ResourceKind kind) {
        switch (kind) {
            case CLOUD_GENERAL:
                return ResourceTypeEnum.GENERAL;
            case CLOUD_COMPUTE_GROUP:
                return ResourceTypeEnum.CLUSTER;
            case CLOUD_STAGE:
                return ResourceTypeEnum.STAGE;
            case CLOUD_STORAGE_VAULT:
                return ResourceTypeEnum.STORAGE_VAULT;
            default:
                throw new IllegalArgumentException(kind + " is not a cloud resource kind");
        }
    }
}
