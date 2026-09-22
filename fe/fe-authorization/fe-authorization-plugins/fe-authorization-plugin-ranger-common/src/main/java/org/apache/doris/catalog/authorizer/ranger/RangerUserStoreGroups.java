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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.contextenricher.RangerAdminUserStoreRetriever;
import org.apache.ranger.plugin.contextenricher.RangerUserStoreEnricher;
import org.apache.ranger.plugin.policyengine.RangerPluginContext;
import org.apache.ranger.plugin.service.RangerAuthContext;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.RangerUserStoreUtil;
import org.apache.ranger.plugin.util.ServiceDefUtil;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Where a request built by a Doris source gets the groups a Ranger policy item may be written against.
 *
 * <p>A Ranger policy item names users, groups and roles, and Ranger's own plugins fill in the groups from
 * the service they run in - the Hive plugin asks Hadoop's group mapping. Doris has nothing to ask: an
 * account holds roles, and no part of the engine knows which groups a user is in. So a request that carries
 * no groups matches no group item at all, and a service whose policies are kept by group - the normal way
 * to run Ranger, so that nobody edits a policy each time somebody joins a team - grants nothing to a Doris
 * user; and, worse, denies nothing either. An item that denies a group is exactly as silent about it.
 *
 * <p>What Ranger provides for a plugin in that position is its <b>user store</b>: the users and groups Ranger
 * Admin itself holds, kept current by usersync from LDAP or the OS, which a plugin downloads next to its
 * policies through a {@link RangerUserStoreEnricher}. Ranger 2.5 and later reads the groups of the request's
 * user out of it when {@code ranger.plugin.<type>.use.rangerGroups} is set; the two sources Doris embeds do
 * the same thing here, in the request builder, so that it neither depends on the Ranger version they were
 * built against - branches still on 2.4 have no such setting - nor on an operator finding a Ranger property
 * no Doris document names.
 *
 * <p>It is on by default, because a group item that is silently ignored is a bug in the deployment's eyes,
 * not a behaviour anybody opted into; the property Ranger reads for this is the one that switches it off,
 * so that a deployment which has already decided the question in Ranger's terms has decided it here too.
 * Switched off, requests carry no groups and this source matches what it matched before.
 */
public final class RangerUserStoreGroups {
    private static final Logger LOG = LogManager.getLogger(RangerUserStoreGroups.class);

    /**
     * Suffix of the property switching this off, {@code ranger.plugin.<type>.use.rangerGroups} in full: the
     * name Ranger itself reads for the same thing, so that one setting decides both.
     */
    public static final String USE_RANGER_GROUPS = ".use.rangerGroups";

    /** How often the plugin asks Ranger Admin for a newer user store, when nothing configures it. */
    private static final long DEFAULT_REFRESH_INTERVAL_MS = 60 * 1000L;

    private RangerUserStoreGroups() {
    }

    /**
     * Whether the plugin configured by {@code config} attaches user store groups; on unless switched off.
     *
     * <p>Read strictly - {@code true}, {@code false}, or nothing - and not through Hadoop's {@code getBoolean},
     * which takes any value it cannot read as the default: a mistyped opt-out ({@code flase}) would switch this
     * on, in a deployment that has just decided the opposite, and Ranger reads the same property with the
     * opposite default, so that the two would disagree about what a single setting says.
     *
     * @throws IllegalArgumentException for a value that is neither; {@link #validate} raises it before the
     *         load, so that a request never meets it
     */
    public static boolean enabledFor(RangerPluginConfig config) {
        // No configuration at all - a plugin stubbed out in a test - has not switched anything off.
        if (config == null) {
            return true;
        }
        String property = config.getPropertyPrefix() + USE_RANGER_GROUPS;
        String value = config.getTrimmed(property);
        if (value == null || value.isEmpty() || value.equalsIgnoreCase("true")) {
            return true;
        }
        if (value.equalsIgnoreCase("false")) {
            return false;
        }
        throw new IllegalArgumentException("Ranger service " + config.getServiceName() + ": " + property + "="
                + value + " is neither true nor false; leave it unset or set it to false to switch off the"
                + " groups requests carry");
    }

    /**
     * How often the plugin asks Ranger Admin for a newer user store, in milliseconds: what
     * {@code userStoreRefresherPollingInterval} says, or a minute.
     *
     * <p>Read here rather than left to the enricher, which parses the option inside the policy engine's
     * construction: a value that is not a number fails there, and one that is not positive fails a step
     * later in {@code Timer.schedule}, after the enricher has downloaded the store and started its refresher
     * thread. {@code RangerBasePlugin.setPolicies} catches both and leaves the plugin without an engine, which
     * {@link LoadedRangerPlugin#init} would refuse for the wrong reason - and, in the second case, with that
     * thread left behind. So both are refused before the load, by {@link #validate}.
     *
     * @throws IllegalArgumentException for a value that is not a positive number of milliseconds
     */
    static long refreshIntervalMsOf(RangerPluginConfig config) {
        String property = RangerUserStoreEnricher.USERSTORE_REFRESHER_POLLINGINTERVAL_OPTION;
        String value = config.getTrimmed(property);
        if (value == null || value.isEmpty()) {
            return DEFAULT_REFRESH_INTERVAL_MS;
        }
        long intervalMs;
        try {
            intervalMs = Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Ranger service " + config.getServiceName() + ": " + property + "="
                    + value + " is not a number of milliseconds", e);
        }
        if (intervalMs <= 0) {
            throw new IllegalArgumentException("Ranger service " + config.getServiceName() + ": " + property + "="
                    + value + " is not a positive number of milliseconds");
        }
        return intervalMs;
    }

    /**
     * Refuses a configuration this cannot run with, before {@code RangerBasePlugin.init()} starts anything:
     * an opt-out that is neither true nor false, and a refresh interval that is not a positive number of
     * milliseconds. See {@link #enabledFor} and {@link #refreshIntervalMsOf} for where each would fail
     * otherwise, and how much worse.
     *
     * @throws IllegalArgumentException naming the property and its value
     */
    public static void validate(RangerPluginConfig config) {
        if (config == null || !enabledFor(config)) {
            return;
        }
        refreshIntervalMsOf(config);
    }

    /**
     * Puts a user store enricher on the service definition Ranger just downloaded, unless one is there or
     * this is switched off, so that the plugin fetches and refreshes the user store {@link #groupsOf} reads.
     *
     * <p>Called by {@link LoadedRangerPlugin#setPolicies} before handing the policies on, on every
     * call and not only the first: a download of policy deltas comes with its own copy of the service
     * definition, which is why {@code RangerBasePlugin} re-adds the enricher on deltas too. The retriever
     * class and the refresh interval are read under the option names Ranger itself reads them under, so that
     * an operator who has tuned them for Ranger's own {@code use.rangerGroups} has tuned them here.
     */
    public static void addUserStoreEnricher(RangerPluginConfig config, ServicePolicies policies) {
        if (policies == null || config == null || !enabledFor(config)) {
            return;
        }
        String retriever = config.get(RangerUserStoreEnricher.USERSTORE_RETRIEVER_CLASSNAME_OPTION,
                RangerAdminUserStoreRetriever.class.getCanonicalName());
        String refreshIntervalMs = Long.toString(refreshIntervalMsOf(config));
        // Ranger logs the addition itself, once per download that needed it; the operator-facing line about
        // why the store is downloaded at all is the plugin's, written once when it starts (see describe).
        if (ServiceDefUtil.addUserStoreEnricher(policies, retriever, refreshIntervalMs) && LOG.isDebugEnabled()) {
            LOG.debug("Ranger service {} will download its user store every {} ms", policies.getServiceName(),
                    refreshIntervalMs);
        }
    }

    /** One line for the plugin's start-up log, saying what this does for it and how to switch it off. */
    public static String describe(RangerPluginConfig config) {
        String property = config.getPropertyPrefix() + USE_RANGER_GROUPS;
        return enabledFor(config)
                ? "Ranger service " + config.getServiceName() + ": requests carry the groups Ranger's user store"
                        + " puts the user in, so that policy items written against a group apply; set "
                        + property + "=false to switch that off"
                : "Ranger service " + config.getServiceName() + ": " + property + "=false, so requests carry no"
                        + " groups and policy items written against a group never apply";
    }

    /**
     * The groups the user store {@code plugin} has downloaded puts {@code user} in.
     *
     * <p>Empty when this is switched off, when no store has arrived - Ranger Admin could not be reached for
     * it and nothing was cached, which {@link LoadedRangerPlugin} says why it does not refuse - and when the
     * store does not know the user, which is the case for every account that exists in Doris only. Empty
     * and not null on purpose: a request with an empty group set matches items written against users and
     * roles exactly as it did before.
     */
    public static Set<String> groupsOf(RangerBasePlugin plugin, String user) {
        if (user == null || !enabledFor(plugin.getConfig())) {
            return Collections.emptySet();
        }
        // The auth context the policy engine publishes is where Ranger's own request processing reads the
        // store from; it is replaced together with the engine, and the store is carried over when it is.
        RangerPluginContext pluginContext = plugin.getPluginContext();
        RangerAuthContext authContext = pluginContext == null ? null : pluginContext.getAuthContext();
        RangerUserStoreUtil userStore = authContext == null ? null : authContext.getUserStoreUtil();
        Set<String> groups = userStore == null ? null : userStore.getUserGroups(user);
        if (groups == null || groups.isEmpty()) {
            return Collections.emptySet();
        }
        // A copy, because the set belongs to the store shared by every request until the next download.
        return new HashSet<>(groups);
    }
}
