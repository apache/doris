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
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.ServicePolicies;

import java.util.concurrent.TimeUnit;

/**
 * What the two Ranger plugins Doris embeds have in common: they are built with their service's policies or
 * not at all, and the requests built over them carry the groups Ranger keeps for a user.
 *
 * <p><b>Built with the policies, or not at all.</b> {@code RangerBasePlugin.init()} downloads the service's
 * roles and policies - and, with the enricher below on them, its user store - before it returns, and it
 * survives a Ranger admin it cannot reach: it answers out of the local policy cache
 * ({@code ranger.plugin.<type>.policy.cache.dir}) when there is one, and out of no policies at all when there
 * is not, refusing every check until a later poll succeeds. That is the right thing for a plugin embedded in
 * a service that has to keep running, and the wrong thing at the moment Doris binds one: an FE whose instance
 * scope {@code access_controller_type=ranger-doris} governs, and which has no policies, is an FE nobody can
 * use - no account bypasses the source - and a catalog bound to a {@code ranger-hive} source in that state
 * refuses every statement against it, in both cases with nothing but a line in fe.log to say why. So
 * {@link #init()} refuses that state instead: a load that ended with no policies, from the admin or the
 * cache - or with policies Ranger could build no engine out of, which it also survives, by logging - stops
 * what it started and throws, with the cause. That fails the FE start, the {@code CREATE CATALOG} (whose
 * dry run builds the plugin), or the statement binding a catalog to its source again, which is where an
 * operator sees it. A configuration the load could not use is refused before it starts, for the same
 * reader; see {@link RangerUserStoreGroups#validate}. Once built, an outage of the admin is Ranger's to
 * survive, as it always was: the refresher keeps the last policies it downloaded and keeps polling.
 *
 * <p>The user store is not part of that. A load that found the policies but no user store is logged and
 * accepted - policy items written against a group do not apply until the store arrives, which the enricher
 * keeps asking for - because "no store has arrived" cannot be told apart from "this admin serves none", and
 * an admin from before the user store download existed, or one that fails that download while serving the
 * policies, ran every deployment before groups were attached at all; see {@link RangerUserStoreGroups}.
 *
 * <p><b>Requests carry Ranger's own groups.</b> Doris has none to offer, and a policy item written against a
 * group matches nothing without them; the store they are read from is asked for with the policies, see
 * {@link #setPolicies} and {@link RangerUserStoreGroups}.
 */
public abstract class LoadedRangerPlugin extends RangerBasePlugin {
    private static final Logger LOG = LogManager.getLogger(LoadedRangerPlugin.class);

    /**
     * The version of the policies the last {@link #setPolicies} was handed and could not build an engine
     * out of; null when it built one, or was handed none. {@code RangerBasePlugin.setPolicies} catches
     * whatever the engine's construction throws and leaves the plugin without an engine, which is the state
     * "no policies" leaves it in as well, and not the same cause: {@link #init} reads this to tell the two
     * apart when it refuses the plugin.
     */
    private volatile Long policiesWithoutEngine;

    protected LoadedRangerPlugin(String serviceType, String serviceName, String appId) {
        super(serviceType, serviceName, appId);
    }

    /**
     * Loads the plugin - {@code RangerBasePlugin.init()}: roles, policies and user store, on this thread -
     * and refuses to leave it without policies; see the class comment.
     *
     * @throws IllegalArgumentException for a configuration the load could not use, before it starts; see
     *         {@link RangerUserStoreGroups#validate}
     * @throws IllegalStateException when the load ended with no policies from either the admin or the
     *         cache, or with policies no engine could be built out of
     */
    @Override
    public void init() {
        RangerUserStoreGroups.validate(getConfig());
        LOG.info(RangerUserStoreGroups.describe(getConfig()));
        long startedAtNanos = System.nanoTime();
        try {
            super.init();
            if (getPoliciesVersion() < 0) {
                Long refusedVersion = policiesWithoutEngine;
                throw new IllegalStateException(refusedVersion == null
                        ? describeNoPolicies() : describeNoEngine(refusedVersion));
            }
        } catch (RuntimeException | Error e) {
            // Stops what the load had started before it failed - the policy refresher and its download
            // timer, and the engine if one was built - so that a plugin nobody will hold does not go on
            // polling the admin. A configuration the load could not use fails inside RangerBasePlugin.init()
            // before any of that is started, and stopping nothing is a no-op.
            try {
                cleanup();
            } catch (RuntimeException | Error stopFailure) {
                e.addSuppressed(stopFailure);
            }
            throw e;
        }
        LOG.info("Ranger service {} loaded in {} ms: policies version {}, roles version {}, user store"
                        + " version {}", getServiceName(),
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAtNanos), getPoliciesVersion(),
                getRolesVersion(), getUserStoreVersion());
        if (RangerUserStoreGroups.enabledFor(getConfig()) && getUserStoreVersion() < 0) {
            // Policies to answer out of, but no user store to read groups from: the admin could not be reached
            // for it and none was cached. Requests carry no groups until one arrives - the enricher keeps
            // asking - which is what this source sent before groups were attached at all, and which a policy
            // item written against a group, a deny included, does not match.
            LOG.warn("Ranger service {} loaded its policies but no user store: until one arrives, requests"
                    + " carry no groups and policy items written against a group do not apply", getServiceName());
        }
    }

    /** Why the plugin was refused: what was asked for the policies, and where they could have been cached. */
    private String describeNoPolicies() {
        String prefix = getConfig().getPropertyPrefix();
        String adminUrl = getConfig().get(prefix + ".policy.rest.url");
        String cacheDir = getConfig().get(prefix + ".policy.cache.dir");
        return "Ranger service " + getServiceName() + " (type " + getServiceType() + ") has no policies to"
                + " authorize against: Ranger Admin at " + adminUrl + " could not be reached or does not know"
                + " the service, and "
                + (cacheDir == null
                        ? "no policy cache directory is configured (" + prefix + ".policy.cache.dir)"
                        : "no policy cache was found under " + cacheDir)
                + ". Fix ranger-" + getServiceType() + "-security.xml in fe/conf, or bring Ranger Admin back,"
                + " and try again";
    }

    /** Why the plugin was refused when its policies did arrive: nothing could be built out of them. */
    private String describeNoEngine(long policiesVersion) {
        return "Ranger service " + getServiceName() + " (type " + getServiceType() + ") has no policies to"
                + " authorize against: its policies (version " + policiesVersion + ") were downloaded, but no"
                + " policy engine could be built out of them; RangerBasePlugin.setPolicies logged the cause"
                + " just before this. Fix what it names and try again";
    }

    /**
     * Takes the policies Ranger downloaded, and asks for the user store with them, so that the requests the
     * source over this plugin builds can carry the groups Ranger keeps for a user; see
     * {@link RangerUserStoreGroups}. Done on the way in rather than left to Ranger, because Ranger only does
     * it on its own from 2.5 on, and behind a property.
     *
     * <p>Notes, for {@link #init}, whether the policies handed over left the plugin without an engine.
     */
    @Override
    public void setPolicies(ServicePolicies policies) {
        RangerUserStoreGroups.addUserStoreEnricher(getConfig(), policies);
        super.setPolicies(policies);
        Long version = policies == null ? null : policies.getPolicyVersion();
        policiesWithoutEngine = version != null && getPoliciesVersion() < 0 ? version : null;
    }
}
