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

package org.apache.doris.authorizationexample;

import org.apache.doris.authorization.AccessAction;
import org.apache.doris.authorization.AccessContext;
import org.apache.doris.authorization.AccessDeniedException;
import org.apache.doris.authorization.AccessRequirement;
import org.apache.doris.authorization.AuthorizedResource;
import org.apache.doris.authorization.AuthorizedSubject;
import org.apache.doris.authorization.RowFilterSpec;
import org.apache.doris.authorization.spi.AuthorizationContext;
import org.apache.doris.authorization.spi.AuthorizationPlugin;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A worked example of an authorization source, complete enough to govern a running FE.
 *
 * <p>It grants by Doris role and knows three of them: whoever holds the admin role may do anything, whoever
 * holds the reader role may read and nothing else, and then only the rows matching one predicate, and whoever
 * holds the auditor role may read the same things with no predicate over them. Everyone else is refused -
 * <em>including</em> an account holding a built-in {@code GRANT}, because the source governing a resource is
 * the only thing consulted about it and this source has never heard of that grant.
 *
 * <p>Read it together with {@code fe-authorization-spi/README.md}. Three things here are worth copying
 * beyond the shape:
 *
 * <ul>
 *   <li><b>Decide by which actions the subject holds, not by which question was asked.</b> The engine asks
 *       about a whole requirement, and not every requirement is one of the named ones in
 *       {@code AccessRequirements}: granting a privilege, for instance, requires holding that privilege
 *       <em>and</em> the right to grant it, and that requirement is composed at run time. A source that
 *       matched on the requirements it recognises would refuse those and look, from the outside, like a
 *       source with a mysteriously incomplete policy.</li>
 *   <li><b>A source installed for the whole instance has to answer for administration itself.</b>
 *       {@link AuthorizationContext#grantedByGlobalScopeAuthority} exists so a catalog-bound source can let
 *       an instance administrator through, but it answers false when the source asking <em>is</em> that
 *       authority - which an instance-wide source always is. Such a source with no admin rule of its own
 *       locks out every account, including the one that would fix the configuration.</li>
 *   <li><b>Returning no row filter is not a decision about access.</b> Whether the table may be read at all
 *       was settled by {@link #checkPrivilege}; the filters only narrow what a read that is already allowed
 *       returns.</li>
 * </ul>
 *
 * <p>This source answers uniformly for every kind of resource, so it has no branch for a kind it does not
 * recognise. One that understands only tables must <em>refuse</em> the other kinds rather than guess: the
 * kinds are closed and known when the plugin is compiled, so anything else means the plugin was built
 * against a different Doris.
 *
 * <p>Lastly, a detail that exists only because this example lives inside the Doris source tree: it is
 * deliberately not in a package under {@code org.apache.doris.authorization}, which the FE loads from
 * itself rather than from the plugin jar. A plugin placed there would be loaded off the FE's own class path,
 * with no jar and so no declared API version. Plugins written outside this repository have their own package
 * and never meet this.
 */
public class ExampleAuthorizationPlugin implements AuthorizationPlugin {

    /** Doris role whose holders may do anything. Defaults to the role the FE's own {@code root} holds. */
    public static final String ADMIN_ROLE_PROPERTY = "example.admin_role";
    public static final String DEFAULT_ADMIN_ROLE = "operator";

    /** Doris role whose holders may read, and only read. */
    public static final String READER_ROLE_PROPERTY = "example.reader_role";
    public static final String DEFAULT_READER_ROLE = "example_reader";

    /**
     * Doris role whose holders may read too, with no row filter over what they read.
     *
     * <p>Here because "who gets a filter" is a decision, and a source that returned the same filter to
     * everybody would be indistinguishable from this one without a second account it treats differently. Not a
     * privilege distinction: an auditor may read exactly what a reader may.
     */
    public static final String AUDITOR_ROLE_PROPERTY = "example.auditor_role";
    public static final String DEFAULT_AUDITOR_ROLE = "example_auditor";

    /** SQL predicate in Doris dialect, restricting which rows a reader sees. */
    public static final String READER_ROW_FILTER_PROPERTY = "example.reader_row_filter";
    public static final String DEFAULT_READER_ROW_FILTER = "region = 'EU'";

    /** Names the policy in diagnostics; a real source would use whatever its own policies are called. */
    static final String ROW_FILTER_IDENT = "example-reader-rows";

    private static final Set<AccessAction> EVERY_ACTION =
            Collections.unmodifiableSet(EnumSet.allOf(AccessAction.class));
    private static final Set<AccessAction> READ_ONLY =
            Collections.unmodifiableSet(EnumSet.of(AccessAction.SELECT));

    private final AuthorizationContext context;
    private final String adminRole;
    private final String readerRole;
    private final String auditorRole;
    private final String readerRowFilter;

    ExampleAuthorizationPlugin(Map<String, String> properties, AuthorizationContext context) {
        this.context = context;
        this.adminRole = properties.getOrDefault(ADMIN_ROLE_PROPERTY, DEFAULT_ADMIN_ROLE);
        this.readerRole = properties.getOrDefault(READER_ROLE_PROPERTY, DEFAULT_READER_ROLE);
        this.auditorRole = properties.getOrDefault(AUDITOR_ROLE_PROPERTY, DEFAULT_AUDITOR_ROLE);
        this.readerRowFilter =
                properties.getOrDefault(READER_ROW_FILTER_PROPERTY, DEFAULT_READER_ROW_FILTER);
    }

    @Override
    public String name() {
        return ExampleAuthorizationPluginFactory.NAME;
    }

    @Override
    public void checkPrivilege(AuthorizedSubject subject, AuthorizedResource resource,
            AccessRequirement requirement, AccessContext accessContext) throws AccessDeniedException {
        if (!requirement.isSatisfiedBy(actionsHeldBy(subject))) {
            // Refusing is throwing. Naming this source is what later lets the operator reading the error
            // know which of the configured sources said no.
            throw AccessDeniedException.of(subject, resource, requirement, name());
        }
    }

    @Override
    public List<RowFilterSpec> getRowFilters(AuthorizedSubject subject, AuthorizedResource.Table table,
            AccessContext accessContext) {
        if (!isReader(subject)) {
            return Collections.emptyList();
        }
        // One policy for every table this source governs, which keeps the example about the contract. A
        // real source looks the policy up by table, and returns several when several apply - RESTRICTIVE
        // ones are ANDed together, PERMISSIVE ones ORed.
        return Collections.singletonList(RowFilterSpec.restrictive(ROW_FILTER_IDENT, readerRowFilter));
    }

    /**
     * What this source grants the subject, as a set of actions. Roles come from the engine because only it
     * can resolve them, and asking for them lazily - rather than carrying them on every subject - matters
     * because listing what an account may see asks thousands of questions per statement.
     */
    private Set<AccessAction> actionsHeldBy(AuthorizedSubject subject) {
        Set<String> roles = context.rolesOf(subject);
        if (roles.contains(adminRole)) {
            return EVERY_ACTION;
        }
        if (roles.contains(readerRole) || roles.contains(auditorRole)) {
            return READ_ONLY;
        }
        return Collections.emptySet();
    }

    private boolean isReader(AuthorizedSubject subject) {
        Set<String> roles = context.rolesOf(subject);
        return !roles.contains(adminRole) && !roles.contains(auditorRole) && roles.contains(readerRole);
    }
}
