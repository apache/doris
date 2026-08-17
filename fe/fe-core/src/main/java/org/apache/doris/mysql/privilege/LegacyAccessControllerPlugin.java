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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.authorization.AccessContext;
import org.apache.doris.authorization.AccessDeniedException;
import org.apache.doris.authorization.AccessRequirement;
import org.apache.doris.authorization.AuthorizedResource;
import org.apache.doris.authorization.AuthorizedSubject;
import org.apache.doris.authorization.DataMaskSpec;
import org.apache.doris.authorization.RowFilterSpec;
import org.apache.doris.authorization.spi.AuthorizationPlugin;
import org.apache.doris.common.AuthorizationException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Presents an access controller written against the older, per-scope interface as an authorization source.
 *
 * <p>That interface asks a separate question per kind of object and answers each with a boolean; this one
 * asks a single question about a typed resource and answers by refusing or not. The translation is the whole
 * of this class, and it is not a temporary shim: {@code CatalogAccessController} is what a catalog's
 * {@code access_controller.class} names, so implementations of it exist outside this repository and keep
 * working unchanged.
 *
 * <p>Part of that translation is a grant the old interface never asked its implementations about. Its scoped
 * methods came in pairs - {@code checkDbPriv(boolean hasGlobal, ...)} in front of
 * {@code checkDbPriv(...)} - and the engine computed {@code hasGlobal} from whoever governed instance scope
 * and passed it in, so a caller holding the privilege globally was granted without the controller being
 * consulted at all. Under the current contract each source decides its own exemptions, and the two Ranger
 * sources do decide this one for themselves. A controller written against the older interface never had the
 * chance to, so the exemption is reproduced here rather than taken away from it.
 */
public class LegacyAccessControllerPlugin implements AuthorizationPlugin {

    /**
     * Whether whoever governs instance scope already grants this - the {@code hasGlobal} the older interface
     * was handed. Unlike {@link org.apache.doris.authorization.spi.AuthorizationContext}'s question of the
     * same shape, this one is asked even when the source asking is itself that authority, because that is
     * what the engine did before: it computed the global verdict from the instance-wide source in every case.
     */
    @FunctionalInterface
    public interface GlobalScopeAuthority {
        /**
         * @param context the circumstances of the check that provoked this question, carried along rather than
         *         read off the thread: whoever governs instance scope may be a source that decides from more
         *         than the subject, and this is a question about the same statement.
         */
        boolean grants(AuthorizedSubject subject, AccessRequirement requirement, AccessContext context);
    }

    private final String name;
    private final CatalogAccessController controller;
    private final GlobalScopeAuthority globalScope;

    public LegacyAccessControllerPlugin(String name, CatalogAccessController controller,
            GlobalScopeAuthority globalScope) {
        this.name = Objects.requireNonNull(name, "name is required");
        this.controller = Objects.requireNonNull(controller, "controller is required");
        this.globalScope = Objects.requireNonNull(globalScope, "global scope authority is required");
    }

    /** The controller this presents, for where the controller itself is the question rather than its answers. */
    public CatalogAccessController getController() {
        return controller;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void checkPrivilege(AuthorizedSubject subject, AuthorizedResource resource,
            AccessRequirement requirement, AccessContext context) throws AccessDeniedException {
        UserIdentity currentUser = AccessTranslation.userIdentityOf(subject);
        PrivPredicate wanted = AccessTranslation.privPredicateOf(requirement);
        switch (resource.getKind()) {
            case GLOBAL:
                // Deliberately without the exemption below: this is the question the exemption is made of.
                refuseUnless(controller.checkGlobalPriv(currentUser, wanted), subject, resource, requirement);
                return;
            case CATALOG:
                if (grantedAtGlobalScope(subject, requirement, context)) {
                    return;
                }
                refuseUnless(controller.checkCtlPriv(currentUser,
                        ((AuthorizedResource.Catalog) resource).getCatalog(), wanted),
                        subject, resource, requirement);
                return;
            case DATABASE: {
                if (grantedAtGlobalScope(subject, requirement, context)) {
                    return;
                }
                AuthorizedResource.Database database = (AuthorizedResource.Database) resource;
                refuseUnless(controller.checkDbPriv(currentUser, database.getCatalog(),
                        database.getDatabase(), wanted), subject, resource, requirement);
                return;
            }
            case TABLE: {
                if (grantedAtGlobalScope(subject, requirement, context)) {
                    return;
                }
                AuthorizedResource.Table table = (AuthorizedResource.Table) resource;
                refuseUnless(controller.checkTblPriv(currentUser, table.getCatalog(), table.getDatabase(),
                        table.getTable(), wanted), subject, resource, requirement);
                return;
            }
            case COLUMNS: {
                if (grantedAtGlobalScope(subject, requirement, context)) {
                    return;
                }
                AuthorizedResource.Columns columns = (AuthorizedResource.Columns) resource;
                try {
                    controller.checkColsPriv(currentUser, columns.getCatalog(), columns.getDatabase(),
                            columns.getTable(), columns.getColumns(), wanted);
                } catch (AuthorizationException e) {
                    // The message names the column that failed; that is the answer, so it is carried over
                    // as written rather than restated in terms of the whole column set. As the bare wording,
                    // not as rendered - the engine wraps it in an AuthorizationException again on the way
                    // out, and that class prefixes its own error code when it renders.
                    throw AccessDeniedException.withMessage(e.getDetailMessage(), resource, name);
                }
                return;
            }
            case RESOURCE:
                refuseUnless(controller.checkResourcePriv(currentUser,
                        ((AuthorizedResource.Named) resource).getName(), wanted),
                        subject, resource, requirement);
                return;
            case WORKLOAD_GROUP:
                refuseUnless(controller.checkWorkloadGroupPriv(currentUser,
                        ((AuthorizedResource.Named) resource).getName(), wanted),
                        subject, resource, requirement);
                return;
            case STORAGE_VAULT:
                refuseUnless(controller.checkStorageVaultPriv(currentUser,
                        ((AuthorizedResource.Named) resource).getName(), wanted),
                        subject, resource, requirement);
                return;
            case CLOUD_GENERAL:
            case CLOUD_COMPUTE_GROUP:
            case CLOUD_STAGE:
            case CLOUD_STORAGE_VAULT:
                refuseUnless(controller.checkCloudPriv(currentUser,
                        ((AuthorizedResource.Named) resource).getName(), wanted,
                        AccessTranslation.cloudTypeOf(resource.getKind())), subject, resource, requirement);
                return;
            default:
                throw new IllegalStateException("access controller " + name + " has no method answering for"
                        + " resource kind " + resource.getKind());
        }
    }

    /** The {@code hasGlobal} the deleted default methods were handed; see this class's own documentation. */
    private boolean grantedAtGlobalScope(AuthorizedSubject subject, AccessRequirement requirement,
            AccessContext context) {
        return globalScope.grants(subject, requirement, context);
    }

    private void refuseUnless(boolean allowed, AuthorizedSubject subject, AuthorizedResource resource,
            AccessRequirement requirement) throws AccessDeniedException {
        if (!allowed) {
            throw AccessDeniedException.of(subject, resource, requirement, name);
        }
    }

    @Override
    public List<RowFilterSpec> getRowFilters(AuthorizedSubject subject, AuthorizedResource.Table table,
            AccessContext context) {
        return controller.evalRowFilterPolicies(AccessTranslation.userIdentityOf(subject), table.getCatalog(),
                table.getDatabase(), table.getTable());
    }

    @Override
    public Map<String, DataMaskSpec> getDataMasks(AuthorizedSubject subject, AuthorizedResource.Table table,
            Set<String> columns, AccessContext context) {
        UserIdentity currentUser = AccessTranslation.userIdentityOf(subject);
        Map<String, DataMaskSpec> masks = new HashMap<>();
        for (String column : columns) {
            // One question per column, which is what the older interface offers. A source reached over the
            // network pays for that per column of every table in the statement; implementing the batch
            // method directly is how a plugin stops paying it.
            Optional<DataMaskSpec> mask = controller.evalDataMaskPolicy(currentUser, table.getCatalog(),
                    table.getDatabase(), table.getTable(), column);
            mask.ifPresent(spec -> masks.put(column, spec));
        }
        return masks;
    }

    @Override
    public void close() {
        controller.close();
    }
}
