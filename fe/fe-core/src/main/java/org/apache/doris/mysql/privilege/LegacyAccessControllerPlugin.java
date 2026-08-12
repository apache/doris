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
 */
public class LegacyAccessControllerPlugin implements AuthorizationPlugin {

    private final String name;
    private final CatalogAccessController controller;

    public LegacyAccessControllerPlugin(String name, CatalogAccessController controller) {
        this.name = Objects.requireNonNull(name, "name is required");
        this.controller = Objects.requireNonNull(controller, "controller is required");
    }

    /**
     * The controller this presents. Needed where an identity, not a behaviour, is the question - a controller
     * asking whether it is itself the one governing instance scope has to compare against the object it is,
     * not against the wrapper it is reached through.
     */
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
                refuseUnless(controller.checkGlobalPriv(currentUser, wanted), subject, resource, requirement);
                return;
            case CATALOG:
                refuseUnless(controller.checkCtlPriv(currentUser,
                        ((AuthorizedResource.Catalog) resource).getCatalog(), wanted),
                        subject, resource, requirement);
                return;
            case DATABASE: {
                AuthorizedResource.Database database = (AuthorizedResource.Database) resource;
                refuseUnless(controller.checkDbPriv(currentUser, database.getCatalog(),
                        database.getDatabase(), wanted), subject, resource, requirement);
                return;
            }
            case TABLE: {
                AuthorizedResource.Table table = (AuthorizedResource.Table) resource;
                refuseUnless(controller.checkTblPriv(currentUser, table.getCatalog(), table.getDatabase(),
                        table.getTable(), wanted), subject, resource, requirement);
                return;
            }
            case COLUMNS: {
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
