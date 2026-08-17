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

import org.apache.doris.authorization.AccessRequirement;
import org.apache.doris.authorization.AuthorizedResource;
import org.apache.doris.authorization.AuthorizedSubject;
import org.apache.doris.authorization.spi.AuthorizationContext;
import org.apache.doris.authorization.spi.AuthorizationPlugin;

import com.google.common.base.Preconditions;

import java.util.Objects;
import java.util.Set;

/**
 * What the engine answers when an authorization source asks it something.
 *
 * <p>One of these is created per source, so that a question whose answer depends on who is asking - "does
 * somebody else already grant this at instance scope?" - can be answered without the source having to
 * identify itself on every call. {@link AccessControllerManager} builds them; nothing else needs to, beyond
 * a test standing a source up the way the manager would.
 */
public class EngineAuthorizationContext implements AuthorizationContext {

    private final AccessControllerManager manager;
    private final Auth auth;
    /**
     * The source this context serves. Assigned once the factory has built it, which is necessarily after this
     * context exists: the factory needs the context to build the source with.
     */
    private volatile AuthorizationPlugin servedSource;

    public EngineAuthorizationContext(AccessControllerManager manager, Auth auth) {
        this.manager = Objects.requireNonNull(manager, "manager is required");
        this.auth = Objects.requireNonNull(auth, "auth is required");
    }

    /** Records which source this context belongs to; called by the manager right after the source is built. */
    public void servedBy(AuthorizationPlugin source) {
        this.servedSource = Objects.requireNonNull(source, "source is required");
    }

    /**
     * {@inheritDoc}
     *
     * <p>Without the per-user default role: it is an artefact of how the built-in model stores a user's own
     * grants, not a role anybody names in a policy, and a source that saw it would be matching on a name the
     * administrator never wrote.
     */
    @Override
    public Set<String> rolesOf(AuthorizedSubject subject) {
        return auth.getRolesByUser(AccessTranslation.userIdentityOf(subject), false);
    }

    @Override
    public boolean grantedByGlobalScopeAuthority(AuthorizedSubject subject, AccessRequirement requirement) {
        Preconditions.checkState(servedSource != null,
                "an authorization source asked about global scope before the engine knew which source it is");
        if (manager.isGlobalScopeAuthority(servedSource)) {
            // Asking would route straight back to the source asking, which is about to answer the same
            // question from the same policies. Same verdict, twice the evaluations.
            return false;
        }
        // Under the circumstances of the check this source is answering, not whatever connection happens to be
        // on the thread: the source that governs instance scope may decide from the client address, and this
        // question is about the same statement. A check can reach here before its connection is installed on
        // the thread - the HTTP cookie path does - where the thread holds another request's, or none.
        return manager.decide(AccessTranslation.userIdentityOf(subject), AuthorizedResource.global(),
                requirement, manager.contextOfCheckInFlight());
    }
}
