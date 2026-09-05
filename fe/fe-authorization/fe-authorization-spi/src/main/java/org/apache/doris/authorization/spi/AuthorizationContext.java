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

package org.apache.doris.authorization.spi;

import org.apache.doris.authorization.AccessRequirement;
import org.apache.doris.authorization.AuthorizedResource;
import org.apache.doris.authorization.AuthorizedSubject;

import java.util.Optional;
import java.util.Set;

/**
 * What the engine will tell a plugin that asks, so that a plugin can decide things it has no data of its own
 * about.
 *
 * <p>The engine implements this and each plugin gets its own instance; everything on it is answered on
 * demand, so a plugin that decides purely from its own policies never pays for any of it. That is the point
 * of the shape: the alternative - handing every check a subject with its roles already resolved - would make
 * every check pay for the one plugin that wanted them.</p>
 */
public interface AuthorizationContext {

    /**
     * The roles {@code subject} holds in Doris.
     *
     * <p>For an external source that keeps its own roles this is the bridge between the two: policies written
     * against Doris roles can be evaluated because the engine, not the source, knows who holds them.</p>
     */
    Set<String> rolesOf(AuthorizedSubject subject);

    /**
     * Whether {@code subject} already holds {@code requirement} at instance scope - as decided by whoever
     * governs instance scope, which may or may not be the plugin asking.
     *
     * <p>This is how a plugin honours "an administrator of this instance may reach what I govern" without the
     * engine imposing it. Asking is a choice: a plugin that refuses to recognise any authority but its own
     * simply never calls this, and one that does call it gets an answer from the same source that would
     * answer a global check, so the exemption it grants matches what the instance actually considers
     * administrative rather than assuming that is the built-in model.</p>
     *
     * <p>Answers false when the plugin asking is itself the instance-scope authority: it would otherwise be
     * asking itself a question it is about to answer anyway, at the price of evaluating the same policies
     * twice.</p>
     */
    boolean grantedByGlobalScopeAuthority(AuthorizedSubject subject, AccessRequirement requirement);

    /**
     * Who owns {@code resource}, when the engine records an owner for it.
     *
     * <p>Ownership is metadata, not a privilege: the engine stores it, and what it entitles the owner to is
     * each plugin's own rule - all rights over the object, only the right to grant it away, or nothing.
     * Empty today for every resource, the channel being here so that adding an owner to a kind of object is a
     * change in the engine rather than in this contract.</p>
     */
    default Optional<String> ownerOf(AuthorizedResource resource) {
        return Optional.empty();
    }
}
