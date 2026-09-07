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

package org.apache.doris.authorization;

import java.util.Objects;
import java.util.Optional;

/**
 * Refusal of an access request, carrying why it was refused and who refused it.
 *
 * <p>A refusal is an answer, not a failure: deciding that a user may not see a table is the normal outcome of
 * asking about a table the user has no grant on, and listing what a user may see asks that question about
 * every object there is. So this exception is built for the common case rather than the exceptional one - it
 * <b>records no stack trace</b>, and its message is composed only if somebody reads it. What makes it an
 * exception at all is that a refusal must not be silently discardable: a plugin that returns without throwing
 * has allowed the access, and there is no third answer to forget to handle.</p>
 *
 * <p>Who refused, and why, is the part that has nowhere else to live. The engine's own callers each phrase
 * their own error message today and so lose it; carrying it here is what later lets an error message name the
 * plugin that governs the object and lets an audit record say the same.</p>
 */
public class AccessDeniedException extends Exception {

    private final AuthorizedSubject subject;
    private final AuthorizedResource resource;
    private final AccessRequirement requirement;
    private final String deniedBy;
    private final String explicitMessage;

    private AccessDeniedException(AuthorizedSubject subject, AuthorizedResource resource,
            AccessRequirement requirement, String deniedBy, String explicitMessage) {
        // No cause, no suppression, and above all no stack trace: filling one in costs more than the whole
        // decision that produced it, and this is thrown once per object a user may not see.
        super(null, null, false, false);
        this.subject = subject;
        this.resource = resource;
        this.requirement = requirement;
        this.deniedBy = deniedBy;
        this.explicitMessage = explicitMessage;
    }

    /**
     * The refusal of {@code requirement} on {@code resource}.
     *
     * @param deniedBy name of the authorization source that refused, or null when it does not identify itself
     */
    public static AccessDeniedException of(AuthorizedSubject subject, AuthorizedResource resource,
            AccessRequirement requirement, String deniedBy) {
        Objects.requireNonNull(subject, "subject is required");
        Objects.requireNonNull(resource, "resource is required");
        Objects.requireNonNull(requirement, "requirement is required");
        return new AccessDeniedException(subject, resource, requirement, deniedBy, null);
    }

    /**
     * A refusal that already has its wording, used where the message is itself the answer - a column check
     * refuses by naming the column that failed, and rephrasing it here would lose which one it was.
     */
    public static AccessDeniedException withMessage(String message, AuthorizedResource resource,
            String deniedBy) {
        return new AccessDeniedException(null, resource, null,
                deniedBy, Objects.requireNonNull(message, "message is required"));
    }

    /** What was being accessed. */
    public AuthorizedResource getResource() {
        return resource;
    }

    /** What was required on it, absent when the refusal came with its own wording. */
    public Optional<AccessRequirement> getRequirement() {
        return Optional.ofNullable(requirement);
    }

    /** The authorization source that refused, when it identifies itself. */
    public Optional<String> getDeniedBy() {
        return Optional.ofNullable(deniedBy);
    }

    @Override
    public String getMessage() {
        if (explicitMessage != null) {
            return explicitMessage;
        }
        StringBuilder message = new StringBuilder("Permission denied: user [").append(subject)
                .append("] does not have privilege for [").append(requirement)
                .append("] on [").append(resource).append("]");
        if (deniedBy != null) {
            message.append(", denied by [").append(deniedBy).append("]");
        }
        return message.toString();
    }
}
