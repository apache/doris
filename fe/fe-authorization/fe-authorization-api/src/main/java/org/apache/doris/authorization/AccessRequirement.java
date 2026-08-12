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

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/**
 * What a caller needs on a resource: a set of actions and how many of them must be held.
 *
 * <p>A requirement is asked as a whole rather than one action at a time, because the two sources of
 * authorization decisions answer it as a whole. The built-in model tests a bit set in a single pass; the
 * Ranger plugin walks the resource hierarchy asking about one privilege per request and remembers which ones
 * an outer level already granted, so an already-answered privilege is never asked about twice. Handing them
 * one action at a time would replace both with N independent evaluations of the same policies - the same
 * answer, several times the cost. A plugin that has no such structure can still be written one action at a
 * time and let the SPI's default implementation take this apart.</p>
 *
 * <p>The action set is never empty. An empty requirement is not a harmless edge case: read as {@link
 * ActionMatch#ALL} it is satisfied by everyone, which is a silent grant of everything the caller was
 * checking.</p>
 */
public final class AccessRequirement {

    private final Set<AccessAction> actions;
    private final ActionMatch match;

    private AccessRequirement(Set<AccessAction> actions, ActionMatch match) {
        this.actions = actions;
        this.match = match;
    }

    /** A requirement satisfied by holding at least one of {@code actions}. */
    public static AccessRequirement anyOf(AccessAction... actions) {
        return of(toSet(actions), ActionMatch.ANY);
    }

    /** A requirement satisfied only by holding every one of {@code actions}. */
    public static AccessRequirement allOf(AccessAction... actions) {
        return of(toSet(actions), ActionMatch.ALL);
    }

    /** A requirement for a single action; the match type is irrelevant for one action. */
    public static AccessRequirement of(AccessAction action) {
        return of(EnumSet.of(Objects.requireNonNull(action, "action is required")), ActionMatch.ANY);
    }

    /**
     * @param actions the actions in question, at least one
     * @param match whether one of them suffices or all of them are needed
     */
    public static AccessRequirement of(Collection<AccessAction> actions, ActionMatch match) {
        Objects.requireNonNull(actions, "actions is required");
        Objects.requireNonNull(match, "match is required");
        if (actions.isEmpty()) {
            throw new IllegalArgumentException("an access requirement must name at least one action");
        }
        EnumSet<AccessAction> copy = EnumSet.noneOf(AccessAction.class);
        for (AccessAction action : actions) {
            copy.add(Objects.requireNonNull(action, "actions must not contain null"));
        }
        return new AccessRequirement(Collections.unmodifiableSet(copy), match);
    }

    public Set<AccessAction> getActions() {
        return actions;
    }

    public ActionMatch getMatch() {
        return match;
    }

    /**
     * Whether {@code granted} - everything the subject holds on the resource - satisfies this requirement.
     * Provided so that every implementation reads the match type the same way instead of writing its own
     * {@code containsAll} versus {@code disjoint} by hand.
     */
    public boolean isSatisfiedBy(Set<AccessAction> granted) {
        Objects.requireNonNull(granted, "granted is required");
        return match == ActionMatch.ALL ? granted.containsAll(actions) : !Collections.disjoint(granted, actions);
    }

    private static EnumSet<AccessAction> toSet(AccessAction... actions) {
        Objects.requireNonNull(actions, "actions is required");
        EnumSet<AccessAction> set = EnumSet.noneOf(AccessAction.class);
        for (AccessAction action : actions) {
            set.add(Objects.requireNonNull(action, "actions must not contain null"));
        }
        return set;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AccessRequirement)) {
            return false;
        }
        AccessRequirement that = (AccessRequirement) o;
        return match == that.match && actions.equals(that.actions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(actions, match);
    }

    @Override
    public String toString() {
        return match + actions.toString();
    }
}
