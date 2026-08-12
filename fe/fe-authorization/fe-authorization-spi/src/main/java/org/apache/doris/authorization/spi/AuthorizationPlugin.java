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

import org.apache.doris.authorization.AccessAction;
import org.apache.doris.authorization.AccessContext;
import org.apache.doris.authorization.AccessDeniedException;
import org.apache.doris.authorization.AccessRequirement;
import org.apache.doris.authorization.ActionMatch;
import org.apache.doris.authorization.AuthorizedResource;
import org.apache.doris.authorization.AuthorizedSubject;
import org.apache.doris.authorization.DataMaskSpec;
import org.apache.doris.authorization.RowFilterSpec;
import org.apache.doris.extension.spi.Plugin;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An authorization source: whatever decides, for the resources it governs, what a user may do with them.
 *
 * <p>Doris ships one of these for its own {@code GRANT} model, and one per external system it integrates
 * with. Which plugin is asked follows from the resource alone - the plugin a catalog is bound to answers for
 * everything inside that catalog, the plugin installed for the instance answers for everything else - and its
 * answer is the whole answer: nothing grants access before it is asked and no second plugin is consulted
 * after it. That is what makes the policies in force on an object readable from the configuration, and it is
 * why exemptions that used to be the engine's ("an administrator may go anywhere") are each plugin's own to
 * grant or refuse, with {@link AuthorizationContext} there to ask the questions such a decision needs.</p>
 *
 * <h2>Refusing</h2>
 *
 * <p>A check that returns has allowed the access; a check that refuses throws {@link
 * AccessDeniedException}. There is deliberately no third outcome for a plugin to express and no boolean for a
 * caller to ignore. Every check method here defaults to refusing, so a plugin gets access control for the
 * questions it did not think about rather than a hole; the two data-policy methods are the exception, and
 * their empty default means "this source defines no policy", which is not the same as allowing anything.</p>
 *
 * <h2>What is asked, and how often</h2>
 *
 * <p>The engine asks about a whole requirement - a set of actions plus whether one of them or all of them are
 * needed - rather than one action at a time, because sources answer that way: the built-in model tests a bit
 * set in one pass, and a policy engine walking a resource hierarchy remembers which actions an outer level
 * already granted. A plugin with no such structure implements {@link #checkAction} alone and lets the default
 * below take the requirement apart.</p>
 *
 * <p>These methods are on the path of every statement, several times over: planning one query checks each
 * table it reads, and listing what a user may see checks every object that exists. Whatever caching an
 * external source needs belongs inside the plugin, where it can be invalidated on the source's own terms; the
 * engine adds none of its own and cannot, since it does not know when a policy changed.</p>
 */
public interface AuthorizationPlugin extends Plugin {

    /** The name this source is configured and reported under, e.g. {@code "ranger-doris"}. */
    String name();

    /**
     * Refuses unless {@code subject} may act on {@code resource} as {@code requirement} demands.
     *
     * <p>The default takes the requirement apart and asks {@link #checkAction} about one action at a time,
     * which is correct for any source but costs one evaluation per action. Override when the source can
     * answer about a set of actions in one go.</p>
     *
     * @param resource what is being accessed; a plugin should treat a kind it does not recognise as a
     *         refusal rather than guess, the resource kinds being closed and known at compile time
     * @param context circumstances of the statement asking, for a decision that depends on more than the
     *         subject and the resource; ignorable
     * @throws AccessDeniedException if the access is not allowed
     */
    default void checkPrivilege(AuthorizedSubject subject, AuthorizedResource resource,
            AccessRequirement requirement, AccessContext context) throws AccessDeniedException {
        if (requirement.getMatch() == ActionMatch.ALL) {
            for (AccessAction action : requirement.getActions()) {
                checkAction(subject, resource, action, context);
            }
            return;
        }
        AccessDeniedException refused = null;
        for (AccessAction action : requirement.getActions()) {
            try {
                checkAction(subject, resource, action, context);
                return;
            } catch (AccessDeniedException e) {
                refused = e;
            }
        }
        // A requirement always names at least one action, so one refusal was recorded; report that one
        // rather than a summary, because it says which action was actually tested last.
        throw refused != null ? refused
                : AccessDeniedException.of(subject, resource, requirement, name());
    }

    /**
     * Refuses unless {@code subject} may perform {@code action} on {@code resource}.
     *
     * <p>This is the hook the default {@link #checkPrivilege} takes a requirement apart into, and the engine
     * reaches it only that way - it always asks about a whole requirement. So a plugin implements this one
     * <em>or</em> {@link #checkPrivilege}, whichever matches how its source answers, and leaving the other at
     * its default is not a hole: nothing calls a {@code checkAction} that a plugin answering whole
     * requirements never meant to provide.</p>
     *
     * @throws AccessDeniedException if the access is not allowed
     */
    default void checkAction(AuthorizedSubject subject, AuthorizedResource resource, AccessAction action,
            AccessContext context) throws AccessDeniedException {
        throw AccessDeniedException.of(subject, resource, AccessRequirement.of(action), name());
    }

    /**
     * The row-level filters this source imposes on {@code table} for {@code subject}, empty when it imposes
     * none.
     *
     * <p>Each filter is a SQL predicate in Doris dialect; the engine parses it, type-checks it and plans it
     * as a filter over the table, combining several of them as each one's merge type says. Returning no
     * filter is not a decision about access - whether the table may be read at all was settled by {@link
     * #checkPrivilege}.</p>
     */
    default List<RowFilterSpec> getRowFilters(AuthorizedSubject subject, AuthorizedResource.Table table,
            AccessContext context) {
        return Collections.emptyList();
    }

    /**
     * How the named columns of {@code table} must be rewritten before {@code subject} may read them: an entry
     * per column that is masked, keyed by the column name as it was asked about, and no entry for a column
     * that is not.
     *
     * <p>Asked about all the columns at once because a source that answers over the network would otherwise
     * be asked once per column of every table in the statement.</p>
     */
    default Map<String, DataMaskSpec> getDataMasks(AuthorizedSubject subject, AuthorizedResource.Table table,
            Set<String> columns, AccessContext context) {
        return Collections.emptyMap();
    }
}
