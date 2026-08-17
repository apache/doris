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

/**
 * The requirements the engine asks about by name, so that a plugin can recognise which question it is being
 * asked.
 *
 * <p>Doris does not ask "may this subject select"; it asks for a set of actions and how many of them are
 * needed. A plugin that answers out of a model of its own has to map that back onto its own vocabulary, and
 * for that it must be able to tell "may this subject read the table" apart from "may this subject see that the
 * table exists" - two questions that differ only in which actions they name. Comparing against these
 * constants is how that is done; they are values, so equality is the comparison.
 *
 * <p>These are the questions the engine has always asked, named here rather than left implicit in the
 * built-in privilege model, because a plugin that lives outside this repository cannot read that model and
 * would otherwise have to restate the action sets and drift when one of them changes.
 *
 * <p>A requirement not listed here is not a defect and must not be treated as one: privilege checks are also
 * built at run time - granting a privilege requires holding both it and the right to grant it - and a plugin
 * that recognises none of these is free to answer from the action set alone.
 */
public final class AccessRequirements {

    /**
     * May the subject see that the object exists at all. Not a privilege anyone grants: holding any privilege
     * that implies knowledge of the object answers it.
     */
    public static final AccessRequirement VISIBILITY = AccessRequirement.anyOf(AccessAction.ADMIN,
            AccessAction.SELECT, AccessAction.LOAD, AccessAction.ALTER, AccessAction.CREATE,
            AccessAction.DROP, AccessAction.SHOW_VIEW);

    /** May the subject read the object's data. */
    public static final AccessRequirement SELECT = AccessRequirement.anyOf(AccessAction.ADMIN,
            AccessAction.SELECT);

    /** May the subject write data into the object. */
    public static final AccessRequirement LOAD = AccessRequirement.anyOf(AccessAction.ADMIN,
            AccessAction.LOAD);

    /** May the subject change the object's definition. */
    public static final AccessRequirement ALTER = AccessRequirement.anyOf(AccessAction.ADMIN,
            AccessAction.ALTER);

    /** May the subject create the object. */
    public static final AccessRequirement CREATE = AccessRequirement.anyOf(AccessAction.ADMIN,
            AccessAction.CREATE);

    /** May the subject drop the object. */
    public static final AccessRequirement DROP = AccessRequirement.anyOf(AccessAction.ADMIN,
            AccessAction.DROP);

    /** Is the subject an administrator of this instance. */
    public static final AccessRequirement ADMINISTRATION = AccessRequirement.of(AccessAction.ADMIN);

    /**
     * Does the subject hold any of the privileges that make an object usable. Asked where a statement needs
     * the object to be usable without knowing yet what will be done with it.
     *
     * <p>Not literally every action: granting and seeing a view's definition are not on the list, because
     * neither makes an object usable on its own. The set is the one Doris has always asked here and is not
     * open to adjustment - it has to keep matching {@code PrivPredicate.ALL}, which is what the built-in
     * model translates it back into.</p>
     */
    public static final AccessRequirement ANY_PRIVILEGE = AccessRequirement.anyOf(AccessAction.NODE,
            AccessAction.ADMIN, AccessAction.SELECT, AccessAction.LOAD, AccessAction.ALTER,
            AccessAction.CREATE, AccessAction.DROP, AccessAction.USAGE, AccessAction.CLUSTER_USAGE,
            AccessAction.STAGE_USAGE);

    private AccessRequirements() {
    }
}
