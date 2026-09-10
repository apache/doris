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
 * One privilege an authorization source can grant on a resource.
 *
 * <p>The constants stand one-to-one with the privileges Doris itself grants, and that is a requirement
 * rather than a coincidence: the built-in privilege model is one of the authorization sources, so any
 * folding here - "cluster usage and stage usage are both just usage", say - would leave it unable to tell
 * which privilege it was actually asked about, and it would answer a different question than the caller
 * asked.</p>
 *
 * <p>There is deliberately no {@code SHOW} action. "May this subject see the object at all" is not a
 * privilege anyone grants; it is the question "does the subject hold <em>any</em> of the privileges that
 * imply visibility", which {@link AccessRequirement#anyOf} expresses.</p>
 */
public enum AccessAction {
    /** Cluster node operations. Only ever granted globally. */
    NODE,
    /** Administration of the cluster. */
    ADMIN,
    /** Granting privileges to others. */
    GRANT,
    /** Reading data. */
    SELECT,
    /** Writing data. */
    LOAD,
    /** Altering an object's definition. */
    ALTER,
    /** Creating an object. */
    CREATE,
    /** Dropping an object. */
    DROP,
    /** Using a resource or a workload group. */
    USAGE,
    /** Using a compute group. Distinct from {@link #USAGE}: it is a privilege of its own. */
    CLUSTER_USAGE,
    /** Using a stage. Distinct from {@link #USAGE}: it is a privilege of its own. */
    STAGE_USAGE,
    /** Reading a view's definition. */
    SHOW_VIEW
}
