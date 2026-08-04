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

/**
 * Shared external meta-cache framework, reused by fe-core and the connector plugins.
 *
 * <p>Under the parent-first {@code org.apache.doris.connector.*} prefix, so all instances load on the app
 * classloader with a single {@code Class} identity across the fe-core &harr; plugin boundary. Classes are
 * moved here from fe-core {@code org.apache.doris.datasource.metacache} + {@code org.apache.doris.common}
 * (see plan-doc/tasks/designs/metacache-framework-unification-design.md, Option A / P1).
 */
package org.apache.doris.connector.cache;
