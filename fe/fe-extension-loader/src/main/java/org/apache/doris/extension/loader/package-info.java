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
 * Runtime loading infrastructure for Doris FE extension plugins.
 *
 * <p>This package provides directory-driven plugin loading for FE runtime:
 * scanning plugin roots, resolving plugin jars, creating classloaders with
 * child-first policy (plus configurable parent-first prefixes), discovering
 * typed factories via {@code ServiceLoader}, and exposing standardized load
 * outcomes via {@link org.apache.doris.extension.loader.LoadReport},
 * {@link org.apache.doris.extension.loader.LoadFailure}, and
 * {@link org.apache.doris.extension.loader.PluginHandle}.
 *
 * <p>Current runtime scope is load-only: {@code loadAll}, {@code get}, and
 * {@code list}. Runtime {@code reload}/{@code unload}, automatic directory
 * watch, and remote repository download/verification are out of scope.
 *
 * <p>This package provides generic runtime capability only and does not carry
 * business-domain semantics.
 */
package org.apache.doris.extension.loader;
