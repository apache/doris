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
 * The plugin runtime BE drives.
 *
 * <p>Deliberately <em>not</em> under {@code org.apache.doris.jni.spi.}: that prefix is what a
 * plugin classloader delegates to BE, so anything placed there becomes part of the contract. The
 * runtime is BE's side of the boundary and no plugin can see it, which is why it is free to change
 * without a plugin API version bump.
 *
 * <p>{@link org.apache.doris.jni.bootstrap.PluginRegistry} is the only entry point BE calls;
 * everything else is an implementation detail of loading a directory of jars in isolation.
 */
package org.apache.doris.jni.bootstrap;
