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
 * The contract between BE and its Java plugins, in both directions: what a plugin implements
 * ({@link org.apache.doris.jni.spi.DorisPlugin} and the factories, {@link
 * org.apache.doris.jni.spi.JniScanner}, {@link org.apache.doris.jni.spi.JniWriter}) and what BE
 * hands it (the off-heap data plane under {@code vec} and {@code utils}).
 *
 * <h2>This package name is load-bearing</h2>
 *
 * <p>{@code org.apache.doris.jni.spi.} is the single prefix the plugin classloader delegates to
 * BE's own classloader; every other class a plugin references is resolved from the plugin's own
 * directory or from the JDK, and nothing else on BE's classpath is reachable. So a type is in this
 * package if and only if BE and the plugin must agree on one class identity for it. Two
 * consequences follow, and both are enforced by the build:
 *
 * <ul>
 *   <li><b>Zero third-party dependencies.</b> Anything reachable from here would have to be shared
 *       with every plugin, which is exactly the coupling this architecture exists to remove. The
 *       data plane pays for this by talking off-heap addresses and {@code Map<String,String>}
 *       instead of a serialization library. Thrift, hadoop, guava, arrow and any logging
 *       implementation are all out; the one payload that genuinely crosses the boundary as thrift,
 *       the UDF constructor parameters, crosses it as {@code byte[]}.</li>
 *   <li><b>A plugin must never package these classes.</b> Plugins depend on the SPI in
 *       {@code provided} scope. A plugin that ships its own copy gets a class-cast failure at the
 *       first call, and the loader reports it with that specific diagnosis rather than letting it
 *       surface as an unrelated error.</li>
 * </ul>
 *
 * <h2>Stability</h2>
 *
 * <p>The whole surface is unstable in v1: Doris does not yet promise source or binary
 * compatibility to plugins built outside this repository, and all plugins that consume it ship in
 * the same release. What the build does promise is that mismatches are caught rather than
 * misbehaving — see {@link org.apache.doris.jni.spi.SpiVersion} for the version gate and its
 * comparison rule. Once the surface settles, the intended next steps are a revapi gate and a
 * deprecate-one-release-before-removal policy.
 *
 * <p>The off-heap layout, the type-string grammar and the reserved parameter keys are not source
 * API but a byte-level protocol shared with BE C++; they are specified in {@code PROTOCOL.md} in
 * this module and must be changed on both sides at once.
 */
package org.apache.doris.jni.spi;
