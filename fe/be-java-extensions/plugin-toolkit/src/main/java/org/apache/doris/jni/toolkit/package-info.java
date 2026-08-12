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
 * Helpers a plugin may compile against, copied into each plugin that asks for them.
 *
 * <h2>Shared source, not shared classes</h2>
 *
 * <p>This jar is not on BE's classpath. A plugin declares an ordinary compile dependency and ends
 * up with its own copy in its own directory, so two plugins using the same helper run two separate
 * classes with separate statics. That is intended: a fix here reaches both, but neither can be
 * disturbed by the other. It also means every static field in this package is per-plugin state,
 * which is worth remembering when writing caches.
 *
 * <h2>What belongs here</h2>
 *
 * <p>Code with more than one plugin needing it, and no third-party dependency. A dependency added
 * here is imposed on every plugin that wanted any part of the toolkit, so code that needs a library
 * belongs in the plugin that needs it. A helper with a single consumer belongs in that consumer:
 * copying one class into one plugin through a shared module buys nothing and hides where the code
 * is used.
 *
 * <h2>Logging from inside a plugin</h2>
 *
 * <p>BE's plugin runtime configures {@code java.util.logging} to write {@code log/jni.log}, and JUL
 * is the one backend every classloader resolves identically. A plugin therefore keeps whatever
 * logging facade its libraries already use and adds the matching bridge as a runtime dependency, so
 * that its output lands in the same file as everything else:
 *
 * <ul>
 *   <li>slf4j callers: {@code org.slf4j:slf4j-jdk14}</li>
 *   <li>log4j2 callers: {@code org.apache.logging.log4j:log4j-to-jul}</li>
 *   <li>log4j 1.x callers: {@code org.apache.logging.log4j:log4j-1.2-api} plus the bridge above</li>
 *   <li>commons-logging callers: {@code org.slf4j:jcl-over-slf4j} plus {@code slf4j-jdk14}</li>
 * </ul>
 *
 * <p>A plugin that instead packages {@code log4j-core} gets a second, independent logging
 * implementation writing wherever its own configuration says - usually nowhere, because the
 * configuration file BE ships is not on the plugin's classpath.
 */
package org.apache.doris.jni.toolkit;
