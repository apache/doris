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

package org.apache.doris.extension.loader.testplugins;

/**
 * Stands in for a dependency a plugin does not bundle. Loader tests fabricate plugin jars that
 * leave this class out and hand the loader a parent that refuses it, which is how a plugin missing
 * a shared library reaches the loader in production.
 */
public class AbsentDependencyProbe {

    public static Object marker() {
        return "absent-dependency-probe";
    }
}
