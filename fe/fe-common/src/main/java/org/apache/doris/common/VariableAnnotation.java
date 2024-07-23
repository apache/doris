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

package org.apache.doris.common;

public enum VariableAnnotation {
    NONE(""),
    // A deprecated item and it will be deleted in future
    DEPRECATED("deprecated_"),
    // An experimental item, it will be shown with `experimental_` prefix
    // And user can set it with or without `experimental_` prefix.
    EXPERIMENTAL("experimental_"),
    // A previous experimental item but now it is GA.
    // it will be shown without `experimental_` prefix.
    // But user can set it with or without `experimental_` prefix, for compatibility.
    EXPERIMENTAL_ONLINE(""),
    // A removed item, not show in variable list, but not throw exception when user call set for it.
    REMOVED("");
    private final String prefix;

    VariableAnnotation(String prefix) {
        this.prefix = prefix;
    }

    public String getPrefix() {
        return prefix;
    }
}
