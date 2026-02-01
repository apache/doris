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

package org.apache.doris.authentication;

/**
 * Authentication binding - binding relationship between user/role and AuthenticationProfile.
 *
 * <p>Similar to MySQL's authentication_plugin mechanism,
 * allows specifying which authentication profile to use for a user.
 */
public class AuthenticationBinding {

    /** Binding type */
    public enum BindingType {
        USER,      // Bind to user
        ROLE,      // Bind to role
        DEFAULT    // Default binding
    }

    private final BindingType type;

    /** Binding target name (username/role name) */
    private final String targetName;

    /** Bound AuthenticationProfile name */
    private final String profileName;

    /** Priority (when multiple bindings exist for same target) */
    private final int priority;

    /** Whether mandatory (not allowed to fallback to other Profile) */
    private final boolean mandatory;

    public AuthenticationBinding(BindingType type, String targetName, String profileName,
                      int priority, boolean mandatory) {
        this.type = type;
        this.targetName = targetName;
        this.profileName = profileName;
        this.priority = priority;
        this.mandatory = mandatory;
    }

    public BindingType getType() {
        return type;
    }

    public String getTargetName() {
        return targetName;
    }

    public String getProfileName() {
        return profileName;
    }

    public int getPriority() {
        return priority;
    }

    public boolean isMandatory() {
        return mandatory;
    }
}
