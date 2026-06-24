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

package org.apache.doris.mysql.authenticate;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.authentication.Principal;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

public class AuthenticateResponse {
    public static final AuthenticateResponse failedResponse = new AuthenticateResponse(false);

    private boolean success;
    private UserIdentity userIdentity;
    private boolean isTemp = false;
    private Principal principal;
    private Set<String> authenticatedRoles = Collections.emptySet();
    private AuthenticationFailureSummary failureSummary;

    public AuthenticateResponse(boolean success) {
        this(success, null, false, null, Collections.emptySet(), null);
    }

    public AuthenticateResponse(boolean success, UserIdentity userIdentity) {
        this(success, userIdentity, false);
    }

    public AuthenticateResponse(boolean success, UserIdentity userIdentity, boolean isTemp) {
        this(success, userIdentity, isTemp, null, Collections.emptySet(), null);
    }

    public AuthenticateResponse(boolean success, UserIdentity userIdentity, boolean isTemp,
            Principal principal, Set<String> authenticatedRoles) {
        this(success, userIdentity, isTemp, principal, authenticatedRoles, null);
    }

    public AuthenticateResponse(boolean success, UserIdentity userIdentity, boolean isTemp,
            Principal principal, Set<String> authenticatedRoles, AuthenticationFailureSummary failureSummary) {
        this.success = success;
        this.userIdentity = userIdentity;
        this.isTemp = isTemp;
        this.principal = principal;
        this.authenticatedRoles = immutableAuthenticatedRoles(authenticatedRoles);
        this.failureSummary = failureSummary;
    }

    public static AuthenticateResponse failed(AuthenticationFailureSummary failureSummary) {
        return new AuthenticateResponse(false, null, false, null, Collections.emptySet(), failureSummary);
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public UserIdentity getUserIdentity() {
        return userIdentity;
    }

    public void setUserIdentity(UserIdentity userIdentity) {
        this.userIdentity = userIdentity;
    }

    public boolean isTemp() {
        return isTemp;
    }

    public void setTemp(boolean temp) {
        isTemp = temp;
    }

    public Principal getPrincipal() {
        return principal;
    }

    public void setPrincipal(Principal principal) {
        this.principal = principal;
    }

    public Set<String> getAuthenticatedRoles() {
        return authenticatedRoles;
    }

    public void setAuthenticatedRoles(Set<String> authenticatedRoles) {
        this.authenticatedRoles = immutableAuthenticatedRoles(authenticatedRoles);
    }

    public AuthenticationFailureSummary getFailureSummary() {
        return failureSummary;
    }

    public void setFailureSummary(AuthenticationFailureSummary failureSummary) {
        this.failureSummary = failureSummary;
    }

    private static Set<String> immutableAuthenticatedRoles(Set<String> authenticatedRoles) {
        if (authenticatedRoles.isEmpty()) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(new LinkedHashSet<>(authenticatedRoles));
    }

    @Override
    public String toString() {
        return "AuthenticateResponse{"
                + "success=" + success
                + ", userIdentity=" + userIdentity
                + ", isTemp=" + isTemp
                + ", principal=" + principal
                + ", authenticatedRoles=" + authenticatedRoles
                + ", failureSummary=" + failureSummary
                + '}';
    }
}
