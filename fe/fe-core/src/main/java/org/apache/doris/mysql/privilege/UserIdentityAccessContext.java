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

package org.apache.doris.mysql.privilege;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.authorization.AccessContext;
import org.apache.doris.authorization.AuthorizedSubject;

import java.util.Optional;

/** Keeps request-scoped identity information across the account-only authorization plugin boundary. */
final class UserIdentityAccessContext implements AccessContext {
    private final UserIdentity identity;
    private final AccessContext delegate;

    private UserIdentityAccessContext(UserIdentity identity, AccessContext delegate) {
        this.identity = identity;
        this.delegate = delegate;
    }

    static AccessContext of(UserIdentity identity, AccessContext context) {
        if (context instanceof ConnectionAccessContext) {
            identity = ((ConnectionAccessContext) context).userIdentityWithRoles(identity);
        }
        if (identity.getAuthenticatedRoles().isEmpty()) {
            return context;
        }
        return new UserIdentityAccessContext(identity, context);
    }

    UserIdentity userIdentityOf(AuthorizedSubject subject) {
        if (identity.getUser().equals(subject.getUser()) && identity.getHost().equals(subject.getHost())
                && identity.isDomain() == subject.isDomain()) {
            return identity;
        }
        return AccessTranslation.userIdentityOf(subject);
    }

    @Override
    public Optional<String> getClientIp() {
        return delegate.getClientIp();
    }

    @Override
    public Optional<String> getQueryId() {
        return delegate.getQueryId();
    }
}
