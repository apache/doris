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

package org.apache.doris.authentication.handler;

import org.apache.doris.authentication.AuthenticationRequest;
import org.apache.doris.authentication.Principal;
import org.apache.doris.authentication.Subject;

import java.util.Set;

/**
 * Builds {@link Subject} objects for authorization.
 */
public interface SubjectBuilder {

    /**
     * Build a Subject from the principal, mapped roles, and request context.
     */
    Subject build(Principal principal, Set<String> mappedRoles, AuthenticationRequest request);
}
