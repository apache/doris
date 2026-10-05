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

package org.apache.doris.authentication.rolemapping;

import org.apache.doris.authentication.AuthenticationException;
import org.apache.doris.authentication.AuthenticationIntegration;
import org.apache.doris.authentication.Principal;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Default role-mapping evaluator that grants no additional roles.
 */
public final class NoOpRoleMappingEvaluator implements RoleMappingEvaluator {

    @Override
    public Set<String> evaluate(AuthenticationIntegration integration, Principal principal)
            throws AuthenticationException {
        Objects.requireNonNull(integration, "integration");
        Objects.requireNonNull(principal, "principal");
        return Collections.emptySet();
    }
}
