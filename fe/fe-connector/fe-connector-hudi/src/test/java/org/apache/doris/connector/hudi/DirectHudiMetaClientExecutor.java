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

package org.apache.doris.connector.hudi;

import java.util.concurrent.Callable;

/**
 * Test {@link HudiMetaClientExecutor} that runs the action directly (no plugin auth, no TCCL pin). Used by
 * tests that do not exercise the metaClient-touching partition/snapshot paths; a checked exception surfaces
 * as an unchecked wrapper so a mis-set-up fixture fails loud.
 */
final class DirectHudiMetaClientExecutor implements HudiMetaClientExecutor {
    @Override
    public <T> T execute(Callable<T> action) {
        try {
            return action.call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
