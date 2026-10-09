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

package org.apache.doris.nereids.trees.plans.commands.insert;

/**
 * Marker for commands that can be cancelled from another thread and whose
 * execution completion can be awaited.
 *
 * <p>{@code cancel()} must be idempotent and is expected to be safe to call even
 * while the command is still executing. {@code waitNotRunning()} blocks until the
 * current {@code run()} invocation has returned (or a bounded timeout elapses),
 * so a caller can be sure the transaction outcome is decided before returning.
 *
 * <p>Both methods are safe to invoke repeatedly.
 */
public interface CancelableCommand {

    /** Requests cancellation of the executing command. Idempotent. */
    void cancel();

    /** Waits (bounded) until the command's {@code run()} has finished. */
    void waitNotRunning();
}
