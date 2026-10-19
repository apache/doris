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

package org.apache.doris.foundation.security;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Strategy interface for executing code inside an authentication context (Kerberos doAs
 * or a plain passthrough), free of Hadoop API types.
 *
 * <p>This is the single doAs abstraction shared by fe-common, fe-filesystem, and fe-core.
 * Identity holders (UGI-based Kerberos/Simple authenticators) live in implementation
 * modules; framework code only sees this interface. Object-storage property models never
 * need an authentication context and use {@link #DIRECT}.</p>
 */
public interface ExecutionAuthenticator {

    /** Passthrough authenticator: runs the task directly with no authentication context. */
    ExecutionAuthenticator DIRECT = new ExecutionAuthenticator() {
        @Override
        public <T> T execute(Callable<T> task) throws Exception {
            return task.call();
        }
    };

    /**
     * Executes the given task within this authenticator's security context.
     *
     * @param task the task to execute
     * @param <T>  the result type of the task
     * @return the result of the task
     * @throws Exception if the task execution throws an exception
     */
    <T> T execute(Callable<T> task) throws Exception;

    /**
     * Executes the given task within this authenticator's security context.
     *
     * @param task the task to execute
     * @throws Exception if the task execution throws an exception
     */
    default void execute(Runnable task) throws Exception {
        execute(() -> {
            task.run();
            return null;
        });
    }

    /**
     * IO-typed convenience for filesystem callers: executes the action within this
     * authenticator's security context, surfacing failures as {@link IOException}.
     *
     * @param action the IO operation to execute
     * @param <T>    the return type
     * @return the result of the action
     * @throws IOException if the action throws or authentication fails
     */
    default <T> T doAs(IOCallable<T> action) throws IOException {
        try {
            return execute(action::call);
        } catch (IOException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
