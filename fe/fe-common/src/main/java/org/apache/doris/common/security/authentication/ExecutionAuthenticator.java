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

package org.apache.doris.common.security.authentication;

import java.util.concurrent.Callable;

/**
 * Strategy interface for executing code with optional authentication context.
 * <p>
 * Implementations of this interface can wrap the execution of tasks (such as {@link Callable}
 * or {@link Runnable}) with authentication logic, for example using Kerberos or other mechanisms.
 * If no authentication is needed, the default implementations simply execute the tasks directly.
 * </p>
 */
public interface ExecutionAuthenticator {

    /**
     * Executes the given task, optionally within an authenticated context.
     * <p>
     * Implementations may override this method to provide custom authentication logic
     * before executing the given {@link Callable} task.
     *
     * @param task the task to execute
     * @param <T>  the result type of the task
     * @return the result of the task
     * @throws Exception if the task execution throws an exception
     */
    default <T> T execute(Callable<T> task) throws Exception {
        return task.call();
    }

    /**
     * Executes the given task, optionally within an authenticated context.
     * <p>
     * Implementations may override this method to provide custom authentication logic
     * before executing the given {@link Runnable} task.
     *
     * @param task the task to execute
     * @throws Exception if the task execution throws an exception
     */
    default void execute(Runnable task) throws Exception {
        task.run();
    }
}
