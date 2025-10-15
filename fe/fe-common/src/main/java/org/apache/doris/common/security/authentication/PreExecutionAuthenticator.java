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

import org.apache.hadoop.conf.Configuration;

import java.util.concurrent.Callable;

/**
 * PreExecutionAuthenticator is a utility class that ensures specified tasks
 * are executed with necessary authentication, particularly useful for systems
 * like Hadoop that require Kerberos-based pre-execution authentication.
 *
 * <p>If a HadoopAuthenticator is provided, this class will execute tasks
 * within a privileged context using Hadoop's authentication mechanisms
 * (such as Kerberos). Otherwise, it will execute tasks normally.
 */
public class PreExecutionAuthenticator {

    private HadoopAuthenticator hadoopAuthenticator;

    /**
     * Default constructor for PreExecutionAuthenticator.
     * This allows setting the HadoopAuthenticator at a later point if needed.
     */
    public PreExecutionAuthenticator() {
    }

    /**
     * Constructor to initialize the PreExecutionAuthenticator object.
     * This constructor is responsible for initializing the Hadoop authenticator required for Kerberos authentication
     * based on the provided configuration information.
     *
     * @param configuration Configuration information used to obtain Kerberos authentication settings
     */
    public PreExecutionAuthenticator(Configuration configuration) {
        AuthenticationConfig config = AuthenticationConfig.getKerberosConfig(configuration);
        this.hadoopAuthenticator = HadoopAuthenticator.getHadoopAuthenticator(config);
    }

    public PreExecutionAuthenticator(HadoopAuthenticator hadoopAuthenticator) {
        this.hadoopAuthenticator = hadoopAuthenticator;
    }


    /**
     * Executes the specified task with necessary authentication.
     * <p>If a HadoopAuthenticator is set, the task will be executed within a
     * privileged context using the doAs method. If no authenticator is present,
     * the task will be executed directly.
     *
     * @param task The task to execute, represented as a Callable
     * @param <T>  The type of the result returned by the task
     * @return The result of the executed task
     * @throws Exception If an exception occurs during task execution
     */
    public <T> T execute(Callable<T> task) throws Exception {
        if (hadoopAuthenticator != null) {
            // Adapts Callable to PrivilegedExceptionAction for use with Hadoop authentication
            return hadoopAuthenticator.doAs(task::call);
        } else {
            // Executes the task directly if no authentication is needed
            return task.call();
        }
    }

    public void execute(Runnable task) throws Exception {
        if (hadoopAuthenticator != null) {
            // Adapts Runnable to PrivilegedExceptionAction for use with Hadoop authentication
            hadoopAuthenticator.doAs(() -> {
                task.run();
                return null;
            });
        } else {
            // Executes the task directly if no authentication is needed
            task.run();
        }
    }

    /**
     * Retrieves the current HadoopAuthenticator.
     * <p>This allows checking if a HadoopAuthenticator is configured or
     * changing it at runtime.
     *
     * @return The current HadoopAuthenticator instance, or null if none is set
     */
    public HadoopAuthenticator getHadoopAuthenticator() {
        return hadoopAuthenticator;
    }

    /**
     * Sets the HadoopAuthenticator, enabling pre-execution authentication
     * for tasks requiring privileged access.
     *
     * @param hadoopAuthenticator An instance of HadoopAuthenticator to be used
     */
    public void setHadoopAuthenticator(HadoopAuthenticator hadoopAuthenticator) {
        this.hadoopAuthenticator = hadoopAuthenticator;
    }
}
