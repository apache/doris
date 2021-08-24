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

package org.apache.doris.stack.shell;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

/**
 * base command
 **/
@Slf4j
public abstract class BaseCommand {

    protected String[] resultCommand;
    protected Long timeout = 10000L;

    protected abstract void buildCommand();

    public Integer run() {
        buildCommand();
        log.info("run command: {}", StringUtils.join(resultCommand, " "));
        ProcessBuilder pb = new ProcessBuilder(resultCommand);
        int exitCode = 1;
        try {
            Process process = pb.start();
            if (waitForProcessTermination(process, timeout)) {
                exitCode = process.exitValue();
            } else {
                process.destroy();
            }
            if (exitCode != 0) {
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
                String errorStream = bufferedReader.lines().parallel().collect(Collectors.joining(System.lineSeparator()));
                log.info("shell command error response:{}", errorStream);
            }
        } catch (IOException ie) {
            log.error("command execute fail", ie);
        }
        return exitCode;
    }

    /**
     * Waits until the process has terminated or waiting time elapses.
     *
     * @param timeout time to wait in miliseconds
     * @return true if process has exited, false otherwise
     */
    protected boolean waitForProcessTermination(Process process, long timeout) {
        long startTime = System.currentTimeMillis();
        do {
            try {
                process.exitValue();
                return true;
            } catch (IllegalThreadStateException ignored) {
                log.error("process exception");
                ignored.printStackTrace();
            }
            // Check if process has terminated once per second
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        while (System.currentTimeMillis() - startTime < timeout);

        return false;
    }
}
