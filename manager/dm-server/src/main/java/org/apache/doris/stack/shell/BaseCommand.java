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
    protected String errorResponse;

    protected abstract void buildCommand();

    public String getErrorResponse() {
        return this.errorResponse;
    }

    public boolean run() {
        buildCommand();
        log.info("run command: {}", StringUtils.join(resultCommand, " "));
        ProcessBuilder pb = new ProcessBuilder(resultCommand);
        Process process = null;
        BufferedReader bufferedReader = null;
        try {
            process = pb.start();
            bufferedReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            errorResponse = bufferedReader.lines().parallel().collect(Collectors.joining(System.lineSeparator()));
            final int exitCode = process.waitFor();
            if (exitCode == 0) {
                return true;
            } else {
                log.error("shell command error, exit with {}, response:{}", exitCode, errorResponse);
                return false;
            }
        } catch (IOException | InterruptedException e) {
            log.error("command execute fail", e);
            return false;
        } finally {
            if (process != null) {
                process.destroy();
            }
            try {
                if (bufferedReader != null) {
                    bufferedReader.close();
                }
            } catch (IOException e) {
                log.error("close buffered reader fail");
            }
        }
    }
}
