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

package org.apache.doris.load.loadv2.etl;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.io.Serializable;

public class SparkLoadCommand implements Serializable {

    public static SparkLoadCommand parse(String[] args) {
        SparkLoadCommand command = new SparkLoadCommand();
        JCommander commander = JCommander
                .newBuilder()
                .programName(SparkLoadCommand.class.getName())
                .addObject(command)
                .build();
        commander.parse(args);
        return command;
    }

    @Parameter(
            names = {"-c", "--config-file"},
            description = "配置文件路径",
            required = true
    )
    private String configFile;

    @Parameter(
            names = {"--enable-hive"},
            description = "使能hive"
    )
    private boolean enableHive = false;

    @Parameter(
            names = {"--debug-output-file-groups-path"},
            description = "把源文件输出到指定路径"
    )
    private String debugFileGroupPath;

    @Parameter(
            names = {"-o", "--output-path"},
            description = "文件输出路径",
            required = true
    )
    private String outputPath;

    @Parameter(
            names = {"--strictMode"},
            description = "文件输出路径"
    )
    private boolean strictMode = false;


    @Parameter(
            names = {"-t", "--table-id"},
            description = "指定表执行"
    )
    private Long tableId;

    public String getConfigFile() {
        return configFile;
    }

    public boolean getEnableHive() {
        return enableHive;
    }

    public String getDebugFileGroupPath() {
        return debugFileGroupPath;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public boolean getStrictMode() {
        return strictMode;
    }

    public Long getTableId() {
        return tableId;
    }
}
