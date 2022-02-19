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

package org.apache.doris.regression

import groovy.transform.CompileStatic
import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.HelpFormatter
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options

@CompileStatic
class ConfigOptions {
    static Option helpOption
    static Option confFileOpt
    static Option defaultDbOpt
    static Option jdbcOpt
    static Option userOpt
    static Option passwordOpt
    static Option feHttpAddressOpt
    static Option feHttpUserOpt
    static Option feHttpPasswordOpt
    static Option pathOpt
    static Option dataOpt
    static Option suiteOpt
    static Option groupsOpt
    static Option confOpt
    static Option genOutOpt
    static Option forceGenOutOpt

    static CommandLine initCommands(String[] args) {
        helpOption = Option.builder("h")
                .required(false)
                .hasArg(false)
                .longOpt("help")
                .desc("print this usage help")
                .build()
        confFileOpt = Option.builder("cf")
                .argName("confFilePath")
                .required(false)
                .hasArg(true)
                .type(String.class)
                .longOpt("confFile")
                .desc("the configure file path")
                .build()
        defaultDbOpt = Option.builder("db")
                .argName("db")
                .required(false)
                .hasArg(true)
                .type(String.class)
                .longOpt("defaultDb")
                .desc("default db")
                .build()
        jdbcOpt = Option.builder("c")
                .argName("url")
                .required(false)
                .hasArg(true)
                .type(String.class)
                .longOpt("jdbc")
                .desc("jdbc url")
                .build()
        userOpt = Option.builder("u")
                .argName("user")
                .required(false)
                .hasArg(true)
                .type(String.class)
                .longOpt("user")
                .desc("the username of jdbc connection")
                .build()
        passwordOpt = Option.builder("p")
                .argName("password")
                .required(false)
                .hasArg(true)
                .optionalArg(true)
                .type(String.class)
                .longOpt("password")
                .desc("the password of jdbc connection")
                .build()
        pathOpt = Option.builder("P")
                .argName("path")
                .required(false)
                .hasArg(true)
                .type(String.class)
                .longOpt("path")
                .desc("the suite path")
                .build()
        dataOpt = Option.builder("D")
                .argName("dataPath")
                .required(false)
                .hasArg(true)
                .type(String.class)
                .longOpt("dataPath")
                .desc("the data path")
                .build()
        suiteOpt = Option.builder("s")
                .argName("suiteName")
                .required(false)
                .hasArg(true)
                .optionalArg(true)
                .type(String.class)
                .longOpt("suite")
                .desc("the suite name wildcard to be test")
                .build()
        groupsOpt = Option.builder("g")
                .argName("groups")
                .required(false)
                .hasArg(true)
                .optionalArg(true)
                .type(String.class)
                .longOpt("groups")
                .desc("the suite group to be test")
                .build()
        feHttpAddressOpt = Option.builder("ha")
                .argName("address")
                .required(false)
                .hasArg(true)
                .type(String.class)
                .longOpt("feHttpAddress")
                .desc("the fe http address, format is ip:port")
                .build()
        feHttpUserOpt = Option.builder("hu")
                .argName("userName")
                .required(false)
                .hasArg(true)
                .type(String.class)
                .longOpt("feHttpUser")
                .desc("the user of fe http server")
                .build()
        feHttpPasswordOpt = Option.builder("hp")
                .argName("password")
                .required(false)
                .hasArg(true)
                .type(String.class)
                .longOpt("feHttpPassword")
                .desc("the password of fe http server")
                .build()
        genOutOpt = Option.builder("genOut")
                .required(false)
                .hasArg(false)
                .desc("generate qt .out file if not exist")
                .build()
        forceGenOutOpt = Option.builder("forceGenOut")
                .required(false)
                .hasArg(false)
                .desc("delete and generate qt .out file")
                .build()
        confOpt = Option.builder("conf")
                .argName("conf")
                .required(false)
                .hasArgs()
                .valueSeparator(('=' as char))
                .longOpt("configurations, format: key=value")
                .desc("set addition context configurations")
                .build()

        Options options = new Options()
                .addOption(helpOption)
                .addOption(jdbcOpt)
                .addOption(userOpt)
                .addOption(passwordOpt)
                .addOption(pathOpt)
                .addOption(dataOpt)
                .addOption(confOpt)
                .addOption(suiteOpt)
                .addOption(groupsOpt)
                .addOption(feHttpAddressOpt)
                .addOption(feHttpUserOpt)
                .addOption(feHttpPasswordOpt)
                .addOption(genOutOpt)
                .addOption(confFileOpt)
                .addOption(forceGenOutOpt)

        CommandLine cmd = new DefaultParser().parse(options, args, true)
        if (cmd.hasOption(helpOption)) {
            printHelp(options)
            return null
        }
        return cmd
    }

    static void printHelp(Options options) {
        HelpFormatter hf = new HelpFormatter()
        hf.printHelp("regression-test", options, true)
    }
}
