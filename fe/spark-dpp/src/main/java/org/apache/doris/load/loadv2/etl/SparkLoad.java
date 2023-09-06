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

import org.apache.doris.load.loadv2.dpp.SparkLoadJobV2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkLoad {

    private static final Logger LOG = LoggerFactory.getLogger(SparkLoad.class);

    public static void main(String[] args) {

        // parse args
        SparkLoadCommand command = SparkLoadCommand.parse(args);

        // get sparkEnv session env
        SparkLoadSparkEnv sparkEnv = SparkLoadSparkEnv.build(command);

        try {
            // parse config
            SparkLoadConf sparkLoadConf = SparkLoadConf.build(
                    command, sparkEnv.getSerializableConfigurationHadoopConf());

            // execute job
            new SparkLoadJobV2(sparkEnv, sparkLoadConf).doDpp();

        } catch (Throwable e) {
            LOG.error("spark load run error.", e);
        } finally {
            sparkEnv.stop();
        }

    }


}
