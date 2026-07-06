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

suite("test_max_compute_validate_connection", "p2,external") {
    String enabled = context.config.otherConfigs.get("enableMaxComputeTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable MaxCompute test.")
        return
    }

    String ak = context.config.otherConfigs.get("ak")
    String sk = context.config.otherConfigs.get("sk")
    String defaultProject = "mc_datalake"
    String invalidEndpoint = "http://127.0.0.1:1/api"
    String catalogDefaultFalse = "test_mc_validate_default_false"
    String catalogExplicitFalse = "test_mc_validate_explicit_false"
    String catalogValidateProject = "test_mc_validate_project"
    String catalogValidateSchema = "test_mc_validate_schema"

    sql """drop catalog if exists ${catalogDefaultFalse}"""
    sql """drop catalog if exists ${catalogExplicitFalse}"""
    sql """drop catalog if exists ${catalogValidateProject}"""
    sql """drop catalog if exists ${catalogValidateSchema}"""

    sql """
        create catalog ${catalogDefaultFalse} properties (
            "type" = "max_compute",
            "mc.default.project" = "${defaultProject}",
            "mc.access_key" = "${ak}",
            "mc.secret_key" = "${sk}",
            "mc.endpoint" = "${invalidEndpoint}",
            "mc.connect_timeout" = "1",
            "mc.read_timeout" = "1",
            "mc.retry_count" = "1"
        );
    """
    sql """drop catalog if exists ${catalogDefaultFalse}"""

    sql """
        create catalog ${catalogExplicitFalse} properties (
            "type" = "max_compute",
            "mc.default.project" = "${defaultProject}",
            "mc.access_key" = "${ak}",
            "mc.secret_key" = "${sk}",
            "mc.endpoint" = "${invalidEndpoint}",
            "mc.connect_timeout" = "1",
            "mc.read_timeout" = "1",
            "mc.retry_count" = "1",
            "test_connection" = "false"
        );
    """
    sql """drop catalog if exists ${catalogExplicitFalse}"""

    test {
        sql """
            create catalog ${catalogValidateProject} properties (
                "type" = "max_compute",
                "mc.default.project" = "${defaultProject}",
                "mc.access_key" = "${ak}",
                "mc.secret_key" = "${sk}",
                "mc.endpoint" = "${invalidEndpoint}",
                "mc.connect_timeout" = "1",
                "mc.read_timeout" = "1",
                "mc.retry_count" = "1",
                "test_connection" = "true"
            );
        """
        exception "Failed to validate MaxCompute project"
    }

    test {
        sql """
            create catalog ${catalogValidateSchema} properties (
                "type" = "max_compute",
                "mc.default.project" = "${defaultProject}",
                "mc.access_key" = "${ak}",
                "mc.secret_key" = "${sk}",
                "mc.endpoint" = "${invalidEndpoint}",
                "mc.connect_timeout" = "1",
                "mc.read_timeout" = "1",
                "mc.retry_count" = "1",
                "mc.enable.namespace.schema" = "true",
                "test_connection" = "true"
            );
        """
        exception "with namespace schema"
    }
}
