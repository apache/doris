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
import groovy.json.JsonSlurper
import org.codehaus.groovy.runtime.IOGroovyMethods

suite("cold_heat_separation", "p2") {
    try_sql """
        drop database regression_test_cold_heat_separation_p2 force;
    """

    try_sql """
        create database regression_test_cold_heat_separation_p2;
    """

    def polices = sql """
        show storage policy;
    """

    for (policy in polices) {
        if (policy[3].equals("STORAGE")) {
            try_sql """
                drop storage policy if exists ${policy[0]}
            """
        }
    }

    def resources = sql """
        show resources;
    """

    for (resource in resources) {
        if (resource[1].equals("s3")) {
            try_sql """
                drop resource if exists ${resource[0]}
            """
        }
    }
}