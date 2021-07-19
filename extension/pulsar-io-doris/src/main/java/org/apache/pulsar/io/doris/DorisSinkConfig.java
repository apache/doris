/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pulsar.io.doris;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.core.annotations.FieldDoc;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

@Data
@Accessors(chain = true)
public class DorisSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    @FieldDoc(
        required = true,
        defaultValue = "xxx.com",
        help = "A comma-separated list of hosts, which are the addresses of Doris Fe services." +
               "It is recommended that Doris Fe service be proxy.")
    private String doris_host = "xxx.com";

    @FieldDoc(
        required = true,
        defaultValue = "",
        help = "The database that this connector connects to")
    private String doris_db;

    @FieldDoc(
        required = true,
        defaultValue = "",
        help = "The Table connected by this connector")
    private String doris_table;

    @FieldDoc(
        required = true,
        defaultValue = "root",
        sensitive = true,
        help = "Username used to connect to Doris")
    private String doris_user = "root";

    @FieldDoc(
        required = true,
        defaultValue = "",
        sensitive = true,
        help = "Password used to connect to the Doris")
    private String doris_password;

    @FieldDoc(
        required = true,
        defaultValue = "8030",
        help = "Http server port on Doris FE")
    private String doris_http_port = "8030";

    @FieldDoc(
        defaultValue = "2",
        help = "Number of job failure retries")
    private String job_failure_retries = "2";

    @FieldDoc(
        defaultValue = "3",
        help = "Because the job label is repeated, the maximum number of repeated submissions is limited")
    private String job_label_repeat_retries = "3";

    @FieldDoc(
        required = true,
        defaultValue = "500",
        help = "Insert into Doris timeout in milliseconds"
    )
    private int timeout = 500;

    @FieldDoc(
        required = true,
        defaultValue = "200",
        help = "The batch size of updates made to the Doris"
    )
    private int batchSize = 200;

    public static DorisSinkConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), DorisSinkConfig.class);
    }

    public static DorisSinkConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), DorisSinkConfig.class);
    }
}
