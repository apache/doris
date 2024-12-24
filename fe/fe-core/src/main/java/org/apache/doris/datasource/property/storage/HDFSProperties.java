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

package org.apache.doris.datasource.property.storage;

import org.apache.doris.datasource.property.ConnectorProperty;

public class HDFSProperties extends StorageProperties {

    @ConnectorProperty(name = "hdfs.authentication.type",
            alternativeNames = {"hadoop.security.authentication"},
            description = "The authentication type of HDFS. The default value is 'none'.")
    private String hdfsAuthenticationType = "none";

    @ConnectorProperty(name = "hdfs.authentication.kerberos.principal",
            alternativeNames = {"hadoop.kerberos.principal"},
            description = "The principal of the kerberos authentication.")
    private String hdfsKerberosPrincipal = "";

    @ConnectorProperty(name = "hdfs.authentication.kerberos.keytab",
            alternativeNames = {"hadoop.kerberos.keytab"},
            description = "The keytab of the kerberos authentication.")
    private String hdfsKerberosKeytab = "";

    @ConnectorProperty(name = "hdfs.impersonation.enabled",
            description = "Whether to enable the impersonation of HDFS.")
    private boolean hdfsImpersonationEnabled = false;

    @ConnectorProperty(name = "hadoop.username",
            description = "The username of Hadoop. Doris will user this user to access HDFS")
    private String hadoopUsername = "";

    @ConnectorProperty(name = "hadoop.config.resources",
            description = "The xml files of Hadoop configuration.")
    private String hadoopConfigResources = "";
}
