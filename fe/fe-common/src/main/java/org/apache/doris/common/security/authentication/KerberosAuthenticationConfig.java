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

import org.apache.doris.common.Config;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

@EqualsAndHashCode(callSuper = true)
@Data
public class KerberosAuthenticationConfig extends AuthenticationConfig {
    private String kerberosPrincipal;
    private String kerberosKeytab;
    private Configuration conf;
    private boolean printDebugLog;

    @Override
    public boolean isValid() {
        return StringUtils.isNotEmpty(kerberosPrincipal) && StringUtils.isNotEmpty(kerberosKeytab);
    }

    public void setKerberosKeytab(String kerberosKeytab) {
        // Convert the provided keytab path to a Path object
        Path keytabPath = Paths.get(kerberosKeytab);

        // Check if the provided path is an absolute path
        if (keytabPath.isAbsolute()) {
            // If it's an absolute path, check if the file exists
            if (keytabPath.toFile().exists()) {
                // If the file exists, set the kerberosKeytab
                this.kerberosKeytab = kerberosKeytab;
            } else {
                // If the file does not exist, throw an exception
                throw new RuntimeException("Keytab file does not exist: " + kerberosKeytab);
            }
        } else {
            // If it's not an absolute path, resolve it with the custom config directory
            String resolvedKeytabPath = Config.custom_config_dir + File.separator + kerberosKeytab;
            keytabPath = Paths.get(resolvedKeytabPath);

            // Check if the resolved path exists
            if (keytabPath.toFile().exists()) {
                // If the file exists, set the kerberosKeytab
                this.kerberosKeytab = keytabPath.toString();
            } else {
                // If the file does not exist, throw an exception
                throw new RuntimeException("Keytab file does not exist: " + keytabPath.toString());
            }
        }
    }
}
