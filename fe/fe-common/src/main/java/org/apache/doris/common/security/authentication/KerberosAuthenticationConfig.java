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
        // Try to convert the provided keytab path to a Path object
        Path keytabPath = Paths.get(kerberosKeytab);
        // If the provided path is an existing file, use it directly
        if (keytabPath.toFile().exists()) {
            this.kerberosKeytab = kerberosKeytab;
            return;
        }
        // If the file does not exist, try to resolve it by concatenating with the custom config directory
        String resolvedKeytabPath = Config.custom_config_dir + File.separator + kerberosKeytab;
        keytabPath = Paths.get(resolvedKeytabPath);

        // If the resolved path also does not exist, throw an exception
        if (!keytabPath.toFile().exists()) {
            throw new RuntimeException("Keytab file does not exist: " + kerberosKeytab);
        }
        // If a valid keytab file path is found, save it
        this.kerberosKeytab = resolvedKeytabPath;
    }
}
