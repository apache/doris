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

package org.apache.doris.hudi;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import sun.management.VMManagement;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;
import java.util.LinkedList;
import java.util.List;

public class Utils {
    public static class Constants {
        public static String HADOOP_USER_NAME = "hadoop.username";
        public static String HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";
        public static String HADOOP_KERBEROS_PRINCIPAL = "hadoop.kerberos.principal";
        public static String HADOOP_KERBEROS_KEYTAB = "hadoop.kerberos.keytab";
    }

    public static UserGroupInformation getUserGroupInformation(Configuration conf) {
        String authentication = conf.get(Constants.HADOOP_SECURITY_AUTHENTICATION, null);
        if ("kerberos".equals(authentication)) {
            conf.set("hadoop.security.authorization", "true");
            conf.set("hadoop.kerberos.keytab.login.autorenewal.enabled", "true");
            UserGroupInformation.setConfiguration(conf);
            String principal = conf.get(Constants.HADOOP_KERBEROS_PRINCIPAL);
            String keytab = conf.get(Constants.HADOOP_KERBEROS_KEYTAB);
            try {
                UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);
                UserGroupInformation.setLoginUser(ugi);
                return ugi;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            String hadoopUserName = conf.get(Constants.HADOOP_USER_NAME);
            if (hadoopUserName != null) {
                return UserGroupInformation.createRemoteUser(hadoopUserName);
            }
        }
        return null;
    }

    public static long getCurrentProcId() {
        try {
            RuntimeMXBean mxbean = ManagementFactory.getRuntimeMXBean();
            Field jvmField = mxbean.getClass().getDeclaredField("jvm");
            jvmField.setAccessible(true);
            VMManagement management = (VMManagement) jvmField.get(mxbean);
            Method method = management.getClass().getDeclaredMethod("getProcessId");
            method.setAccessible(true);
            return (long) (Integer) method.invoke(management);
        } catch (Exception e) {
            throw new RuntimeException("Couldn't find PID of current JVM process.", e);
        }
    }

    public static List<Long> getChildProcessIds(long pid) {
        try {
            Process pgrep = (new ProcessBuilder("pgrep", "-P", String.valueOf(pid))).start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(pgrep.getInputStream()));
            List<Long> result = new LinkedList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                result.add(Long.valueOf(line.trim()));
            }
            pgrep.waitFor();
            return result;
        } catch (Exception e) {
            throw new RuntimeException("Couldn't get child processes of PID " + pid, e);
        }
    }

    public static String getCommandLine(long pid) {
        try {
            return FileUtils.readFileToString(new File(String.format("/proc/%d/cmdline", pid))).trim();
        } catch (IOException e) {
            return null;
        }
    }

    public static void killProcess(long pid) {
        try {
            Process kill = (new ProcessBuilder("kill", "-9", String.valueOf(pid))).start();
            kill.waitFor();
        } catch (Exception e) {
            throw new RuntimeException("Couldn't kill process PID " + pid, e);
        }
    }

    public static HoodieTableMetaClient getMetaClient(Configuration conf, String basePath) {
        UserGroupInformation ugi = getUserGroupInformation(conf);
        HoodieTableMetaClient metaClient;
        if (ugi != null) {
            try {
                ugi.checkTGTAndReloginFromKeytab();
                metaClient = ugi.doAs(
                        (PrivilegedExceptionAction<HoodieTableMetaClient>) () -> HoodieTableMetaClient.builder()
                                .setConf(conf).setBasePath(basePath).build());
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException("Cannot get hudi client.", e);
            }
        } else {
            metaClient = HoodieTableMetaClient.builder().setConf(conf).setBasePath(basePath).build();
        }
        return metaClient;
    }
}
