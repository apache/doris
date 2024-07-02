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

package org.apache.doris.deploy;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import org.junit.Test;

import java.io.File;

public class K8sDeployManagerTest {
    /**
     * The purpose of this test case is to verify whether there is a problem with the referenced Jackson version
     * when initializing the DefaultKubernetesClient.
     * The prerequisite for executing this test case is that the system has a file named:~/.kube/config.
     * The file demo is as follows:
     *
     * apiVersion: v1
     * clusters:
     * - cluster:
     *     certificate-authority-data: xxx
     *     server: https://172.xx.xx.16
     *   name: cls-m5vzryyb
     * contexts:
     * - context:
     *     cluster: cls-m5vzryyb
     *     user: "100029465455"
     *   name: cls-m5vzryyb-100029465455-context-default
     * current-context: cls-m5vzryyb-100029465455-context-default
     * kind: Config
     * preferences: {}
     * users:
     * - name: "100029465455"
     *   user:
     *     client-certificate-data: xxx
     *     client-key-data: xxx
     *
     * When the test case fails,the error message is as follows:
     *
     * java.lang.NoClassDefFoundError: org/yaml/snakeyaml/LoaderOptions
     * detailed information: https://github.com/apache/doris/pull/18046
     */
    @Test
    public void testClient() {
        File kubeConfigFile = new File(Config.getKubeconfigFilename());
        if (!kubeConfigFile.isFile()) {
            System.out.println("Did not find Kubernetes config at: [" + kubeConfigFile.getPath() + "]. Ignoring.");
            return;
        }
        new DefaultKubernetesClient();
    }
}
