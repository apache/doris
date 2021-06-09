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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.util.Util;
import org.apache.doris.deploy.impl.AmbariDeployManager;

import org.apache.commons.codec.binary.Base64;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class AmbariDeployManagerTest {

    private AmbariDeployManager manager;
    private Catalog catalog;

    @Before
    public void setUp() throws NoSuchFieldException, SecurityException,
            IllegalArgumentException, IllegalAccessException {
        manager = new AmbariDeployManager();

        Field authInfoF = manager.getClass().getDeclaredField("authInfo");
        authInfoF.setAccessible(true);
        authInfoF.set(manager, "admin:admin");

        Field encodedAuthInfoF = manager.getClass().getDeclaredField("encodedAuthInfo");
        encodedAuthInfoF.setAccessible(true);
        encodedAuthInfoF.set(manager, Base64.encodeBase64String("admin:admin".getBytes()));

        Field ambariUrlF = manager.getClass().getDeclaredField("ambariUrl");
        ambariUrlF.setAccessible(true);
        ambariUrlF.set(manager, "127.0.0.1:8080");

        Field clusterNameF = manager.getClass().getDeclaredField("clusterName");
        clusterNameF.setAccessible(true);
        clusterNameF.set(manager, "BDP");

        Field serviceNameF = manager.getClass().getDeclaredField("serviceName");
        serviceNameF.setAccessible(true);
        serviceNameF.set(manager, "PALO");

        Field blueprintF = manager.getClass().getDeclaredField("blueprintUrl");
        blueprintF.setAccessible(true);
        blueprintF.set(manager, "http://127.0.0.1:8080/api/v1/clusters/BDP?format=blueprint");
    }

    @Test
    public void getPropertyFromBlueprintTest() throws NoSuchMethodException, SecurityException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException, NoSuchFieldException {
        String res = getBlueprint();
        
        Field bpF = manager.getClass().getDeclaredField("blueprintJson");
        bpF.setAccessible(true);
        bpF.set(manager, res);

        Method getPropM = manager.getClass().getDeclaredMethod("getPropertyFromBlueprint", String.class, String.class);
        getPropM.setAccessible(true);
    }

    @Test
    public void getHostTest() throws NoSuchMethodException, SecurityException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException, NoSuchFieldException {
        String res = getComponent("PALO_FE");

        System.out.println(res);
    }

    private String getBlueprint() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        String res = Util.getResultForUrl("http://127.0.0.1:8080/api/v1/clusters/BDP?format=blueprint",
                null, 2000, 2000);
        return res;
    }

    private String getComponent(String comp)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        String res = Util.getResultForUrl("http://127.0.0.1:8080/api/v1/clusters/BDP/services/PALO/components/"
                + comp, null, 2000, 2000);

        return res;
    }
}
