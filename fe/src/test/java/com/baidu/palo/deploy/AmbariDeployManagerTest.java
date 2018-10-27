package com.baidu.palo.deploy;

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.deploy.impl.AmbariDeployManager;

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
        encodedAuthInfoF.set(manager, new sun.misc.BASE64Encoder().encode("admin:admin".getBytes()));

        Field ambariUrlF = manager.getClass().getDeclaredField("ambariUrl");
        ambariUrlF.setAccessible(true);
        ambariUrlF.set(manager, "180.76.168.210:8080");

        Field clusterNameF = manager.getClass().getDeclaredField("clusterName");
        clusterNameF.setAccessible(true);
        clusterNameF.set(manager, "BDP");

        Field serviceNameF = manager.getClass().getDeclaredField("serviceName");
        serviceNameF.setAccessible(true);
        serviceNameF.set(manager, "PALO");

        Field blueprintF = manager.getClass().getDeclaredField("blueprintUrl");
        blueprintF.setAccessible(true);
        blueprintF.set(manager, "http://180.76.168.210:8080/api/v1/clusters/BDP?format=blueprint");
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
        Method getResultForUrlM = manager.getClass().getDeclaredMethod("getResultForUrl", String.class);
        getResultForUrlM.setAccessible(true);
        String res = (String) getResultForUrlM.invoke(manager,
                                                      "http://180.76.168.210:8080/api/v1/clusters/BDP?format=blueprint");
        return res;
    }

    private String getComponent(String comp)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Method getResultForUrlM = manager.getClass().getDeclaredMethod("getResultForUrl", String.class);
        getResultForUrlM.setAccessible(true);
        String res = (String) getResultForUrlM.invoke(manager,
                                                      "http://180.76.168.210:8080/api/v1/clusters/BDP/services/PALO/components/"
                                                              + comp);

        return res;
    }
}
