package com.baidu.palo.deploy.impl;

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.common.Pair;
import com.baidu.palo.deploy.DeployManager;
import com.baidu.palo.system.SystemInfoService;

import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

/*
 * This for test only.
 * The LocalEnvDeployManager will watch the change of the Local environment variables
 *  'ELECTABLE_FE_SERVICE_GROUP', 'OBSERVER_FE_SERVICE_GROUP' and 'BACKEND_SERVICE_GROUP'
 *  which are 3 local files: fe_host.list, ob_host.list, be_host.list
 *  
 *  eg:
 *  
 *      export ELECTABLE_FE_SERVICE_GROUP=fe_host.list
 *      export OBSERVER_FE_SERVICE_GROUP=ob_host.list
 *      export BACKEND_SERVICE_GROUP=be_host.list
 */
public class LocalFileDeployManager extends DeployManager {
    private static final Logger LOG = LogManager.getLogger(LocalFileDeployManager.class);

    public static final String ENV_ELECTABLE_FE_SERVICE_GROUP = "ELECTABLE_FE_SERVICE_GROUP";
    public static final String ENV_OBSERVER_FE_SERVICE_GROUP = "OBSERVER_FE_SERVICE_GROUP";
    public static final String ENV_BACKEND_SERVICE_GROUP = "BACKEND_SERVICE_GROUP";

    public LocalFileDeployManager(Catalog catalog, long intervalMs) {
        super(catalog, intervalMs);
        initEnvVariables(ENV_ELECTABLE_FE_SERVICE_GROUP, ENV_OBSERVER_FE_SERVICE_GROUP, ENV_BACKEND_SERVICE_GROUP, "");
    }

    @Override
    public List<Pair<String, Integer>> getGroupHostPorts(String groupName) {
        List<Pair<String, Integer>> result = Lists.newArrayList();
        LOG.debug("begin to get group: {}", groupName);
        File file = new File(groupName);
        if (!file.exists()) {
            LOG.warn("failed to get file from ");
            return null;
        }

        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String line = null;
            while ((line = reader.readLine()) != null) {
                try {
                    Pair<String, Integer> hostPorts = SystemInfoService.validateHostAndPort(line);
                    result.add(hostPorts);
                } catch (Exception e) {
                    LOG.warn("failed to read host port from {}", groupName, e);
                    return null;
                }
            }
        } catch (IOException e) {
            LOG.warn("failed to read from file.", e);
            return null;
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e2) {
                    e2.printStackTrace();
                    return null;
                }
            }
        }

        LOG.info("get hosts from {}: {}", groupName, result);
        return result;
    }
}
