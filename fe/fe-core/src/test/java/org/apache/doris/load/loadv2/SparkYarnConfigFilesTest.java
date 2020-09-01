package org.apache.doris.load.loadv2;

import mockit.Mocked;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.LoadException;
import com.google.common.collect.Maps;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Map;

public class SparkYarnConfigFilesTest {
    private static final String RESOURCE_NAME = "spark0";
    private static final String SPARK_HADOOP_PREFIX = "spark.hadoop.";
    private static final String YARN_CONFIG_DIR = "./yarn_config";

    private Map<String, String> properties;

    @Mocked
    Catalog catalog;

    @Before
    public void setUp() {
        properties = Maps.newHashMap();
        properties.put(SPARK_HADOOP_PREFIX + "hadoop.job.ugi", "test,test");
        properties.put(SPARK_HADOOP_PREFIX + "hadoop.security.authentication", "simple");
        properties.put(SPARK_HADOOP_PREFIX + "yarn.resourcemanager.address", "host:port");
        properties.put(SPARK_HADOOP_PREFIX + "yarn.resourcemanager.scheduler.address", "host:port");
    }

    @Test
    public void testNormal() {
        SparkYarnConfigFiles sparkYarnConfigFiles = new SparkYarnConfigFiles(RESOURCE_NAME, YARN_CONFIG_DIR, properties);
        try {
            // prepare config files
            sparkYarnConfigFiles.prepare();
            // get config files' parent directory
            String configDir = sparkYarnConfigFiles.getConfigDir();
            File dir = new File(configDir);
            File[] configFiles = dir.listFiles();
            Assert.assertEquals(2, configFiles.length);
        } catch (LoadException  e) {
            Assert.fail();
        }
    }

    @After
    public void clear() {
        delete(YARN_CONFIG_DIR);
    }

    private void delete(String deletePath) {
        File file = new File(deletePath);
        if (!file.exists()) {
            return;
        }
        if (file.isFile()) {
            file.delete();
            return;
        }
        File[] files = file.listFiles();
        for (File file1 : files) {
            delete(file1.getAbsolutePath());
        }
        file.delete();
    }
}
