package org.apache.doris.load.loadv2.etl;

import org.apache.doris.load.loadv2.dpp.SparkLoadJobV2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkLoad {

    private static final Logger LOG = LoggerFactory.getLogger(SparkLoad.class);

    public static void main(String[] args) {

        try {
            // Thread.sleep(3 * 1000);

            // parse args
            SparkLoadCommand command = SparkLoadCommand.parse(args);

            // parse config
            SparkLoadConf sparkLoadConf = SparkLoadConf.build(command.getConfigFile());

            // get spark session env
            SparkLoadSparkEnv spark = SparkLoadSparkEnv.build(sparkLoadConf);

            SparkLoadJobV2 sparkDpp = new SparkLoadJobV2(spark, sparkLoadConf);
            sparkDpp.doDpp();

            LOG.info("spark load end");
        } catch (Exception e) {
            LOG.error("spark job run error.", e);
        } catch (Throwable e) {
            LOG.error("error", e);
        }

    }


}
