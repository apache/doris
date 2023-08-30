package org.apache.doris.load.loadv2.etl;

import org.apache.doris.load.loadv2.dpp.SparkLoadJobV2;

import org.apache.spark.SparkEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkLoad {

    private static final Logger LOG = LoggerFactory.getLogger(SparkLoad.class);

    public static void main(String[] args) {

        try {
            // Thread.sleep(3 * 1000);

            // parse args
            SparkLoadCommand command = SparkLoadCommand.parse(args);

            // get sparkEnv session env
            SparkLoadSparkEnv sparkEnv = SparkLoadSparkEnv.build(command);

            // parse config
            SparkLoadConf sparkLoadConf = SparkLoadConf.build(command, sparkEnv);

            SparkLoadJobV2 sparkDpp = new SparkLoadJobV2(sparkEnv, sparkLoadConf);
            sparkDpp.doDpp();

            LOG.info("sparkEnv load end");
        } catch (Exception e) {
            LOG.error("spark job run error.", e);
        } catch (Throwable e) {
            LOG.error("error", e);
        } finally {
        }

    }


}
