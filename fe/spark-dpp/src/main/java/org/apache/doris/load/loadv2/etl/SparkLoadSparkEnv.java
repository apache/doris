package org.apache.doris.load.loadv2.etl;

import org.apache.doris.load.loadv2.dpp.DorisKryoRegistrator;
import org.apache.doris.load.loadv2.dpp.StringAccumulator;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlFileGroup;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;
import org.apache.spark.util.LongAccumulator;
import org.apache.spark.util.SerializableConfiguration;

import java.io.Serializable;

public class SparkLoadSparkEnv implements Serializable {

    SparkSession spark;

    private LongAccumulator abnormalRowAcc = null;
    private LongAccumulator scannedRowsAcc = null;
    private LongAccumulator fileNumberAcc = null;
    private LongAccumulator fileSizeAcc = null;
    // accumulator to collect invalid rows
    private final StringAccumulator invalidRows = new StringAccumulator();

    // save the hadoop configuration from spark session.
    // because hadoop configuration is not serializable,
    // we need to wrap it so that we can use it in executor.
    private SerializableConfiguration serializableHadoopConf;

    static public SparkLoadSparkEnv build(SparkLoadCommand command) {

        SparkLoadSparkEnv sparkLoadSparkEnv = new SparkLoadSparkEnv();

        sparkLoadSparkEnv.initSparkSession(command);
        sparkLoadSparkEnv.registerAccumulator();

        return sparkLoadSparkEnv;
    }

    public void initSparkSession(SparkLoadCommand command) {

        SparkConf sparkConf = new SparkConf();
        //serialization conf
        sparkConf.set("spark.serializer", KryoSerializer.class.getName());
        sparkConf.set("spark.kryo.registrator", DorisKryoRegistrator.class.getName());
        sparkConf.set("spark.kryo.registrationRequired", "false");

        Builder builder = SparkSession.builder();
        // Set<Long> hiveSourceTableSet = sparkLoadConf.getHiveSourceTables();
        // if (!hiveSourceTableSet.isEmpty()) {
        //     builder.enableHiveSupport();
        //
        //     // init hive configs like metastore service
        //     hiveSourceTableSet.forEach(id -> {
        //         EtlTable etlTable = sparkLoadConf.getDstTables().get(id);
        //         EtlFileGroup etlFileGroup = etlTable.fileGroups.get(0);
        //         etlFileGroup.hiveTableProperties.forEach((key, val) -> {
        //             sparkConf.set(key, val);
        //             sparkConf.set("spark.hadoop." + key, val);
        //         });
        //     });
        // }

        if (command.getEnableHive()) {
            builder.enableHiveSupport();
        }

        spark = builder
                .appName("doris spark load job")
                .config(sparkConf)
                .getOrCreate();

        this.serializableHadoopConf = new SerializableConfiguration(spark.sparkContext().hadoopConfiguration());
    }

    public void registerAccumulator() {
        abnormalRowAcc = spark.sparkContext().longAccumulator("abnormalRowAcc");
        scannedRowsAcc = spark.sparkContext().longAccumulator("scannedRowsAcc");
        fileNumberAcc = spark.sparkContext().longAccumulator("fileNumberAcc");
        fileSizeAcc = spark.sparkContext().longAccumulator("fileSizeAcc");
        spark.sparkContext().register(invalidRows, "InvalidRowsAccumulator");
    }

    public SparkSession getSpark() {
        return spark;
    }

    public Configuration getHadoopConf() {
        return serializableHadoopConf.value();
    }

    public SerializableConfiguration getSerializableConfigurationHadoopConf() {
        return serializableHadoopConf;
    }

    public void addAbnormalRowAcc() {
        abnormalRowAcc.add(1);
    }

    public Long getAbnormalRowAccValue() {
        return abnormalRowAcc.value();
    }

    public void addScannedRowsAcc() {
        scannedRowsAcc.add(1);
    }

    public Long getScannedRowsAccValue() {
        return scannedRowsAcc.value();
    }

    public void addFileNumberAcc() {
        fileNumberAcc.add(1);
    }

    public Long getFileNumberAccValue() {
        return fileNumberAcc.value();
    }

    public void addFileSizeAcc(long size) {
        fileSizeAcc.add(size);
    }

    public Long getFileSizeAccValue() {
        return fileSizeAcc.value();
    }

    public void addInvalidRows(String row) {
        // at most add 5 rows to invalidRows
        if (abnormalRowAcc.value() < 5) {
            invalidRows.add(row);
        }
    }

    public String getInvalidRowsValue() {
        return invalidRows.value();
    }
}
