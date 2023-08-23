package org.apache.doris.load.loadv2.etl;

import org.apache.doris.common.SparkDppException;
import org.apache.doris.load.loadv2.dpp.DorisKryoRegistrator;
import org.apache.doris.load.loadv2.dpp.GlobalDictBuilder;
import org.apache.doris.load.loadv2.dpp.SparkDpp;
import org.apache.doris.sparkdpp.EtlJobConfig;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlColumn;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlColumnMapping;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlFileGroup;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlIndex;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlTable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.CharStreams;
import org.apache.commons.collections.map.MultiValueMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class SparkLoadConf implements Serializable {
        private static final Logger LOG = LoggerFactory.getLogger(SparkLoadConf.class);

    private static final String BITMAP_DICT_FUNC = "bitmap_dict";
    private static final String TO_BITMAP_FUNC = "to_bitmap";
    private static final String BITMAP_HASH = "bitmap_hash";
    private static final String BINARY_BITMAP = "binary_bitmap";

    // private String jobConfigFilePath;
    private EtlJobConfig etlJobConfig;
    private Set<Long> hiveSourceTables = Sets.newHashSet();
    private Map<Long, Set<String>> tableToBitmapDictColumns = Maps.newHashMap();
    private Map<Long, Set<String>> tableToBinaryBitmapColumns = Maps.newHashMap();
    // private final SparkConf conf;
    private final SparkConf sparkConf = new SparkConf();
    private final SparkConf conf = new SparkConf();
    private SparkSession spark;

    // public SparkLoadConf(String jobConfigFilePath) {
    //     this.jobConfigFilePath = jobConfigFilePath;
    //     this.etlJobConfig = null;
    //     this.hiveSourceTables = Sets.newHashSet();
    //     this.tableToBitmapDictColumns = Maps.newHashMap();
    //     this.tableToBinaryBitmapColumns = Maps.newHashMap();
    //     // conf = new SparkConf();
    // }

    private SparkLoadConf(EtlJobConfig etlJobConfig) {
        this.etlJobConfig = etlJobConfig;
    }

    public static SparkLoadConf build(String jobConfigFilePath) throws Exception {
        SparkLoadConf sparkLoadConf = new SparkLoadConf(getConfigFromHadoop(jobConfigFilePath));

        sparkLoadConf.checkConfigForHiveTableSource();
        sparkLoadConf.confForHiveTable();
        sparkLoadConf.initSpark();

        // sparkLoadConf.processDpp();
        return sparkLoadConf;
    }

    private void initSpark() {
        //serialization conf
        // conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        // conf.set("spark.kryo.registrator", "org.apache.doris.load.loadv2.dpp.DorisKryoRegistrator");
        // conf.set("spark.kryo.registrationRequired", "false");
        // spark = SparkSession.builder().master("local[1]").config(conf).getOrCreate();

        //serialization conf
        // TODO wuwenchi spark支持hive,包括 initSparkConfigs
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.serializer", KryoSerializer.class.getName());
        sparkConf.set("spark.kryo.registrator", DorisKryoRegistrator.class.getName());
        sparkConf.set("spark.kryo.registrationRequired", "false");
        spark = SparkSession.builder()
                // TODO 这里把LOCAL环境去掉
                .master("local[1]")
                .appName("doris spark load job")
                .config(sparkConf)
                .getOrCreate();
        // return spark;
    }

    public SparkSession getSparkSession() {
        //serialization conf
        // conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        // conf.set("spark.kryo.registrator", "org.apache.doris.load.loadv2.dpp.DorisKryoRegistrator");
        // conf.set("spark.kryo.registrationRequired", "false");
        // spark = SparkSession.builder().master("local[1]").config(conf).getOrCreate();

        //serialization conf
        // TODO wuwenchi spark支持hive,包括 initSparkConfigs
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.serializer", KryoSerializer.class.getName());
        sparkConf.set("spark.kryo.registrator", DorisKryoRegistrator.class.getName());
        sparkConf.set("spark.kryo.registrationRequired", "false");
        spark = SparkSession.builder()
                // TODO 这里把LOCAL环境去掉
                .master("local[1]")
                .appName("doris spark load job")
                .config(sparkConf)
                .getOrCreate();
        return spark;
    }

    private void initSparkConfigs(Map<String, String> configs) {
        if (configs == null) {
            return;
        }
        for (Map.Entry<String, String> entry : configs.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
            conf.set("spark.hadoop." + entry.getKey(), entry.getValue());
        }
    }

    static public EtlJobConfig getConfigFromHadoop(String jobConfigFilePath) throws IOException {

        LOG.debug("job config file path: " + jobConfigFilePath);
        // SparkHadoopUtil jk = SparkHadoopUtil.get();
        // Configuration hadoopConf = jk.newConfiguration(this.conf);
        String jsonConfig;
        Path path = new Path(jobConfigFilePath);
        // TODO wuwenchi 这里会读取 hdfs-site.xml 文件吗
        try (FileSystem fs = path.getFileSystem(new HdfsConfiguration()); DataInputStream in = fs.open(path)) {
            jsonConfig = CharStreams.toString(new InputStreamReader(in));
        }
        LOG.debug("rdd read json config: " + jsonConfig);
        // etlJobConfig = EtlJobConfig.configFromJson(jsonConfig);
        // LOG.debug("etl job config: " + etlJobConfig);
        return EtlJobConfig.configFromJson(jsonConfig);
    }

    /*
     * 1. check bitmap column
     * 2. fill tableToBitmapDictColumns
     * 3. remove bitmap_dict and to_bitmap mapping from columnMappings
     */
    private void checkConfigForHiveTableSource() throws SparkDppException {
        for (Map.Entry<Long, EtlTable> entry : etlJobConfig.tables.entrySet()) {
            boolean isHiveSource = false;
            Set<String> bitmapDictColumns = Sets.newHashSet();
            Set<String> binaryBitmapColumns = Sets.newHashSet();

            for (EtlFileGroup fileGroup : entry.getValue().fileGroups) {
                if (fileGroup.sourceType == EtlJobConfig.SourceType.HIVE) {
                    isHiveSource = true;
                }
                Map<String, EtlColumnMapping> newColumnMappings = Maps.newHashMap();
                for (Map.Entry<String, EtlColumnMapping> mappingEntry : fileGroup.columnMappings.entrySet()) {
                    String columnName = mappingEntry.getKey();
                    String exprStr = mappingEntry.getValue().toDescription();
                    String funcName = functions.expr(exprStr).expr().prettyName();

                    switch (funcName.toLowerCase(Locale.ROOT)) {
                        case BINARY_BITMAP:
                            binaryBitmapColumns.add(columnName.toLowerCase());
                            break;
                        case BITMAP_DICT_FUNC:
                            bitmapDictColumns.add(columnName.toLowerCase());
                            break;
                        case TO_BITMAP_FUNC:
                            newColumnMappings.put(mappingEntry.getKey(), mappingEntry.getValue());
                        case BITMAP_HASH:
                            throw new SparkDppException("spark load not support " + funcName + " now");
                        default:
                            break;
                    }
                }

                // reset new columnMappings
                fileGroup.columnMappings = newColumnMappings;
            }
            if (isHiveSource) {
                hiveSourceTables.add(entry.getKey());
            }
            if (!bitmapDictColumns.isEmpty()) {
                tableToBitmapDictColumns.put(entry.getKey(), bitmapDictColumns);
            }
            if (!binaryBitmapColumns.isEmpty()) {
                tableToBinaryBitmapColumns.put(entry.getKey(), binaryBitmapColumns);
            }
        }

        LOG.info("init hiveSourceTables: " + hiveSourceTables
                + ",tableToBitmapDictColumns: " + tableToBitmapDictColumns
        + ",tableToBinaryBitmapColumns: " + tableToBinaryBitmapColumns);

        // // spark etl must have only one table with bitmap type column to process.
        // if (hiveSourceTables.size() > 1
        //         || tableToBitmapDictColumns.size() > 1
        //         || tableToBinaryBitmapColumns.size() > 1) {
        //     throw new Exception("spark etl job must have only one hive table with bitmap type column to process");
        // }

        SparkDppException.checkArgument(hiveSourceTables.size() < 2, "spark etl job must have only one hive table.");
        SparkDppException.checkArgument(tableToBitmapDictColumns.size() < 2, "spark etl job must have only one table with bitmap_dict columns");
        SparkDppException.checkArgument(tableToBinaryBitmapColumns.size() < 2, "spark etl job must have only one table with bitmap columns");
    }

    // private void processDpp() throws Exception {
    //     SparkDpp sparkDpp = new SparkDpp(spark, etlJobConfig, tableToBitmapDictColumns, tableToBinaryBitmapColumns);
    //     sparkDpp.init();
    //     sparkDpp.doDpp();
    // }

    private String buildGlobalDictAndEncodeSourceTable(EtlTable table, long tableId) {
        // dict column map
        MultiValueMap dictColumnMap = new MultiValueMap();
        for (String dictColumn : tableToBitmapDictColumns.get(tableId)) {
            dictColumnMap.put(dictColumn, null);
        }

        // doris schema
        List<String> dorisOlapTableColumnList = Lists.newArrayList();
        for (EtlIndex etlIndex : table.indexes) {
            if (etlIndex.isBaseIndex) {
                for (EtlColumn column : etlIndex.columns) {
                    dorisOlapTableColumnList.add(column.columnName);
                }
            }
        }

        // hive db and tables
        EtlFileGroup fileGroup = table.fileGroups.get(0);
        String sourceHiveDBTableName = fileGroup.hiveDbTableName;
        String dorisHiveDB = sourceHiveDBTableName.split("\\.")[0];
        String taskId = etlJobConfig.outputPath.substring(etlJobConfig.outputPath.lastIndexOf("/") + 1);
        String globalDictTableName = String.format(EtlJobConfig.GLOBAL_DICT_TABLE_NAME, tableId);
        String distinctKeyTableName = String.format(EtlJobConfig.DISTINCT_KEY_TABLE_NAME, tableId, taskId);
        String dorisIntermediateHiveTable = String.format(
                EtlJobConfig.DORIS_INTERMEDIATE_HIVE_TABLE_NAME, tableId, taskId);
        String sourceHiveFilter = fileGroup.where;

        // others
        List<String> mapSideJoinColumns = Lists.newArrayList();
        int buildConcurrency = 1;
        List<String> veryHighCardinalityColumn = Lists.newArrayList();
        int veryHighCardinalityColumnSplitNum = 1;

        LOG.info("global dict builder args, dictColumnMap: " + dictColumnMap
                         + ", dorisOlapTableColumnList: " + dorisOlapTableColumnList
                         + ", sourceHiveDBTableName: " + sourceHiveDBTableName
                         + ", sourceHiveFilter: " + sourceHiveFilter
                         + ", distinctKeyTableName: " + distinctKeyTableName
                         + ", globalDictTableName: " + globalDictTableName
                         + ", dorisIntermediateHiveTable: " + dorisIntermediateHiveTable);
        try {
            GlobalDictBuilder globalDictBuilder = new GlobalDictBuilder(dictColumnMap, dorisOlapTableColumnList,
                    mapSideJoinColumns, sourceHiveDBTableName, sourceHiveFilter, dorisHiveDB, distinctKeyTableName,
                    globalDictTableName, dorisIntermediateHiveTable, buildConcurrency, veryHighCardinalityColumn,
                    veryHighCardinalityColumnSplitNum, spark);
            globalDictBuilder.createHiveIntermediateTable();
            globalDictBuilder.extractDistinctColumn();
            globalDictBuilder.buildGlobalDict();
            globalDictBuilder.encodeDorisIntermediateHiveTable();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return String.format("%s.%s", dorisHiveDB, dorisIntermediateHiveTable);
    }

    private void confForHiveTable() {
        if (!hiveSourceTables.isEmpty()) {
            // only one table
            long tableId = -1;
            EtlTable table = null;
            for (Map.Entry<Long, EtlTable> entry : etlJobConfig.tables.entrySet()) {
                tableId = entry.getKey();
                table = entry.getValue();
                break;
            }

            // init hive configs like metastore service
            EtlFileGroup fileGroup = table.fileGroups.get(0);
            initSparkConfigs(fileGroup.hiveTableProperties);
            fileGroup.dppHiveDbTableName = fileGroup.hiveDbTableName;

            // build global dict and encode source hive table if it has bitmap dict columns
            if (!tableToBitmapDictColumns.isEmpty() && tableToBitmapDictColumns.containsKey(tableId)) {
                String dorisIntermediateHiveDbTableName = buildGlobalDictAndEncodeSourceTable(table, tableId);
                // set with dorisIntermediateHiveDbTable
                fileGroup.dppHiveDbTableName = dorisIntermediateHiveDbTableName;
            }
        }
    }

    // public void run() throws Exception {
    //     initConfig();
    //     initSpark();
    //     confForHiveTable();
    //
    //     processDpp();
    // }

    public EtlJobConfig getEtlJobConfig() {
        return etlJobConfig;
    }

    public Set<Long> getHiveSourceTables() {
        return hiveSourceTables;
    }

    public Map<Long, EtlTable> getDstTables() {
        return etlJobConfig.tables;
    }

    public String getOutputPath() {
        return etlJobConfig.outputPath;
    }

    public String getOutputFilePattern() {
        return etlJobConfig.outputFilePattern;
    }

    public Map<Long, Set<String>> getTableToBitmapDictColumns() {
        return tableToBitmapDictColumns;
    }

    public Map<Long, Set<String>> getTableToBinaryBitmapColumns() {
        return tableToBinaryBitmapColumns;
    }
}
