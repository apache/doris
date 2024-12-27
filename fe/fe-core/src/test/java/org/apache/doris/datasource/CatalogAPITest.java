package org.apache.doris.datasource;

import org.apache.doris.common.security.authentication.AuthenticationConfig;
import org.apache.doris.common.security.authentication.HadoopAuthenticator;

import com.aliyun.datalake.metastore.hive2.ProxyMetaStoreClient;
import com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class CatalogAPITest {
    public static String ak = "";
    public static String sk = "";

    @Test
    public void testAWSCatalogMetastoreClient() throws Exception {
        String endpoint = "https://glue.ap-northeast-1.amazonaws.com";
        final HiveMetaHookLoader DUMMY_HOOK_LOADER = t -> null;
        HiveConf hiveConf = new HiveConf();
        // See AWSGlueConfig.java for property keys
        hiveConf.set("aws.catalog.credentials.provider.factory.class",
                "com.amazonaws.glue.catalog.credentials.ConfigurationAWSCredentialsProviderFactory");
        hiveConf.set("aws.glue.access-key", ak);
        hiveConf.set("aws.glue.secret-key", sk);
        hiveConf.set("aws.glue.endpoint", endpoint);
        IMetaStoreClient client = RetryingMetaStoreClient.getProxy(hiveConf, DUMMY_HOOK_LOADER,
                AWSCatalogMetastoreClient.class.getName());
        List<String> dbs = client.getAllDatabases();
        System.out.println(dbs);
    }

    @Test
    public void testGlueCatalog() {
        String region = "ap-northeast-1";
        GlueCatalog glueCatalog = new GlueCatalog();
        Map<String, String> properties = Maps.newHashMap();
        // See AwsClientProperties.java for property keys
        properties.put("client.credentials-provider",
                "com.amazonaws.glue.catalog.credentials.ConfigurationAWSCredentialsProvider2x");
        properties.put("client.credentials-provider.glue.access_key", ak);
        properties.put("client.credentials-provider.glue.secret_key", sk);
        properties.put("client.region", region);
        glueCatalog.initialize("glue", properties);
        List<Namespace> dbs = glueCatalog.listNamespaces();
        System.out.println(dbs);
    }

    @Test
    public void testPaimonDLFCatalog() {
        Options options = new Options();

        options.set("warehouse", "oss://emr-dev-oss/benchmark/paimon");
        options.set("fs.oss.endpoint", "oss-cn-beijing.aliyuncs.com");
        options.set("fs.oss.accessKeyId", ak);
        options.set("fs.oss.accessKeySecret", sk);

        options.set("metastore", "hive");
        options.set("metastore.client.class", ProxyMetaStoreClient.class.getName());
        options.set("dlf.catalog.region", "cn-beijing");
        options.set("dlf.catalog.endpoint", "dlf.cn-beijing.aliyuncs.com");
        options.set("dlf.catalog.proxyMode", "DLF_ONLY");
        options.set("dlf.catalog.accessKeyId", ak);
        options.set("dlf.catalog.accessKeySecret", sk);
        options.set("dlf.catalog.accessPublic", "false");
        options.set("dlf.catalog.uid", "217316283625971977");
        options.set("dlf.catalog.createDefaultDBIfNotExist", "false");

        Configuration conf = new Configuration(false);
        CatalogContext context = CatalogContext.create(options, conf);
        Catalog paimonCatalog = CatalogFactory.createCatalog(context);
        List<String> dbs = paimonCatalog.listDatabases();
        System.out.println(dbs);
    }

    @Test
    public void testPaimonFileCatalog() {
        Options options = new Options();

        options.set("warehouse", "s3://emr-dev-oss/benchmark/paimon");
        options.set("s3.endpoint", "oss-cn-beijing.aliyuncs.com");
        options.set("s3.access-key", ak);
        options.set("s3.secret-key", sk);

        Configuration conf = new Configuration(false);
        CatalogContext context = CatalogContext.create(options, conf);
        Catalog paimonCatalog = CatalogFactory.createCatalog(context);
        List<String> dbs = paimonCatalog.listDatabases();
        System.out.println(dbs);
    }

    @Test
    public void testPaimonHMSCatalog() throws IOException {
        Options options = new Options();

        options.set("warehouse", "hdfs://hdfs-cluster/paimon/");
        options.set("metastore", "hive");
        options.set("uri", "thrift://master-1-1.c-0596176698bd4d17.cn-beijing.emr.aliyuncs.com:9083");

        options.set("dlf.catalog.region", "cn-beijing");
        options.set("dlf.catalog.endpoint", "dlf.cn-beijing.aliyuncs.com");
        options.set("dlf.catalog.proxyMode", "DLF_ONLY");
        options.set("dlf.catalog.accessKeyId", ak);
        options.set("dlf.catalog.accessKeySecret", sk);
        options.set("dlf.catalog.accessPublic", "false");
        options.set("dlf.catalog.uid", "217316283625971977");
        options.set("dlf.catalog.createDefaultDBIfNotExist", "false");

        Configuration conf = new Configuration(false);
        conf.set("dfs.nameservices", "hdfs-cluster");
        conf.set("dfs.ha.namenodes.hdfs-cluster", "nn1,nn2,nn3");
        conf.set("dfs.namenode.rpc-address.hdfs-cluster.nn1", "172.20.32.156:8020");
        conf.set("dfs.namenode.rpc-address.hdfs-cluster.nn2", "172.20.32.155:8020");
        conf.set("dfs.namenode.rpc-address.hdfs-cluster.nn3", "172.20.32.151:8020");
        conf.set("dfs.client.failover.proxy.provider.hdfs-cluster",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        conf.set("dfs.data.transfer.protection", "integrity");
        conf.set("hive.metastore.kerberos.principal",
                "hive/172.20.32.156@EMR.C-0596176698BD4D17.COM");

        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hadoop.kerberos.keytab", "/Users/morningman/workspace/kerberos/aliemr/hdfs.keytab");
        conf.set("hadoop.kerberos.principal",
                "hdfs/172.20.32.156@EMR.C-0596176698BD4D17.COM");
        AuthenticationConfig authConf = AuthenticationConfig.getKerberosConfig(conf);
        HadoopAuthenticator hadoopAuthenticator = HadoopAuthenticator.getHadoopAuthenticator(authConf);
        CatalogContext context = CatalogContext.create(options, conf);
        hadoopAuthenticator.doAs(() -> {
            Catalog paimonCatalog = CatalogFactory.createCatalog(context);
            List<String> dbs = paimonCatalog.listDatabases();
            System.out.println(dbs);
            return paimonCatalog;
        });
    }
}