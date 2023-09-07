import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.security.UserGroupInformation;

import java.security.PrivilegedAction;

public class TestExternalCatalog {
    public static void main(String[] args) {
        // our();
        user();
    }

    private static void user() {
        HiveConf hiveConf = new HiveConf();

        System.out.println(System.getProperty("java.security.krb5.conf"));
        System.out.println(System.getProperty("java.security.krb5.conf.path"));
        System.out.println(System.getProperty("sun.security.krb5.debug"));


        System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
        System.setProperty("java.security.krb5.conf.path", "/etc/krb5.conf");

        System.out.println(System.getProperty("java.security.krb5.conf"));
        System.out.println(System.getProperty("java.security.krb5.conf.path"));

        hiveConf.set("hive.metastore.kerberos.principal", "hive/cnwulcdhmast03@SGMWT.COM");
        hiveConf.set("hive.metastore.uris", "thrift://cnwulcdhmast03:9083");
        hiveConf.set("hive.metastore.sasl.enabled", "true");
        hiveConf.set("hive.metastore.kerberos.keytab.file", "/etc/cnwulcdhmast03.keytab");
        hiveConf.set("hadoop.security.authentication", "kerberos");
        hiveConf.set("hadoop.security.authorization", "true");
        hiveConf.setBoolean("ipc.client.fallback-to-simple-auth-allowed", true);

        //        hiveConf.set("dfs.nameservices", "hdfs");
        //        hiveConf.set("dfs.ha.namenodes.hdfs", "namenode43,namenode45");
        //        hiveConf.set("dfs.namenode.rpc-address.hdfs.namenode43", "cnwulcdhmast03:8020");
        //        hiveConf.set("dfs.namenode.rpc-address.hdfs.namenode45", "cnwulcdhmast04:8020");
        //        hiveConf.set("dfs.client.failover.proxy.provider.hdfs",
        //                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        //
        //        hiveConf.set("hadoop.security.authentication", "kerberos");
        //        hiveConf.set("hadoop.kerberos.keytab", "/etc/etl.keytab");
        //        hiveConf.set("hadoop.kerberos.principal", "etl@SGMWT.COM");
        //
        //        hiveConf.set("yarn.resourcemanager.principal", "yarn/cnwulcdhnodet07@SGMWT.COM");
        try {
            System.out.println(hiveConf.get("hive.metastore.kerberos.principal", "111"));
            System.out.println(hiveConf.get("hive.metastore.kerberos.keytab.file", "222"));
            UserGroupInformation.setConfiguration(hiveConf);
            UserGroupInformation ugi = UserGroupInformation
                    .loginUserFromKeytabAndReturnUGI(
                            "hive/cnwulcdhmast03@SGMWT.COM", "/etc/cnwulcdhmast03.keytab");
            ugi.doAs((PrivilegedAction<?>) () -> {
                try {
                    IMetaStoreClient client = RetryingMetaStoreClient.getProxy(hiveConf, t -> null,
                            HiveMetaStoreClient.class.getName());
                    client.getAllDatabases().forEach(System.out::println);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return null;
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void our() {
        HiveConf hiveConf = new HiveConf();
        try {
            hiveConf.set("hive.metastore.kerberos.principal", "hadoop/_HOST@EMR-LGUPF4P6");
            hiveConf.set("hive.metastore.uris", "thrift://172.21.16.13:7004");
            hiveConf.set("hive.metastore.kerberos.keytab.file", "/var/krb5kdc/emr.keytab");
            hiveConf.set("hive.metastore.sasl.enabled", "true");
            hiveConf.set("hadoop.security.authentication", "kerberos");

            System.out.println(hiveConf.get("hive.metastore.kerberos.principal", "111"));
            System.out.println(hiveConf.get("hive.metastore.kerberos.keytab.file", "222"));
            UserGroupInformation.setConfiguration(hiveConf);
            UserGroupInformation ugi = UserGroupInformation
                    .loginUserFromKeytabAndReturnUGI(
                            "hadoop/172.21.16.13@EMR-LGUPF4P6", "/var/krb5kdc/emr.keytab");
            ugi.doAs((PrivilegedAction<?>) () -> {
                try {
                    IMetaStoreClient client = RetryingMetaStoreClient.getProxy(hiveConf, t -> null,
                            HiveMetaStoreClient.class.getName());
                    client.getAllDatabases().forEach(System.out::println);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
