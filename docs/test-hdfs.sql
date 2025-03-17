SELECT * FROM image
    INTO OUTFILE "hdfs://hdfs-cluster/ck/outfile_test"
FORMAT AS CSV
PROPERTIES
(
'hadoop.security.authentication' = 'kerberos',
'fs.defaultFS'='hdfs://hdfs-cluster/',
'fs.hdfs.support'='true',
    'hadoop.kerberos.keytab' = '/Users/calvinkirs/soft/be/conf/hdfs.keytab',   
    'hadoop.kerberos.principal' = 'hdfs/master-1-1.c-0596176698bd4d17.cn-beijing.emr.aliyuncs.com@EMR.C-0596176698BD4D17.COM',
    'dfs.nameservices'='hdfs-cluster',
    'dfs.ha.namenodes.hdfs-cluster'='nn1,nn2,nn3',
    'dfs.namenode.rpc-address.hdfs-cluster.nn1'='master-1-1.c-0596176698bd4d17.cn-beijing.emr.aliyuncs.com:8020',
    'dfs.namenode.rpc-address.hdfs-cluster.nn2'='master-1-2.c-0596176698bd4d17.cn-beijing.emr.aliyuncs.com:8020',
    'dfs.namenode.rpc-address.hdfs-cluster.nn3'='master-1-3.c-0596176698bd4d17.cn-beijing.emr.aliyuncs.com:8020',
    'dfs.client.failover.proxy.provider.hdfs-cluster'='org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider',
    'dfs.data.transfer.protection' = 'integrity'
)