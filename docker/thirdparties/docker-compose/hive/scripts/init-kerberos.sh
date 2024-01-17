/etc/init.d/sshd start
mkdir -p ~/.ssh && cd ~/.ssh/ && ssh-keygen -t rsa -N '' -f /root/.ssh/id_rsa -q && cat ./id_rsa.pub >> ./authorized_keys

# add kerberos principal
HOSTNAME=$(hostname)

sed -i 's/localhost/'$HOSTNAME'/g' /root/hadoop/hadoop-2.7.5/etc/hadoop/core-site.xml
echo "$HOSTNAME" > /root/hadoop/hadoop-2.7.5/etc/hadoop/slaves


sed -i 's/localhost/'$HOSTNAME'/g' /etc/krb5.conf

kadmin.local -q "addprinc -randkey hdfs/$HOSTNAME@NODE.DC1.CONSUL"
kadmin.local -q "ktadd -k /etc/krb5.keytab hdfs/$HOSTNAME@NODE.DC1.CONSUL"

kinit -c /tmp/krb5cc_0 -t /etc/krb5.keytab -k hdfs/$HOSTNAME@NODE.DC1.CONSUL
kinit -c /tmp/krb5cc_1 -t /etc/https/krb5.keytab -k HTTP@NODE.DC1.CONSUL
hdfs namenode -format

/root/hadoop/hadoop-2.7.5/sbin/start-dfs.sh

cd /root/hadoop/apache-hive-3.1.3-bin && hive --service metastore &

hive --hiveconf hive.root.logger=DEBUG,console -e "CREATE TABLE IF NOT EXISTS types ( hms_int INT, hms_smallint SMALLINT, hms_bigint BIGINT, hms_double DOUBLE, hms_string STRING, hms_decimal DECIMAL(12,4), hms_char CHAR(50), hms_varchar VARCHAR(50), hms_bool BOOLEAN, hms_timstamp TIMESTAMP, hms_date DATE ) ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat';"

hive --hiveconf hive.root.logger=DEBUG,console -e 'insert into types values(1123,5126,51,4534.63463,"wastxali",235.2351,"a23f","1234vb",false,"2023-04-23 21:23:34.123","2023-04-23")';

