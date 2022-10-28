#!/bin/bash
source ../conf/e_table.conf

#mkdir files to store tables and tables.sql
mkdir -p ../result/hive

#The default path is ../result/mysql_to_doris.sql for create table sql
path=${1:-../result/hive/hive_to_doris.sql}

#delete sql file if it is exists
rm -f $path

#get show create table sql from hive
for table in $(cat ../conf/hive_tables |grep -v '#' | awk -F '\n' '{print $1}' sed 's/ //g' | sed '/^$/d')
        do
        h_db=`echo $table| awk -F '.' '{print $1}'`
        h_dt=`echo $table| awk -F '.' '{print $2}'`
        echo "show create table \`$h_db\`.\`$h_dt\`;" >> ../result/hive/tmp
done

#execute sql file by hive command
hive -f ../result/hive/tmp >> $path

#delete temporary file
rm -rf ../result/hive/tmp

#delete other keywords
sed -i '/^createtab_stmt$/d' $path

#Intercepts the content between two keywords
sed -n '/^CREATE/,/)$/p' $path >> ../result/hive/tmp

#delete dir and change dir name
rm -rf $path
mv ../result/hive/tmp $path

#add key
sed -i '/)$/a ENGINE=HIVE\nCOMMENT \"HIVE\"\nPROPERTIES (\n);' $path
sed -i "/PROPERTIES (/a \'table\' = \'hive_table\'" $path
sed -i "/PROPERTIES (/a \'database\' = \'hive_db\'," $path

#add hive conf
for i in $(cat ../conf/e_table.conf |sed -n '/Hive/,+16p' |grep -v '#' |awk -F '\n' '{print $1}' |sed 's/ //g' | sed '/^$/d')
do
        ifnull=`echo $i |awk -F '=' '{print $2}'`
        key=`echo $i |awk -F '=' '{print $1}'`
        value=`echo $i |awk -F '=' '{print $2}'`
        if [ $ifnull != \'\'  ];then
                sed -i "/^PROPERTIES (/a'$key' = $value," $path
        fi

done

#alter hive conf
sed -i "/'hive_metastore_uris'/s/_/\./g" $path
sed -i "s/'dfs_nameservices'/'dfs.nameservices'/g" $path
sed -i "s/'dfs_ha_namenodes_hacluster'/'dfs.ha.namenodes.hacluster'/g" $path
sed -i "s/'dfs_namenode_rpc_address_hacluster_n1'/'dfs.namenode.rpc-address.hacluster.n1'/g" $path
sed -i "s/'dfs_namenode_rpc_address_hacluster_n2'/'dfs.namenode.rpc-address.hacluster.n2'/g" $path
sed -i "s/'dfs_client_failover_proxy_provider_hacluster'/'dfs.client.failover.proxy.provider.hacluster'/g" $path
sed -i "s/'dfs_namenode_kerberos_principal'/'dfs.namenode.kerberos.principal'/g" $path
sed -i "s/'hadoop_security_authentication'/'hadoop.security.authentication'/g" $path
sed -i "s/'hadoop_kerberos_principal'/'hadoop.kerberos.principal'/g" $path
sed -i "s/'hadoop_kerberos_keytab'/'hadoop.kerberos.keytab'/g" $path


#replace hive database and table
for t_name in $(cat ../conf/hive_tables |grep -v '#' | awk -F '\n' '{print $1}' |sed 's/ //g' | sed '/^$/d')
        do
        h_d=`echo $t_name | awk -F '.' '{print $1}'`
        h_t=`echo $t_name | awk -F '.' '{print $2}'`
        sed -i "0,/hive_db/s/hive_db/$h_d/" $path
        sed -i "0,/hive_table/s/hive_table/$h_t/" $path
done

##do transfrom from hive to doris external
sh ../lib/hive_to_doris.sh $path


#alter hive table name become doris name table add if not exists statement
x=0
for table in $(cat ../conf/doris_tables |grep -v '#' |sed 's/ //g' | sed '/^$/d')
        do
        let x++
        h_t=`cat ../conf/hive_tables |grep -v '#' | awk "NR==$x{print}" |sed 's/ //g' | sed '/^$/d'`
        sed -i "s/TABLE \`$h_t\`/TABLE IF NOT EXISTS $table/g" $path
done

#create database and use database
for d_doris in $(cat ../conf/doris_tables |grep -v '#' | awk -F '\n' '{print $1}' |awk -F '.' '{print $1}' |sed 's/ //g' | sed '/^$/d' |sort -u)
do
                sed -i "1icreate database if not exists $d_doris;\nuse $d_doris;" $path
done
