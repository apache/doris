#!/bin/bash
#引用自定义配置文件
source ./conf/mysql.conf
source ./conf/doris.conf

#自定义输入输出参数
d_mysql=$1
d_doris=$2

#参数判断，不能为空
if [ ! -n "$1" ];then
        echo "请输入源数据库名称"
        exit
fi
if [ ! -n "$2" ];then
        echo "请输入目标数据库名称"
        exit
fi

#TO TEST用户权限问题，创建files文件存放生成的表名文件和建表语句文件，多次创建会删除掉上次生成的文件
mkdir -p files
rm -rf ./files/tables tables.sql

#从mysql获取到库下所有的表名写入到files下的tables里面
echo "use $d_mysql; show tables;" |mysql -h$mysql  -uroot -p$mysql_password >> ./files/tables

#tables中有第一行是我们不想要的，删除掉
sed -i '1d' ./files/tables

#对tables进行变量引用获取到表名写入到files下的tables.sql里面
for table in $(awk -F '\n' '{print $1}' ./files/tables)
        do
        sed -i "/${table}view/d" ./files/tables
        echo "use $d_mysql; show create table ${table};" |mysql -h$mysql -uroot -p$mysql_password >> ./files/tables.sql
done

#输出的sql文件并不规则，对其进行调整
awk -F '\t' '{print $2}' ./files/tables.sql |awk '!(NR%2)' |awk '{print $0 ";"}' > ./files/tables1.sql
sed -i 's/\\n/\n/g' ./files/tables1.sql
sed -n '/CREATE TABLE/,/ENGINE\=/p' ./files/tables1.sql > ./files/tables2.sql
#删除表的特定结构
sed -i '/^  CON/d' ./files/tables2.sql
sed -i '/^  KEY/d' ./files/tables2.sql
rm -rf ./files/tables.sql
rm -rf ./files/tables1.sql
mv ./files/tables2.sql ./files/tables.sql

#开始对表结构进行转换，缺少注释，后面优化;添加如果存在则创建
sed -i '/ENGINE=/a) ENGINE=MYSQL\n COMMENT "MYSQL"\nPROPERTIES (\n"host" = "ApacheDorisHostIp",\n"port" = "3306",\n"user" = "root",\n"password" = "ApacheDorisHostPassword",\n"database" = "ApacheDorisDataBases",\n"table" = "ApacheDorisTables");' ./files/tables.sql

#删除匹配行
sed -i '/ENGINT=/d' ./files/tables.sql
sed -i '/PRIMARY KEY/d' ./files/tables.sql
sed -i '/UNIQUE KEY/d' ./files/tables.sql
#删除以(开头的上一行的，
sed -i '/,\s*$/{:loop; N; /,\(\s*\|\n\))/! bloop; s/,\s*[\n]\?\s*)/\n)/}' ./files/tables.sql

#删除指定关键字的上方的一行
sed -i -e '$!N;/\n.*ENGINE=MYSQL/!P;D' ./files/tables.sql
#循环替换mysql的password、database、table、host等
for t_name in $(awk -F '\n' '{print $1}' ./files/tables)
        do
        sed -i "0,/ApacheDorisHostIp/s/ApacheDorisHostIp/${mysql}/" ./files/tables.sql
        sed -i "0,/ApacheDorisHostPassword/s/ApacheDorisHostPassword/${mysql_password}/" ./files/tables.sql
        sed -i "0,/ApacheDorisDataBases/s/ApacheDorisDataBases/${d_mysql}/" ./files/tables.sql
        sed -i "0,/ApacheDorisTables/s/ApacheDorisTables/${t_name}/" ./files/tables.sql

done
######################################################################################################################################################################
#mysql类型替换
sed -i 's/text/string/g' ./files/tables.sql
sed -i 's/tinyblob/string/g' ./files/tables.sql
sed -i 's/blob/string/g' ./files/tables.sql
sed -i 's/mediumblob/string/g' ./files/tables.sql
sed -i 's/longblob/string/g' ./files/tables.sql
sed -i 's/tinystring/string/g' ./files/tables.sql
sed -i 's/mediumstring/string/g' ./files/tables.sql
sed -i 's/longstring/string/g' ./files/tables.sql
sed -i 's/timestamp/datetime/g' ./files/tables.sql
sed -i 's/AUTO_INCREMENT//g' ./files/tables.sql
sed -i 's/unsigned//g' ./files/tables.sql
sed -i 's/zerofill//g' ./files/tables.sql
sed -i 's/json/string/g' ./files/tables.sql
sed -i 's/enum/string/g' ./files/tables.sql
sed -i 's/set/string/g' ./files/tables.sql
sed -i 's/bit/string/g' ./files/tables.sql
sed -i 's/CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci//g'  ./files/tables.sql
sed -i 's/CHARACTER SET utf8mb4 COLLATE utf8_general_ci//g' ./files/tables.sql
sed -i 's/CHARACTER SET utf8 COLLATE utf8_general_ci//g' ./files/tables.sql
sed -i 's/COLLATE utf8mb4_general_ci//g' ./files/tables.sql
sed -i 's/COLLATE utf8_general_ci//g'  ./files/tables.sql
sed -i 's/DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP//g' ./files/tables.sql
sed -i 's/DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP//g' ./files/tables.sql
sed -i 's/CHARACTER SET utf8 COLLATE utf8_bin//g' ./files/tables.sql
sed -i 's/COLLATE utf8_general_ci//g'  ./files/tables.sql
sed -i 's/datetime([0-9])/string/g' ./files/tables.sql
sed -i 's/CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci//g' ./files/tables.sql
sed -i 's/binary/string/g' ./files/tables.sql
sed -i 's/varbinary/string/g' ./files/tables.sql
sed -i 's/binary([0-9])/string/g' ./files/tables.sql
sed -i 's/varbinary([0-9])/string/g' ./files/tables.sql
sed -i 's/string([0-9])/string/g' ./files/tables.sql
sed -i 's/binary([0-9][0-9])/string/g' ./files/tables.sql
sed -i 's/varbinary([0-9][0-9])/string/g' ./files/tables.sql
sed -i 's/string([0-9][0-9])/string/g' ./files/tables.sql
sed -i 's/binary([0-9][0-9][0-9])/string/g' ./files/tables.sql
sed -i 's/varbinary([0-9][0-9][0-9])/string/g' ./files/tables.sql
sed -i 's/string([0-9][0-9][0-9])/string/g' ./files/tables.sql
sed -i 's/CHARACTER SET utf8mb4 COLLATE utf8mb4_bin//g' ./files/tables.sql


#######################################
#入库
echo " create database if not exists $d_doris ;use $d_doris ; source ./files/tables.sql;" |mysql -h$master_host -P$master_port -uroot -p$doris_password
