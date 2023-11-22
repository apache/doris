# Tutorial of Nereids minidump
Minidump is a basic dump file for Nereids optimizer which arms  
to replay problems and used as unit test tool for developers.  
It include minimal necessary informations for Nereids optimizer  
to replay a scenario in Optimizer.

## Save minidump file
In two cases fe would save minidump file
- when query throw an exception
- when use 'set enable_minidump=true;' switch by User

## File structure of minidump file
Minidump files would save in $DORIS_HOME/log/minidump path  
and named by queryId like: c9bf82b70ce34c28-8d281e31412f3ee6.json  
File structure of minidump file includes:  
| key | Value |   
|:----|:----|  
|Sql  | original sql string  |  
|SessionVariables  | sessionVariables different than default |  
|DatabaseName  | database name|  
|Tables  | schemas in table, partition info and some table properties |
|ColocateTableIndex | table distribute informations |  
|ColumnStatistics | all ColumnStatistics in table |  
|Histogram | all histogram information in table|  
|ParsedPlan | plan after parsed in json format|  
|ResultPlan | plan after all optimized in json format|

## Use of nereids_ut.sh
When we want to use this shell, we need to go to output/fe/minidump path  
Now only one parameter is allowed for this shell which is directory  
of minidump files like $DORIS_HOME/log/minidump
```shell
cd output/fe/minidump
./nereids_ut.sh --d $minidump_files_path
```
The running reports can be found in output/fe/minidump/log  
which is minidump.out

