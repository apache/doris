#!/bin/bash

env_name=$1
port=$2
case_level=$3
#be_list=(`ps -ef|grep $port|grep -v grep|awk '{print $2}'`)

echo "===================================="
echo "START TO DETECT RESIDUAL PROCESSES!"

check=$(lsof -i:$port|awk '{print $2}'|wc -l)
if [ "check$check" != "check0" ];then
    if [ "check"${case_level} == "check" ];then
        check_res=$(pwdx `lsof -i:$port|grep -v PID|awk '{print $2}'`|grep $env_name|grep deleted||wc -l)
    else
        check_res=$(pwdx `lsof -i:$port|grep -v PID|awk '{print $2}'`|grep $env_name|grep $case_level|grep deleted||wc -l)
    fi
    if [ "check$check_res" != "check0" ];then
        be_pid=(`lsof -i:$port|grep -v PID|awk '{print $2}'`)
        echo "Detected residual processes: ${be_pid}"
        echo "kill residual processes: kill -9 ${be_pid}"
        kill -9 $be_pid
    fi
else
        echo "No residual processes"
fi


echo "FINISH DETECT RESIDUAL PROCESSES!"
echo "==================================="
