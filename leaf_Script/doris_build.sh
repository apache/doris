#!/bin/bash
#git clone代码、启动容器进行编译

set -e

# 安装日志
install_log=/var/log/doris_build.log
tm=$(date +'%Y%m%d %T')

# 日志颜色
COLOR_G="\x1b[0;32m"  # green
RESET="\x1b[0m"

function info(){
    echo -e "${COLOR_G}[$tm] [Info] ${1}${RESET}"
}

function run_cmd(){
  sh -c "$1 | $(tee -a "$install_log")"
}

function run_function(){
  $1 | tee -a "$install_log"
}

git status | tee -a "$install_log"
time=`date +%y%m%d%H%M%y`
cp -rf ./incubator-doris doris_$time

docker run -td --privileged -v /home/leaf/.m2:/root/.m2 -v /opt/doris_$time:/root/doris_$time/ apache/incubator-doris:build-env-for-0.15.0 /bin/sh -c "bash /root/doris_$time/build.sh >> /root/build.log 2>&1"



