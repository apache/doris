#!/bin/bash
work_path=$1
id=$(echo $RANDOM)

docker run -i --rm --name doris-compile-$id -e TZ=Asia/Shanghai -v /etc/localtime:/etc/localtime:ro -v /home/work/.m2:/root/.m2 -v /home/work/.npm:/root/.npm -v $work_path:/root/doris apache/incubator-doris:build-env-ldb-toolchain-latest /bin/bash -c "cd /root/doris && sh builds.sh"
