#!/bin/bash
#docker cmds used in pipline
USER=work

usage() {
    echo "
Usage: $0 <options>
  Optional options:
     --docker_complile work_path                  run a docker compile task in work_path, make sure build file in work_path
     --docker_run_fe work_path                    run a docker fe ut in work_path, make sure fe ut files in work_path
     --docker_run_be work_path                    run a docker be ut in work_path, make sure be ut files in work_path

  Eg.
    $0                                  teamcity api
    $0 --docker_complile /home/work_doris
       like: "docker run -i --rm --name doris-compile-0 -e TZ=Asia/Shanghai -v /etc/localtime:/etc/localtime:ro -v /home/work/.m2:/root/.m2 -v /home/work/.npm:/root/.npm -v /home/work_doris:/root/doris apache/incubator-doris:build-env-ldb-toolchain-latest /bin/bash -c "cd /root/doris && sh builds.sh"
    
    $0 --cancel_running_build 303       cancel 303 running build
    $0 --cancel_pending_build 304       cancel 304 pending build
    $0 --show_build_status 305          show 305 build status
    $0 --show_build_state 306           show 306 build state
    $0 --show_latest_builds             show 100 latest builds
    $0 --show_queued_builds             show all queued builds
  "
  exit 1

}

gen_docker_cmd() {
    user=$USER
    work_path=$1
    exe_cmd=$2
    docker_name=$3
    docker_cmd="docker run -i --rm --name $docker_name"
}

docker_compile() {
   
    id=1
    docker_name="doris-compile-"$(echo $RANDOM)

    cmd="docker run -i --rm --name doris-compile-$id -e TZ=Asia/Shanghai -v /etc/localtime:/etc/localtime:ro -v /home/work/.m2:/root/.m2 -v /home/work/.npm:/root/.npm -v %system.teamcity.build.workingDir%:/root/doris apache/incubator-doris:build-env-ldb-toolchain-latest /bin/bash -c "cd /root/doris && sh builds.sh"
"
}
