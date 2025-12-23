## Supported OSes

* CentOS 7
* Debian 10 and 11
* Fedora 37
* Ubuntu 18, 20 and 22

## Pre-built Images

Apache ORC community provides a set of pre-built docker images and uses it during testing.

    docker pull apache/orc-dev:ubuntu22

You can find all tags here.

    https://hub.docker.com/r/apache/orc-dev/tags

## Test

To test against all of the Linux OSes against Apache's main branch:

    cd docker
    ./run-all.sh apache main

Using `local` as the owner will cause the scripts to use the local repository.

The scripts are:
* `run-all.sh` *owner* *branch* - test the given owner's branch on all OSes
* `run-one.sh` *owner* *branch* *os* - test the owner's branch on one OS
* `reinit.sh` - rebuild all of the base images without the image cache

`run-all.sh`, `run-one.sh` and `reinit.sh` tests both on jdk8 and 11 across OSes

A base image for each OS is built using:

    cd docker/$os
    FOR jdk8:  docker build -t "orc-$os-jdk8" --build-arg jdk=8 .
    FOR jdk11: docker build -t "orc-$os-jdk11" --build-arg jdk=11 .

## Clean up

    docker container prune
    docker image prune
