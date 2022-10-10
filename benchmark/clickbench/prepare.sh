#!/bin/bash

echo 'check if skip'


set -ex

prepare_home=/home/ec2-user/
checkout_home=$(pwd)



echo '====prepare===='
cd "$prepare_home"
if [[ ! -d "$prepare_home/selectdb-qa" ]]; then
    git clone git@github.com:selectdb/selectdb-qa.git
fi
echo 'update scripts from git@github.com:selectdb/selectdb-qa.git'
qa_home="$prepare_home/selectdb-qa"
cd "$qa_home" && git stash && git checkout main && git pull && cd -


