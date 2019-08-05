#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

#this shell adds keywords to MD files without keywords

IFS=`echo -en "\n\b"`

ROOTDIR=`dirname "$0"`
ROOTDIR=`cd "$ROOT"; pwd`

scandir() {
    for file in `ls $*`; do
        if [[ ! -d $*"/"$file ]]; then
            if [[ $file == *".md" ]]; then
                readfile $*"/"${file}
            fi
        else
            scandir $*"/"${file}
        fi
    done
}

readfile() {
    local file=$*
    local topic=`cat $file | grep "^#[^#].*" | grep -o "[^# ]\+\( \+[^ ]\+\)*"`
    local keywordNum=`cat $file | grep "^##[^#]*keyword[ ]*$" | wc -l`
    if [[ $keywordNum != 0 || -z $topic ]]; then
        return
    fi
    local SAVEIFS=$IFS
    IFS=' '
    local array=`echo $topic | tr '\`' ' ' | tr ',' ' '`
    local keywords=
    for keyword in ${array[*]}; do
        keywords=$keywords","$keyword
    done
    array=`echo $array | tr '_' ' '`
    for keyword in ${array[*]}; do
        keywords=$keywords","$keyword
    done
    keywords=`echo ${keywords:1} | tr 'a-z' 'A-Z'`
    IFS=$SAVEIFS
    file=`echo $file | sed 's/[ \(\)]/\\\&/g'`
    eval sed -i '"\$a ##keyword"' $file
    eval sed -i '"\$a ${keywords}"' $file
}

main() {
    scandir $ROOTDIR
}

main "$@"
exit 0
