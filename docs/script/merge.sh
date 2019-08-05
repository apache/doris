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

#run split.h first
#this shell merges user-defined heads in the MD file into "description" head
################      #################
# #topic       #      # #topic        #
#              #      #               #
# ##syntax     #      # ##descrption  #
# somelines    #      # somelines     #
#              #      #               #
# ##descrption #      #  syntax       #
# somelines    #  ==> #  somelines    #
#              #      #               #
# ##example    #      #  parameter    #
# somelines    #      #  somelines    #
#              #      #               #
# ##parameter  #      # ##example     #
# somelines    #      # somelines     #
#              #      #               #
################      #################

IFS=`echo -en "\n\b"`

ROOTDIR=`dirname "$0"`
ROOTDIR=`cd "$ROOT"; pwd`

keywords="
examples
example
description
keywords
keyword
url
"

matchKeyword(){
    for keyword in ${keywords[*]}; do
    if [[ "$1" == $keyword ]]; then
        return 0
    fi
    done
    return 1
}

merge(){
    file=$*
    sed -n '/^#[^#]/p' $file > ${ROOTDIR}/tempp
    sed -n '/^<>TARGET<>/,/^<>END<>/p' $file >> ${ROOTDIR}/tempp
    sed -n '/^>>>/,/^<<</p' $file >> ${ROOTDIR}/tempp
    sed -n '/^\^\^\^/,/^\$\$\$/p' $file >> ${ROOTDIR}/tempp
    sed -i 's/^<>TARGET<>//;s/^<>END<>//;s/^>>>//;s/^<<<//;s/^\^\^\^//;s/^\$\$\$//' ${ROOTDIR}/tempp
}

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
    local tempfile=${ROOTDIR}"/temp"
    cp $* $tempfile

    local row=0
    local headlevel=0
    local style=
    local TotalRow=`wc -l temp | awk '{print $1}'`
    sed -i 's/##[#]\+/##/' $tempfile
    while read line; do
        ((row++))

        if [[ $row != $TotalRow && $line != "#"* ]]; then
            continue
        else
            if [[ $headlevel == 2 ]]; then
                case $style in
                    "match") 
                        eval sed -i '"$(($row==$TotalRow?row:row-1))s/^/\$\$\$/"' $tempfile
                    ;;
                    "unmatch") 
                        eval sed -i '"$(($row==$TotalRow?row:row-1))s/^/<<</"' $tempfile
                    ;;
                    "description") 
                        eval sed -i '"$(($row==$TotalRow?row:row-1))s/^/<>END<>/"' $tempfile
                    ;;
                    *) 
                        echo "Internal error" ; exit 1 
                    ;;
                esac
            fi

            if [[ $row == $TotalRow && headlevel > 0 ]]; then
                merge $tempfile
                continue
            fi

            if [[ $line == "##"* ]]; then
                headlevel=2
                line=`echo ${line:2} | tr '[A-Z]' '[a-z]' | grep -o "[^ ]\+\( \+[^ ]\+\)*"`
                if [[ $line == "description" ]]; then
                    eval sed -i '"${row}s/description/description/i"' $tempfile
                elif [[ $line == "examples" ]]; then
                    eval sed -i '"${row}s/examples/example/i"' $tempfile
                elif [[ $line == "keywords" ]]; then
                    eval sed -i '"${row}s/keywords/keyword/i"' $tempfile
                fi
                matchKeyword ${line}
                if [[ $? == 1 ]]; then
                    style="unmatch"
                    eval sed -i '"${row}s/^##/>>>/"' $tempfile
                else
                    if [[ $line == "description" ]]; then
                        style="description"
                        eval sed -i '"${row}s/^/<>TARGET<>/"' $tempfile
                        continue
                    fi
                    style="match"
                    eval sed -i '"${row}s/^/\^\^\^/"' $tempfile
                fi
            elif [[ $line == "#"* ]]; then
                if [[ headlevel == 0 ]]; then
                    headlevel=1
                    continue
                fi
                headleve=1
            fi
        fi
    done < $tempfile
    if [[ -f $tempfile ]]; then
        rm $tempfile
    fi
    if [[ -f ${ROOTDIR}/tempp ]]; then
        cp ${ROOTDIR}/tempp $* && rm ${ROOTDIR}/tempp
    fi
}

main() {
    scandir $ROOTDIR
}

main "$@"
exit 0
