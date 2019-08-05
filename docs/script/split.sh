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

#this shell splits topics in the same MD file
IFS=`echo -en "\n\b"`

ROOTDIR=`dirname "$0"`
ROOTDIR=`cd "$ROOT"; pwd`

scandir() {
    for file in `ls $*`; do
		if [[ ! -d $*"/"$file ]]; then
			if [[ $file == *".md" ]]; then
				splitfile $*"/"${file}
			fi
		else
			scandir $*"/"${file}
		fi
	done
}

splitfile() {
	local file=$*
	local filedir=${file%/*}
	local evalfile=`echo $file | sed 's/[ \(\)]/\\\&/g'`

	local row=0
	local split=1
	local name=
	local TotalRow=`wc -l $file | awk '{print $1}'`
	local TopicNum=`grep -o '^#[^#].*' $file | wc -l`
	if [ $TopicNum -lt 2 ]; then
		return
	fi
	while read line; do
		((row++))

		if [[ $row == $TotalRow || $line =~ ^#[^#].* ]]; then
			if [[ -n $name && $split != $row ]]; then
				eval awk '"NR==${split},NR==$(($row==$TotalRow?row:row-1))"' ${evalfile} > ${ROOTDIR}/tempp
				cp ${ROOTDIR}/tempp ${filedir}/${name}.md
			fi
			name=`echo $line | grep -o "[^# ]\+\( \+[^ ]\+\)*"`
			split=$row
		fi
	done < $file
	if [[ -f ${ROOTDIR}/tempp ]]; then
		rm ${ROOTDIR}/tempp
	fi
	rm $file
}

main() {
	scandir $ROOTDIR
}

main "$@"
exit 0
