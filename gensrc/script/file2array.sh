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

####################################################################
# This script is used to build Doris used in Baidu

# Convert file to a C array with provided name.
#
# Also produce length variable of type size_t with _len suffix.
#
# We add an extra null byte on the end of the array, which is not
# included in the length, to allow it to be used as a string if needed.
#
set -e

arrname=""
modifiers=""
null_terminate=0

usage () {
  echo "Usage: $0 [options] [input file] " >&2
  echo "Options:" >&2
  echo "  -v <c array variable name>" >&2
  echo "     Name of C variable in output file. Must be provided." >&2
  echo "  -m <array variable modifiers>" >&2
  echo "     Modifiers for C variable in output file. Default is const." >&2
  echo "  -n" >&2
  echo "     Add a trailing null byte, not included in length." >&2
  exit 1
}

while getopts "m:nv:" opt; do
  case $opt in
    m)
      if [[ $modifiers != "" ]]; then
        echo "-m specified twice" >&2
        usage
      fi
      modifiers=$OPTARG
      ;;
    n)
      null_terminate=1
      ;;
    v)
      if [[ $arrname != "" ]]; then
        echo "-v specified twice" >&2
        usage
      fi
      arrname=$OPTARG
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      usage
  esac
done
shift $((OPTIND - 1))

infile=$1
if [[ $# > 1 ]]; then
  echo "Too many remaining arguments: $@" >&2
  usage
fi

if [ -z "$arrname" ]; then
  echo "-v not provided or empty." >&2
  usage
fi

if [ -z "$modifiers" ]; then
  # Default is const with global linking visibility
  modifiers="const"
fi

echo "#include <stddef.h>" # For size_t
echo
# Preceding extern declaration guarantees external linkage in C++
echo "extern $modifiers unsigned char $arrname[];";
echo "extern $modifiers size_t ${arrname}_len;"
echo
echo "$modifiers unsigned char $arrname[] = {"
xxd -i < $infile
if [ ${null_terminate} = 1 ]; then
  echo ", 0x0"
fi
echo "};"

echo -n "$modifiers size_t ${arrname}_len = "
if [ ${null_terminate} = 1 ]; then
  echo "sizeof(${arrname}) - 1;"
else
  echo "sizeof(${arrname});"
fi

