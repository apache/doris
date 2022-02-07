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
# This file is copied from
# https://github.com/ClickHouse/ClickHouse/blob/master/docker/test/performance-comparison/download.sh
# and modified by Doris.

set -ex
set -o pipefail
trap "exit" INT TERM
trap 'kill $(jobs -pr) ||:' EXIT

S3_URL=${S3_URL:="https://doris-build.oss-cn-beijing-internal.aliyuncs.com/doris-build/"}

git --version

# Sometimes AWS responde with DNS error and it's impossible to retry it with
# current curl version options.
function curl_with_retry
{
    for _ in 1 2 3 4; do
        if curl --fail --head "$1";then
            return 0
        else
            sleep 0.5
        fi
    done
    return 1
}

# Use the packaged repository to find the revision we will compare to.
function find_reference_sha
{
    cd right/doris
    git log -1 origin/master
    git log -1 pr
    # Go back from the revision to be tested, trying to find the closest published
    # testing release. The PR branch may be either pull/*/head which is the
    # author's branch, or pull/*/merge, which is head merged with some master
    # automatically by Github. We will use a merge base with master as a reference
    # for tesing (or some older commit). A caveat is that if we're testing the
    # master, the merge base is the tested commit itself, so we have to step back
    # once.
    start_ref=$(git merge-base origin/master pr)
    if [ "$PR_TO_TEST" == "0" ]
    then
        start_ref=$start_ref~
    fi

    # Loop back to find a commit that actually has a published perf test package.
    while :
    do
        # FIXME the original idea was to compare to a closest testing tag, which
        # is a version that is verified to work correctly. However, we're having
        # some test stability issues now, and the testing release can't roll out
        # for more that a weak already because of that. Temporarily switch to
        # using just closest master, so that we can go on.
        #ref_tag=$(git -C ch describe --match='v*-testing' --abbrev=0 --first-parent "$start_ref")
        ref_tag="$start_ref"

        echo Reference tag is "$ref_tag"
        # We use annotated tags which have their own shas, so we have to further
        # dereference the tag to get the commit it points to, hence the '~0' thing.
        REF_SHA=$(git rev-parse "$ref_tag~0")

        # FIXME sometimes we have testing tags on commits without published builds.
        # Normally these are documentation commits. Loop to skip them.
        # Historically there were various path for the performance test package,
        # test all of them.
        if curl_with_retry "${S3_URL}/0/$REF_SHA/performance/output/performance.tgz"
        then
            start_ref="$REF_SHA~"
            break
        fi
    done

    REF_PR=0
    cd -
}

# Download the package for the version we are going to test.
if curl_with_retry "$S3_URL/$PR_TO_TEST/$SHA_TO_TEST/performance/output/performance.tgz"
then
    right_path="$S3_URL/$PR_TO_TEST/$SHA_TO_TEST/performance/output/performance.tgz"
fi

rm right -rf
mkdir right ||:
wget -nv -nd -c "$right_path" -O- | tar -C right --strip-components=1 -zxv
/usr/bin/cp conf/fe/* right/fe/conf/ -rf
/usr/bin/cp conf/be/* right/be/conf/ -rf
mkdir right/be/doris.SSD -p
mkdir right/log -p
mkdir right/doris-meta -p

# Find reference revision if not specified explicitly
if [ "$REF_SHA" == "" ]; then find_reference_sha; fi
if [ "$REF_SHA" == "" ]; then echo Reference SHA is not specified ; exit 1 ; fi
if [ "$REF_PR" == "" ]; then echo Reference PR is not specified ; exit 1 ; fi

cd right/doris
(
    git log -1 --decorate "$SHA_TO_TEST" ||:
    echo
    echo Real tested commit is:
    git log -1 --decorate "pr"
) | tee right-commit.txt
cd -
/usr/bin/cp right/doris/right-commit.txt ./ -f

# Even if we have some errors, try our best to save the logs.
set +e

PATH="$(readlink -f right/)":"$PATH"
export PATH

export REF_PR
export REF_SHA

rm left -rf ||:
mkdir left ||:

left_pr=$REF_PR
left_sha=$REF_SHA

# right_pr=$3 not used for now
right_sha=$SHA_TO_TEST

left_path="$S3_URL/$left_pr/$left_sha/performance/output/performance.tgz"

# Might have the same version on left and right (for testing) -- in this case we just copy
# already downloaded 'right' to the 'left. There is the third case when we don't have to
# download anything, for example in some manual runs. In this case, SHAs are not set.
if ! [ "$left_sha" = "$right_sha" ]
then
    wget -nv -nd -c "$left_path" -O- | tar -C left --strip-components=1 -zxv  &
elif [ "$right_sha" != "" ]
then
    mkdir left ||:
    cp -an right/* left &
fi

# Show what we're testing
cd left/doris
(
    git log -1 --decorate "$REF_SHA" ||:
) | tee left-commit.txt
cd -

/usr/bin/cp left/doris/left-commit.txt ./ -f

/usr/bin/cp conf/fe/* left/fe/conf/ -rf
/usr/bin/cp conf/be/* left/be/conf/ -rf

mkdir left/be/doris.SSD -p
mkdir left/log -p
mkdir left/doris-meta -p

wait
