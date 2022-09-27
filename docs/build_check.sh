#!/usr/bin/env bash
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

##############################################################
# This is a native document format compilation check script
##############################################################

#!/bin/bash

set -eo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

cd "${ROOT}/.."

git clone https://github.com/apache/doris-website.git website
rm -rf website/docs
cp -R docs/en/docs website/
rm -rf website/community
cp -R docs/en/community website/
rm -rf website/i18n/zh-CN/docusaurus-plugin-content-docs/*
mkdir  website/i18n/zh-CN/docusaurus-plugin-content-docs/current
cp -R docs/zh-CN/docs/* website/i18n/zh-CN/docusaurus-plugin-content-docs/current/
cp docs/dev.json website/i18n/zh-CN/docusaurus-plugin-content-docs/current.json
rm -rf  website/i18n/zh-CN/docusaurus-plugin-content-docs-community/*
mkdir website/i18n/zh-CN/docusaurus-plugin-content-docs-community/current
cp -R docs/zh-CN/community/* website/i18n/zh-CN/docusaurus-plugin-content-docs-community/current/
cp -R docs/sidebarsCommunity.json website/
cp -R docs/sidebars.json website/
cp -R docs/images website/static/
sed '2,3d' website/versions.json > website/versions.json1
rm -rf website/versions.json
mv website/versions.json1 website/versions.json
sed '123,128d' website/docusaurus.config.js > website/docusaurus.config.js1
rm -rf website/docusaurus.config.js
mv website/docusaurus.config.js1 website/docusaurus.config.js
cd website
npm install -g npm@8.19.1
npm install -g yarn
yarn cache clean
yarn && yarn build   
cd ../
rm -rf website   

echo "***************************************"
echo "Docs build check pass"
echo "***************************************"
