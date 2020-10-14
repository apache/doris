<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

## Getting Started

Installation dependencies

```bash
$ npm install
```
or use yarn
```
$ npm install -g yarn
$ yarn install --pure-lockfile
```

Start server.

```bash
$ npm run dev

# visit http://localhost:8030
```

Submit code

```bash
$ git commit -m "xxx" will automatically run npm run lint to check grammar rules
```

Construct

```bash
$ npm run build
```

Technology stack convention

```
react + react-router-dom + ant-design + rxjs
```

## File introduction

```
  public: some static resources
  
  src: development home directory
    assets: static resources, pictures, etc., refer to webpack
    components: common components
    pages: subpages, can contain subcomponents
    utils: public methods

  webpack.config.js: webpack configuration

```

