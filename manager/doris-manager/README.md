<!--  Licensed to the Apache Software Foundation (ASF) under one
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
    under the License. -->
# Doris Manager Frontend Style Guide

## 整体风格
### 单一职责原则SRP（https://wikipedia.org/wiki/Single_responsibility_principle）
+ 每个文件只定义一个内容（减少心智负担）
+ 单文件要求不超过400行，超出进行拆分（易阅读）
+ 尽量定义内容小的函数，函数体不要过长

### 文件/文件夹夹命名：
- 字母全部小写
- 不带空格
- 用中划线（-）和 (.) 连接单词
- 规范为[feature].[type].ts（如ab-cd.hooks.ts）
- 建议使用英文单词全拼（易于理解，避免英文不好缩写错误），不好命名的可使用
https://unbug.github.io/codelf/ 进行搜索看看大家怎么命名

### Interface / Class 命名：
- Upper Camel Case 大驼峰命名，强类型语言惯例
- 文件名称为： [feature].interface.ts
- 单文件原则，尽量为 Interface 独立一个文件
- TypeScript Type 和 Interface 在同一个文件里

### 常量、配置、枚举命名
- 单文件原则，尽量为常量,枚举独立一个文件
- 文件名称为：[feature].data.ts
- 常量和配置都用 const 关键字，使用大写下划线命名
```
const PAGINATIONS = [10, 20, 50, 100, 200];
const SERVER_URL = 'http://127.0.0.1:8001';
```
- 枚举使用 Upper Camel Case 大驼峰命名，且后面加上Enum与   Interface / Class 区分
```
enum ImportTaskStatusEnum {
    Created: 0,
    Running: 1,
    Failed: 2,
}
```

### 行尾逗号
- 全部打开，便于在修改后在Git上看到清晰的比对（修改时追加 “,” 会让Git比对从一行变成2行）

### 组件
- 尽量不要在.tsx的组件里写太多逻辑，尽可能用hooks或工具类/服务（service）拆出去，保证视图层的干净清爽

### 圈复杂度（Cyclomatic complexity, CC）

参考：http://kaelzhang81.github.io/2017/06/18/%E8%AF%A6%E8%A7%A3%E5%9C%88%E5%A4%8D%E6%9D%82%E5%BA%A6/

VS Code插件：CodeMetrics

使用圈复杂度插件检查代码的负责度，可做参考，因为React本身负责度过高，心智负担重，圈复杂度插件检查主要还是在独立函数中可以作为参考

### 单词拼写检查

VS Code插件：Code Spell Checker

使用Code Spell Checker检查单词拼写，错误单词会以绿色波浪线提示，遇到特殊名词可以右键点击添加到文件夹目录，到将其添加cspell.json中。

### Import Sorter

VS Code插件：Typescript Imports Sort
设置 typescript.extension.sortImports.sortOnSave 为 true，在保存时自动sort import内容

# HOW TO START
require NodeJS > 10.0

INSTALL DEPENDENCE

```npm install```

START SERVER

```npm run start```


# HOW TO BUILD

```npm run build```