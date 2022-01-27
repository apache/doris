---
{
    "title": "Java 代码格式化",
    "language": "zh-CN"
}
---

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

# Java 代码格式化

Doris 中 Java 部分代码的格式化工作通常有 IDE 来自动完成。这里仅列举通用格式规则，开发这需要根据格式规则，在不同 IDE 中设置对应的代码风格。

## Import Order

```
org.apache.doris
<blank line>
com
<blank line>
org
<blank line>
java
<blank line>
javax
<blank line>
```

* 禁止使用 `import *`
* 禁止使用 `import static`

## Checkstyle

现在的 `CI` 之中会有 `formatter-check` 进行代码格式化检测。

如果使用 `IDEA` 进行 Java 开发，请在设置中安装 `Checkstyle-IDEA` 插件。

在 `Tools->Checkstyle` 的 `Configuration File` 里点击 `Use a local Checkstyle file`，选择项目的 `fe/checkstyle.xml` 文件。

如果使用 VS Code 进行 Java 开发，请安装 `Checkstyle for Java` 插件，按照[文档](https://code.visualstudio.com/docs/java/java-linting)里的说明和动图进行配置。

