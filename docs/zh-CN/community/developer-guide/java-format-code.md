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
third party package
<blank line>
standard java package
<blank line>
```

* 禁止使用 `import *`
* 禁止使用 `import static`

## 编译时检查

现在，在使用`maven`进行编译时，会默认进行`CheckStyle`检查。此检查会略微降低编译速度。如果想跳过此检查，请使用如下命令进行编译
```
mvn clean install -DskipTests -Dcheckstyle.skip
```

## Checkstyle 插件

现在的 `CI` 之中会有 `formatter-check` 进行代码格式化检测。

### IDEA

如果使用 `IDEA` 进行 Java 开发，请在设置中安装 `Checkstyle-IDEA` 插件。

在 `Tools->Checkstyle` 的 `Configuration File` 里点击 `Use a local Checkstyle file`，选择项目的 `fe/check/checkstyle/checkstyle.xml` 文件。

**注意：** 保证`Checkstyle`的版本在9.3及以上（推荐使用最新版本）。

![](/images/idea-checkstyle-version.png)

**可以使用 `Checkstyle-IDEA` 插件来对代码进行 `Checkstyle` 检测**。

![](/images/idea-checkstyle-plugin-cn.png)

### VS Code

如果使用 VS Code 进行 Java 开发，请安装 `Checkstyle for Java` 插件，按照[文档](https://code.visualstudio.com/docs/java/java-linting)里的说明和动图进行配置。

## IDEA

###  自动格式化

推荐使用 `IDEA` 的自动格式化功能。

在 `Preferences->Editor->Code Style->Java` 的配置标识点击 `Import Scheme`，点击 `IntelliJ IDEA code style XML`，选择项目的 `build-support/IntelliJ-code-format.xml` 文件。

### Rearrange Code

Checkstyle 会按照 [Class and Interface Declarations](https://www.oracle.com/java/technologies/javase/codeconventions-fileorganization.html#1852) 检测代码的 declarations 顺序。

在导入上面的 `build-support/IntelliJ-code-format.xml` 文件后，使用 `Code/Rearrange Code` 自动完成排序

![](/images/idea-rearrange-code.png)

## Remove unused header

默认快捷键 **CTRL + ALT + O --->** 仅仅删除未使用的导入。

自动移除并且 Reorder ：

点击 `Preferences->Editor->General->Auto Import->Optimize Imports on the Fly`