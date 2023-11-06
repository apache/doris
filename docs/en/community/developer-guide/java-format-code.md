---
{
  "title": "Java Format Code",
  "language": "en"
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

# Java Format Code

The formatting of the Java part of the code in Doris is usually done automatically by the IDE. Only the general format rules are listed here. For developer, you need to set the corresponding code styles in different IDEs according to the format rules.

## Import Order

```
org.apache.doris
<blank line>
third party package
<blank line>
standard java package
<blank line>
```

* Do not use `import *`
* Do not use `import static`

## Check when compile

Now, when compiling with `caven`, `CheckStyle` checks are done by default. This will slightly slow down compilation. If you want to skip checkstyle, please use the following command to compile
```
mvn clean install -DskipTests -Dcheckstyle.skip
```

## Checkstyle Plugin

Now we have `formatter-check` in `CI` to check the code format.

### IDEA

If you use `IDEA` to develop Java code, please install `Checkstyle-IDEA` plugin.

Setting the `checkstyle.xml` file in `Tools->Checkstyle`.

Click the plus sign under Configuration File, select `Use a local Checkstyle file`, and select the `fe/check/checkstyle/checkstyle.xml` file.

**NOTE:** Make sure that the version of `Checkstyle` is 9.3 or newer (the latest version is recommended).

![](/images/idea-checkstyle-version.png)

**You can use `Checkstyle-IDEA` plugin to check `Checkstyle` of your code real-time.**

![](/images/idea-checkstyle-plugin-en.png)

### VS Code

If you use VS Code to develop Java code, please install `Checkstyle for Java` plugin, and config according to the [document](https://code.visualstudio.com/docs/java/java-linting) and the picture

## IDEA

### Auto format code

The automatic formatting function of `IDEA` is also recommended.

Go to `Preferences->Editor->Code Style->Java` click the config sign and select `Import Scheme`，select `IntelliJ IDEA code style XML`，and select the `build-support/IntelliJ-code-format.xml` file.

### Auto rearrange code

Checkstyle will check declarations order according to [Class and Interface Declarations](https://www.oracle.com/java/technologies/javase/codeconventions-fileorganization.html#1852) .

After add the `build-support/IntelliJ-code-format.xml` file. Click `Code/Rearrange Code` to auto rearrange code.

![](/images/idea-rearrange-code.png)

## Spotless Plugin

An error was found when checking the project code through `mvn spotless:check`, and then used `mvn spotless:apply` to format the code; when checking again, the formatting error disappeared.

Tip: We use incremental code formatting, spotless will apply only to files which have changed since `origin/master`. If a `No such reference` error is prompted, please calling `git fetch origin master` before you call Spotless.
Please refer to [how-can-i-enforce-formatting-gradually-aka-ratchet](https://github.com/diffplug/spotless/tree/main/plugin-maven#how-can-i-enforce-formatting-gradually-aka-ratchet) for details.

## Remove unused header

**CTRL + ALT + O --->** to remove the unused imports in windows.

Auto remove unused header and reorder according to configure xml:

Click `Preferences->Editor->Auto Import->Optimize Imports on the Fly`
