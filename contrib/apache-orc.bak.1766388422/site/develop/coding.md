---
layout: page
title: Coding Guidelines
---

## General rules

* All files must have an Apache copyright header at the top of the file.
* Code should be removed rather than commented out.
* All public functions should have javadoc comments.
* Always use braces to surround branches.
* try-finally should be avoided.

## Formatting

* All files must have an 100 character maximum line length.
* Indentation should be 2 spaces.
* Files should use spaces instead of tabs.
* Wrapping lines
  * Break after a comma.
  * Break before an operator.
  * Prefer higher-level breaks to lower-level breaks.
  * Align the new line with beginning of the expression at the same level
    on the previous line.
  * If the above rules lead to confusing code, just indent 8 spaces.
* One variable declaration per a line.

## Naming

* Packages should be all lowercase.
  * C++ code should be in `orc::`.
  * Java code should be in `org.apache.orc`, except for compatibility classes,
    which should be in `org.apache.hadoop.hive.ql.io.orc`.
* Classes should be in mixed case.
* Variables should be in camel case.
* Constants should be in upper case.

## Static code analysis tool

* [CheckStyle-IDEA](https://plugins.jetbrains.com/plugin/1065-checkstyle-idea)

You can download this plugin to load `java/checkstyle.xml` to check the java code style. This way you get checkstyle errors/warnings already when you are coding.