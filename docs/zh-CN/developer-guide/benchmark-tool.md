---
{
    "title": "Doris BE存储层Benchmark工具",
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

# Doris BE存储层Benchmark工具

## 用途

    可以用来测试BE存储层的一些部分(例如segment、page)的性能。根据输入数据构造出指定对象,利用google benchmark进行性能测试。

## 编译

1. 确保环境已经能顺利编译Doris本体,可以参考[编译与部署](../install/source-install/compilation.html)。

2. 运行目录下的`run-be-ut.sh`

3. 编译出的可执行文件位于`./be/ut_build_ASAN/test/tools/benchmark_tool`

## 使用

#### 使用随机生成的数据集进行Segment读取测试

会先利用数据集写入一个`segment`文件,然后对scan整个`segment`的耗时进行统计。

> ./benchmark_tool --operation=SegmentScan --column_type=int,varchar --rows_number=10000 --iterations=0

这里的`column_type`可以设置表结构,`segment`层的表结构类型目前支持`int、char、varchar、string`,`char`类型的长度为`8`,`varchar`和`string`类型长度限制都为最大值。默认值为`int,varchar`。

数据集按以下规则生成。
>int: 在[1,1000000]内随机。

字符串类型的数据字符集为大小写英文字母,长度根据类型不同。
> char: 长度在[1,8]内随机。
> varchar: 长度在[1,128]内随机。 
> string: 长度在[1,100000]内随机。

`rows_number`表示数据的行数,默认值为`10000`。

`iterations`表示迭代次数,benchmark会重复进行测试,然后计算平均耗时。如果`iterations`为`0`则表示由benchmark自动选择迭代次数。默认值为`10`。

#### 使用随机生成的数据集进行Segment写入测试

对将数据集添加进segment并写入磁盘的流程进行耗时统计。

> ./benchmark_tool --operation=SegmentWrite

#### 使用从文件导入的数据集进行Segment读取测试

> ./benchmark_tool --operation=SegmentScanByFile --input_file=./sample.dat

这里的`input_file`为导入的数据集文件。
数据集文件第一行为表结构定义,之后每行分别对应一行数据,每个数据用`,`隔开。

举例: 
```
int,char,varchar
123,hello,world
321,good,bye
```

类型支持同样为`int`、`char`、`varchar`、`string`,注意`char`类型数据长度不能超过8。

#### 使用从文件导入的数据集进行Segment写入测试

> ./benchmark_tool --operation=SegmentWriteByFile --input_file=./sample.dat

#### 使用随机生成的数据集进行page字典编码测试

> ./benchmark_tool --operation=BinaryDictPageEncode --rows_number=10000 --iterations=0

会随机生成长度在[1,8]之间的varchar,并对编码进行耗时统计。

#### 使用随机生成的数据集进行page字典解码测试

> ./benchmark_tool --operation=BinaryDictPageDecode

会随机生成长度在[1,8]之间的varchar并编码,并对解码进行耗时统计。

## Custom测试

这里支持用户使用自己编写的函数进行性能测试,具体可以实现在`/be/test/tools/benchmark_tool.cpp`。
例如实现有：
```cpp
void custom_run_plus() {
    int p = 100000;
    int q = 0;
    while (p--) {
        q++;
        if (UNLIKELY(q == 1024)) q = 0;
    }
}
void custom_run_mod() {
    int p = 100000;
    int q = 0;
    while (p--) {
        q++;
        if (q %= 1024) q = 0;
    }
}
```
则可以通过注册`CustomBenchmark`来加入测试。
```cpp
benchmarks.emplace_back(
                    new doris::CustomBenchmark("custom_run_plus", 0,
                        custom_init, custom_run_plus));
benchmarks.emplace_back(
                    new doris::CustomBenchmark("custom_run_mod", 0,
                        custom_init, custom_run_mod));
```
这里的`init`为每轮测试的初始化步骤(不会计入耗时),如果用户有需要初始化的对象则可以通过`CustomBenchmark`的派生类来实现。
运行后有如下结果:
```
2021-08-30T10:29:35+08:00
Running ./benchmark_tool
Run on (96 X 3100.75 MHz CPU s)
CPU Caches:
  L1 Data 32 KiB (x48)
  L1 Instruction 32 KiB (x48)
  L2 Unified 1024 KiB (x48)
  L3 Unified 33792 KiB (x2)
Load Average: 0.55, 0.53, 0.39
----------------------------------------------------------
Benchmark                Time             CPU   Iterations
----------------------------------------------------------
custom_run_plus      0.812 ms        0.812 ms          861
custom_run_mod        1.30 ms         1.30 ms          539
```
