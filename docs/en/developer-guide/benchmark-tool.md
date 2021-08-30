---
{
    "title": "Doris BE Storage Layer Benchmark Tool",
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

# Doris BE Storage Layer Benchmark Tool

## usage

It can be used to test the performance of some parts of the BE storage layer (for example, segment, page). According to the input data, the designated object is constructed, and the google benchmark is used for performance testing. 

## Compilation

1. To ensure that the environment has been able to successfully compile the Doris ontology, you can refer to [Installation and deployment] (https://doris.apache.org/master/en/installing/compilation.html)。

2. Execute`run-be-ut.sh`

3. The compiled executable file is located in `./be/ut_build_ASAN/test/tools/benchmark_tool`

## operator

#### Use randomly generated data set for segment read test 

The data set will be used to write a `segment` file first, and then the time-consuming scan of the entire `segment` will be counted. 

> ./benchmark_tool --operation=SegmentScan --column_type=int,varchar --rows_number=10000 --iterations=0

The `column_type` here can set the schema, the column type of the `segment` layer currently supports `int, char, varchar, string`, the length of the `char` type is `8`, and both `varchar` and `string` types have length restrictions Is the maximum value. The default value is `int,varchar`. 

The data set is generated according to the following rules. 
>int: Random in [1,1000000]. 

The data character set of string type is uppercase and lowercase English letters, and the length varies according to the type. 
> char: Length random in [1,8]。
> varchar: Length random in [1,128]。 
> string: Length random in [1,100000]。

`rows_number` indicates the number of rows of data, the default value is `10000`. 

`iterations` indicates the number of iterations, the benchmark will repeat the test, and then calculate the average time. If `iterations` is `0`, it means that the number of iterations is automatically selected by the benchmark. The default value is `10`. 

#### Use randomly generated data set for segment write test 

Perform time-consuming statistics on the process of adding data sets to segments and writing them to disk. 

> ./benchmark_tool --operation=SegmentWrite

#### Use the data set imported from the file for segment read test 

> ./benchmark_tool --operation=SegmentScanByFile --input_file=./sample.dat

The `input_file` here is the imported data set file. 
The first row of the data set file defines the schema, and each row corresponds to a row of data, and each data is separated by `,`. 

Example: 
```
int,char,varchar
123,hello,world
321,good,bye
```

The type support is also `int`, `char`, `varchar`, `string`. Note that the data length of the `char` type cannot exceed 8. 

#### Use the data set imported from the file for segment write test 

> ./benchmark_tool --operation=SegmentWriteByFile --input_file=./sample.dat

#### Use randomly generated data set for page dictionary encoding test 

> ./benchmark_tool --operation=BinaryDictPageEncode --rows_number=10000 --iterations=0

Randomly generate varchar with a length between [1,8], and perform time-consuming statistics on encoding. 

#### Use randomly generated data set for page dictionary decoding test 

> ./benchmark_tool --operation=BinaryDictPageDecode

Randomly generate varchar with a length between [1,8] and encode, and perform time-consuming statistics on decoding. 

## Custom test

Here, users are supported to use their own functions for performance testing, which can be implemented in `/be/test/tools/benchmark_tool.cpp`. 

For example: 
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
You can join the test by registering `CustomBenchmark`. 
```cpp
benchmarks.emplace_back(
                    new doris::CustomBenchmark("custom_run_plus", 0,
                    	custom_init, custom_run_plus));
benchmarks.emplace_back(
                    new doris::CustomBenchmark("custom_run_mod", 0,
                    	custom_init, custom_run_mod));
```
The `custom_init` here is the initialization step of each round of testing (not counted as time-consuming). If the user has an object that needs to be initialized, it can be implemented by a derived class of `CustomBenchmark`. 
After running, the results are as follows: 
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
