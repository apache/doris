---
{
    "title": "调试工具",
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

# 调试工具

在Doris的使用、开发过程中，经常会遇到需要对Doris进行调试的场景，这里介绍一些常用的调试工具。

**文中的出现的BE二进制文件名称 `doris_be`，在之前的版本中为 `palo_be`。**

## FE 调试

FE 是 Java 进程。这里只列举一下简单常用的 java 调试命令。

1. 统计当前内存使用明细

    ```
    jmap -histo:live pid > 1.jmp
    ```

    该命令可以列举存活的对象的内存占用并排序。（pid 换成 FE 进程 id）

    ```
     num     #instances         #bytes  class name
    ----------------------------------------------
       1:         33528       10822024  [B
       2:         80106        8662200  [C
       3:           143        4688112  [Ljava.util.concurrent.ForkJoinTask;
       4:         80563        1933512  java.lang.String
       5:         15295        1714968  java.lang.Class
       6:         45546        1457472  java.util.concurrent.ConcurrentHashMap$Node
       7:         15483        1057416  [Ljava.lang.Object;
    ```

    可以通过这个方法查看目前存活对象占用的总内存（在文件最后），以及分析哪些对象占用了更多的内存。

    注意，这个方法因指定了 `:live`，因此会触发 FullGC。

2. 查看 JVM 内存使用

    ```
    jstat -gcutil pid 1000 1000
    ```

    该命令可以滚动查看当前 JVM 各区域的内存使用情况。（pid 换成 FE 进程 id）

    ```
      S0     S1     E      O      M     CCS    YGC     YGCT    FGC    FGCT     GCT
      0.00   0.00  22.61   3.03  95.74  92.77     68    1.249     5    0.794    2.043
      0.00   0.00  22.61   3.03  95.74  92.77     68    1.249     5    0.794    2.043
      0.00   0.00  22.61   3.03  95.74  92.77     68    1.249     5    0.794    2.043
      0.00   0.00  22.92   3.03  95.74  92.77     68    1.249     5    0.794    2.043
      0.00   0.00  22.92   3.03  95.74  92.77     68    1.249     5    0.794    2.043
    ```

    其中主要关注 Old区（O）的占用百分比（如示例中为 3%）。如果占用过高，则可能出现 OOM 或 FullGC。

3. 打印 FE 线程堆栈

    ```
    jstack -l pid > 1.js
    ```

    该命令可以打印当前 FE 的线程堆栈。（pid 换成 FE 进程 id）。

    `-l` 参数会同时检测是否有死锁。该方法可以查看 FE 线程运行情况，是否有死锁，哪里卡住了等问题。

## BE 调试

### 环境准备

[pprof](https://github.com/google/pprof): 来自gperftools，用于将gperftools所产生的内容转化成便于人可以阅读的格式，比如pdf, svg, text等.

[graphviz](http://www.graphviz.org/): 在没有这个库的时候pprof只可以转化为text格式，但这种方式不易查看。那么安装这个库后，pprof可以转化为svg、pdf等格式，对于调用关系则更加清晰明了。

[perf](https://perf.wiki.kernel.org/index.php/Main_Page): linux内核自带性能分析工具。[这里](http://www.brendangregg.com/perf.html)有一些perf的使用例子。

[FlameGraph](https://github.com/brendangregg/FlameGraph): 可视化工具，用于将perf的输出以火焰图的形式展示出来。

### 内存

对于内存的调试一般分为两个方面。一个是内存使用的总量是否合理，内存使用量过大一方面可能是由于系统存在内存泄露，另一方面可能是因为程序内存使用不当。其次就是是否存在内存越界、非法访问的问题，比如程序访问一个非法地址的内存，使用了未初始化内存等。对于内存方面的调试我们一般使用如下几种方式来进行问题追踪。

Doris 1.2.1 及之前版本使用 TCMalloc，Doris 1.2.2 版本开始默认使用 Jemalloc，根据使用的 Doris 版本选择内存调试方法，如需切换 TCMalloc 可以这样编译 `USE_JEMALLOC=OFF sh build.sh --be`。

#### 查看日志

当发现内存使用量过大的时候，我们可以先查看 BE 日志，看看是否有大内存申请。

###### TCMalloc

当使用 TCMalloc 时，遇到大内存申请会将申请的堆栈打印到be.out文件中，一般的表现形式如下：

```
tcmalloc: large alloc 1396277248 bytes == 0x3f3488000 @  0x2af6f63 0x2c4095b 0x134d278 0x134bdcb 0x133d105 0x133d1d0 0x19930ed
```

这个表示在Doris BE在这个堆栈上尝试申请`1396277248 bytes`的内存。我们可以通过`addr2line`命令去把堆栈还原成我们能够看懂的信，具体的例子如下所示。

```
$ addr2line -e lib/doris_be  0x2af6f63 0x2c4095b 0x134d278 0x134bdcb 0x133d105 0x133d1d0 0x19930ed

/home/ssd0/zc/palo/doris/core/thirdparty/src/gperftools-gperftools-2.7/src/tcmalloc.cc:1335
/home/ssd0/zc/palo/doris/core/thirdparty/src/gperftools-gperftools-2.7/src/tcmalloc.cc:1357
/home/disk0/baidu-doris/baidu/bdg/doris-baidu/core/be/src/exec/hash_table.cpp:267
/home/disk0/baidu-doris/baidu/bdg/doris-baidu/core/be/src/exec/hash_table.hpp:86
/home/disk0/baidu-doris/baidu/bdg/doris-baidu/core/be/src/exec/hash_join_node.cpp:239
/home/disk0/baidu-doris/baidu/bdg/doris-baidu/core/be/src/exec/hash_join_node.cpp:213
thread.cpp:?
```

##### JEMALLOC

Doris绝大多数的大内存申请都使用 Allocator，比如 HashTable、数据序列化，这部分内存申请是预期中的，会被有效管理起来，除此之外的大内存申请不被预期，会将申请的堆栈打印到 be.INFO 文件中，这通常用于调试，一般的表现形式如下：
```
MemHook alloc large memory: 8.2GB, stacktrace:
Alloc Stacktrace:
    @     0x55a6a5cf6b4d  doris::ThreadMemTrackerMgr::consume()
    @     0x55a6a5cf99bf  malloc
    @     0x55a6ae0caf98  operator new()
    @     0x55a6a57cb013  doris::segment_v2::PageIO::read_and_decompress_page()
    @     0x55a6a57719c0  doris::segment_v2::ColumnReader::read_page()
    ……
```

#### HEAP PROFILE

##### TCMalloc

有时内存的申请并不是大内存的申请导致，而是通过小内存不断的堆积导致的。那么就没有办法通过查看日志定位到具体的申请信息，那么就需要通过其他方式来获得信息。

这个时候我们可以利用TCMalloc的[HEAP PROFILE](https://gperftools.github.io/gperftools/heapprofile.html)的功能。如果设置了HEAPPROFILE功能，那么我们可以获得进程整体的内存申请使用情况。使用方式是在启动Doris BE前设置`HEAPPROFILE`环境变量。比如：

```
export HEAPPROFILE=/tmp/doris_be.hprof
./bin/start_be.sh --daemon
```

这样，当满足HEAPPROFILE的dump条件时，就会将内存的整体使用情况写到指定路径的文件中。后续我们就可以通过使用`pprof`工具来对输出的内容进行分析。

```
$ pprof --text lib/doris_be /tmp/doris_be.hprof.0012.heap | head -30

Using local file lib/doris_be.
Using local file /tmp/doris_be.hprof.0012.heap.
Total: 668.6 MB
   610.6  91.3%  91.3%    610.6  91.3% doris::SystemAllocator::allocate_via_malloc (inline)
    18.1   2.7%  94.0%     18.1   2.7% _objalloc_alloc
     5.6   0.8%  94.9%     63.4   9.5% doris::RowBatch::RowBatch
     5.1   0.8%  95.6%      7.1   1.1% butil::ResourcePool::add_block (inline)
     3.7   0.5%  96.2%      3.7   0.5% butil::iobuf::create_block (inline)
     3.4   0.5%  96.7%      3.4   0.5% butil::FlatMap::init
     3.2   0.5%  97.2%      5.2   0.8% butil::ObjectPool::add_block (inline)
     2.6   0.4%  97.6%      2.6   0.4% __gnu_cxx::new_allocator::allocate (inline)
     2.0   0.3%  97.9%      2.0   0.3% butil::ObjectPool::add_block_group (inline)
     2.0   0.3%  98.2%      2.0   0.3% butil::ResourcePool::add_block_group (inline)
     1.7   0.3%  98.4%      1.7   0.3% doris::SegmentReader::_load_index
```

上述文件各个列的内容：

* 第一列：函数直接申请的内存大小，单位MB
* 第四列：函数以及函数所有调用的函数总共内存大小。
* 第二列、第五列分别是第一列与第四列的比例值。
* 第三列是个第二列的累积值。

当然也可以生成调用关系图片，更加方便分析。比如下面的命令就能够生成SVG格式的调用关系图。

```
pprof --svg lib/doris_be /tmp/doris_be.hprof.0012.heap > heap.svg 
```

**注意：开启这个选项是要影响程序的执行性能的，请慎重对线上的实例开启**

###### pprof remote server

HEAP PROFILE虽然能够获得全部的内存使用信息，但是也有比较受限的地方。1. 需要重启BE进行。2. 需要一直开启这个命令，导致对整个进程的性能造成影响。

对Doris BE也可以使用动态开启、关闭heap profile的方式来对进程进行内存申请分析。Doris内部支持了GPerftools的[远程server调试](https://gperftools.github.io/gperftools/pprof_remote_servers.html)。那么可以通过`pprof`直接对远程运行的Doris BE进行动态的HEAP PROFILE。比如我们可以通过以下命令来查看Doris的内存的使用增量

```
$ pprof --text --seconds=60 http://be_host:be_webport/pprof/heap 

Total: 1296.4 MB
   484.9  37.4%  37.4%    484.9  37.4% doris::StorageByteBuffer::create
   272.2  21.0%  58.4%    273.3  21.1% doris::RowBlock::init
   157.5  12.1%  70.5%    157.5  12.1% doris::RowBatch::RowBatch
    90.7   7.0%  77.5%     90.7   7.0% doris::SystemAllocator::allocate_via_malloc
    66.6   5.1%  82.7%     66.6   5.1% doris::IntegerColumnReader::init
    47.9   3.7%  86.4%     47.9   3.7% __gnu_cxx::new_allocator::allocate
    20.8   1.6%  88.0%     35.4   2.7% doris::SegmentReader::_load_index
    12.7   1.0%  89.0%     12.7   1.0% doris::DecimalColumnReader::init
    12.7   1.0%  89.9%     12.7   1.0% doris::LargeIntColumnReader::init
    12.7   1.0%  90.9%     12.7   1.0% doris::StringColumnDirectReader::init
    12.3   0.9%  91.9%     12.3   0.9% std::__cxx11::basic_string::_M_mutate
    10.4   0.8%  92.7%     10.4   0.8% doris::VectorizedRowBatch::VectorizedRowBatch
    10.0   0.8%  93.4%     10.0   0.8% doris::PlainTextLineReader::PlainTextLineReader
```

这个命令的输出与HEAP PROFILE的输出及查看方式一样，这里就不再详细说明。这个命令只有在执行的过程中才会开启统计，相比HEAP PROFILE对于进程性能的影响有限。

##### JEMALLOC

###### 1. realtime heap dump
将 `be.conf` 中 `JEMALLOC_CONF` 的 `prof:false` 修改为 `prof:true` 并重启BE，然后使用jemalloc heap dump http接口，在对应的BE机器上生成heap dump文件。

```shell
curl http://be_host:be_webport/jeheap/dump
```

heap dump文件所在目录可以在 ``be.conf`` 中通过``jeprofile_dir``变量进行配置，默认为``${DORIS_HOME}/log``

默认采样间隔为 512K，这通常只会有 10% 的内存被heap dump记录，对性能的影响通常小于 10%，可以修改 `be.conf` 中 `JEMALLOC_CONF` 的 `lg_prof_sample`，默认为 `19` (2^19 B = 512K)，减小 `lg_prof_sample` 可以更频繁的采样使 heap profile 接近真实内存，但这会带来更大的性能损耗。

如果你在做性能测试，保持 `prof:false` 来避免 heap dump 的性能损耗。

###### 2. regular heap dump
同样要将 `be.conf` 中 `JEMALLOC_CONF` 的 `prof:false` 修改为 `prof:true`，同时将 `be.conf` 中 `JEMALLOC_PROF_PRFIX` 修改为任意值并重启BE。

heap dump文件所在目录默认为 `${DORIS_HOME}/log`, 文件名前缀是 `JEMALLOC_PROF_PRFIX`。

1. 内存累计申请一定值时dump:

   默认内存累计申请 4GB 生成一次dump，可以修改 `be.conf` 中 `JEMALLOC_CONF` 的 `lg_prof_interval` 调整dump间隔，默认值 `32` (2^32 B = 4GB)。
2. 内存每次达到新高时dump:

   将 `be.conf` 中 `JEMALLOC_CONF` 的 `prof_gdump` 修改为 `true` 并重启BE。
3. 程序退出时dump, 并检测内存泄漏:

   将 `be.conf` 中 `JEMALLOC_CONF` 的 `prof_leak` 和 `prof_final` 修改为 `true` 并重启BE。
4. dump内存累计值(growth)，而不是实时值:

   将 `be.conf` 中 `JEMALLOC_CONF` 的 `prof_accum` 修改为 `true` 并重启BE。
   使用 `jeprof --alloc_space` 展示 heap dump 累计值。

##### 3. heap dump profiling

```
需要 addr2line 版本为 2.35.2, 见下面的 QA 1.
```

1.  单个heap dump文件生成纯文本分析结果
```shell
   jeprof lib/doris_be heap_dump_file_1
   ```

2.  分析两个heap dump的diff
   ```shell
   jeprof lib/doris_be --base=heap_dump_file_1 heap_dump_file_2
   ```
   
3. 生成调用关系图片

   安装绘图所需的依赖项
   ```shell
   yum install ghostscript graphviz
   ```
   通过在一短时间内多次运行上述命令可以生成多份dump 文件，可以选取第一份dump 文件作为baseline 进行diff对比分析
   
   ```shell
   jeprof --dot lib/doris_be --base=heap_dump_file_1 heap_dump_file_2
   ```
   执行完上述命令，终端中会输出dot语法的图，将其贴到[在线dot绘图网站](http://www.webgraphviz.com/)，生成内存分配图，然后进行分析，此种方式能够直接通过终端输出结果进行绘图，比较适用于传输文件不是很方便的服务器。
   
   也可以通过如下命令直接生成调用关系result.pdf文件传输到本地后进行查看
   ```shell
   jeprof --pdf lib/doris_be --base=heap_dump_file_1 heap_dump_file_2 > result.pdf
   ```

##### 4. QA

1. 运行 jeprof 后出现很多错误: `addr2line: Dwarf Error: found dwarf version xxx, this reader only handles version xxx`.

GCC 11 之后默认使用 DWARF-v5 ，这要求Binutils 2.35.2 及以上，Doris Ldb_toolchain 用了 GCC 11。see: https://gcc.gnu.org/gcc-11/changes.html。

替换 addr2line 到 2.35.2，参考：
```
// 下载 addr2line 源码
wget https://ftp.gnu.org/gnu/binutils/binutils-2.35.tar.bz2

// 安装依赖项，如果需要
yum install make gcc gcc-c++ binutils

// 编译&安装 addr2line
tar -xvf binutils-2.35.tar.bz2
cd binutils-2.35
./configure --prefix=/usr/local
make
make install

// 验证
addr2line -h

// 替换 addr2line
chmod +x addr2line
mv /usr/bin/addr2line /usr/bin/addr2line.bak
mv /bin/addr2line /bin/addr2line.bak
cp addr2line /bin/addr2line
cp addr2line /usr/bin/addr2line
hash -r
```
注意，不能使用 addr2line 2.3.9, 这可能不兼容，导致内存一直增长。

#### LSAN

[LSAN](https://github.com/google/sanitizers/wiki/AddressSanitizerLeakSanitizer)是一个地址检查工具，GCC已经集成。在我们编译代码的时候开启相应的编译选项，就能够开启这个功能。当程序发生可以确定的内存泄露时，会将泄露堆栈打印。Doris BE已经集成了这个工具，只需要在编译的时候使用如下的命令进行编译就能够生成带有内存泄露检测版本的BE二进制

```
BUILD_TYPE=LSAN ./build.sh
```

当系统检测到内存泄露的时候，就会在be.out里面输出对应的信息。为了下面的演示，我们故意在代码中插入一段内存泄露代码。我们在`StorageEngine`的`open`函数中插入如下代码

```
    char* leak_buf = new char[1024];
    strcpy(leak_buf, "hello world");
    LOG(INFO) << leak_buf;
```

我们就在be.out中获得了如下的输出

```
=================================================================
==24732==ERROR: LeakSanitizer: detected memory leaks

Direct leak of 1024 byte(s) in 1 object(s) allocated from:
    #0 0xd10586 in operator new[](unsigned long) ../../../../gcc-7.3.0/libsanitizer/lsan/lsan_interceptors.cc:164
    #1 0xe333a2 in doris::StorageEngine::open(doris::EngineOptions const&, doris::StorageEngine**) /home/ssd0/zc/palo/doris/core/be/src/olap/storage_engine.cpp:104
    #2 0xd3cc96 in main /home/ssd0/zc/palo/doris/core/be/src/service/doris_main.cpp:159
    #3 0x7f573b5eebd4 in __libc_start_main (/opt/compiler/gcc-4.8.2/lib64/libc.so.6+0x21bd4)

SUMMARY: LeakSanitizer: 1024 byte(s) leaked in 1 allocation(s).
```

从上述的输出中，我们能看到有1024个字节被泄露了，并且打印出来了内存申请时的堆栈信息。

**注意：开启这个选项是要影响程序的执行性能的，请慎重对线上的实例开启**

**注意：如果开启了LSAN开关的话，tcmalloc就会被自动关闭**

#### ASAN

除了内存使用不合理、泄露以外。有的时候也会发生内存访问非法地址等错误。这个时候我们可以借助[ASAN](https://github.com/google/sanitizers/wiki/AddressSanitizer)来辅助我们找到问题的原因。与LSAN一样，ASAN也集成在了GCC中。Doris通过如下的方式进行编译就能够开启这个功能

```
BUILD_TYPE=ASAN ./build.sh
```

执行编译生成的二进制文件，当检测工具发现有异常访问时，就会立即退出，并将非法访问的堆栈输出在be.out中。对于ASAN的输出与LSAN是一样的分析方法。这里我们也主动注入一个地址访问错误，来展示下具体的内容输出。我们仍然在`StorageEngine`的`open`函数中注入一段非法内存访问，具体的错误代码如下

```
    char* invalid_buf = new char[1024];
    for (int i = 0; i < 1025; ++i) {
        invalid_buf[i] = i;
    }
    LOG(INFO) << invalid_buf;
```

然后我们就会在be.out中获得如下的输出

```
=================================================================
==23284==ERROR: AddressSanitizer: heap-buffer-overflow on address 0x61900008bf80 at pc 0x00000129f56a bp 0x7fff546eed90 sp 0x7fff546eed88
WRITE of size 1 at 0x61900008bf80 thread T0
    #0 0x129f569 in doris::StorageEngine::open(doris::EngineOptions const&, doris::StorageEngine**) /home/ssd0/zc/palo/doris/core/be/src/olap/storage_engine.cpp:106
    #1 0xe2c1e3 in main /home/ssd0/zc/palo/doris/core/be/src/service/doris_main.cpp:159
    #2 0x7fa5580fbbd4 in __libc_start_main (/opt/compiler/gcc-4.8.2/lib64/libc.so.6+0x21bd4)
    #3 0xd30794  (/home/ssd0/zc/palo/doris/core/output3/be/lib/doris_be+0xd30794)

0x61900008bf80 is located 0 bytes to the right of 1024-byte region [0x61900008bb80,0x61900008bf80)
allocated by thread T0 here:
    #0 0xdeb040 in operator new[](unsigned long) ../../../../gcc-7.3.0/libsanitizer/asan/asan_new_delete.cc:82
    #1 0x129f50d in doris::StorageEngine::open(doris::EngineOptions const&, doris::StorageEngine**) /home/ssd0/zc/palo/doris/core/be/src/olap/storage_engine.cpp:104
    #2 0xe2c1e3 in main /home/ssd0/zc/palo/doris/core/be/src/service/doris_main.cpp:159
    #3 0x7fa5580fbbd4 in __libc_start_main (/opt/compiler/gcc-4.8.2/lib64/libc.so.6+0x21bd4)

SUMMARY: AddressSanitizer: heap-buffer-overflow /home/ssd0/zc/palo/doris/core/be/src/olap/storage_engine.cpp:106 in doris::StorageEngine::open(doris::EngineOptions const&, doris::StorageEngine**)
```

从这段信息中该可以看到在`0x61900008bf80`这个地址我们尝试去写一个字节，但是这个地址是非法的。我们也可以看到 `[0x61900008bb80,0x61900008bf80)`这个地址的申请堆栈。

**注意：开启这个选项是要影响程序的执行性能的，请慎重对线上的实例开启**

**注意：如果开启了ASAN开关的话，tcmalloc就会被自动关闭**

另外，如果be.out中输出了堆栈信息，但是并没有函数符号，那么这个时候需要我们手动的处理下才能获得可读的堆栈信息。具体的处理方法需要借助一个脚本来解析ASAN的输出。这个时候我们需要使用[asan_symbolize](https://llvm.org/svn/llvm-project/compiler-rt/trunk/lib/asan/scripts/asan_symbolize.py)来帮忙解析下。具体的使用方式如下：

```
cat be.out | python asan_symbolize.py | c++filt
```

通过上述的命令，我们就能够获得可读的堆栈信息了。

### CPU

当系统的CPU Idle很低的时候，说明系统的CPU已经成为了主要瓶颈，这个时候就需要分析一下当前的CPU使用情况。对于Doris的BE可以有如下两种方式来分析Doris的CPU瓶颈。

#### pprof

由于Doris内部已经集成了并兼容了GPerf的REST接口，那么用户可以通过`pprof`工具来分析远程的Doris BE。具体的使用方式如下：

```
pprof --svg --seconds=60 http://be_host:be_webport/pprof/profile > be.svg 
```

这样就能够生成一张BE执行的CPU消耗图。

![CPU Pprof](/images/cpu-pprof-demo.png)

#### perf + flamegragh

这个是相当通用的一种CPU分析方式，相比于`pprof`，这种方式必须要求能够登陆到分析对象的物理机上。但是相比于pprof只能定时采点，perf是能够通过不同的事件来完成堆栈信息采集的。具体的使用方式如下：

```
perf record -g -p be_pid -- sleep 60
```

这条命令会统计60秒钟BE的CPU运行情况，并且生成perf.data。对于perf.data的分析，可以通过perf的命令来进行分析

```
perf report
```

分析得到如下的图片

![Perf Report](/images/perf-report-demo.png)

来对生成的内容进行分析。当然也可以使用flamegragh完成可视化展示。

```
perf script | ./FlameGraph/stackcollapse-perf.pl | ./FlameGraph/flamegraph.pl > be.svg
```

这样也会生成一张当时运行的CPU消耗图。

![CPU Flame](/images/cpu-flame-demo.svg)
