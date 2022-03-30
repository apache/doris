---
{
    "title": "MINI LOAD",
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

# MINI LOAD
## description

    MINI LOAD 和 STREAM LOAD 的导入实现方式完全一致。在导入功能支持上，MINI LOAD 的功能是 STREAM LOAD 的子集。
	后续的导入新功能只会在 STREAM LOAD 中支持，MINI LOAD 将不再新增功能。建议改用 STREAM LOAD，具体使用方式请 HELP STREAM LOAD。

	MINI LOAD 是 通过 http 协议完成的导入方式。用户可以不依赖 Hadoop，也无需通过 Mysql 客户端，即可完成导入。
	用户通过 http 协议描述导入，数据在接受 http 请求的过程中被流式的导入 Doris , **导入作业完成后** 返回给用户导入的结果。

    * 注：为兼容旧版本 mini load 使用习惯，用户依旧可以通过 'SHOW LOAD' 命令来查看导入结果。

    语法：
    导入：

        curl --location-trusted -u user:passwd -T data.file http://host:port/api/{db}/{table}/_load?label=xxx

    查看导入信息
    
        curl -u user:passwd http://host:port/api/{db}/_load_info?label=xxx

    HTTP协议相关说明

        权限认证            当前 Doris 使用 http 的 Basic 方式权限认证。所以在导入的时候需要指定用户名密码
                            这种方式是明文传递密码的，暂不支持加密传输。

        Expect              Doris 需要发送过来的 http 请求带有 'Expect' 头部信息，内容为 '100-continue'。
                            为什么呢？因为我们需要将请求进行 redirect，那么必须在传输数据内容之前，
                            这样可以避免造成数据的多次传输，从而提高效率。

        Content-Length      Doris 需要在发送请求时带有 'Content-Length' 这个头部信息。如果发送的内容比
                            'Content-Length' 要少，那么 Doris 认为传输出现问题，则提交此次任务失败。
                            NOTE: 如果，发送的数据比 'Content-Length' 要多，那么 Doris 只读取 'Content-Length'
                            长度的内容，并进行导入


    参数说明：

        user:               用户如果是在default_cluster中的，user即为user_name。否则为user_name@cluster_name。

        label:              用于指定这一批次导入的 label，用于后期进行作业查询等。
                            这个参数是必须传入的。

        columns:            用于描述导入文件中对应的列名字。
                            如果不传入，那么认为文件中的列顺序与建表的顺序一致，
                            指定的方式为逗号分隔，例如：columns=k1,k2,k3,k4

        column_separator:   用于指定列与列之间的分隔符，默认的为'\t'
                            NOTE: 需要进行url编码，譬如
                            需要指定'\t'为分隔符，那么应该传入'column_separator=%09'
                            需要指定'\x01'为分隔符，那么应该传入'column_separator=%01'
                            需要指定','为分隔符，那么应该传入'column_separator=%2c'


        max_filter_ratio:   用于指定允许过滤不规范数据的最大比例，默认是0，不允许过滤
                            自定义指定应该如下：'max_filter_ratio=0.2'，含义是允许20%的错误率

        timeout:            指定 load 作业的超时时间，单位是秒。当load执行时间超过该阈值时，会自动取消。默认超时时间是 600 秒。
                            建议指定 timeout 时间小于 86400 秒。
                            
        hll:                用于指定数据里面和表里面的HLL列的对应关系，表中的列和数据里面指定的列
                            （如果不指定columns，则数据列面的列也可以是表里面的其它非HLL列）通过","分割
                            指定多个hll列使用“:”分割，例如: 'hll1,cuid:hll2,device'

        strict_mode:        指定当前导入是否使用严格模式，默认为 false。严格模式下，非空原始数据在列类型转化后结果为 NULL 的会被过滤。
                            指定方式为 'strict_mode=true'
    
    NOTE: 
        1. 此种导入方式当前是在一台机器上完成导入工作，因而不宜进行数据量较大的导入工作。
        建议导入数据量不要超过 1 GB

        2. 当前无法使用 `curl -T "{file1, file2}"` 这样的方式提交多个文件，因为curl是将其拆成多个
        请求发送的，多个请求不能共用一个label号，所以无法使用

        3. mini load 的导入方式和 streaming 完全一致，都是在流式的完成导入后，同步的返回结果给用户。
		后续查询虽可以查到 mini load 的信息，但不能对其进行操作，查询只为兼容旧的使用方式。

        4. 当使用 curl 命令行导入时，需要在 & 前加入 \ 转义，否则参数信息会丢失。

## example

    1. 将本地文件'testData'中的数据导入到数据库'testDb'中'testTbl'的表（用户是defalut_cluster中的）
        curl --location-trusted -u root -T testData http://host:port/api/testDb/testTbl/_load?label=123

    2. 将本地文件'testData'中的数据导入到数据库'testDb'中'testTbl'的表（用户是test_cluster中的）。超时时间是 3600 秒
        curl --location-trusted -u root@test_cluster:root -T testData http://fe.host:port/api/testDb/testTbl/_load?label=123\&timeout=3600

    3. 将本地文件'testData'中的数据导入到数据库'testDb'中'testTbl'的表, 允许20%的错误率（用户是defalut_cluster中的）
        curl --location-trusted -u root -T testData http://host:port/api/testDb/testTbl/_load?label=123\&max_filter_ratio=0.2

    4. 将本地文件'testData'中的数据导入到数据库'testDb'中'testTbl'的表, 允许20%的错误率，并且指定文件的列名（用户是defalut_cluster中的）
        curl --location-trusted -u root -T testData http://host:port/api/testDb/testTbl/_load?label=123\&max_filter_ratio=0.2\&columns=k1,k2,k3

    5. 使用streaming方式导入（用户是defalut_cluster中的）
        seq 1 10 | awk '{OFS="\t"}{print $1, $1 * 10}' | curl --location-trusted -u root -T - http://host:port/api/testDb/testTbl/_load?label=123

    6. 导入含有HLL列的表，可以是表中的列或者数据中的列用于生成HLL列（用户是defalut_cluster中的

        curl --location-trusted -u root -T testData http://host:port/api/testDb/testTbl/_load?label=123\&max_filter_ratio=0.2
              \&columns=k1,k2,k3\&hll=hll_column1,k1:hll_column2,k2

        curl --location-trusted -u root -T testData http://host:port/api/testDb/testTbl/_load?label=123\&max_filter_ratio=0.2
              \&hll=hll_column1,tmp_k4:hll_column2,tmp_k5\&columns=k1,k2,k3,tmp_k4,tmp_k5

    7. 查看提交后的导入情况

        curl -u root http://host:port/api/testDb/_load_info?label=123

    8. 指定非严格模式导入
        curl --location-trusted -u root -T testData http://host:port/api/testDb/testTbl/_load?label=123\&strict_mode=false

## keyword
    MINI, LOAD

