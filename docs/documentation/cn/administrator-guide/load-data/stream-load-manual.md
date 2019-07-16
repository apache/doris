# Stream load
Stream load 是一个同步的导入方式，用户通过发送 HTTP 请求将本地或内存中文件导入到 Doris 中。Stream load 同步执行导入并返回导入结果。用户可直接通过请求的返回体判断本次导入是否成功。

Stream load 主要适用于待导入文件在发送导入请求端，或可被发送导入请求端获取的情况。

# 基本原理
Stream load 是单机版本的导入，整个导入过程中参与 ETL 的节点只有一个 BE。由于导入后端是流式的所以并不会受单个 BE 的内存限制影响。

下图展示了 Stream load 的主要流程，省略了一些导入细节。

```
^                  +
|                  |
|                  |   user create stream load
| 5. BE return     |
| the result of    v
| stream load  +---+-----+
|              |         |
|              |FE       |
|              |         |
|              ++-+------+
| 1.FE redirect | ^  2. BE fetch the plan
| request to be | |  of stream load
|               | |   ^
|               | |   | 4. BE commit the txn
|               v |   |
|              ++-+---+--+
|              |         +------+
+--------------+BE       |      | 3. BE execute plan
               |         +<-----+
               +---------+
```

# 基本操作
## 创建导入
创建导入的详细语法执行 ``` HELP STREAM LOAD；``` 查看语法帮助。各个导入通用的属性在上级 LOAD 文档中已经介绍，这里主要介绍 Stream load 的特有属性和注意事项。

### timeout
目前 Stream load 并不支持自定义导入的 timeout 时间，默认的 timeout 时间为300秒。如果导入的源文件无法再规定时间内完成导入，则需要调整 FE 的参数```stream_load_default_timeout_second```。

## 查看导入
Stream load 导入方式并不会在 Doris 系统中记录元信息，所以用户无法异步的通过查看导入命令看到 Stream load。用户需监听创建导入请求的返回值获取导入结果。

## 取消导入
用户无法手动取消 Stream load，Stream load 在超时或者导入错误后会被系统自动取消。

# Mini load

Mini load 支持的数据源，导入方式以及支持的 ETL 功能是 Stream load 的子集，且导入的效率和 Stream load 一致。**强烈建议使用 Stream load 替代 Mini load**

唯一不同的是，Mini load 由于为了兼容旧版本的使用习惯，用户不仅可以根据创建请求的返回值查看导入状态，也可以异步的通过查看导入命令查看导入状态。

Mini load的创建语法帮助可执行 ``` HELP MINI LOAD```。




