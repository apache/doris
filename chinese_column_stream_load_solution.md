# 中文列名Stream Load问题解决方案

## 问题描述
客户在使用Stream Load导入包含中文列名的数据时，出现以下错误：
- 错误信息：`Duplicate column: ????`
- 中文列名在传输过程中被转换为`????`字符

## 根本原因分析

通过详细的日志分析，问题出现在**客户端HTTP请求构造阶段**：

### 日志证据
```
FE HTTP接收 (LoadAction.getAllHeaders):
Columns header value: [OBJECTID,????,???,????,??,?????,????,??????,????,????,??????,???]
Question mark count in columns header: 45
Columns header UTF-8 bytes: [4f 42 4a 45 43 54 49 44 2c 3f 3f 3f 3f 2c 3f 3f 3f 2c 3f 3f 3f 3f 2c 3f 3f 2c 3f 3f 3f 3f 3f 2c 3f 3f 3f 3f 2c 3f 3f 3f 3f 3f 3f 2c 3f 3f 3f 3f 2c 3f 3f 3f 3f 2c 3f 3f 3f 3f 3f 3f 2c 3f 3f 3f]

FE StreamLoadPut处理 (FrontendServiceImpl.streamLoadPut):
BE sent columns to FE: [OBJECTID,????,???,????,??,?????,????,??????,????,????,??????,???]
Question mark count in BE request: 45
```

### 结论
1. **FE在HTTP接收阶段就已经收到了`????`** - 说明客户端发送的HTTP请求中，columns header就已经是损坏的中文字符
2. **字节内容完全一致** - 都是`3f 3f 3f 3f`（即`????`的UTF-8编码）
3. **问题出现在客户端** - 客户端构造HTTP请求时没有正确处理中文字符编码

## 解决方案

### 方案1：URL编码中文列名（推荐）
在发送HTTP请求时，对columns头部中的中文字符进行URL编码：

**示例（curl）：**
```bash
# 错误的方式（会导致中文变成????）
curl -H "columns:OBJECTID,流程编号,发起人" ...

# 正确的方式（URL编码中文字符）
curl -H "columns:OBJECTID,%E6%B5%81%E7%A8%8B%E7%BC%96%E5%8F%B7,%E5%8F%91%E8%B5%B7%E4%BA%BA" ...
```

**各语言实现：**

**Java:**
```java
String columns = "OBJECTID,流程编号,发起人";
String encodedColumns = URLEncoder.encode(columns, "UTF-8");
request.setHeader("columns", encodedColumns);
```

**Python:**
```python
import urllib.parse
columns = "OBJECTID,流程编号,发起人"
encoded_columns = urllib.parse.quote(columns, encoding='utf-8')
headers['columns'] = encoded_columns
```

**Go:**
```go
import "net/url"
columns := "OBJECTID,流程编号,发起人"
encodedColumns := url.QueryEscape(columns)
req.Header.Set("columns", encodedColumns)
```

### 方案2：设置正确的字符编码
确保HTTP客户端使用UTF-8编码：

**Java HttpClient:**
```java
// 设置请求编码
request.setHeader("Content-Type", "text/plain; charset=UTF-8");
// 或者在构建HttpClient时设置默认编码
```

**Python requests:**
```python
import requests
headers = {'columns': '流程编号,发起人'}
# 确保使用UTF-8编码
response = requests.post(url, headers=headers, 
                        data=data.encode('utf-8'))
```

### 方案3：检查客户端环境
确保客户端运行环境支持UTF-8：

**Java:**
```bash
# 启动Java程序时设置编码
java -Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8 YourApp
```

**Linux环境变量:**
```bash
export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8
```

## 验证方法

### 1. 检查BE日志
在BE日志中查看以下信息：
```
=== Stream Load Columns Header Debug ===
Original header value: [...]
Original hex bytes: [...]
Question mark count: ...
```

如果看到`Question mark count > 0`，说明中文字符被转换成了问号。

### 2. 预期的正确输出
正确的中文字符应该显示为UTF-8字节序列，例如：
- `流程编号` 应该是：`e6 b5 81 e7 a8 8b e7 bc 96 e5 8f b7`
- 而不是：`3f 3f 3f 3f`

## 注意事项
1. **URL编码是最可靠的解决方案**，因为它确保中文字符在HTTP传输中不会丢失
2. **不同的HTTP客户端库**可能有不同的默认编码行为
3. **服务器端已经支持UTF-8解码**，问题主要在客户端编码

## 技术支持
如果问题仍然存在，请提供：
1. BE日志中的调试信息
2. 客户端使用的HTTP库和版本
3. 客户端运行环境信息
