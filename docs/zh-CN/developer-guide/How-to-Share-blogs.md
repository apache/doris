---
{
    "title": "如何分享Blog",
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

# 如何分享Blog


## 编写Blog文件

编写一个Blog文件，首先要在头部包含Front Matter信息，主要包含以下内容：

| 变量名 | 默认值 | 含义 |
|--------|----------------------------|----------|
| title| - | Blog标题|
| description | - | Blog描述|
| date | - | Blog发布时间 |
| author | - | Blog作者 |
| metaTitle | - | 浏览文章时候浏览器显示的标题 |
| language | en/zh-CN | 语言 |
| layout | Article | 布局组件 |
| sidebar | false | 隐藏侧边栏 |
| isArticle | true | 是否是文章，默认不要修改 |

>**注意**
>
>其中title、description、date、author，metaTitle字段值由Blog编写者填写，其他字段为固定值。
>
>language：en，zh-CN，主要不要书写错误

文件头示例：
```
---
{
    "title": "This is title",
    "description": "This is description",
    "date": "2021-11-03",
    "author": "Alex",
    "metaTitle": "This is title",
    "language": "zh-CN",
    "layout": "Article",
    "sidebar": false
    "isArticle":true
}
---
```
同时在文件头后面添加Apache License内容：

```
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
```
最后编写Blog正文内容。

## Blog文件的存放目录

Blog文件编写完成之后，将其放到相应的目录中。中文语言的目录是：zh-CN/article/articles/，
英文语言对应的目录是：en/article/articles/。

## 查看Blog

编写好Blog并将其放入相应目录，就可以去查看Blog了。点击首页的最新动态按钮进入Blog列表页，再次点击Blog标题即可查看Blog详情。