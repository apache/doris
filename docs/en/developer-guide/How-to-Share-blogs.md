---
{
    "title": "How to Share blogs",
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

# How to share a blog


## Write a blog file 

To write a blog file, you must first include Front Matter information in the header，It mainly contains the following contents：

| Variable | default | description |
|--------|----------------------------|----------|
| title| - | Blog title|
| description | - | Blog description|
| date | - | Blog date |
| author | - | Blog author |
| metaTitle | - | The title displayed by the browser when browsing the article |
| language | en/zn-CN | language |
| layout | Article | Layout of the components |
| sidebar | false | Hide the sidebar |
| isArticle | true | Whether it is an article, do not modify by default |

>**Attention**
>
>The title, description, date, author, and metaTitle field values are filled in by the blog writer, and the other fields are fixed values.
>
>language: en, zh-CN, mainly don’t make mistakes

File header example：
```json
---
{
    "title": "This is title",
    "description": "This is description",
    "date": "2021-11-03",
    "author": "Alex",
    "metaTitle": "article",
    "language": "zh-CN",
    "layout": "Article",
    "sidebar": false
    "isArticle":true
}
---
```
At the same time add Apache License content after the file header：

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
Finally, write the content of the blog body。

## Blog file storage directory

After the blog file is written, put it in the corresponding directory. The Chinese language directory is：zh-CN/article/articles/，The directory corresponding to the English language is：en/article/articles/。

## View blog

Write a blog and put it in the corresponding directory, you can go to view the blog。Click the latest news button on the homepage to enter the blog list page, and click the blog title again to view the blog details。