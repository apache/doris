---
{
    "title": "代码提交指南",
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

# 代码提交指南

在 [Github](https://github.com/apache/incubator-doris) 上面可以很方便地提交 [Pull Request (PR)](https://help.github.com/articles/about-pull-requests/)，下面介绍 Doris 项目的 PR 方法。

### 1. Fork仓库

进入 apache/incubator-doris 的 [github 页面](https://github.com/apache/incubator-doris) ，点击右上角按钮 `Fork` 进行 Fork。

![Fork](/images/fork-repo.png)

### 2. 配置git和提交修改

#### （1）将代码克隆到本地：

```
git clone https://github.com/<your_github_name>/incubator-doris.git
```

注意：请将 \<your\_github\_name\> 替换为您的 github 名字。
  
clone 完成后，origin 会默认指向 github 上的远程 fork 地址。

#### （2）将 apache/incubator-doris 添加为本地仓库的远程分支 upstream：

```
cd  incubator-doris
git remote add upstream https://github.com/apache/incubator-doris.git
```

#### （3）检查远程仓库设置：

```
git remote -v
origin https://github.com/<your_github_name>/incubator-doris.git (fetch)
origin    https://github.com/<your_github_name>/incubator-doris.git (push)
upstream  https://github.com/apache/incubator-doris.git (fetch)
upstream  https://github.com/apache/incubator-doris.git (push)
```

#### （4）新建分支以便在分支上做修改：

```
git checkout -b <your_branch_name>
```

注意： \<your\_branch\_name\> 为您自定义的分支名字。

创建完成后可进行代码更改。

#### （5）提交代码到远程分支：

```
git commit -a -m "<you_commit_message>"
git push origin <your_branch_name>
```

更多 git 使用方法请访问：[git 使用](https://www.atlassian.com/git/tutorials/setting-up-a-repository)，这里不赘述。

### 3. 创建PR

#### （1）新建 PR
在浏览器切换到自己的 github 页面，切换分支到提交的分支 \<your\_branch\_name\> ，点击 `New pull request` 按钮进行创建，如下图所示：

![new PR](/images/new-pr.png)

#### （2）准备分支
这时候，会出现 `Create pull request` 按钮，如果没有请检查是否正确选择了分支，也可以点击 “compare across forks” 重新选择 repo 和分支。

![create PR](/images//create-pr.png)

#### （3）填写 Commit Message
这里请填写 comment 的总结和详细内容，然后点击 `Create pull request` 进行创建。

关于如何写 Commit Message，下面列出了一些 Tips：

* 请用英文 动词 + 宾语 的形式，动词不用过去式，语句用祈使句；
* 消息主题（Subject）和具体内容（Body）都要写，它们之间要有空行分隔（GitHub PR界面上分别填写即可）;
* 消息主题长度不要超过**50**个字符；
* 消息内容每行不要超过**72**个字符，超过的需要手动换行；
* 消息内容用于解释做了什么、为什么做以及怎么做的；
* 消息主题第一个字母要**大写**，句尾**不要**有句号；
* 消息内容中写明关联的issue(如果有)，例如 #233;

更详细的内容请参考 <https://chris.beams.io/posts/git-commit>

![create PR](/images/create-pr2.png)

#### （4）完成创建
创建成功后，您可以看到 Doris 项目需要 review，您可以等待我们 review 和合入，您也可以直接联系我们。

![create PR](/images/create-pr3.png)

至此，您的PR创建完成，更多关于 PR 请阅读 [collaborating-with-issues-and-pull-requests](https://help.github.com/categories/collaborating-with-issues-and-pull-requests/) 。

### 4. 冲突解决

提交PR时的代码冲突一般是由于多人编辑同一个文件引起的，解决冲突主要通过以下步骤即可：

#### （1）切换至主分支

``` 
git checkout master
```
   
#### （2）同步远端主分支至本地

``` 
git pull upstream master
```
   
#### （3）切换回刚才的分支（假设分支名为fix）

``` 
git checkout fix
```
   
#### （4）进行rebase
   
``` 
git rebase -i master
```
   
此时会弹出修改记录的文件，一般直接保存即可。然后会提示哪些文件出现了冲突，此时可打开冲突文件对冲突部分进行修改，将提示的所有冲突文件的冲突都解决后，执行
   
```
git add .
git rebase --continue
```
   
依此往复，直至屏幕出现类似 *rebase successful* 字样即可，此时您可以进行往提交PR的分支进行更新：
   
```
git push -f origin fix
```
   
### 5. 一个例子

#### （1）对于已经配置好 upstream 的本地分支 fetch 到最新代码

```
$ git branch
* master

$ git fetch upstream          
remote: Counting objects: 195, done.
remote: Compressing objects: 100% (68/68), done.
remote: Total 141 (delta 75), reused 108 (delta 48)
Receiving objects: 100% (141/141), 58.28 KiB, done.
Resolving deltas: 100% (75/75), completed with 43 local objects.
From https://github.com/apache/incubator-doris
   9c36200..0c4edc2  master     -> upstream/master
```

#### （2）进行rebase

```
$ git rebase upstream/master  
First, rewinding head to replay your work on top of it...
Fast-forwarded master to upstream/master.
```

#### （3）检查看是否有别人提交未同步到自己 repo 的提交

```
$ git status
# On branch master
# Your branch is ahead of 'origin/master' by 8 commits.
#
# Untracked files:
#   (use "git add <file>..." to include in what will be committed)
#
#       custom_env.sh
nothing added to commit but untracked files present (use "git add" to track)
```

#### （4）合并其他人提交的代码到自己的 repo

```
$ git push origin master
Counting objects: 195, done.
Delta compression using up to 32 threads.
Compressing objects: 100% (41/41), done.
Writing objects: 100% (141/141), 56.66 KiB, done.
Total 141 (delta 76), reused 140 (delta 75)
remote: Resolving deltas: 100% (76/76), completed with 44 local objects.
To https://lide-reed:fc35ff925bd8fd6629be3f6412bacee99d4e5f97@github.com/lide-reed/incubator-doris.git
   9c36200..0c4edc2  master -> master
```

#### （5）新建分支，准备开发

```
$ git checkout -b my_branch
Switched to a new branch 'my_branch'

$ git branch
  master
* my_branch
```

#### （6）代码修改完成后，准备提交

```
$ git add -u
```

#### （7）填写 message 并提交到本地的新建分支上

```
$ git commit -m "Fix a typo"
[my_branch 55e0ba2] Fix a typo
 1 files changed, 2 insertions(+), 2 deletions(-)
```

#### （8）将分支推到 GitHub 远端自己的 repo 中

```
$ git push origin my_branch
Counting objects: 11, done.
Delta compression using up to 32 threads.
Compressing objects: 100% (6/6), done.
Writing objects: 100% (6/6), 534 bytes, done.
Total 6 (delta 4), reused 0 (delta 0)
remote: Resolving deltas: 100% (4/4), completed with 4 local objects.
remote: 
remote: Create a pull request for 'my_branch' on GitHub by visiting:
remote:      https://github.com/lide-reed/incubator-doris/pull/new/my_branch
remote: 
To https://lide-reed:fc35ff925bd8fd6629be3f6412bacee99d4e5f97@github.com/lide-reed/incubator-doris.git
 * [new branch]      my_branch -> my_branch
```

至此，就可以按照前面的流程进行创建 PR 了。