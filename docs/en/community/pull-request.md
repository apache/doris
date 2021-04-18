---
{
    "title": "Code Submission Guide",
    "language": "en"
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

# Code Submission Guide

[Pull Request (PR)](https://help.github.com/articles/about-pull-requests/) can be easily submitted on [Github](https://github.com/apache/incubator-doris). The PR method of Doris project is described below.

## Fork Repository

Go to the [github page](https://github.com/apache/incubator-doris) of apache/incubator-doris , and click the button `Fork` in the upper right corner for Fork.

![Fork](/images/fork-repo.png)

### 2. Configuring GIT and submitting modifications

#### (1) Clone the code locally:

```
git clone https://github.com/<your_github_name>/incubator-doris.git
```

Note: Please replace your GitHub name with your yourgithubname\\\\\\\\\\\\\\.

When clone is completed, origin defaults to the remote fork address on github.

#### (2) Add apache/incubator-doris to the remote branch upstream of the local warehouse:

```
cd  incubator-doris
git remote add upstream https://github.com/apache/incubator-doris.git
```

#### (3) Check remote warehouse settings:

```
git remote -v
origin https://github.com/<your_github_name>/incubator-doris.git (fetch)
origin    https://github.com/<your_github_name>/incubator-doris.git (push)
upstream  https://github.com/apache/incubator-doris.git (fetch)
upstream  https://github.com/apache/incubator-doris.git (push)
```

#### (4) New branches to modify them:

```
git checkout -b <your_branch_name>
```

Note: \<your\_branch\_name\> name is customized for you.

Code changes can be made after creation.

#### (5) Submit code to remote branch:

```
git commit -a -m "<you_commit_message>"
git push origin <your_branch_name>
```

For more git usage, please visit: [git usage](https://www.atlassian.com/git/tutorials/set-up-a-repository), not to mention here.

### 3. Create PR

#### (1) New PR
Switch to your GitHub page in the browser, switch to the submitted branch yourbranchname\\ and click the `New pull request` button to create it, as shown in the following figure:

![new PR](/images/new-pr.png)

#### (2) preparation branch
At this time, the `Create pull request` button will appear. If not, please check whether the branch is selected correctly or click on `compare across forks' to re-select the repo and branch.

![create PR](/images//create-pr.png)

#### (3) Fill Commit Message
Here, please fill in the summary and details of the comment, and then click `Create pull request` to create it.

For how to write Commit Message, here are some Tips:

* Please use the form of English verb + object. The verb does not use the past tense and the sentence uses imperative sentence.
* Subject and body should be written, and they should be separated by blank lines (fill in separately on GitHub PR interface).
* Message topic length should not exceed **50** characters;
* Message content should not exceed **72** characters per line, and the excess should be replaced manually.
* Message content is used to explain what has been done, why and how.
* The first letter of the message subject should be **capitalized**, and the end of the sentence **should not** have a full stop.
* The message content specifies the associated issue (if any), such as # 233;

For more details, see <https://chris.beams.io/posts/git-commit>.

![create PR](/images/create-pr2.png)

#### (4) Complete the creation
After successful creation, you can see that Doris project needs review, you can wait for us to review and join, you can also contact us directly.

![create PR](/images/create-pr3.png)

So far, your PR creation is complete. Read more about PR [collaborating-with-issues-and-pull-requests] (https://help.github.com/categories/collaborating-with-issues-and-pull-requests/).

### 4. Conflict Resolution

When submitting PR, code conflicts are usually caused by multiple people editing the same file. The main steps to resolve conflicts are as follows:

#### (1) Switch to the main branch

```
git checkout master
```

#### (2) Synchronize remote main branch to local

```
git pull upstream master
```

#### (3) Switch back to the previous branch (assuming the branch is named fix)

```
git checkout fix
```

#### (4) rebase

```
git rebase -i master
```

At this point, a file that modifies the record will pop up and can be saved directly. Then, we will prompt which files have conflicts. At this time, we can open the conflict file to modify the conflict part. After all the conflicts of the conflict files are resolved, we will execute them.

```
git add .
git rebase --continue
```

Then you can go back and forth until the screen appears something like * rebase successful * and then you can update the branch that submitted PR:

```
git push -f origin fix
```

### 5. An example

#### (1) fetch to the latest code for the local branch of upstream that has been configured

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

#### (2) rebase

```
$ git rebase upstream/master
First, rewinding head to replay your work on top of it...
Fast-forwarded master to upstream/master.
```

#### (3) Check to see if other submissions are not synchronized to their own repo submissions

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

#### (4) Merge code submitted by others into their own repo

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

#### (5) New branch, ready for development

```
$ git checkout -b my_branch
Switched to a new branch 'my_branch'

$ git branch
  master
* my_branch
```

#### (6) Prepare to submit after code modification is completed

```
$ git add -u
```

#### (7) Fill in the message and submit it it to the new local branch

```
$ git commit -m "Fix a typo"
[my_branch 55e0ba2] Fix a typo
1 files changed, 2 insertions(+), 2 deletions(-)
```

#### (8) Push the branch into GitHub's own repo far away

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

At this point, you can create PR according to the previous process.
