---
{
    "title": "Committer指南",
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

# Committer指南

## Committer新人指南

### 订阅公共邮件列表

请订阅{dev,commits}@doris.apache.org邮件列表，通过发送邮件到{dev,commits}-subscribe@doris.apache.org完成订阅。
commits邮件非常重要，因为所有的GitHub Issue，PR提交都会发往这个邮件列表。

### 订阅private邮件列表

订阅private@doris.apache.org通过发送邮件到private-subscribe@doris.apache.org完成订阅。
这个订阅操作需要邮件列表的moderator进行审核才能订阅成功。
如果你是PPMC的话, 可以通过通过[邮件列表订阅帮手](https://whimsy.apache.org/committers/subscribe)直接完成订阅。

### 关联你的 ASF 账号与 Github 账号

我们用GitHub来管理我们的Issue以及用户贡献。
所以你需要把你的ASF账号与GitHub账号进行关联来获得[Doris仓库](https://github.com/apache/incubator-doris)的写入权限。
通过在[Gitbox](https://gitbox.apache.org/setup/)完成操作后，你就可以对GitHub仓库中的Issue，PR进行管理。

## Code Review指南

1. 始终保持一个较高的标准来进行review，这样才能更好地保证整个产品的质量。

2. 对于用户接口类的、整体架构方面的修改，需要在社区进行充分地讨论，可以在邮件组发起，也可以在issue上发起。
用户接口的改变包括支持新的SQL函数，支持新的HTTP接口，支持新的功能等。这样能够保证产品的一致性。

3. 测试覆盖。新增的逻辑需要有对应的测试来覆盖。对于已有老代码，不好增加的可以酌情考虑。

4. 文档。新增加的功能必须要有文档来说明，否则这样的代码不允许合入。必须要有英文文档，最好有中文文档。

5. 代码的可读性。如果review的同学对于代码逻辑不是很清晰，那么可以要求contributor去解释这段逻辑，并且需要在代码里写充分的注释来解释逻辑。

6. 尽量在评论的结尾给出明确的结论。是同意，还是要change request。如果是小问题，可以只留评论。

7. 如果你已经看过了代码，觉得没有问题，但是觉得需要其他同学来确认下，可以留下+1 Comment。

8. 互相尊重，互相学习。在评论的时候保持礼貌的口吻，提建议尽量给出建议的理由。

## Pull Request指南

1. 一个PR合入需要三种角色的参与。Contributor：PR的提交者；Reviewer：对PR进行代码级评论的人；Moderator：PR合入的协调者。
Moderator主要负责给PR设定相关标签，推动相关reviewer进行评论，推动作者对PR进行修改，合入PR等工作。
在一个具体的PR中，一个人可能充当不同的角色，比如一个Committer自己提交的PR，既是Contributor，又是这个PR的Moderator。

2. Committer可以把一个PR分配给自己作为整个PR的moderator，负责后续PR的推动工作。分配给自己之后，其他的Committer就知道这个PR有相关人负责了。

3. 鼓励Committer作为自己PR的Moderator。

4. Reviewer需要进行代码级的review，可以参考Code Review Guideline。

5. Reviewer一旦评论了某个PR之后，需要持续跟进这个PR的后续改动，不鼓励评论了之后就不再管Contributor的后续回复了。

6. 一个PR至少要获得一个非作者外的Committer +1才能进行被合入。

7. PR获得第一个+1后，至少要等一个工作日后才能进行合入。主要目的是等待社区其他同学来进行review。

8. 对于接口类、整体架构方面的修改，至少要获得3个+1才能进行合入。

9. 需要回归测试全部通过才能被合入。

10. Moderator需要在确定所有的评论都被回复之后才能进行代码合入。

11. 代码合入选择“squash and merge”方式进行合入。

12. 对于一个修改不同的reviewer有争议时，可以尝试讨论解决。如果讨论没有办法解决，可以在private@doris中发邮件投票解决，采取少数服从多数的原则。

