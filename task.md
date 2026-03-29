
## 环境准备
* 到 /mnt/disk1/gavinchou/workspace/agent-workspace/apache-doris 目录下 git
	fetch 最新的remote origin 以及 remote gavin的代码. 只fetch, 不用checkout
* 从 /mnt/disk1/gavinchou/workspace/agent-workspace/apache-doris 为分支gavin-http-admin-auth 创建一个worktree 到当前目录下
* 把 /mnt/disk1/gavinchou/workspace/agent-workspace/apache-doris/custom_env.sh /mnt/disk1/gavinchou/workspace/agent-workspace/apache-doris/switch_installed.sh /mnt/disk1/gavinchou/workspace/agent-workspace/apache-doris/format_code.sh copy 到worktree目录下
* worktree 目录下执行 sh switch_installed.sh installed-master-latest



## 运行单元测试以及代码风格修正的方法
* sh format_code.sh 这个脚本可以format cpp 的代码, 提交代码之前先 执行它对**更改
	的cpp代码进行format**, 用法 `sh format_code.sh be` 或者 `sh format_code.sh cloud` 或者 `sh format_code.sh <specific_file>`, FE目录不是用这个做检查
* 执行cloud目录相关改动的单测 sh run-cloud-ut.sh --run --filter "txn_kv_test:*.*" --fdb "fdb_cluster0:cluster0@10.26.20.4:4500"
* 执行BE目录相关改动的单测 `sh run-be-ut.sh --run --filter "xxxx"`

## 任务要求
gavin-http-admin-auth 这个分支已经创建了PR https://github.com/apache/doris/pull/60761
需要解决这PR的comment, airbone2和simba的评论都是对的, 按照他们的评论进行针对的修复.
当前PR 已经有了不少冲突, 同时也解决下冲突的问题. 
本地变更要求能编译通过FE/BE, 同时对应的单测需要能跑通.

代码更改完成之后不要 amend本地的commit, 新增commit. 不使用force push 到remote gavin,
直接push. push 完成之后给 PR comment 一个 run buildall 触发ci cd
