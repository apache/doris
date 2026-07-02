# CDC Stream Delete Sign 设计方案

## 目标

允许 `cdc_stream` 用户显式开启 CDC 删除标记列，使 `INSERT INTO` streaming
job 能够将 `__DORIS_DELETE_SIGN__` 写入 Unique Key Merge-on-Write 表。

## 接口设计

为 TVF 增加可选布尔属性 `include_delete_sign`。默认值为 `false`，确保现有
`cdc_stream` 查询的输出 schema 保持不变。

开启后，`cdc_stream` 在 TVF 返回的源表业务列末尾追加一个名为
`__DORIS_DELETE_SIGN__` 的非空 `TINYINT` 列。CDC client 已经生成该字段：
INSERT 和 UPDATE 事件的值为 `0`，DELETE 事件的值为 `1`。

用户需要在目标列列表中显式指定 hidden column，从而启用删除语义：

```sql
INSERT INTO target(k1, v1, __DORIS_DELETE_SIGN__)
SELECT * FROM cdc_stream(
    "type" = "mysql",
    ...,
    "include_delete_sign" = "true"
);
```

实现不自动改写 sink 或 SELECT 投影，也不改变普通
`INSERT INTO ... SELECT` 对 hidden column 的绑定规则。

## 参数校验与兼容性

`include_delete_sign` 只接受 `true` 或 `false`，沿用现有大小写不敏感的布尔
参数校验方式。非法值在 TVF 分析阶段直接报错。该属性不会作为新的 streaming
job 状态持久化，因此不影响 FE image 和 edit log 兼容性。

## 测试方案

增加 FE 单元测试，覆盖默认 schema、开启参数后的 schema，以及非法参数值。
扩展 MySQL 和 PostgreSQL streaming job 回归测试，显式写入 TVF 暴露的
delete sign，并覆盖 snapshot、INSERT、UPDATE 和 DELETE。测试通过轮询可观测的
job 和数据状态进行同步，不使用固定时长的 sleep。
