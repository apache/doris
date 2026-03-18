# Cloud MOW 两阶段提交 - 模块总览

## 背景

存算分离下MOW表高频高并发导入时，当前一阶段提交方案存在严重瓶颈：
- FE表级内存锁保护范围过大（MS表锁获取→partition version读取→delete bitmap计算同步等待→MS事务提交）
- MS事务提交中的KV操作量与tablet数量成正比
- 导入和compaction通过MS表级分布式锁互斥
- 无法通过水平扩展BE来提升吞吐

## 方案概述

改为两阶段提交：
- **Commit阶段**（快速，持FE表锁）：仅在MS更新partition commit version+1 + TxnInfoPB
- **Publish阶段**（异步，不持FE表锁）：后台线程下发delete bitmap计算任务，每个tablet独立完成：计算delete bitmap → MS写delete bitmap → MS转正rowset meta → BE本地应用rowset。所有tablet完成后，轻量级MS commit（仅visible version+1）

## 关键设计决策

1. **Tablet级跨事务依赖**：delete bitmap计算+MS转正完成后，BE本地应用rowset（max_version提升），后续事务同tablet可立即开始计算，不需等整个事务完成
2. **Per-tablet独立转正**：tmp rowset转正不在最终MS commit中，而是每个tablet算完delete bitmap后独立进行。最终MS commit变为O(1)
3. **MS转正成功后才更新BE本地元数据**：避免compaction和转正的冲突
4. **导入参数持久化到TxnInfoPB**：使用TOlapTableSchemaParam的Thrift序列化bytes存储，整个导入只需持久化一次
5. **Delete bitmap更新用现有RPC**，rowset转正用新RPC
6. **MS新增tablet级别锁**：替代现有表级别锁，用于导入和compaction在同一tablet上互斥（大多数情况BE内存锁已够用，MS tablet锁主要处理跨BE场景）
7. **启用两阶段提交的表不再需要lazy commit和大事务机制**
8. **TXN_STATUS_COMMITTED语义变更**：在启用两阶段提交的表上，COMMITTED表示两阶段commit完成（而非lazy commit的含义）
9. **仅限新表**，通过表属性控制是否启用。**所有老代码完整保留**，通过条件分支分叉
10. **Commit可由FE或BE发起**（coordinator），导入参数传递方式统一通过TOlapTableSchemaParam
11. **Per-BE批量下发**：FE按BE分组发任务，每个请求包含该BE上所有涉及的tablet，BE内部独立完成全流程

## 模块列表

| 模块 | 文件 | 说明 | 依赖 |
|------|------|------|------|
| Module 0 | `01-proto-kv-schema.md` | Proto & KV Schema变更 | 无（基础模块）|
| Module 1 | `02-ms-commit-phase.md` | MS侧Commit阶段API | Module 0 |
| Module 2 | `03-ms-convert-rowset.md` | MS侧Per-Tablet Rowset转正（新RPC） | Module 0 |
| Module 3 | `04-ms-lightweight-publish.md` | MS侧轻量级Publish | Module 0 |
| Module 4 | `05-fe-commit-phase.md` | FE侧Commit阶段 + Committed Txns管理 | Module 0, 1 |
| Module 5 | `06-fe-publish-daemon.md` | FE侧Publish后台线程 | Module 0, 2, 3, 4 |
| Module 6 | `07-fe-recovery.md` | FE重启/切主恢复 | Module 0, 4 |
| Module 7 | `08-be-calc-bitmap-apply.md` | BE侧CalcDeleteBitmapTask改造 + 本地Apply | Module 0, 2 |
| Module 8 | `09-cleanup-locks.md` | 清理：移除MS表锁和pending delete bitmap | 所有其他模块 |

## 依赖关系图

```
Module 0 (Proto/KV)
├── Module 1 (MS Commit)
│   └── Module 4 (FE Commit + Txns Manager)
│       ├── Module 5 (FE Publish Daemon)
│       └── Module 6 (FE Recovery)
├── Module 2 (MS Convert Rowset)
│   ├── Module 5 (FE Publish Daemon)
│   └── Module 7 (BE CalcBitmap + Apply)
├── Module 3 (MS Lightweight Publish)
│   └── Module 5 (FE Publish Daemon)
└── Module 8 (Cleanup) ← 依赖所有其他模块

```

## 每个tablet的完整publish流水线

```
FE Publish Daemon（一个调度线程 + 线程池执行）从 committed txns set 取出事务
  │
  ├── 按BE分组，每个BE发一个请求（包含该BE上所有涉及的tablet）
  │
  ├── BE收到请求后，对每个tablet独立完成全流程：
  │     → 拿BE内存tablet写锁（rowset_update_lock）
  │     → 检查版本连续性（max_version+1 == commit_version）
  │     → 计算增量rowset的delete bitmap
  │     → 调用MS update_delete_bitmap (现有RPC)
  │     → 拿MS tablet级别锁（和compaction互斥）
  │     → 调用MS convert_tmp_rowset (新RPC): tmp→formal rowset + tablet stats
  │     → 将rowset和delete bitmap放入BE内存CloudTablet (max_version=N)
  │     → 释放MS tablet级别锁
  │     → 事务B的tablet1可以开始计算（tablet级跨事务依赖）
  │
  ├── BE返回每个tablet的成功/失败结果
  │
  └── 所有tablet完成
      → 拿FE表内存写锁
      → FE调用MS轻量级publish: visible_version+1 + TxnInfoPB→VISIBLE
      → 释放FE表内存写锁
      → 唤醒等待publish的导入线程
```

## 审查指引

请按以下顺序审查各模块文档：
1. `01-proto-kv-schema.md` — 基础数据结构，所有模块依赖
2. `02-ms-commit-phase.md` + `03-ms-convert-rowset.md` + `04-ms-lightweight-publish.md` — MS侧三个API
3. `05-fe-commit-phase.md` — FE commit流程和内存数据结构
4. `06-fe-publish-daemon.md` — FE publish后台线程（核心调度逻辑）
5. `07-fe-recovery.md` — FE重启恢复
6. `08-be-calc-bitmap-apply.md` — BE侧改造
7. `09-cleanup-locks.md` — 清理旧机制
