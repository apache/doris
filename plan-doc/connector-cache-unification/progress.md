# Progress Log — 连接器缓存框架统一 (connector cache unification)

> **append-only**：每 session 追加一条（日期 / 做了什么 / 结论 / 下一步）；不覆盖、不删旧条。
> 滚动上下文在 [`HANDOFF.md`](./HANDOFF.md)，进度总览在 [`tasklist.md`](./tasklist.md)。

---

## 2026-07-23 — 搭建伞形追踪空间（未启动执行）

- **做了什么**：读 `00-research-report.md` + `data/connector-audits.json`（hive/hudi/paimon 全文）+ 参考空间 `perf-hotpath-iceberg/`（README/HANDOFF/tasklist）；据此在本目录建 `HANDOFF.md` + `tasklist.md` + 本 `progress.md`。
- **结论**：
  - 本空间定位为**伞形协调空间**——追踪 workstream 级（WS-HUDI / WS-MC / WS-ES / WS-DOC / WS-P2）+ 4 个 owner 决策（D1–D4）；逐连接器执行各自另开 `perf-hotpath-<c>/` 兄弟空间（报告 §9）。
  - 未改 `README.md`（它是完成态的调研交付物）；稳定流程/铁律/build 坑放进 HANDOFF 底部稳定区。
  - **未做任何拍板、未启动任何执行、未改产品代码。**
- **下一步（交下个 session）**：先向用户讲清 D1–D4 拿签字（中文，见 HANDOFF「下一步」），默认推荐先启动 WS-HUDI（唯一 P1 真缺口）并新开 `plan-doc/perf-hotpath-hudi/`。动码前按 HEAD 重侦察（行号信 grep）。
