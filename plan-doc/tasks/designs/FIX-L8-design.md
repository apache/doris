# FIX-L8 — kerberos doAs 把 InterruptedException→IOException 不 restore interrupt

> 来源：reverify §1 表 L8（原 P3b-4）。🟡 低（一行）。范围：`HadoopAuthenticator.doAs`。
> HEAD 复核基线：`e27602d4ab6`。

## Problem / Root Cause

`HadoopAuthenticator.doAs`（默认方法，:31-43）`catch (InterruptedException e) { throw new IOException(e); }`——
把 `InterruptedException` 吞成 checked `IOException` 却**不 restore 线程中断标志**。上层调用方因此无法感知线程曾被中断,
破坏协作式取消。

## Design（一行）

`throw new IOException(e);` 前加 `Thread.currentThread().interrupt();`。标准 Java 最佳实践:吞掉 `InterruptedException`
（不原样上抛）时须回置中断标志。

## Risk

零功能变更,仅恢复中断标志。fe-kerberos 独立模块。

## Test Plan

- build-compile。一行标准惯用法,无需 UT/red-team。

## 备注

与 L7 同模块合并编译。
