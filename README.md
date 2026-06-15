# stupid-kv

一个用 **Rust** 实现的键值数据库教程项目。通过从零开始逐步构建一个支持 **MVCC（Multi-Version Concurrency Control）** 的 KV 数据库，深入理解数据库内核的核心概念。

## 项目简介

本项目参考学习自 [`surrealdb/surrealmx`](https://github.com/surrealdb/surrealmx)，在其设计基础上进行简化和教学化实现，便于学习数据库内核开发。

## 实现进度

每一节对应一个 git tag，可通过 `git checkout <tag>` 查看对应版本的代码。

### 已完成

| Tag | 章节 | 说明 |
|-----|------|------|
| [`0.0.1`](https://github.com/letterbeezps/stupid-kv/tree/0.0.1) | Section 0.0.1 — 基本 MVCC 事务 | 纯内存 KV，MVCC 多版本并发控制，快照隔离，写-写冲突检测 |

详细设计文档：[docs/001_basic_transaction.md](docs/001_basic_transaction.md)

### 后续规划

- **GC** — 历史版本清理，提交队列清理
- **持久化** — WAL / Snapshot 持久化到磁盘
- **SSI** — Serializable Snapshot Isolation
- ...以及更多可能的优化章节

## 快速开始

```bash
# 运行示例
cargo run --example basic

# 运行单元测试
cargo test
```

## 架构概览

```
                     ┌──────────────────────────────────┐
                     │         Database                 │
                     │  Arc<Inner>                      │
                     │                                  │
                     │  Oracle ── global clock          │
                     │  commit queue                    │
                     │  merge queue                     │
                     │  datastore                       │
                     └───────────┬──────────────────────┘
                                 │ shared ref
                                 ▼
                     ┌──────────────────────────┐
                     │      Transaction         │
                     │  commit: snapshot id     │
                     │  version: timestamp      │
                     │  writeset: local mods    │
                     │  get/set/del             │
                     └──────────────────────────┘
```

## 项目结构

```
stupid-kv/
├── src/
│   ├── db/           # 数据库入口与核心状态
│   ├── oracle/       # Oracle 全局时间戳分配器
│   ├── tx/           # 事务实现
│   ├── versions/       # 多版本数据管理
│   ├── queue/          # 提交队列与合并队列
│   ├── kv/             # key/value 类型转换
│   └── error/        # 错误类型定义
│   └── lib.rs
├── examples/       # 示例代码
├── docs/           # 设计文档
└── Cargo.toml
```

## 设计文档

每一节实现对应一个设计文档：

| 章节 | 文档 | 标签 |
|------|------|------|
| 基本 MVCC 事务 | [001_basic_transaction.md](docs/001_basic_transaction.md) | `0.0.1` |

## License

本项目为学习项目，仅供学习研究使用。
