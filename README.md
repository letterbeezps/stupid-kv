# stupid-kv

一个 **Rust** 学习者在探索数据库内核过程中的小练习。通过参考优秀的开源项目 [`surrealdb/surrealmx`](https://github.com/surrealdb/surrealmx)，尝试从零开始实现一个支持 **MVCC（Multi-Version Concurrency Control）** 的 KV 数据库，以加深对数据库核心概念的理解。

## 项目简介

本项目**完全参考学习**自 [`surrealdb/surrealmx`](https://github.com/surrealdb/surrealmx)。surrealmx 是一个非常出色的数据库内核实现，我在学习过程中借鉴了它的设计思路，并尝试用自己的方式实现出来。

这是一个**纯学习性质**的项目，代码质量和实现细节可能存在很多不足，仅供学习研究使用。如果你也对数据库内核感兴趣，欢迎一起交流讨论！

## 学习进度

我会把学习过程中的每一个阶段做成一个教程，每一节对应一个 git tag，可以通过 `git checkout <tag>` 查看对应版本的代码。

### 完成进度

| Tag | 章节 | 内容 |
|-----|------|----------|
| [`0.0.1`](https://github.com/letterbeezps/stupid-kv/tree/0.0.1) | Section 0.0.1 — 基本 MVCC 事务 | 纯内存 KV，MVCC 多版本并发控制，快照隔离，写-写冲突检测 |
| [`0.0.2`](https://github.com/letterbeezps/stupid-kv/tree/0.0.2) | Section 0.0.2 — SSI 与 Bloom 过滤器 | Serializable Snapshot Isolation，readset 追踪，读-写冲突检测，Bloom 过滤器加速冲突检测 |
| [`0.0.3`](https://github.com/letterbeezps/stupid-kv/tree/0.0.3) | Section 0.0.3 — 运行时鲁棒性加固 | 写路径并发安全（封堵静默丢数据窗口），auto_commit / atomic_merge 自适应退避，Oracle 后台 resync 抗时钟漂移，DatabaseOptions 参数入口 |
| [`0.0.4`](https://github.com/letterbeezps/stupid-kv/tree/0.0.4) | Section 0.0.4 — 提交队列 GC | 活跃事务引用计数 `counter_by_commit`，Dekker 风格双 fence 协议（`register_counter` ↔ `earliest_active`），后台清理线程 + 优雅停机，避免 `transaction_commit_queue` 无限增长 |
| [`0.0.5`](https://github.com/letterbeezps/stupid-kv/tree/0.0.5) | Section 0.0.5 — 版本历史 GC | 复用 0.0.4 引用计数框架扩到 datastore：`counter_by_oracle` + `gc_floor` 事前警告线拦截"新事务落到被回收版本"的竞争，增量 `gc_dirty_keys` 队列 + 周期性全量兜底，`Versions::gc_older_versions` 就地压缩版本链，与 write-path `is_removed()` 握手协议衔接 |
| [`0.0.6`](https://github.com/letterbeezps/stupid-kv/tree/0.0.6) | Section 0.0.6 — 快照持久化 | 全量快照持久化：`Persistence` 模块 + `SnapshotMode` 配置，`tmp → rename → sync_all` 三段式原子落盘协议，bincode 流式序列化 / 反序列化，后台周期快照线程，`Database::new_with_persistence` 构造 + 三阶段 shutdown 顺序 |
| [`0.0.7`](https://github.com/letterbeezps/stupid-kv/tree/0.0.7) | Section 0.0.7 — LZ4 快照压缩 | 透明压缩层：`CompressionMode` 两态枚举（None/Lz4），`CompressedWriter` / `CompressedReader` 封装压缩细节，LZ4 level 7 压缩算法，基于 magic number 的自动格式探测，向后兼容 0.0.6 未压缩快照 |
| [`0.0.8`](https://github.com/letterbeezps/stupid-kv/tree/0.0.8) | Section 0.0.8 — AOL 增量日志 | Append-Only Log 增量持久化：`AolMode` 三态（Never/Sync/Async）、`FsyncMode` 三态（Never/EveryAppend/Interval）、`crossbeam_deque` 无锁异步批量写、`snapshot + AOL truncate` 协同、崩溃恢复窗口从快照间隔缩小到毫秒级 |
| [`0.0.9`](https://github.com/letterbeezps/stupid-kv/tree/0.0.9) | Section 0.0.9 — Workspace 与 HTTP Server | 项目升级为 Cargo Workspace（lib + server 双 crate），基于 axum 实现 RESTful CRUD API，5 个端点 + 12 个集成测试，`Arc<Database>` 全局唯一实例语义 |
| [`0.0.10`](https://github.com/letterbeezps/stupid-kv/tree/0.0.10) | Section 0.0.10 — 事务对象池 | `crossbeam_array_queue` 有界无锁池复用 TransactionInner，`Pool::get()` 命中时 `reset()` 重置状态避免 3 次 heap 分配（HashSet / BloomFilter / BTreeMap），`reset_threshold` 条件清理 writeset 平衡 allocator 抖动，Drop 两段式退场先释放 GC counter 再回收 |

## 学习笔记

每一节学习内容都有对应的笔记文档，记录了我的学习过程和理解：

| 章节 | 笔记文档 | 标签 |
|------|----------|------|
| 基本 MVCC 事务 | [001_basic_transaction.md](docs/001_basic_transaction.md) | `0.0.1` |
| SSI 与 Bloom 过滤器 | [002_ssi_bloom_filter.md](docs/002_ssi_bloom_filter.md) | `0.0.2` |
| 运行时鲁棒性加固 | [003_runtime_hardening.md](docs/003_runtime_hardening.md) | `0.0.3` |
| 提交队列 GC | [004_commit_queue_gc.md](docs/004_commit_queue_gc.md) | `0.0.4` |
| 版本历史 GC | [005_version_history_gc.md](docs/005_version_history_gc.md) | `0.0.5` |
| 快照持久化 | [006_snapshot.md](docs/006_snapshot.md) | `0.0.6` |
| LZ4 快照压缩 | [007_lz4_compression.md](docs/007_lz4_compression.md) | `0.0.7` |
| AOL 增量日志 | [008_aol_module.md](docs/008_aol_module.md) | `0.0.8` |
| Workspace 与 HTTP Server | [009_workspace_and_http_server.md](docs/009_workspace_and_http_server.md) | `0.0.9` |
| 事务对象池 | [010_transaction_object_pool.md](docs/010_transaction_object_pool.md) | `0.0.10` |

### 番外篇

正文以外的补充材料，从不同视角把主线内容串起来：

| 番外 | 笔记文档 | 关联章节 |
|------|----------|----------|
| 从测试用例走读隔离级别 | [extras/001_isolation_tests_walkthrough.md](docs/extras/001_isolation_tests_walkthrough.md) | `0.0.1` / `0.0.2` |
| 用 PyO3 把 stupid-kv 暴露给 Python | [extras/002_python_bindings.md](docs/extras/002_python_bindings.md) | `0.0.10` 补充 |

### 计划学习的内容

这些是我接下来想学习的内容，进度可能会比较慢，也可能随时调整：

- **WAL 正式化**：将当前 AOL 从简化版升级为带检查点标记的正式 WAL，支持更精确的恢复边界定位
- **数据校验**：为 AOL 条目增加 CRC32 校验，让日志文件具备损坏检测能力
- **快照格式加固**：Magic + Version + CRC32 校验，跨架构可交换
- **条目级压缩与校验**：AOL 条目级独立压缩/校验，实现单条粒度的损坏隔离
- **HTTP 持久化配置**：为 Server 启动参数暴露 AOL/Snapshot 配置，让 HTTP API 数据真正持久化到磁盘
- **多数据库与批量操作**：在 URL 中引入 database namespace，添加 `POST /batch` 批量端点

## 安装

由于本项目处于学习阶段，**暂未发布到 crates.io**。如需在外部项目中引用，请通过 **GitHub git 依赖** 引入：

### 在外部项目中使用

```bash
# 推荐：锁定到具体 tag（可复现，不随 main 漂移）
cargo add --git https://github.com/letterbeezps/stupid-kv stupid-kv --tag 0.0.10
```

或手动在 `Cargo.toml` 中添加：

```toml
[dependencies]
stupid-kv = { git = "https://github.com/letterbeezps/stupid-kv", tag = "0.0.10" }
```

可用 tag 列表见 [学习进度](#学习进度) 章节（当前 `0.0.1` ~ `0.0.10`）。也可临时跟踪 main 分支试用最新开发版本：

```toml
stupid-kv = { git = "https://github.com/letterbeezps/stupid-kv", branch = "main" }
```

> ⚠️ 本项目仍在持续演进，主分支 API 可能不稳定。生产或长期使用请固定到具体 tag。

### 在本仓库内引用（workspace 模式）

本仓库本身是 Cargo workspace，`server/` 子 crate 通过 path 依赖直接引用根 lib：

```toml
# server/Cargo.toml
[dependencies]
stupid-kv = { path = ".." }
```

---

## 快速开始

### Lib 模式：作为库嵌入使用

```rust
use stupid_kv::Database;

fn main() {
    let db = Database::new();

    // 写入
    let mut tx = db.transaction(true);
    tx.set("key1", "value1").unwrap();
    tx.commit().unwrap();

    // 读取
    let tx = db.transaction(false);
    assert_eq!(tx.get("key1").unwrap(), Some("value1".into()));

    // 删除
    let mut tx = db.transaction(true);
    tx.del("key1").unwrap();
    tx.commit().unwrap();
}
```

```bash
# 运行示例
cargo run --example 001_basic
cargo run --example 002_ssi

# 运行单元测试
cargo test
```

### Python 模式：原生 Python 调用

通过 PyO3 绑定，可以像 `sqlite3` 一样在 Python 中直接 `import stupid_kv` 使用：

```bash
# 一次性安装 uv（已装可跳过）
# macOS / Linux：
curl -LsSf https://astral.sh/uv/install.sh | sh
# 或 macOS：
brew install uv

# 创建 env + 装 maturin + 构建扩展，一条命令搞定
cd stupid-kv-py
uv sync                                # 建 .venv + 装 maturin（dev-dep）
uv run maturin develop --release       # 编译并装到 .venv
```

之后用 `uv run python` 或 `uv run python examples/001_basic.py` 跑脚本即可，uv 会自动激活环境。

```python
import stupid_kv

db = stupid_kv.Database()

# 写
tx = db.transaction(write=True)
tx.set(b"hello", b"world")
tx.commit()

# 读（sqlite 风格的 with：正常退出自动 commit，抛异常自动 rollback）
with db.transaction(write=True) as tx:
    tx.set(b"k", b"v")

assert db.transaction(write=False).get(b"hello") == b"world"
```

**API 速览：**

| 类 / 方法 | 说明 |
|------|------|
| `stupid_kv.Database()` | 内存数据库；`with_options(opts)` / `with_persistence(opts, persist)` 用于高级配置 |
| `db.transaction(write: bool)` | 开启事务；支持 `with` 自动 commit / rollback |
| `tx.set(k, v)` / `tx.put(k, v)` / `tx.delete(k)` | 写操作；`put` 仅在 key 不存在时插入 |
| `tx.get(k)` / `tx.exists(k)` | 读操作 |
| `tx.with_snapshot_isolation()` | 切换到 SI（builder 链式） |
| `tx.with_serializable_snapshot_isolation()` | 切换到 SSI |
| `tx.commit()` / `tx.cancel()` | 显式提交 / 回滚 |

**异常体系：**

```python
except stupid_kv.KeyWriteConflict: ...    # 写-写冲突
except stupid_kv.KeyReadConflict: ...     # SSI 读-写冲突（写倾斜防护）
except stupid_kv.KeyAlreadyExists: ...    # put 到已存在 key
except stupid_kv.TxNotWritable: ...       # 只读事务上写
except OSError: ...                       # AOL / IO 失败
```

完整可运行示例见 `stupid-kv-py/examples/`（`001_basic.py`、`002_isolation.py`）。

### Bin 模式：启动 HTTP Server

```bash
# 默认端口 3000
cargo run -p server

# 自定义端口（方式一：环境变量，推荐）
PORT=8080 cargo run -p server

# 自定义端口（方式二：命令行参数）
cargo run -p server -- --port 8080

# 查看帮助
cargo run -p server -- --help
```

#### CRUD 接口

| 方法 | 路径 | 参数 | 说明 |
|------|------|------|------|
| GET | `/get` | `key` (query) | 读取键值 |
| GET | `/exists` | `key` (query) | 检查键是否存在 |
| POST | `/insert` | `key`, `value` (body) | 创建键值（已存在返回 409） |
| POST | `/update` | `key`, `value` (body) | 创建或更新键值（幂等 upsert） |
| DELETE | `/delete` | `key` (query) | 删除键值 |

#### curl 示例

```bash
# 创建键
curl -X POST http://127.0.0.1:3000/insert \
  -H 'Content-Type: application/json' \
  -d '{"key": "hello", "value": "world"}'
# => {"key":"hello","value":"world"}

# 读取键
curl http://127.0.0.1:3000/get?key=hello
# => {"key":"hello","value":"world"}

# 更新键
curl -X POST http://127.0.0.1:3000/update \
  -H 'Content-Type: application/json' \
  -d '{"key": "hello", "value": "updated"}'
# => {"key":"hello","value":"updated"}

# 检查存在
curl http://127.0.0.1:3000/exists?key=hello
# => {"key":"hello","exists":true}

# 删除键
curl -X DELETE http://127.0.0.1:3000/delete?key=hello
# => {"key":"hello","value":"updated"}

# 验证删除
curl http://127.0.0.1:3000/exists?key=hello
# => {"key":"hello","exists":false}
```

#### 运行 Server 测试

```bash
cargo test -p server
```

## 架构概览（学习笔记）

这是我目前理解的数据库架构，可能存在理解不准确的地方：

```
                     ┌──────────────────────────────────────┐
                     │         Database                     │
                     │  Arc<Inner>                          │
                     │                                      │
                     │  Oracle ── global clock              │
                     │  commit queue                        │
                     │  merge queue                         │
                     │  datastore                           │
                     │  counter_by_commit ── active tx      │
                     │  counter_by_oracle ── active ver     │
                     │  cleanup worker    ── GC thread      │
                     │  gc worker         ── GC thread      │
                     │  persistence       ── snapshot + AOL│
                     │  compression       ── LZ4           │
                     │  pool              ── tx pool reuse │
                     └───────────┬──────────────────────────┘
                                 │ shared ref
                                 ▼
                     ┌──────────────────────────┐
                     │      Transaction         │
                     │  commit: snapshot id     │
                     │  version: timestamp      │
                     │  readset: read keys      │
                     │  writeset: local mods    │
                     │  get/set/del             │
                     └──────────────────────────┘

                     ┌──────────────────────────┐
                     │      Persistence         │
                     │  snapshot() ── disk dump  │
                     │  load() ── restore        │
                     │  AOL append ── WAL       │
                     │  AOL truncate ── reclaim │
                     │  snapshot worker thread   │
                     │  append worker thread     │
                     │  fsync worker thread      │
                     └──────────────────────────┘

                     ┌──────────────────────────┐
                     │      Compression         │
                     │  CompressedWriter ── LZ4 │
                     │  CompressedReader ── det │
                     │  CompressionMode cfg     │
                     └──────────────────────────┘
```

## 项目结构

```
stupid-kv/
├── src/
│   ├── db/           # 数据库入口与核心状态（含后台 cleanup worker）
│   ├── oracle/       # Oracle 全局时间戳分配器（含后台 resync）
│   ├── bloom/        # Bloom 过滤器
│   ├── tx/           # 事务实现（含自适应退避、活跃事务引用计数）
│   ├── versions/     # 多版本数据管理
│   ├── queue/          # 提交队列与合并队列
│   ├── kv/             # key/value 类型转换
│   ├── options/        # 运行时参数入口 DatabaseOptions + PersistenceOptions
│   ├── persistence/    # 快照持久化 + AOL 增量日志模块（snapshot/load/后台线程/AOL append/truncate）
│   ├── compression/    # LZ4 压缩模块（CompressedWriter/Reader + auto-detect）
│   ├── pool/           # 事务对象池（crossbeam ArrayQueue 无锁复用 TransactionInner）
│   ├── error/          # 错误类型定义（TxError + PersistenceError）
│   └── lib.rs          # crate 根模块（re-export + mod 声明）
├── server/          # axum HTTP Server（bin crate，提供 CRUD REST API）
├── examples/       # 示例代码
├── docs/           # 学习笔记
└── Cargo.toml
```

## License

本项目为个人学习项目，仅供学习研究使用。
