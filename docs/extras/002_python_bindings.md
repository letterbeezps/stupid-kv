# 番外篇 · 二：用 PyO3 把 stupid-kv 暴露给 Python

> 本文是 stupid-kv 系列教程的番外篇，配套阅读文件：
> - [`stupid-kv-py/`](../stupid-kv-py/)：本次新增的 workspace member
> - [`stupid-kv-py/src/lib.rs`](../stupid-kv-py/src/lib.rs)：PyO3 wrapper 主文件
> - [`stupid-kv-py/examples/`](../stupid-kv-py/examples/)：Python 示例
> - [`src/db/db.rs`](../src/db/db.rs) / [`src/tx/mod.rs`](../src/tx/mod.rs) / [`src/lib.rs`](../src/lib.rs)：为了让绑定能引用到 `Transaction` 类型而做的最小上游改动

主线教程从 0.0.1 到 0.0.10 把 Rust 端的能力一步步推到位，但所有调用者都是同一个 Rust 进程里的另一个模块。本文换一个视角——**把这些能力如何再包一层、让 Python 进程也能直接 `import stupid_kv` 用上**这件事拆开讲清楚。希望读者看完之后，能判断「我的下一个 Rust 项目该不该上 PyO3」「上的时候有哪些坑要提前避」。

---

## 1. 为什么需要 Python 绑定

主线教程完成后，stupid-kv 已经有两种对外暴露方式：

| 方式 | 形态 | 痛点 |
|------|------|------|
| Rust lib | `use stupid_kv::Database` | 调用方必须是同一个 workspace / path dep 的 Rust 项目 |
| HTTP server | `axum` 起的 REST API | 需要起进程、`requests` 调用、序列化开销 |

Python 数据科学 / 脚本生态几乎所有人都在用，但 HTTP 那条路对脚本场景太重（每次调用都要序列化 + 网络往返）。理想状态是：

```python
import stupid_kv                                # 像 sqlite3 一样
db = stupid_kv.Database()                      # 一行启动
tx = db.transaction(write=True)
tx.set(b"hello", b"world")
tx.commit()
```

这就要靠 **Rust ↔ Python 的 FFI 绑定**。

---

## 2. 技术选型：PyO3 + maturin

Rust → Python 绑定在 2026 年有几条路：

| 方案 | 现状 | 选不选 |
|------|------|--------|
| **PyO3 + maturin** | 业界事实标准，polars / orjson / ruff / pydantic-core / cryptography 全用 | ✅ |
| Mozilla uniffi | IDL 风格、多语言导出 | 多语言场景才考虑 |
| rust-cpython | 维护停滞 | ❌ |
| 裸 ctypes / cbindgen | 手工、容易写错 | 极少数情况 |

PyO3 拿下 90%+ 市场的原因：
- `#[pyclass]` / `#[pymethods]` derive 宏，编译期类型检查
- `Python::allow_threads` 一行释放 GIL
- maturin 把 wheel 构建 + abi3 + 多 Python 版本支持打包
- `create_exception!` 让 Rust 错误类型 → Python 异常一气呵成

本文所有代码都用 PyO3 0.21 + maturin 1.x。

---

## 3. workspace 结构

主线 0.0.9 已经把项目升级成 Cargo workspace。这次只是再加一个 member：

```
stupid-kv/
├── Cargo.toml                  # workspace.members += "stupid-kv-py"
├── src/...                     # lib crate（不改）
├── server/...                  # server crate（不改）
└── stupid-kv-py/               # NEW
    ├── Cargo.toml              # pyo3 + stupid-kv（path dep）
    ├── pyproject.toml          # maturin 构建配置
    ├── src/lib.rs              # PyO3 wrapper
    └── examples/               # 001_basic.py / 002_isolation.py
```

**关键点：`[lib] name = "stupid_kv"` + `crate-type = ["cdylib"]`**

```toml
[lib]
name = "stupid_kv"              # ← Python import 用的名字
crate-type = ["cdylib"]

[dependencies]
stupid-kv = { path = "../" }    # ← 引用根 lib crate
pyo3 = { version = "0.21", features = ["extension-module", "abi3-py38"] }
```

- **crate name** 是 `stupid-kv-py`（PyPI 包名，跟主 crate `stupid-kv` 错开）
- **lib name** 是 `stupid_kv`（maturin 读这个作为 Python 模块名 → `import stupid_kv`）
- `abi3-py38`：一份 wheel 跨 Python 3.8~3.14 全版本，体积小、构建快

---

## 4. 上游改动：把内部类型变成公共类型

主线教程里 `Transaction` 是这么存在的：

```
src/tx/transaction.rs:   pub struct Transaction           ← 自己是 pub
src/tx/mod.rs:           mod transaction;                 ← 但父模块 private
                          pub(crate) use transaction::*;  ← 只在 crate 内可见
src/db/db.rs:            use crate::tx::{Transaction};    ← 私下 import
src/db/db.rs:            pub fn transaction(...) -> Transaction   ← 返回值类型对外部不可命名
```

外部 crate 想 `let tx: stupid_kv::Transaction = ...` —— **做不到**，因为 `Transaction` 这个名字从来没被 re-export 到 crate root。

绑定之前必须把这层 visibility 打通。三处最小改动：

| 文件 | 改动 | 目的 |
|------|------|------|
| `src/tx/mod.rs` | `pub(crate) use transaction::*;` → `pub use transaction::Transaction;` | 显式把 `Transaction` 公开 re-export 到 `tx` 模块 |
| `src/lib.rs` | （可选）`pub use self::tx::Transaction;` | 让 `stupid_kv::Transaction` 这个路径可用 |
| `src/options/persistence_options.rs` | `#[derive(Clone)]` | PyO3 包装类需要 Clone 值语义 |

> 主线教程里写「`Transaction` 是不带生命周期的 owned 类型」这件事到这里变成了关键优势：不用 Arc 化、不用 ouroboros、不用生命周期体操，PyO3 直接吃就行。

---

## 5. PyO3 wrapper 实现

`stupid-kv-py/src/lib.rs` 全文 ~350 行，按职责切成四块：Database / Transaction / DatabaseOptions / PersistenceOptions + 一组异常。

### 5.1 Database：最朴素的封装

```rust
#[pyclass(name = "Database")]
pub struct PyDatabase {
    inner: kv::Database,
}

#[pymethods]
impl PyDatabase {
    #[new]
    fn new() -> Self { Self { inner: kv::Database::new() } }

    fn transaction(&self, write: bool) -> PyTransaction {
        PyTransaction { inner: Some(self.inner.transaction(write)) }
    }
}
```

`Database` 本身没有生命周期，没有复杂的内部状态，PyO3 的 `#[pyclass]` + `#[new]` 就够了。

### 5.2 Transaction：Option 包一层用来表达「已关闭」

```rust
#[pyclass(name = "Transaction")]
pub struct PyTransaction {
    inner: Option<kv::Transaction>,    // None = closed
}
```

把 `Transaction` 包在 `Option` 里，是为了让 Python 端在 `commit` / `cancel` 之后还能调方法（比如不小心又调一次）时抛 `TxClosed` 而不是 panic。Rust 端 `Drop` 在 Python GC 时触发，回收 TransactionInner 到对象池——这条对用户不可见。

### 5.3 builder 模式：`PyRefMut.into()`

主线教程里 builder 长这样：

```rust
pub fn with_snapshot_isolation(mut self) -> Self {
    self.inner.as_mut().unwrap().mode = IsolationLevel::SnapshotIsolation;
    self
}
```

消费 `self`、返回 `Self`，链式调用很顺手。但 Python 端的 PyO3 method 拿不到 `self` 的所有权，只能拿到 `PyRef` / `PyRefMut`。**两个方案**：

**方案 A（不推荐）：每次 `Clone` 内部状态**

事务内部包含 `HashSet` / `BTreeMap` / `BloomFilter`，clone 开销不小。

**方案 B（采用）：`std::mem::take` + `PyRefMut.into()`**

```rust
fn with_snapshot_isolation(mut slf: PyRefMut<'_, Self>) -> Py<Self> {
    let taken = std::mem::take(&mut slf.inner);          // 把 Option 掏空
    slf.inner = taken.map(|t| t.with_snapshot_isolation());  // 调 builder，再塞回去
    slf.into()                                            // PyRefMut → Py<Self>
}
```

`slf.into()` 把 `PyRefMut<'_, Self>` 转成 `Py<Self>`，让 Python 端继续持有这个对象。链式调用在 Python 侧等价于：

```python
db.transaction(write=True).with_snapshot_isolation().set(b"k", b"v").commit()
```

### 5.4 `bytes::Bytes` ↔ `PyBytes`

Rust 端 `Transaction::get` 返回 `Result<Option<Bytes>, Error>`。`Bytes` 是 `bytes::Bytes`，Arc 共享的字节缓冲。Python 端对应 `bytes`。两者边界转换：

**Rust → Python：**
```rust
fn get(&self, py: Python<'_>, key: &[u8]) -> PyResult<Option<Py<PyBytes>>> {
    let inner = self.inner.as_ref().ok_or(...)?;
    let result = py.allow_threads(|| inner.get(key)).map_err(tx_err_to_py)?;
    Ok(result.map(|b| PyBytes::new_bound(py, b.as_ref()).into()))   // ← 关键
}
```

**`b.as_ref()`**：`Bytes` deref 到 `&[u8]`，PyO3 用它构造 Python bytes。代价是 **一次内存拷贝**（`Bytes::to_vec()` 内部），后续可以优化成 zero-copy，目前不阻塞功能。

**Python → Rust：** 直接用 `&[u8]` 抽取器，PyO3 把 `bytes` / `bytearray` / `memoryview` 都接住：

```rust
fn set(&mut self, py: Python<'_>, key: &[u8], value: &[u8]) -> PyResult<()> { ... }
```

### 5.5 释放 GIL：`Python::allow_threads`

`commit()` 内部会做 commit-queue 扫描、Bloom 比对、AOL 同步写盘（如果开了持久化），可能阻塞几十毫秒。Python 单线程不要紧，多线程会卡住其他协程。PyO3 的 `Python::allow_threads` 一行解决：

```rust
fn commit(&mut self, py: Python<'_>) -> PyResult<()> {
    let inner = self.inner.as_mut().ok_or_else(|| TxClosed::new_err("..."))?;
    py.allow_threads(|| inner.commit()).map_err(tx_err_to_py)
}
```

要点：
- `inner.commit()` 是 `Send` 的闭包（`Transaction` 的字段全是 `Send + Sync`），`allow_threads` 编译器层校验
- 返回值从闭包里透传出来，不丢错误
- 所有「可能慢」的方法（commit / cancel / set / put / delete / get / exists）都加这一行

### 5.6 错误映射：`create_exception!`

```rust
pyo3::create_exception!(stupid_kv, StupidKvError, PyException);
pyo3::create_exception!(stupid_kv, KeyWriteConflict, StupidKvError);
pyo3::create_exception!(stupid_kv, KeyReadConflict, StupidKvError);
pyo3::create_exception!(stupid_kv, KeyAlreadyExists, StupidKvError);
pyo3::create_exception!(stupid_kv, TxClosed, StupidKvError);
pyo3::create_exception!(stupid_kv, TxNotWritable, StupidKvError);

fn tx_err_to_py(e: kv::error::Error) -> PyErr {
    use kv::error::Error;
    match e {
        Error::KeyWriteConflict => KeyWriteConflict::new_err("write-write conflict detected"),
        Error::KeyReadConflict => KeyReadConflict::new_err("read-write conflict (SSI)"),
        Error::KeyAlreadyExists => KeyAlreadyExists::new_err("key already exists"),
        Error::TxClosed => TxClosed::new_err("transaction is closed"),
        Error::TxNotWritable => TxNotWritable::new_err("transaction is read-only"),
        Error::TxCommitNotPersisted(p) => PyOSError::new_err(p.to_string()),
    }
}
```

Python 端：

```python
except stupid_kv.KeyWriteConflict: ...      # 写-写冲突
except stupid_kv.KeyReadConflict: ...       # SSI 读-写冲突
except stupid_kv.KeyAlreadyExists: ...      # put 撞已存在 key
except stupid_kv.TxNotWritable: ...         # 只读事务上写
except OSError: ...                          # AOL / 持久化 IO 错误
```

异常继承关系是 `KeyWriteConflict -> StupidKvError -> Exception`，可以 `except stupid_kv.StupidKvError:` 一把抓。

> **坑点**：`create_exception!(stupid_kv, ...)` 的第一个参数是「Python 模块名」，会被展开成当前 crate 里一个叫 `stupid_kv` 的模块。这跟外部 crate `stupid-kv` 同名了，于是 `stupid_kv::Database` 会出现「ambiguous」错误。解决：`extern crate stupid_kv as kv;` 在 wrapper crate 顶部重命名，wrapper 里所有引用走 `kv::Database` / `kv::Transaction` / `kv::error::Error`。Python 端的 import 名字不变（还是 `stupid_kv`）。

### 5.7 上下文管理器：sqlite 风格

```rust
fn __exit__(
    &mut self,
    py: Python<'_>,
    exc_type: &Bound<'_, PyAny>,
    _exc_value: &Bound<'_, PyAny>,
    _traceback: &Bound<'_, PyAny>,
) -> PyResult<bool> {
    if self.closed() {
        return Ok(false);
    }
    if exc_type.is_none() {
        // 正常退出：自动 commit
        let _ = py.allow_threads(|| self.inner.as_mut().unwrap().commit());
    } else {
        // 异常逃逸：自动 rollback
        let _ = py.allow_threads(|| self.inner.as_mut().unwrap().cancel());
    }
    Ok(false)
}
```

跟 sqlite3 的 `with conn:` 一致：正常退出 → commit，异常退出 → rollback。Python 端写起来很自然：

```python
with db.transaction(write=True) as tx:
    tx.set(b"k", b"v")
# 这里已经是 commit 完的状态
```

如果用户在 with 块里已经显式 commit 过，`closed()` 返回 true，`__exit__` 直接跳过，不会重提交。

---

## 6. API 一览（Rust → Python）

| Rust | Python |
|------|--------|
| `Database::new()` | `stupid_kv.Database()` |
| `Database::new_with_options(opts)` | `stupid_kv.Database.with_options(opts)` |
| `Database::new_with_persistence(opts, persist)` | `stupid_kv.Database.with_persistence(opts, persist)` |
| `db.transaction(true)` | `db.transaction(write=True)` |
| `tx.set(k, v)` | `tx.set(k, v)`（参数都是 `bytes`） |
| `tx.put(k, v)` | `tx.put(k, v)`（撞 KeyAlreadyExists） |
| `tx.del(k)` | `tx.delete(k)`（避免 Python 关键字） |
| `tx.get(k) -> Option<Bytes>` | `tx.get(k) -> Optional[bytes]` |
| `tx.exists(k) -> bool` | `tx.exists(k) -> bool` |
| `tx.with_snapshot_isolation()` | `tx.with_snapshot_isolation()` |
| `tx.with_serializable_snapshot_isolation()` | `tx.with_serializable_snapshot_isolation()` |
| `tx.commit()` | `tx.commit()`（抛异常代替 `Result`） |
| `tx.cancel()` | `tx.cancel()` |
| `tx.version() -> u64` | `tx.version() -> int` |
| `tx.closed() -> bool` | `tx.closed() -> bool` |
| — | `tx.__enter__/__exit__`（auto commit / rollback） |

---

## 7. 构建/安装工作流

maturin 负责把 Rust 编译产物装进当前 Python 环境。`stupid-kv-py/pyproject.toml` 里 maturin 是 build-backend。

**第一次安装（开发者或用户都适用）：**

```bash
cd stupid-kv-py
uv sync                                # 创建 .venv + 装 maturin（dev-dep）
uv run maturin develop --release       # editable install
```

Rust 源文件改了之后：

```bash
uv run maturin develop --release       # 增量重编
```

打 wheel 分发（以后上 PyPI）：

```bash
uv run maturin build --release
ls target/wheels/                       # stupid_kv-0.0.10-cp38-abi3-*.whl
```

> **`uv sync` 不只是装包**：因为 pyproject.toml 的 `build-backend = "maturin"`，uv sync 触发构建，会把 wheel 装进 `.venv`。配合 `[dependency-groups].dev`（PEP 735）把 maturin 声明为开发依赖，整个 dev-loop 只需要「uv sync」+「uv run maturin develop」两条命令。

---

## 8. 验证

构建之后跑两组测试：

**Rust 端（确保 wrapper 没破坏 lib）：**
```bash
cargo test --all
```

**Python 端（确保 import + API 全通）：**
```python
import stupid_kv

db = stupid_kv.Database()
tx = db.transaction(write=True)
tx.set(b"hello", b"world")
tx.commit()
assert db.transaction(write=False).get(b"hello") == b"world"

# builder 链
tx = db.transaction(write=True).with_snapshot_isolation()
tx.set(b"k", b"v"); tx.commit()

# 上下文管理器
with db.transaction(write=True) as tx:
    tx.set(b"auto", b"close")

# 冲突检测
db2 = stupid_kv.Database()
tx1 = db2.transaction(write=True)
tx2 = db2.transaction(write=True)
tx1.set(b"k", b"v1"); tx2.set(b"k", b"v2")
tx1.commit()
try: tx2.commit()
except stupid_kv.KeyWriteConflict: print("OK")
```

完整可运行示例见 [`stupid-kv-py/examples/`](../stupid-kv-py/examples/)。

---

## 9. 限制 / 已知坑

| 项 | 现状 | 影响 |
|----|------|------|
| `tx.get()` 返回时 `Bytes::to_vec()` 一次拷贝 | 每次 ~O(n) | 大 value 时有性能开销，后续可优化 zero-copy |
| `Database.__del__` 阻塞 ~1.5s | Drop join 后台线程 | Python GC 触发时有感知延迟；显式 `del db` 也会等 |
| `pool_size` 等选项只能设整数 | `DatabaseOptions` 没暴露 Duration 字段 | ms 精度足够，但 Rust 端 `Duration::from_nanos` 用不到 |
| 没有暴露 `AolMode` / `FsyncMode` 等 enum | 用便捷 builder 代替 | 自定义组合得改 wrapper 加 enum 映射 |
| PyO3 0.21 ABI | 跟 PyO3 0.20 不兼容 | wheel 升级 PyO3 时必须重 build |
| `readme = "../README.md"` | maturin 会把这文件打进 wheel | 避免路径里有上级目录以外的东西 |

---

## 10. 小结

把这篇对照主线教程读一遍，可以从「使用者视角」把 Rust → Python 的桥梁看清楚：

- **选型**：PyO3 + maturin 在 2026 年是事实标准，没特殊原因就用它。
- **workspace 加 member** 而不是 feature flag：把 binding 完全独立成一个 crate，主 lib 保持纯 Rust 用户的清爽。
- **上游改动要小且语义无害**：`pub use Transaction`、`#[derive(Clone)]` 都是「本来就该有」的可见性，PyO3 只是顺便吃了红利。
- **Transaction 是 owned** 是命中注定的好运：不用 Arc 化、不用生命周期体操，wrapper 几乎 1:1 翻译。
- **builder 用 `PyRefMut.into()` 模式**：不 clone 内部状态，符合 Rust ownership 语义。
- **每个阻塞方法都 `allow_threads`**：Python 多线程不会卡。
- **create_exception! + `extern crate ... as kv`** 解决命名冲突：这两个坑文档少、调试成本高，提前避。
- **上下文管理器对齐 sqlite 语义**：用户心智模型可以无缝迁移。

下一次主线教程结束之后如果有新的对外暴露需求（比如 Go binding、Node binding），把这一篇的模板套上去就能再写一篇番外。
