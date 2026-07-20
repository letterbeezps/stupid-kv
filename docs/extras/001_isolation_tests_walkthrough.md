# 番外篇 · 一：从测试用例走读隔离级别

> 本文是 stupid-kv 系列教程的番外篇，配套阅读文件：
> - `tests/isolations.rs`：本次新增的集成测试
> - [`docs/001_basic_transaction.md`](../001_basic_transaction.md)：MVCC 与 SI 的实现原理
> - [`docs/002_ssi_bloom_filter.md`](../002_ssi_bloom_filter.md)：SSI 与 Bloom 过滤器加速冲突检测

主线教程从"实现视角"讲清了 SI 和 SSI 的构造：writeset、readset、commit queue、Bloom 过滤器、冲突检测三层快速排除。本文换一个视角——**从测试用例出发**，把这些机制串成一个个可以运行的、有业务语义的小场景，帮助读者建立"看到代码 → 预测行为 → 用测试验证"的直觉。

---

## 1. 测试组织

`tests/isolations.rs` 按隔离级别与关注点分成三组：

| 分组 | 用例数 | 关注点 |
|------|--------|--------|
| Snapshot Isolation | 2 | SI 的两条基本承诺：快照一致读、不相交写并发 |
| Serializable Snapshot Isolation | 7 | SSI 相较 SI 新增的读依赖追踪与写偏斜防护 |
| Mixed Isolation Level | 3 | SI 与 SSI 的行为对比，以及经典 lost-update 场景 |

所有用例都通过 `Database::new()` 直接构造内存数据库，不涉及持久化。事务通过 `db.transaction(true|false)` 创建；SSI 通过链式的 `.with_serializable_snapshot_isolation()` 显式开启，默认仍是 SI。

---

## 2. Snapshot Isolation：两条基本承诺

### 2.1 一致快照读：`snapshot_isolation_read_sees_consistent_snapshot`

**测试要点**：一个只读事务在其创建时刻观察到的数据视图，不会被随后并发提交的写事务污染。

**模拟场景**：报表事务读取多个 key 时，业务写事务同时在修改这些 key。SI 承诺报表看到的是"某一时刻的静态照片"。

```rust
// 初始化：key1=value1, key2=value2
let mut tx = db.transaction(true);
tx.set("key1", "value1").unwrap();
tx.set("key2", "value2").unwrap();
tx.commit().unwrap();

// 只读事务开启（快照点 = 此刻的 Oracle timestamp）
let mut read_tx = db.transaction(false);

// 一个并发写事务把 key1、key2 都改掉并提交
let mut write_tx = db.transaction(true);
write_tx.set("key1", "modify1").unwrap();
write_tx.set("key2", "modify2").unwrap();
write_tx.commit().unwrap();

// 只读事务仍然看到原始值 —— 这是 SI 的核心承诺
assert_eq!(read_tx.get("key1").unwrap(), Some(Bytes::from("value1")));
assert_eq!(read_tx.get("key2").unwrap(), Some(Bytes::from("value2")));
```

**为什么成立**：`read_tx.version` 是它创建时 Oracle 分配的时间戳 `V_r`。`write_tx` 提交后，`datastore["key1"]` 的 `Versions` 列表新增了一个 `version = V_w > V_r` 的条目。`fetch_version(V_r)` 在有序版本列表里找 `≤ V_r` 的最大版本，跳过 `V_w`，命中原始版本。对应实现：`src/versions/versions.rs` 的 `fetch_version`。

**收尾验证**：只读事务取消后，开一个新事务，可以看到 `write_tx` 的修改——证明"看不到"仅限于快照点之前的事务。

### 2.2 不相交写并发：`snapshot_isolation_allows_concurrent_writes_to_different_keys`

**测试要点**：SI 只在"两个并发事务修改了相同 key"时才判定冲突；操作不同 key 的并发写事务应当都能提交成功。

**模拟场景**：多个用户各自更新自己的资料（不同的 key），系统不应引入不必要的串行化。

```rust
let mut tx1 = db.transaction(true);
let mut tx2 = db.transaction(true);

tx1.set("key1", "tx1_value").unwrap();
tx2.set("key2", "tx2_value").unwrap();

assert!(tx1.commit().is_ok());
assert!(tx2.commit().is_ok()); // 不同 key，无冲突
```

**为什么成立**：`is_disjoint_writeset_bloom` 的第一层就是 key range 快速排除——`tx1.writeset` 的 max_key 是 `"key1"`，`tx2.writeset` 的 min_key 是 `"key2"`，`"key1" < "key2"` 直接判定不相交。对应实现：`src/queue/commit.rs`。

---

## 3. Serializable Snapshot Isolation：读依赖追踪的力量

SSI 与 SI 的核心差异只有一句话：**SSI 额外追踪 readset**，并在提交时检查"我读过的 key 有没有被并发事务改掉"。以下七个用例，覆盖了这条规则的各种触发面。

### 3.1 写-写冲突（同一 key）：`ssi_detects_write_write_confict_on_same_key`

**测试要点**：这一条其实和 SI 语义相同，但用来确认 SSI 没有回归掉基础的写冲突检测。

**模拟场景**：两个客户端并发把同一条记录改成不同的值——经典的 lost update。

```rust
let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
let mut tx2 = db.transaction(true).with_serializable_snapshot_isolation();

tx1.set("key", "value1").unwrap();
tx2.set("key", "value2").unwrap();

assert!(tx1.commit().is_ok());           // First-Committer-Wins
assert!(tx2.commit().is_err());          // KeyWriteConflict

// 最终值来自第一个提交的事务
let mut verify = db.transaction(false);
assert_eq!(verify.get("key").unwrap(), Some(Bytes::from("value1")));
```

**触发路径**：`tx2.commit()` 时扫描 commit queue，命中 `tx1` 的记录，Bloom 命中 → 精确检测 → 交集非空 → 返回 `KeyWriteConflict`。

### 3.2 读-写冲突（写偏斜的最小样例）：`ssi_detects_read_write_conflict`

**测试要点**：这是 SSI **相对于 SI 的关键增益**。tx1 读了某个 key 用于决策，之后 tx2 修改了这个 key 并先行提交；即使 tx1 自己动的是完全不同的 key，也应当被判定失败。

**模拟场景**：
- tx1：读 `key1` 决定 `other_key` 要写什么（"如果 X 满足条件，则修改 Y"的模式）
- tx2：修改 `key1`
- 如果 tx1 允许提交，就相当于 tx1 用**一个已经过期的观察值**做出了写决策——正是写偏斜。

```rust
let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
let a = tx1.get("key1").unwrap();        // 读入 readset
assert_eq!(a, Some(Bytes::from("value1")));

let mut tx2 = db.transaction(true).with_serializable_snapshot_isolation();
tx2.set("key1", "modified").unwrap();
assert!(tx2.commit().is_ok());           // tx2 先提交

tx1.set("other_key", "value").unwrap();  // tx1 修改另一个 key
assert!(tx1.commit().is_err());          // KeyReadConflict
```

**触发路径**：tx1 的 `readset = {"key1"}`，Bloom 中打上 `key1` 的位。tx1 提交时扫描到 tx2 的 commit，先做 `is_disjoint_writeset_bloom`（`{"other_key"}` vs `{"key1"}`，无交集，通过），随后做 `is_disjoint_readset_bloom`（tx2.writeset `{"key1"}` vs tx1.readset `{"key1"}`，命中）→ 返回 `KeyReadConflict`。对应实现见 `src/tx/transaction_inner.rs`。

### 3.3 不相交写：SSI 也不误报：`ssi_isoloation_concurrent_writes_to_disjoint_keys`

**测试要点**：SSI 引入了额外的读集检测，但**不应该**让原本合法的"操作不同 key"变得不能并发。

```rust
let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
let mut tx2 = db.transaction(true).with_serializable_snapshot_isolation();

tx1.set("key_a", "value_a").unwrap();
tx2.set("key_b", "value_b").unwrap();

assert!(tx1.commit().is_ok());
assert!(tx2.commit().is_ok());
```

**为什么成立**：两个事务的 writeset 不相交（key range 一层就排除掉），且两者都没有 `get`/`exists` 调用，readset 都为空——`is_disjoint_readset_bloom` 因 `bloom.is_empty()` 直接返回 true。

### 3.4 幻读的最小形态：`ssi_read_on_non_existent_key_then_concurrent_insert`

**测试要点**：**"读到不存在"这件事本身也算读**。tx1 读 `empty` 得到 `None`（意即"我看到 empty 不存在"），如果之后 tx2 插入了 `empty`，tx1 的决策依据就已经过期。

**模拟场景**：用户名/邮箱查重。"我先查一下这个用户名有没有人用，没人用就注册"——如果不追踪不存在的读，会出现两个人同时注册同一个名字的写偏斜。

```rust
let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
assert!(tx1.get("empty").unwrap().is_none()); // 读到 None 仍进 readset

let mut tx2 = db.transaction(true).with_serializable_snapshot_isolation();
tx2.set("empty", "value").unwrap();
assert!(tx2.commit().is_ok());

tx1.set("other", "data").unwrap();
assert!(tx1.commit().is_err()); // KeyReadConflict
```

**实现关键**：读取路径无论 `fetch_in_datastore` 返回 `Some` 还是 `None`，SSI 都会把 lookup key 插入 readset。这条对应 `src/tx/transaction_inner.rs` 中 `get`/`exists` 内的 readset 追踪分支——判断的是"是否走了 datastore 查询路径"，而不是"是否读到了值"。

### 3.5 `exists` 也建立读依赖：`ssi_exists_check_creates_read_dependency`

**测试要点**：把上一个用例的 `get` 换成 `exists`——语义相同，也应形成读依赖。

**模拟场景**：把 3.4 的用户名查重换成"看看这个 key 存不存在"，比如唯一约束的最简实现。

```rust
let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
assert!(!tx1.exists("key").unwrap()); // exists 也追踪

let mut tx2 = db.transaction(true).with_serializable_snapshot_isolation();
tx2.set("key", "value").unwrap();
assert!(tx2.commit().is_ok());

tx1.set("other", "data").unwrap();
assert!(tx1.commit().is_err());
```

**为什么和 3.4 是同一个证明**：`exists` 和 `get` 在实现上共享 readset 追踪代码——两者都是"从 datastore 读一次"的谓词。这条用例其实是"接口面"的回归测试。

### 3.6 删除也是修改：`ssi_delete_conflict`

**测试要点**：SSI 的写-读冲突不区分"值变化"和"删除"。tombstone（`None` 版本）算作一次修改，任何读过被删 key 的事务都应被拒绝。

**模拟场景**：读了库存决定要扣减；与此同时另一个事务清理了这条记录。基于"我看到过存货"做出的写决策必须被拒绝。

```rust
let mut tx = db.transaction(true);
tx.set("key", "value").unwrap();
tx.commit().unwrap();

let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
assert!(tx1.get("key").unwrap().is_some()); // readset += {"key"}

let mut tx2 = db.transaction(true).with_serializable_snapshot_isolation();
tx2.del("key").unwrap();
assert!(tx2.commit().is_ok());              // writeset = {"key": None}

tx1.set("key", "new_value").unwrap();
assert!(tx1.commit().is_err());             // KeyReadConflict
```

**实现关键**：`writeset` 的类型是 `BTreeMap<Bytes, Option<Bytes>>`，`del` 存的是 `None`。冲突检测比较的是**是否出现在同一个 key 集合中**，不看 value——所以删除与更新在冲突判定上等价。

### 3.7 多读者一写者：`ssi_multiple_readers_one_writer`

**测试要点**：多个事务同时读一个 key 都进入各自的 readset，其中一个升级为写者。第一个提交成功后，其他读者若继续做任何写操作，都会因读依赖被拒。

**模拟场景**：多个客户端"读-改-写"同一份共享状态，SSI 让其中最多一个赢。

```rust
let mut tx = db.transaction(true);
tx.set("key", "initial").unwrap();
tx.commit().unwrap();

let mut reader1 = db.transaction(true).with_serializable_snapshot_isolation();
let mut reader2 = db.transaction(true).with_serializable_snapshot_isolation();

let _ = reader1.get("key").unwrap();  // 都读到 initial
let _ = reader2.get("key").unwrap();

reader1.set("key", "modified").unwrap();
assert!(reader1.commit().is_ok());     // 先提交者赢

reader2.set("other", "value").unwrap(); // 即使 reader2 改的是别的 key
assert!(reader2.commit().is_err());     // 也会因 readset 命中而失败
```

**注意 reader2 的写**：即使写的是 `"other"` 而非 `"key"`，reader2 也过不了——因为它读过 `"key"`，而 reader1 的 writeset 包含 `"key"`。这正是 SSI 阻止写偏斜的核心。

---

## 4. 隔离级别对比与 lost-update

### 4.1 SI 允许写偏斜：`si_mode_allows_read_write_anomaly`

**测试要点**：把 3.2 的 SSI 换成默认 SI，同一操作序列应当能全部提交成功——因为 SI 不追踪 readset。这是"SSI 之所以必要"的反证。

```rust
let mut tx1 = db.transaction(true); // 默认 SI
let _ = tx1.get("key").unwrap();

let mut tx2 = db.transaction(true);
tx2.set("key", "modified").unwrap();
assert!(tx2.commit().is_ok());

tx1.set("other", "value").unwrap();
assert!(tx1.commit().is_ok()); // SI 放行 —— 潜在的写偏斜
```

**结论**：`SI + 只做 disjoint-writeset 检测 = 不能防写偏斜`。这也是主线文档 §7.1 的实证。

### 4.2 SSI 检测幻读：`ssi_detects_phantom_via_read_tracking`

这个用例与 3.4 是**同一个证明的不同变量命名**——SSI 下"读到不存在 → 之后被并发插入"会被 readset 追踪捕获。保留它是为了让"通过 read tracking 检测幻读"这条语义在测试文件里有一个直呼其名的入口。

```rust
let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
let mut tx2 = db.transaction(true).with_serializable_snapshot_isolation();

assert!(tx1.get("key").unwrap().is_none()); // readset += {"key"}
tx2.set("key", "value").unwrap();
assert!(tx2.commit().is_ok());

tx1.set("other", "data").unwrap();
assert!(tx1.commit().is_err()); // KeyReadConflict
```

若追求最小测试集，可将 3.4 与本用例合并；保留两份则相当于对同一机制做了两次断言，成本低、抗回归能力略强。

### 4.3 Lost update 的防护：`concurrent_counter_increment_conflict`

**测试要点**：SSI 下"读—计算—写"同一个 key 的经典场景（计数器自增）能被安全串行化。

**模拟场景**：两个客户端同时读到 counter = 0，各自计算 0+1=1 并写回。若没有冲突检测，结果会是 1 而不是 2——这就是 lost update。SSI 保证这种场景下**至少一个事务失败并被要求重试**。

```rust
let mut tx = db.transaction(true);
tx.set("counter", "0").unwrap();
tx.commit().unwrap();

let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
let mut tx2 = db.transaction(true).with_serializable_snapshot_isolation();

let val1 = tx1.get("counter").unwrap().unwrap();
let val2 = tx2.get("counter").unwrap().unwrap();
assert_eq!(val1.as_ref(), b"0");
assert_eq!(val2.as_ref(), b"0");

tx1.set("counter", "1").unwrap();
tx2.set("counter", "1").unwrap();

assert!(tx1.commit().is_ok());
assert!(tx2.commit().is_err()); // lost update 被拦下

let mut verify = db.transaction(false);
assert_eq!(verify.get("counter").unwrap(), Some(Bytes::from("1")));
```

**触发路径**：tx2 提交时有**两条路径**都能命中冲突——
1. `is_disjoint_writeset_bloom`：两者都写 `counter`，写-写冲突 → `KeyWriteConflict`。
2. `is_disjoint_readset_bloom`：tx2 读过 `counter`，tx1 写过 `counter`，读-写冲突 → `KeyReadConflict`。

实际返回的是先命中的那一个（当前实现里写冲突检查在前），但两者足以说明：**SSI 对这种"读—改—写"模式提供了双重保险**。

---

## 5. 用例覆盖的机制矩阵

把主线两篇文档中的机制点，与本次测试之间做一次对照，方便读者查漏补缺：

| 机制 | 对应实现 | 覆盖用例 |
|------|----------|----------|
| Version 快照读 | `Versions::fetch_version` | 3.1 快照隔离读 |
| Writeset 隔离 | 事务内部 `writeset` 未提交 | 3.1、3.2 |
| First-Committer-Wins | `auto_commit` + commit queue | 3.1（同 key）、4.3 |
| Bloom 快速排除（writeset） | `is_disjoint_writeset_bloom` | 全部 SSI 用例（隐式） |
| Key range 快速排除 | `Commit.max_key/min_key` | 3.3 |
| Readset 追踪（`get`） | `TransactionInner::get` | 3.2、3.6、3.7 |
| Readset 追踪（`get` on absent） | 同上，`None` 也进 readset | 3.4、4.2 |
| Readset 追踪（`exists`） | `TransactionInner::exists` | 3.5 |
| Bloom 快速排除（readset） | `is_disjoint_readset_bloom` | 3.2、3.4、3.5、3.6、3.7、4.3 |
| Tombstone 参与冲突判定 | `writeset: Option<Bytes>` | 3.6 |
| SI 不做读集检测 | `mode < SerializableSnapshotIsolation` 分支 | 4.1 |

未被本次测试直接触达的机制（可作为后续测试补充方向）：

- **Merge queue 可见性**：读路径在 merge queue 中命中"已提交尚未刷入 datastore"的数据（`docs/001` §4.2）。当前测试中提交都很快完成，很难人为造出 merge queue 里悬停的条目。
- **Bloom 误判率退化**：writeset/readset 极大时 Bloom 命中率如何。这是性能测试，不是正确性测试。
- **Commit queue GC（0.0.4）**：活跃事务引用计数下的水位推进。可以通过创建长事务持有低水位来验证。

---

## 6. 从测试反过来读实现的建议路径

如果你是第一次读这个 repo，推荐这样使用本文：

1. 先运行 `cargo test --test isolations` 让所有用例通过一遍，建立"东西是能跑的"直觉。
2. 挑一个感兴趣的用例（推荐 3.2 或 3.4，是 SSI 的"最小惊奇"），用 `cargo test <name> -- --nocapture` 单独跑。
3. 在 `src/tx/transaction_inner.rs` 的 `get`、`commit` 两个方法里打断点或加 `println!`，观察 readset/writeset 内容和冲突检测走的分支。
4. 把该用例的隔离级别从 SSI 改成 SI（去掉链式调用），观察断言变化——`is_err()` 应该变成 `is_ok()`，从而理解 SSI 独有的贡献。
5. 回头读 [`docs/002_ssi_bloom_filter.md`](../002_ssi_bloom_filter.md) 中的 §5、§6，一切都会更亲切。

---

## 7. 小结

这批测试用例的核心价值不在数量，而在于**每一个都对应实现里一处具体的分支或数据结构**：

- SI 的两条承诺（快照读一致 + 不相交写并发）各一个用例，作为基础断言。
- SSI 的读依赖追踪覆盖了 `get`、`get-not-found`、`exists`、`del` 四种触发面，以及"多读者—一写者"的经典模式。
- 与 SI 的对比用例把"SSI 增益"用一行 `assert` 说清楚。
- Lost update 场景则展示了 SSI 对"读—改—写"这种最常见的应用模式的双重防护。

读到这里，如果你能对着 `tests/isolations.rs` 的每一条 `assert` 说清楚"为什么应该是这个结果、走了哪条代码路径"，那就说明 SI / SSI 这一层的心智模型已经建成了。下一站可以继续读 [`docs/003_runtime_hardening.md`](../003_runtime_hardening.md) 和 [`docs/004_commit_queue_gc.md`](../004_commit_queue_gc.md)——那两篇的重点从"隔离性正确"转向了"长时间运行下的稳定性"，是完全不同的问题面。
