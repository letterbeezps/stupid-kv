/// SSI (Serializable Snapshot Isolation) 示例
///
/// 演示 SSI 相较于 SI 额外防止的异常：Write-Skew（写偏斜）。
///
/// 场景：医院值班系统，规则是"至少有一名医生在班"。
///   - Alice 和 Bob 各自查询在班人数，发现有 2 人。
///   - Alice 认为"还有人，我可以下班"，写入自己下班。
///   - Bob 认为"还有人，我可以下班"，写入自己下班。
///   - SI 下：两人都能提交，但结果是 0 人在班，违反规则。
///   - SSI 下：Bob 提交时发现他读取的 on_call_count 被 Alice 修改过，拒绝提交。
use stupid_kv::Database;

fn main() {
    println!("=== Write-Skew under SI (allowed — demonstrates the anomaly) ===");
    write_skew_si();

    println!();

    println!("=== Write-Skew under SSI (rejected — correct behavior) ===");
    write_skew_ssi();

    println!();

    println!("=== SSI: non-conflicting transactions still succeed ===");
    ssi_no_conflict();
}

/// SI 下 Write-Skew 不被检测，两个事务都能提交。
fn write_skew_si() {
    let db = Database::new();

    // 初始状态：Alice 和 Bob 都在班
    let mut setup = db.transaction(true);
    setup.set("alice_on_call", "true").unwrap();
    setup.set("bob_on_call", "true").unwrap();
    setup.commit().unwrap();

    // Alice 和 Bob 同时开启事务
    let mut tx_alice = db.transaction(true);
    let mut tx_bob = db.transaction(true);

    // Alice 读到 Bob 在班，决定自己下班
    let bob_on = tx_alice.get("bob_on_call").unwrap();
    println!("  Alice sees bob_on_call = {:?}", bob_on);
    tx_alice.set("alice_on_call", "false").unwrap();

    // Bob 读到 Alice 在班，决定自己下班
    let alice_on = tx_bob.get("alice_on_call").unwrap();
    println!("  Bob   sees alice_on_call = {:?}", alice_on);
    tx_bob.set("bob_on_call", "false").unwrap();

    // 两个事务写不同的 key，SI 不检测读冲突，都能提交
    let r1 = tx_alice.commit();
    let r2 = tx_bob.commit();
    println!("  alice commit: {:?}", r1);
    println!("  bob   commit: {:?}", r2);

    let verify = db.transaction(false);
    println!(
        "  final state: alice={:?}, bob={:?}  <- Write-Skew! nobody on call",
        verify.get("alice_on_call").unwrap(),
        verify.get("bob_on_call").unwrap(),
    );
}

/// SSI 下 Write-Skew 被检测，后提交的事务被拒绝。
fn write_skew_ssi() {
    let db = Database::new();

    let mut setup = db.transaction(true);
    setup.set("alice_on_call", "true").unwrap();
    setup.set("bob_on_call", "true").unwrap();
    setup.commit().unwrap();

    let mut tx_alice = db.transaction(true).with_serializable_snapshot_isolation();
    let mut tx_bob = db.transaction(true).with_serializable_snapshot_isolation();

    // Alice 读 bob_on_call，决定自己下班
    let bob_on = tx_alice.get("bob_on_call").unwrap();
    println!("  Alice sees bob_on_call = {:?}", bob_on);
    tx_alice.set("alice_on_call", "false").unwrap();

    // Bob 读 alice_on_call，决定自己下班
    let alice_on = tx_bob.get("alice_on_call").unwrap();
    println!("  Bob   sees alice_on_call = {:?}", alice_on);
    tx_bob.set("bob_on_call", "false").unwrap();

    // Alice 先提交成功
    let r1 = tx_alice.commit();
    println!("  alice commit: {:?}", r1);

    // Bob 提交时：SSI 检测到他读取的 alice_on_call 已被 Alice 修改，拒绝
    let r2 = tx_bob.commit();
    println!("  bob   commit: {:?}  <- rejected by SSI read-conflict detection", r2);

    let verify = db.transaction(false);
    println!(
        "  final state: alice={:?}, bob={:?}  <- at least one on call",
        verify.get("alice_on_call").unwrap(),
        verify.get("bob_on_call").unwrap(),
    );
}

/// SSI 下，读写不重叠的事务仍然可以并发成功。
fn ssi_no_conflict() {
    let db = Database::new();

    let mut tx1 = db.transaction(true).with_serializable_snapshot_isolation();
    let mut tx2 = db.transaction(true).with_serializable_snapshot_isolation();

    // tx1 读 key_a，写 key_b
    let _ = tx1.get("key_a").unwrap();
    tx1.set("key_b", "from_tx1").unwrap();

    // tx2 读 key_c，写 key_d（与 tx1 完全不重叠）
    let _ = tx2.get("key_c").unwrap();
    tx2.set("key_d", "from_tx2").unwrap();

    let r1 = tx1.commit();
    let r2 = tx2.commit();
    println!("  tx1 commit: {:?}", r1);
    println!("  tx2 commit: {:?}", r2);
}
