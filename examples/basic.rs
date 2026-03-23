use mini_kv::Database;

fn main() {
    let db = Database::new();

    let mut tx = db.transaction(true);

    tx.set("key1", "value1").unwrap();
    tx.set("key2", "value2").unwrap();

    println!("exists(key1) = {}", tx.exists("key1").unwrap());
    println!("get(key1) = {:?}", tx.get("key1").unwrap());
    println!("get(key2) = {:?}", tx.get("key2").unwrap());

    tx.commit().unwrap();

    let tx = db.transaction(false);
    println!("after commit, get(key1) = {:?}", tx.get("key1").unwrap());
    println!("after commit, get(key2) = {:?}", tx.get("key2").unwrap());
}
