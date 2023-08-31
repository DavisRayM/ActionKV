use libactionkv::{ActionKV, ByteStr, ByteString};
use std::collections::HashMap;

#[cfg(target_os = "windows")]
const USAGE: &str = "
Usage:
    akv_disk.exe FILE get KEY
    akv_disk.exe FILE delete KEY
    akv_disk.exe FILE insert KEY VALUE
    akv_disk.exe FILE update KEY VALUE
";

#[cfg(not(target_os = "windows"))]
const USAGE: &str = "
Usage
    akv_disk FILE get KEY
    akv_disk FILE delete KEY
    akv_disk FILE insert KEY VALUE
    akv_disk FILE update KEY VALUE
";

/// Serializes and stores the Key-Value store index
///
/// Stores the KV Index in the storage file in a serialized format in order to
/// give the application a faster start time
fn store_index_on_disk(a: &mut ActionKV, index_key: &ByteStr) {
    a.index.remove(index_key);
    let index_as_bytes = bincode::serialize(&a.index).unwrap();
    a.index = HashMap::new();
    a.insert(index_key, &index_as_bytes).unwrap();
}

fn main() {
    const INDEX_KEY: &ByteStr = b"+index";

    let args: Vec<String> = std::env::args().collect();
    let fname = args.get(1).expect(&USAGE);
    let action = args.get(2).expect(&USAGE).as_ref();
    let key = args.get(3).expect(&USAGE).as_ref();
    let pos_value = args.get(4);

    let fpath = std::path::Path::new(&fname);
    let mut store = ActionKV::open(fpath).expect("unable to open file");
    store.load().expect("unable to load data");

    match action {
        "get" => {
            let index_as_bytes = store.get(&INDEX_KEY).unwrap().unwrap();
            let index_decoded = bincode::deserialize(&index_as_bytes);

            let index: HashMap<ByteString, u64> = index_decoded.unwrap();

            match index.get(key) {
                None => {
                    eprintln!("{:?} not found", key);
                }
                Some(&pos) => {
                    let kv = store.get_at(pos).unwrap();
                    println!("{:?}", String::from_utf8_lossy(&kv.value));
                }
            }
        }
        "delete" => store.delete(key).unwrap(),
        "insert" => {
            let value = pos_value.expect(&USAGE).as_ref();
            store.insert(key, value).unwrap();
            store_index_on_disk(&mut store, INDEX_KEY);
        }
        "update" => {
            let value = pos_value.expect(&USAGE).as_ref();
            store.update(key, value).unwrap();
            store_index_on_disk(&mut store, INDEX_KEY);
        }
        _ => eprintln!("{}", &USAGE),
    }
}
