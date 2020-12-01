use std::collections::HashMap;

pub struct KvStore {
    map: HashMap<String, String>,
}

impl KvStore {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        // 标准答案里面key是String，但我觉得……怎么能传owned呢
        self.map.get(key).map(|v| &v[..])
    }

    pub fn set(&mut self, key: String, value: String) {
        self.map.insert(key, value);
    }

    pub fn remove(&mut self, key: &str) {
        // 标准答案里key也是String
        self.map.remove(key);
    }
}
