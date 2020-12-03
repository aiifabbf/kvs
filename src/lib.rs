use serde::Deserialize;
use serde::Serialize;

use std::collections::HashMap;
use std::error::Error;
use std::fmt::Display;
use std::fs::create_dir_all;
use std::fs::File;
use std::io::Read;
use std::io::Write;
use std::path::PathBuf;

pub type Result<T> = std::result::Result<T, KvsError>;

#[derive(Debug)]
pub enum KvsError {
    Io(std::io::Error),
    Serde(serde_json::Error),
    NotFound, // 我不明白为什么not found是个错误，明明用None就能表示
}

impl Display for KvsError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            KvsError::NotFound => write!(f, "Key not found"),
            _ => write!(f, "{}", format!("{:#?}", self)),
        }
    }
}

impl Error for KvsError {}

// 我一直以为From和Into是完全一样的
impl From<std::io::Error> for KvsError {
    fn from(error: std::io::Error) -> Self {
        KvsError::Io(error)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(error: serde_json::Error) -> Self {
        KvsError::Serde(error)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum Command {
    Set(String, String),
    Remove(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Storage {
    Disk(usize),    // value在硬盘上
    Memory(String), // value已经缓存在内存里了
}

pub struct KvStore {
    // cache: HashMap<String, String>, // 还是不要搞cache了……这样key存了两次
    map: HashMap<String, Storage>, // key是key，value是定义这个key-value pair最新的command的offset（好绕啊）。感觉是个坑啊，key就一定要是utf8吗？不能是bytes吗？
    seek: usize,                   // 下一个command的offset。或者可以说是当前读了多少个command
    path: PathBuf,                 // 存log的目录。PathBuf和Path的关系类似String和&str
}

impl KvStore {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            seek: 0,
            path: PathBuf::new(), // 空的path会是啥呢……
        }
    }

    pub fn open<T>(path: T) -> Result<Self>
    where
        T: Into<PathBuf>,
    {
        let mut path = path.into();
        create_dir_all(&path)?; // 把存log的目录先建了

        let mut map = HashMap::new();
        let mut seek = 0;

        for i in 0.. {
            // 把command一个一个读出来
            path.push(format!("{}", i)); // 第10个command的路径是path/10
            if let Ok(mut file) = File::open(&path) {
                path.pop();
                let mut string = String::new();
                file.read_to_string(&mut string)?;
                let command: Command = serde_json::from_str(&string[..])?;
                match command {
                    Command::Set(key, _) => map.insert(key, Storage::Disk(i)),
                    Command::Remove(key) => map.remove(&key[..]), // 如果log本身就有问题呢……比如第一次出现了Remove(key)而key当时还并不存在
                };

                seek += 1;
            } else {
                // 0, 1, 2发现没有3，说明读完了
                path.pop();
                break;
                // 标准答案里面是用扩展名来判断是不是log的
            }
        }

        return Ok(Self {
            map: map,
            seek: seek,
            path: path,
        });
    }

    pub fn get(&mut self, key: &str) -> Result<Option<&str>> {
        // 标准答案里面key是String，但我觉得……怎么能传owned呢，所以改掉了
        match self.map.get_mut(key) {
            None => Ok(None),
            Some(storage) => {
                match storage {
                    Storage::Disk(offset) => {
                        let mut path = self.path.clone();
                        path.push(format!("{}", offset));
                        let mut file = File::open(&path)?;

                        let mut string = String::new();
                        file.read_to_string(&mut string)?;
                        let command: Command = serde_json::from_str(&string[..])?;

                        match command {
                            Command::Set(_, value) => {
                                *storage = Storage::Memory(value); // 先放进cache
                                match storage {
                                    Storage::Memory(value) => Ok(Some(&value[..])),
                                    _ => unreachable!(),
                                } // 虽然这里确定了storage肯定是Memory，但是流程还是要这么写哈哈
                            }
                            _ => {
                                // 如果读到的是Remove，那么key应该不存在……出现了不一致，按理说这种情况是不允许发生的
                                eprintln!(
                                    "Inconsistency detected: {} in memory but not on disk",
                                    key
                                );
                                // self.map.remove(key); // 这里提示borrowed twice，我不懂为啥
                                Ok(None)
                            }
                        }
                    }
                    Storage::Memory(value) => Ok(Some(&value[..])),
                }
            }
        }
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let mut path = self.path.clone(); // 其实没必要clone
        path.push(format!("{}", self.seek));
        let mut file = File::create(&path)?; // 但万一这里提前return了……

        let command = Command::Set(key.clone(), value.clone());
        let string = serde_json::to_string(&command)?;
        file.write(string.as_bytes())?;

        self.map.insert(key.clone(), Storage::Memory(value));
        self.seek += 1;

        Ok(())
    }

    pub fn remove(&mut self, key: &str) -> Result<()> {
        // 标准答案里key也是String，我给改了
        if self.map.contains_key(key) {
            let mut path = self.path.clone();
            path.push(format!("{}", self.seek));
            let mut file = File::create(&path)?;

            let command = Command::Remove(key.to_string());
            let string = serde_json::to_string(&command)?;
            file.write(string.as_bytes())?;

            self.map.remove(key);
            self.seek += 1;

            Ok(())
        } else {
            Err(KvsError::NotFound) // 再次提问……remove的时候key不存在，不管不就好了吗
        }
    }
}
