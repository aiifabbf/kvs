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

pub struct KvStore {
    map: HashMap<String, String>, // key是key，value是定义这个key-value pair最新的command的offset（好绕啊）。感觉是个坑啊，key就一定要是utf8吗？不能是bytes吗？
    seek: usize,                  // 下一个command的offset。或者可以说是当前读了多少个command
    path: PathBuf,                // 存log的目录。PathBuf和Path的关系类似String和&str
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
                    Command::Set(key, value) => map.insert(key, value),
                    Command::Remove(key) => map.remove(&key[..]),
                };

                seek += 1;
            } else {
                // 0, 1, 2发现没有3，说明读完了
                path.pop();
                break;
            }
        }

        return Ok(Self {
            map: map,
            seek: seek,
            path: path,
        });
    }

    pub fn get(&self, key: &str) -> Result<Option<&str>> {
        // 标准答案里面key是String，但我觉得……怎么能传owned呢，所以改掉了
        match self.map.get(key) {
            None => Ok(None),
            Some(value) => Ok(Some(value)),
        }
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let mut path = self.path.clone(); // 其实没必要clone
        path.push(format!("{}", self.seek));
        let mut file = File::create(&path)?; // 但万一这里提前return了……

        let command = Command::Set(key.clone(), value.clone());
        let string = serde_json::to_string(&command)?;
        file.write(string.as_bytes())?;

        self.map.insert(key, value);
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
