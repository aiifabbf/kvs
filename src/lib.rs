use serde::Deserialize;
use serde::Serialize;

use sled::Db;

use std::collections::HashMap;
use std::error::Error;
use std::fmt::Display;
use std::fs::create_dir_all;
use std::fs::remove_file;
use std::fs::rename;
use std::fs::File;
use std::io::Read;
use std::io::Write;
use std::net::Shutdown;
use std::net::TcpListener;
use std::net::TcpStream;
use std::net::ToSocketAddrs;
use std::path::Path;
use std::path::PathBuf;

pub type Result<T> = std::result::Result<T, KvsError>;

#[derive(Debug)]
pub enum KvsError {
    Io(std::io::Error),
    Serde(serde_json::Error),
    Sled(sled::Error),
    NotFound {
        key: String,
    }, // 我不明白为什么not found是个错误，明明用None就能表示
    Remote {
        message: String,
    }, // 远端错误
    UnsupportedEngine {
        name: String,
    },
    BadArchive {
        path: PathBuf,
        should: String, // 应该是什么engine
        tried: String,  // 现在试图用什么engine打开
    }, // 如果磁盘上的持久化明明是sled engine，但是现在要运行kvs engine，就会出这个错误
}

impl Display for KvsError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            KvsError::NotFound { key: k } => write!(f, "Key not found: {}", k),
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

impl From<sled::Error> for KvsError {
    fn from(error: sled::Error) -> Self {
        KvsError::Sled(error)
    }
}

// 听说要支持sled后端
pub trait KvsEngine {
    fn get(&mut self, key: &str) -> Result<Option<&str>>;
    fn set(&mut self, key: String, value: String) -> Result<()>;
    fn remove(&mut self, key: &str) -> Result<()>;
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum Command {
    Set(String, String),
    Remove(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Storage {
    /// value在硬盘上，要去读名为 `value` 的文件
    Disk(usize),
    /// value已经缓存在内存里了，可以直接读出来
    Memory(String),
}

#[derive(Debug)]
pub struct KvStore {
    /// `map["a"] == 2` 表示 `"a": "33"` 存在磁盘上名为 `2` 的文件里，同时`logs[2] == ("a", Disk(2))` 或者 `("a", Memory("33"))`
    map: HashMap<String, usize>, // 感觉是个坑啊，key就一定要是utf8吗？不能是bytes吗？
    /// `logs[2] == ("a", Disk(2))` 表示 `"a": "33"` 存在磁盘上名为 `2` 的文件里
    logs: Vec<(String, Storage)>,
    /// 下一个包含没有出现过的key的command应该存在名为 `seek` 的文件里，比如假如之前从来没出现过 `"a": "33"` ，`seek` 目前是8，那么set的时候这个command会存到名为 `8` 的文件里
    seek: usize,
    /// 存log的目录。PathBuf和Path的关系类似String和&str
    root: PathBuf,
}

/// 目录下面建一个叫做.kvs的文件，如果里面存kvs，说明当前目录的记录是kvs engine；如果存sled，说明是sled engine
fn archive_type<T>(root: T) -> Result<String>
where
    T: AsRef<Path>,
{
    match File::open(root.as_ref().join(".kvs")) {
        Ok(mut manifest) => {
            let mut string = String::new();
            manifest.read_to_string(&mut string)?;
            Ok(string)
        }
        Err(e) => Err(KvsError::Io(e)),
    }
}

impl KvStore {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            logs: vec![],
            seek: 0,
            root: PathBuf::new(), // 空的path会是啥呢……
        }
    }

    pub fn open<T>(root: T) -> Result<Self>
    where
        T: Into<PathBuf>,
    {
        let root = root.into();
        create_dir_all(&root)?; // 把存log的目录先建了

        match archive_type(&root) {
            Ok(name) => {
                if name != "kvs" {
                    // 发现当前目录存了其他engine的记录
                    return Err(KvsError::BadArchive {
                        path: root,
                        should: name,
                        tried: format!("kvs"),
                    });
                }
            }
            Err(KvsError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                // 当前目录是新的，没有存过任何engine的记录
                let mut file = File::create(root.join(".kvs"))?;
                file.write("kvs".as_bytes())?;
            }
            Err(e) => {
                return Err(e);
            }
        }

        let mut map = HashMap::new();
        let mut logs = vec![];
        let mut seek = 0;

        for i in 0.. {
            // 把command一个一个读出来
            let path = root.join(format!("{}", i)); // 第10个command的路径是path/10
            if let Ok(mut file) = File::open(&path) {
                let mut string = String::new();
                file.read_to_string(&mut string)?;
                let command: Command = serde_json::from_str(&string[..])?;
                match command {
                    Command::Set(key, _) => {
                        if let Some(offset) = map.get(&key[..]).cloned() {
                            // 之前出现过a: 1了，假设存在文件1里，现在又来了个a: 2，假设存在文件5里。直接把5重命名为2就好了，其他什么都不用变
                            let new_path = root.join(format!("{}", offset)); // 原来还有join这个好用的方法……
                            rename(&path, &new_path)?; // 把5重命名为2
                        } else {
                            // 来了个a: 1，之前没见过，把a: 1存在名为seek的文件里
                            let new_path = root.join(format!("{}", seek));
                            rename(&path, &new_path)?;

                            map.insert(key.clone(), seek); // 更新map，让map[a] = seek
                            logs.push((key, Storage::Disk(seek))); // 更新logs，让logs[seek] = (a, Disk(seek))
                            seek += 1;
                        }
                    }
                    Command::Remove(key) => {
                        if let Some(offset) = map.get(&key[..]).cloned() {
                            // 之前出现过a: 1，假设存在文件2里。那么要删掉文件2，可是这样就留下了2这个空洞，怎么办呢？把最后一个command放到2里，填充这个空洞
                            if seek != 0 {
                                // 假设这时候有6个command，那么此时seek = 6
                                seek -= 1; // 先把seek往下移动一格，这样seek = 5
                                let path = root.join(format!("{}", seek)); // 最后一个command存放在文件5里
                                let new_path = root.join(format!("{}", offset)); // 假设要删除的a: 1存在文件2里
                                rename(&path, &new_path)?; // 把文件5重命名为2就好了，这样a: 1就跑到文件2里去了

                                // 更新一下内存里的表示
                                let mut log = logs.pop().unwrap(); // 最后一个command
                                match log.1 {
                                    Storage::Disk(_) => {
                                        log.1 = Storage::Disk(offset); // 最后一个command本来存在文件5里的，现在存到文件2里面去了
                                    }
                                    _ => {} // 如果已经缓存到内存里了，就不用管了
                                }
                                logs[offset] = log; // 内存里的空洞也要填充一下
                                map.insert(logs[offset].0.clone(), offset); // 更新map
                            } // 出现了奇怪的情况，文件0里面是Remove(a, 2)，按理说是无效command
                        }
                        // 如果log本身就有问题呢……比如出现了Remove(key)而key当时还并不存在
                        map.remove(&key[..]);
                    }
                }
            } else {
                // 0, 1, 2发现没有3，说明读完了
                // [seek, i)之间的文件都是冗余的，全部删掉
                for j in seek..i {
                    let path = root.join(format!("{}", j));
                    remove_file(&path)?;
                }

                break;
                // 标准答案里面是用扩展名来判断是不是log的，所以没有空洞的问题
            }
        }

        return Ok(Self {
            map: map,
            logs: logs,
            seek: seek,
            root: root,
        });
    }
}

impl KvsEngine for KvStore {
    // 标准答案里面key是String，但我觉得……怎么能传owned呢，所以改掉了
    fn get(&mut self, key: &str) -> Result<Option<&str>> {
        // 假设现在get("a")
        match self.map.get_mut(key) {
            None => Ok(None), // 内存和磁盘永远是一致的，内存里没有，磁盘上肯定也没有
            Some(offset) => {
                // 发现a存在文件2里
                let storage = &mut self.logs.get_mut(*offset).unwrap().1; // logs[2] == ("a", Disk(2))或者logs[2] == ("a", Memory("1"))
                match storage {
                    Storage::Disk(offset) => {
                        // logs[2] == ("a", Disk(2))，在磁盘上还没读出来
                        let path = self.root.join(format!("{}", offset)); // a存在文件2里
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
                                // 如果读到的是Remove(a)，那么key应该在内存里也不存在……出现了不一致，按理说这种情况是不允许发生的
                                eprintln!(
                                    "Inconsistency detected: {} in memory but not on disk",
                                    key
                                );
                                self.map.remove(key);
                                Ok(None)
                            }
                        }
                    }
                    Storage::Memory(value) => Ok(Some(&value[..])), // 已经在内存里的话，就直接返回好了
                }
            }
        }
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
        // 假设set("a", "1")
        if let Some(offset) = self.map.get(&key[..]) {
            // 之前已经有a: 2了，要覆盖掉
            let path = self.root.join(format!("{}", offset)); // 假设之前的a: 2存在文件5里
            let mut file = File::create(&path)?; // 直接把文件5清空，写入a: 1

            let command = Command::Set(key.clone(), value.clone());
            let string = serde_json::to_string(&command)?;
            file.write(string.as_bytes())?;

            // 更新内存里的表示
            let log = &mut self.logs[*offset];
            match &log.1 {
                Storage::Memory(_) => {
                    log.1 = Storage::Memory(value); // 如果已经读出来了，要把a: 2刷成a: 1
                }
                _ => {} // 如果没读出来，不用管
            }
        } else {
            // 之前没见过a，假设当前总共有6个command，那么要把a: 1写到文件6里
            let path = self.root.join(format!("{}", self.seek)); // a: 1应该存到文件6里
            let mut file = File::create(&path)?; // 但万一这里提前return了……

            let command = Command::Set(key.clone(), value.clone());
            let string = serde_json::to_string(&command)?;
            file.write(string.as_bytes())?;

            // 更新内存里的表示
            self.map.insert(key.clone(), self.seek);
            self.logs.push((key, Storage::Memory(value))); // write-through策略？set的时候不仅写到磁盘里，也写到内存里
            self.seek += 1;
        }

        Ok(())
    }

    // 标准答案里key也是String，我给改了
    fn remove(&mut self, key: &str) -> Result<()> {
        // 假设删除a: 1
        if let Some(offset) = self.map.get(key).cloned() {
            // a: 1确实在数据库里，假设存在文件2里，那么如果删掉文件2，会留下2这个空洞。把最后一个command填充到文件2里，就没有空洞啦
            self.seek -= 1; // 假设现在数据库里有6个command，所以seek是6，最后一个command存在文件5里
            let path = self.root.join(format!("{}", self.seek)); // 最后一个command存在文件5里
            let new_path = self.root.join(format!("{}", offset)); // 要删除的a: 1存在文件2里

            if self.seek != offset {
                rename(&path, &new_path)?; // 把文件5重命名为2，就填充了2这个空洞

                // 不要忘了更新内存里的表示
                let mut log = self.logs.pop().unwrap();
                match log.1 {
                    Storage::Disk(_) => {
                        log.1 = Storage::Disk(offset); // 现在最后一个command存在文件2里了
                    }
                    _ => {} // 已经在内存里缓存的话就不用管了
                }
                self.logs[offset] = log;
                self.map.insert(self.logs[offset].0.clone(), offset);
            } else {
                // 也有可能a: 1是数据库里唯一的entry
                remove_file(&path)?; // 直接删掉就好了

                self.logs.pop(); // 内存里也是
                self.map.remove(key);
            }

            Ok(())
        } else {
            // a: 1不在数据库里，数据库里面没有a这个key
            Err(KvsError::NotFound {
                key: key.to_string(),
            }) // 再次提问……remove的时候key不存在，不管不就好了吗
        }
    }
}

// 这个名字起的实在是太奇怪了，Engine让人感觉是interface，可是这里SledKvsEngine却又是个struct。按照这样的命名，KvsStore也应该改名叫KvsStoreEngine
pub struct SledKvsEngine {
    store: Db,
    stash: Option<String>,
}

impl SledKvsEngine {
    pub fn open<T>(root: T) -> Result<Self>
    where
        T: Into<PathBuf>,
    {
        let root = root.into();
        create_dir_all(&root)?;

        match archive_type(&root) {
            Ok(name) => {
                if name != "sled" {
                    return Err(KvsError::BadArchive {
                        path: root,
                        should: name,
                        tried: format!("sled"),
                    });
                }
            }
            Err(KvsError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                let mut file = File::create(root.join(".kvs"))?;
                file.write("sled".as_bytes())?;
            }
            Err(e) => {
                return Err(e);
            }
        }

        Ok(Self {
            store: sled::open(root)?,
            stash: None,
        })
    }
}

impl KvsEngine for SledKvsEngine {
    fn get(&mut self, key: &str) -> Result<Option<&str>> {
        match self.store.get(key.as_bytes()) {
            Ok(Some(v)) => {
                self.stash = Some(std::str::from_utf8(v.as_ref()).unwrap().to_string());
                Ok(self.stash.as_ref().map(|v| &v[..])) // 因为存的时候只允许存String，所以这里应该不会panic
            }
            Ok(None) => Ok(None),
            Err(e) => Err(KvsError::Sled(e)),
        }
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
        match self.store.insert(key.as_bytes(), value.as_bytes()) {
            Ok(_) => {
                self.store.flush()?; // 巨坑，千万千万不要忘记flush，这样才会写回磁盘
                Ok(())
            }
            Err(e) => Err(KvsError::Sled(e)),
        }
    }

    fn remove(&mut self, key: &str) -> Result<()> {
        match self.store.remove(key.as_bytes()) {
            Ok(Some(_)) => {
                self.store.flush()?;
                Ok(())
            }
            Ok(None) => Err(KvsError::NotFound {
                key: key.to_string(),
            }), // 到底是为什么key不存在算是个错误
            Err(e) => Err(KvsError::Sled(e)),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
enum Request {
    Get(String),
    Set(String, String),
    Remove(String),
}

#[derive(Serialize, Deserialize, Debug)]
enum Response {
    Done(Option<String>),
    Failed(String),
}

pub struct KvsClient {
    address: String,
}

impl KvsClient {
    pub fn connect(address: String) -> Result<Self> {
        Ok(Self { address: address }) // 假的connect，每次请求都要打开新的socket，不能复用socket
    }

    /// 发送请求，等待回应
    fn request(&mut self, request: Request) -> Result<Response> {
        let mut stream = TcpStream::connect(&self.address)?; // 打开socket
        let mut string = serde_json::to_string(&request)?;
        stream.write_all(string.as_bytes())?; // 发请求
        stream.shutdown(Shutdown::Write)?; // 这很关键，要关闭上传通道，这样服务器才会收到EOF，不然死锁

        string.clear();
        stream.read_to_string(&mut string)?; // 收响应
        let response: Response = serde_json::from_str(&string[..])?;
        return Ok(response);
    }

    /// 无聊的CRUD……
    pub fn get(&mut self, key: &str) -> Result<Option<String>> {
        let response = self.request(Request::Get(key.to_string()))?;
        match response {
            Response::Done(v) => Ok(v),
            Response::Failed(e) => Err(KvsError::Remote { message: e }),
        }
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let response = self.request(Request::Set(key, value))?;
        match response {
            Response::Done(_) => Ok(()),
            Response::Failed(e) => Err(KvsError::Remote { message: e }),
        }
    }

    pub fn remove(&mut self, key: &str) -> Result<()> {
        let response = self.request(Request::Remove(key.to_string()))?;
        match response {
            Response::Done(_) => Ok(()),
            Response::Failed(e) => Err(KvsError::Remote { message: e }),
        }
    }
}

pub struct KvsServer<T> {
    engine: T,
}

impl<T> KvsServer<T>
where
    T: KvsEngine,
{
    pub fn new(engine: T) -> Self {
        Self { engine: engine }
    }

    /// 只服务一次请求就return
    fn serve(&mut self, stream: &mut TcpStream) -> Result<()> {
        let mut string = String::new();
        stream.read_to_string(&mut string)?; // 收请求
        let request: Request = serde_json::from_str(&string[..])?;
        let response = match request {
            Request::Get(key) => match self.engine.get(&key[..]) {
                Ok(value) => Response::Done(value.map(|v| v.to_string())),
                Err(e) => Response::Failed(format!("{}", e)),
            },
            Request::Set(key, value) => match self.engine.set(key, value) {
                Ok(_) => Response::Done(None),
                Err(e) => Response::Failed(format!("{}", e)),
            },
            Request::Remove(key) => match self.engine.remove(&key[..]) {
                Ok(_) => Response::Done(None),
                Err(e) => Response::Failed(format!("{}", e)),
            },
        };
        let string = serde_json::to_string(&response)?;
        stream.write_all(string.as_bytes())?; // 发响应
        Ok(())
    }

    /// 在某个ip:port上一直处理请求
    pub fn run<U>(&mut self, address: U) -> Result<()>
    where
        U: ToSocketAddrs,
    {
        let listener = TcpListener::bind(address)?;
        for stream in listener.incoming() {
            match stream {
                Ok(mut stream) => match self.serve(&mut stream) {
                    Ok(_) => {
                        println!("{:?}", stream);
                    }
                    Err(e) => {
                        eprintln!("{}", e);
                    }
                },
                Err(e) => eprintln!("{}", e),
            }
        }
        Ok(())
    }
}
