use clap::App;
use clap::Arg;

use kvs::KvStore;
use kvs::KvsError;
use kvs::Result;

// 想把main写成返回Result，是因为担心std::process::exit是不是会导致main里的对象没有drop。结果真的会 <https://doc.rust-lang.org/std/process/fn.exit.html>
fn main() -> Result<()> {
    let matches = App::new("kvs")
        .version(env!("CARGO_PKG_VERSION")) // 哇这个可神奇了，cargo在编译阶段会传入一些环境变量 <https://doc.rust-lang.org/cargo/reference/environment-variables.html> 因为是编译时替换，所以即使不用cargo run，直接跑编译出来的二进制也没问题
        .subcommand(
            App::new("get")
                .about("Get the string value of a given string key")
                .arg(Arg::with_name("KEY").required(true)),
        ) // 我还在想subcommand为什么传入的是Subcommand但是文档却说它们一样……原来Subcommand::with_name直接返回了一个App……
        .subcommand(
            App::new("set")
                .about("Set the value of a string key to a string")
                .arg(Arg::with_name("KEY").required(true))
                .arg(Arg::with_name("VALUE").required(true)),
        )
        .subcommand(
            App::new("rm")
                .about("Remove a given key")
                .arg(Arg::with_name("KEY").required(true)),
        )
        .get_matches();

    let mut store = KvStore::open("./")?;

    match matches.subcommand() {
        ("get", Some(app)) => {
            let key = app.value_of("KEY").unwrap();
            let some = store.get(&key)?;
            if let Some(value) = some {
                println!("{}", value);
                Ok(())
            } else {
                println!("Key not found"); // 为什么错误信息要print到stdout上？
                Ok(()) // get不存在返回的是0，可是rm不存在返回的却是1……
            }
        }
        ("set", Some(app)) => {
            let key = app.value_of("KEY").unwrap();
            let value = app.value_of("VALUE").unwrap();
            store.set(key.to_string(), value.to_string())?;
            Ok(())
        }
        ("rm", Some(app)) => {
            let key = app.value_of("KEY").unwrap();
            match store.remove(&key) {
                Err(KvsError::NotFound) => {
                    println!("Key not found");
                    Err(KvsError::NotFound) // get不存在返回的是0，可是rm不存在返回的却是1……
                }
                v => v,
            }
        }
        _ => unreachable!(),
    }
}
