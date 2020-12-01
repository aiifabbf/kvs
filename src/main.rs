use clap::App;
use clap::Arg;

use kvs::KvStore;

// 想把main写成返回Result，是因为担心std::process::exit是不是会导致main里的对象没有drop。结果真的会 <https://doc.rust-lang.org/std/process/fn.exit.html>
fn main() -> Result<(), ()> {
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

    match matches.subcommand() {
        ("get", Some(app)) => {
            println!("{}", app.value_of("KEY").unwrap()); // 因为前面加了required，所以这里unwrap没问题
            eprintln!("unimplemented");
            Err(())
        }
        ("set", Some(app)) => {
            let key = app.value_of("KEY").unwrap();
            let value = app.value_of("VALUE").unwrap();
            let mut kvs = KvStore::new();
            kvs.set(key.to_string(), value.to_string());
            println!("{} {}", key, kvs.get(key).unwrap());
            eprintln!("unimplemented");
            Err(())
        }
        ("rm", Some(app)) => {
            println!("{}", app.value_of("KEY").unwrap());
            eprintln!("unimplemented");
            Err(())
        }
        _ => unreachable!(),
    }
}
