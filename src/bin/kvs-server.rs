use clap::App;
use clap::AppSettings;
use clap::Arg;

use kvs::KvsServer;
use kvs::Result;

fn main() -> Result<()> {
    let matches = App::new("kvs")
        .version(env!("CARGO_PKG_VERSION"))
        .arg(
            Arg::with_name("IP-PORT")
                .long("--addr")
                .value_name("IP-PORT"),
        )
        .arg(
            Arg::with_name("ENGINE-NAME")
                .long("--engine")
                .value_name("ENGINE-NAME"),
        )
        .setting(AppSettings::ArgRequiredElseHelp)
        .get_matches();

    let address = matches.value_of("IP-PORT").unwrap_or("127.0.0.1:4000");
    let mut server = KvsServer::bind(address.to_string())?;
    eprintln!("kvs {} {}", env!("CARGO_PKG_VERSION"), address); // 懒得用log库了。这个信息为什么输出到stderr呢，我觉得应该输出到stdout，毕竟不算错误
    server.run_forever();
    Ok(())
}
