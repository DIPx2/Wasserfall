use clap::Parser;
use feurix::ignite;

/// Feurix -- Rust-модуль в архитектуре Feuerlilie
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// ¬ключить режим отладки дл€ вывода дополнительной информации
    #[arg(short, long)]
    debug: bool,
}

fn main() {
    let args = Args::parse();

    if args.debug {
        println!("?? Feurix запущен в режиме отладки");
        // ћожно добавить дополнительную логику дл€ отладки здесь
    } else {
        println!("?? Feurix запущен");
    }

    // ¬ызываем основную логику из библиотеки
    ignite();
}
