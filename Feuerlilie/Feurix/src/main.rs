use clap::Parser;
use feurix::ignite;

/// Feurix -- Rust-������ � ����������� Feuerlilie
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// �������� ����� ������� ��� ������ �������������� ����������
    #[arg(short, long)]
    debug: bool,
}

fn main() {
    let args = Args::parse();

    if args.debug {
        println!("?? Feurix ������� � ������ �������");
        // ����� �������� �������������� ������ ��� ������� �����
    } else {
        println!("?? Feurix �������");
    }

    // �������� �������� ������ �� ����������
    ignite();
}
