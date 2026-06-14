use clap::Parser;
use rooms_tui::cli::{Args, build_attach_url};
use rooms_tui::ui::{UiModel, run_tui};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let attach_config = args.attach_config();
    let attach_url = build_attach_url(&attach_config)?;

    if args.print_url {
        println!("{attach_url}");
        return Ok(());
    }

    run_tui(UiModel::new(attach_config, attach_url))?;
    Ok(())
}
