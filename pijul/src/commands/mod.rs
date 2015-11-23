extern crate clap;

pub type StaticSubcommand = clap::App<'static, 'static, 'static, 'static, 'static, 'static>;

mod fs_operation;
pub mod info;
pub mod check;
pub mod init;
pub mod record;
pub mod add;
pub mod pull;
pub mod get;
pub mod remove;
mod test;

pub fn all_command_invocations() -> Vec<StaticSubcommand> {
    return vec![
        check::invocation(),
        info::invocation(),
        init::invocation(),
        record::invocation(),
        add::invocation(),
        pull::invocation(),
        get::invocation(),
        remove::invocation()
        ];
}

