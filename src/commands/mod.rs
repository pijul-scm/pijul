extern crate clap;

pub type StaticSubcommand = clap::App<'static, 'static, 'static, 'static, 'static, 'static>;

pub mod info;
pub mod check;

pub fn all_command_invocations() -> Vec<StaticSubcommand> {
    return vec![
        info::invocation(),
        check::invocation()
        ];
}

