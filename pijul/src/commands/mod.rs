extern crate clap;

pub type StaticSubcommand = clap::App<'static, 'static, 'static, 'static, 'static, 'static>;

mod fs_operation;
mod remote;
mod escape;
mod ask;

pub mod info;
pub mod check;
pub mod init;
pub mod record;
pub mod add;
pub mod pull;
pub mod push;
pub mod apply;
pub mod clone;
pub mod remove;
pub mod mv;
pub mod ls;
pub mod revert;
#[cfg(test)]
mod test;
pub mod error;

pub fn all_command_invocations() -> Vec<StaticSubcommand> {
    return vec![
        check::invocation(),
        info::invocation(),
        init::invocation(),
        record::invocation(),
        add::invocation(),
        pull::invocation(),
        push::invocation(),
        apply::invocation(),
        clone::invocation(),
        remove::invocation(),
        mv::invocation(),
        ls::invocation(),
        revert::invocation()
        ];
}

