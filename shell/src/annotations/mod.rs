macro_rules! named_complete {
    ($name:ident<$t:ty>, $submac:ident!( $($args:tt)* )) => (
        fn $name( i: nom::types::CompleteByteSlice ) -> nom::IResult<nom::types::CompleteByteSlice, $t, u32> {
            $submac!(i, $($args)*)
        }
    );
    (pub $name:ident<$t:ty>, $submac:ident!( $($args:tt)* )) => (
        pub fn $name( i: nom::types::CompleteByteSlice ) -> nom::IResult<nom::types::CompleteByteSlice, $t, u32> {
            $submac!(i, $($args)*)
        }
    )
}
pub mod annotation_parser;
pub mod fileinfo;
pub mod grammar;
pub mod interpreter;
pub mod parser;
pub mod shell_interpreter;
pub mod shell_parse;

// how do we do this shell parsing thing?
// it seems that you can't just blindly go for text in between the pipes
// Because there could be subcommands
// There might be stdin directives
// What's a good general algorithm for parsing this?
//
// ALGORITHM: parse any subcommands:
//  e.g. look for anything specified between ().
//  Could make the requirement that you cannot combine characters with others unless it's some
//  sort of argument (e.g. this is just a prototype)
//  Then look for split by pipe and stdout and stderr directives
//  Then can parse each subcommand
//  So need to have some datastructure that represents this linked list of subcommands
//  then parse each subcommand, and link them back together
//  So need to parse these subshells first
//  But then how do we represent them?
//      Just the normal node graph where we create
//      It's nice to have a separate datastructure as the result of the shell parsing
