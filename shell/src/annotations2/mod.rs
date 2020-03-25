use super::config;
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

/// Parsing and building annotations.
pub mod annotation_parser;
/// Object to store annotated commands.
pub mod argument_matcher;
/// Builds parser for command line syntax for a single command.
pub mod cmd_parser;
/// Defines command line syntax.
pub mod grammar;
/// Parser to match command line with any of the annotations.
pub mod parser;
