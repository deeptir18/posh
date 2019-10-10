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
