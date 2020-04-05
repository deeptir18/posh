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
/// Caches full paths to save calls to stat.
pub mod filecache;
/// How do we query for file sizes?
pub mod filesize;
/// Information about where certain files are located and speeds between various machines.
pub mod network;
