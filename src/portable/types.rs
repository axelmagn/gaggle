use serde::{Deserialize, Serialize};
use std::{
    any::{self, Any},
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
};

use super::{data::DataTable, thunk::FnTable};

/// A portable TypeId.
///
/// This is shared between workers running the same binary, acting as a portable
/// identifier for shared code.
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TypeId {
    // Currently any::TypeId is represented by a u128 internally, so we could
    // probably just mem::transmute it to a UUID.  However this would couple it
    // unnecessarily to the internal implementation. Instead, we'll hash the
    // any::TypeId as a whole and store the result.
    inner_hash: u64,
}

impl TypeId {
    pub fn of<T: Any>() -> Self {
        any::TypeId::of::<T>().into()
    }
}

impl From<any::TypeId> for TypeId {
    fn from(type_id: any::TypeId) -> Self {
        let mut s = DefaultHasher::new();
        type_id.hash(&mut s);
        Self {
            inner_hash: s.finish(),
        }
    }
}

#[test]
fn test_type_id_is_unique() {
    let str_type_id: TypeId = any::TypeId::of::<String>().into();
    let i32_type_id: TypeId = any::TypeId::of::<i32>().into();
    let vec_u8_type_id: TypeId = any::TypeId::of::<Vec<u8>>().into();
    let vec_b_type_id: TypeId = any::TypeId::of::<Vec<bool>>().into();
    let type_ids = [str_type_id, i32_type_id, vec_u8_type_id, vec_b_type_id];
    for i in 0..type_ids.len() - 1 {
        for j in i + 1..type_ids.len() {
            if i == j {
                assert_eq!(type_ids[i], type_ids[j]);
            } else {
                assert_ne!(type_ids[i], type_ids[j]);
            }
        }
    }
}

/// A generic wrapper that negotiates between the capabilities of a specific
/// type and its portable representations.
///
struct TypeTable {
    type_id: TypeId,
    /// Type name for debugging purposes.
    type_name: &'static str,
    data_table: Option<DataTable>,
    fn_table: Option<FnTable>,
}

impl TypeTable {
    fn new<T: Any>() -> Self {
        Self {
            type_id: TypeId::of::<T>(),
            type_name: any::type_name::<T>(),
            data_table: None,
            fn_table: None,
        }
    }
}

struct TypeIndex {
    types: HashMap<TypeId, TypeTable>,
}

impl TypeIndex {}
