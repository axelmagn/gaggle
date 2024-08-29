use std::any::{type_name, Any};

use anyhow::Context;
use serde::{de::DeserializeOwned, Serialize};

pub trait PortableData: Serialize + DeserializeOwned + Any {}
impl<T> PortableData for T where T: Serialize + DeserializeOwned + Any {}

pub struct DataTable {
    encoder: Box<dyn Fn(&dyn Any) -> anyhow::Result<Vec<u8>>>,
    decoder: Box<dyn Fn(&[u8]) -> anyhow::Result<Box<dyn Any>>>,
}

impl DataTable {
    pub fn new<T>() -> Self
    where
        T: PortableData,
    {
        Self {
            encoder: Box::new(|val: &dyn Any| {
                let val: &T = val
                    .downcast_ref::<T>()
                    .with_context(|| format!("could not downcast to `{}`", type_name::<T>()))?;
                let out: Vec<u8> = serde_json::to_vec(val)?;
                Ok(out)
            }),
            decoder: Box::new(|data: &[u8]| {
                let val: T = serde_json::from_slice(data)?;
                Ok(Box::new(val))
            }),
        }
    }
    pub fn encode<T>(&self, val: &T) -> anyhow::Result<Vec<u8>>
    where
        T: PortableData,
    {
        (self.encoder)(val)
    }

    pub fn decode<T>(&self, data: &[u8]) -> anyhow::Result<Box<T>>
    where
        T: PortableData,
    {
        let val = (self.decoder)(data)?;
        val.downcast::<T>().map_err(|_| {
            anyhow::Error::msg(format!("could not downcast to `{}`", type_name::<T>(),))
        })
    }
}
#[test]
fn test_data_table_roundtrip() {
    fn test_roundtrip<T>(val: T)
    where
        T: PortableData + PartialEq + std::fmt::Debug,
    {
        let data_table = DataTable::new::<T>();
        let data = data_table.encode(&val).unwrap();
        let decoded = data_table.decode::<T>(&data).unwrap();
        assert_eq!(*decoded, val);
    }

    test_roundtrip(10_i32);
    test_roundtrip(String::from("hello test"));
    test_roundtrip((10_i32, 20_i32));
}
