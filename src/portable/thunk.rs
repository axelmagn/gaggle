use std::any::{self, Any};
use std::fmt::Debug;

use super::{data::PortableData, types::TypeId};

pub struct FnTable {
    input_type: TypeId,
    output_type: TypeId,
    thunk: Box<dyn Fn(&dyn Any) -> anyhow::Result<Box<dyn Any>>>,
}

impl FnTable {
    pub fn from_fn<F, I, O>(f: F) -> Self
    where
        F: Fn(&I) -> O + Any,
        I: Any,
        O: Any,
    {
        let thunk: Box<dyn Fn(&dyn Any) -> anyhow::Result<Box<dyn Any>>> =
            Box::new(move |input: &dyn Any| {
                let input = input.downcast_ref::<I>().ok_or_else(|| {
                    anyhow::anyhow!("input downcast to `{}` failed", any::type_name::<I>())
                })?;
                let output = f(input);
                let output = Box::new(output);
                Ok(Box::new(output))
            });

        Self {
            input_type: any::TypeId::of::<I>().into(),
            output_type: any::TypeId::of::<O>().into(),
            thunk,
        }
    }

    pub fn call<I, O>(&self, input: &I) -> anyhow::Result<O>
    where
        I: Any,
        O: Any,
    {
        let output = (self.thunk)(input)?;
        let output = output.downcast::<O>().map_err(|_e| {
            anyhow::anyhow!("output downcast to `{}` failed", any::type_name::<O>())
        })?;
        Ok(*output)
    }
}

#[test]
fn test_fn_table_call() {
    fn test_call<F, I, O>(f: F, inputs: &[I], expected: &[O])
    where
        F: Fn(&I) -> O + Any,
        I: PortableData + PartialEq + Debug,
        O: PortableData + PartialEq + Debug,
    {
        let fn_table = FnTable::from_fn(f);
        for i in 0..inputs.len() {
            let input = &inputs[i];
            let expected = &expected[i];
            let output = fn_table.call::<I, O>(&input).unwrap();
            assert_eq!(output, *expected);
        }
    }

    test_call(|x: &i32| x + 1, &[1, 2, 3], &[2, 3, 4]);
}
