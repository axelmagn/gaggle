use std::{
    any::{type_name, type_name_of_val, Any, TypeId},
    collections::HashMap,
    future::{self, Future},
    hash::{DefaultHasher, Hash, Hasher},
    sync::mpsc,
};

use anyhow::Context;
use clap::Parser;
use futures::StreamExt;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tarpc::{
    client, context,
    server::{self, incoming::Incoming, Channel},
    tokio_serde::formats::Json,
};
// use tokio::sync::mpsc;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
struct PortableTypeId {
    // Currently TypeId is represented by a u128 internally, so we could probably
    // just mem::transmute it to a UUID.  However this would couple it unnecessarily
    // to the internal implementation. Instead, we'll hash the TypeId as a whole
    // and store the result.
    type_id_hash: u64,
}

impl From<TypeId> for PortableTypeId {
    fn from(type_id: TypeId) -> Self {
        let mut s = DefaultHasher::new();
        type_id.hash(&mut s);
        Self {
            type_id_hash: s.finish(),
        }
    }
}

trait PortableData: Serialize + DeserializeOwned + 'static {}
impl<T> PortableData for T where T: Serialize + DeserializeOwned + 'static {}

struct DataCoder {
    encoder: Box<dyn Fn(&dyn Any) -> anyhow::Result<Vec<u8>>>,
    decoder: Box<dyn Fn(&[u8]) -> anyhow::Result<Box<dyn Any>>>,
}

impl DataCoder {
    fn new<T>() -> Self
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

    fn encode<T>(&self, val: &T) -> anyhow::Result<Vec<u8>>
    where
        T: PortableData,
    {
        (self.encoder)(val)
    }

    fn decode<T>(&self, data: &[u8]) -> anyhow::Result<Box<T>>
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
fn test_datacoder_roundtrip() {
    fn test_roundtrip<T>(val: T)
    where
        T: PortableData + PartialEq + std::fmt::Debug,
    {
        let coder = DataCoder::new::<T>();
        let data = (coder.encoder)(&val).unwrap();
        let decoded = (coder.decoder)(&data).unwrap();
        assert_eq!(*decoded.downcast::<T>().unwrap(), val);

        let data = coder.encode(&val).unwrap();
        let decoded = coder.decode::<T>(&data).unwrap();
        assert_eq!(*decoded, val);
    }

    test_roundtrip(10_i32);
    test_roundtrip(String::from("hello test"));
    test_roundtrip((10_i32, 20_i32));
}

impl<T> From<T> for DataCoder
where
    T: PortableData,
{
    fn from(val: T) -> Self {
        Self::new::<T>()
    }
}

struct PortableFnWrapper {
    type_id: PortableTypeId,
    type_name: &'static str, // for debugging
    arg_coder: DataCoder,
    output_coder: DataCoder,
    thunk: Box<dyn Fn(&dyn Any) -> anyhow::Result<Box<dyn Any>>>,
}

impl PortableFnWrapper {
    fn from_fn<F, I, O>(f: F) -> Self
    where
        F: Fn(&I) -> O + 'static,
        I: PortableData,
        O: PortableData,
    {
        let fn_type_id = TypeId::of::<F>().into();
        let fn_type_name = type_name::<F>();
        let args_coder = DataCoder::new::<I>();
        let output_coder = DataCoder::new::<O>();
        let thunk: Box<dyn Fn(&dyn Any) -> anyhow::Result<Box<dyn Any>>> =
            Box::new(move |data: &dyn Any| {
                let data = data.downcast_ref::<I>().with_context(|| {
                    format!(
                        "could not downcast to input type &{} (fn: {})",
                        type_name::<I>(),
                        type_name::<F>(),
                    )
                })?;
                let result = f(data);
                Ok(Box::new(result))
            });
        Self {
            type_id: fn_type_id,
            type_name: fn_type_name,
            arg_coder: args_coder,
            output_coder,
            thunk,
        }
    }

    fn call_raw(&self, arg_data: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        let arg = (self.arg_coder.decoder)(&arg_data)?;
        let output = (self.thunk)(&arg)?;
        let output_data = (self.output_coder.encoder)(&*output)?;
        Ok(output_data)
    }

    fn encode_arg<I>(&self, arg: &I) -> anyhow::Result<Vec<u8>>
    where
        I: PortableData,
    {
        let arg_data = (self.arg_coder.encoder)(arg)?;
        Ok(arg_data)
    }

    fn decode_output<O>(&self, data: &[u8]) -> anyhow::Result<Box<O>>
    where
        O: PortableData,
    {
        let out_dyn = (self.output_coder.decoder)(data)?;
        let out = out_dyn.downcast::<O>().map_err(|_| {
            anyhow::Error::msg(format!(
                "could not downcast to output type {} (fn: {})",
                type_name::<O>(),
                self.type_name,
            ))
        })?;
        Ok(out)
    }

    fn portable_call<I>(&self, arg: &I) -> anyhow::Result<PortableFnCall>
    where
        I: PortableData,
    {
        let arg_data = self.encode_arg(arg)?;
        Ok(PortableFnCall {
            type_id: self.type_id,
            arg_data,
        })
    }
}

// impl<F, I, O> From<F> for PortableFnWrapper
// where
//     F: Fn(&I) -> O + 'static,
//     I: PortableData,
//     O: Serialize + DeserializeOwned,
// {
//     fn from(f: F) -> Self {
//         Self::from_fn(f)
//     }
// }

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
struct PortableFnCall {
    type_id: PortableTypeId,
    arg_data: Vec<u8>,
}

impl PortableFnCall {
    fn new<F, I, O>(_f: F, args: &I) -> anyhow::Result<Self>
    where
        F: Fn(&I) -> O + 'static,
        I: PortableData,
        O: PortableData,
    {
        let type_id = TypeId::of::<F>().into();
        let arg_data = serde_json::to_vec(args)?;
        Ok(Self { type_id, arg_data })
    }
}

struct PortableFnRegistry {
    thunks: HashMap<PortableTypeId, PortableFnWrapper>,
}

impl PortableFnRegistry {
    fn new() -> Self {
        Self {
            thunks: HashMap::new(),
        }
    }

    fn register<F, I, O>(&mut self, f: F)
    where
        F: Fn(&I) -> O + 'static,
        I: PortableData,
        O: PortableData,
    {
        let thunk = PortableFnWrapper::from_fn(f);
        self.thunks.insert(thunk.type_id, thunk);
    }

    fn call_raw(&self, call: PortableFnCall) -> anyhow::Result<Vec<u8>> {
        let thunk = self
            .thunks
            .get(&call.type_id)
            .with_context(|| format!("no function registered for type_id {:?}", call.type_id))?;
        thunk.call_raw(call.arg_data)
    }

    fn get<F>(&self, _f: F) -> Option<&PortableFnWrapper>
    where
        F: 'static,
    {
        let type_id = TypeId::of::<F>().into();
        self.thunks.get(&type_id)
    }
}

fn add(args: &(i32, i32)) -> i32 {
    args.0 + args.1
}

fn mult(args: &(i32, i32)) -> i32 {
    args.0 * args.1
}

fn create_registry() -> PortableFnRegistry {
    // let add_wrapper = PortableFnWrapper::from_static_fn(&(add as fn(_) -> _));
    // let mutl_wrapper = PortableFnWrapper::from_static_fn(&(add as fn(_) -> _));
    let mut registry = PortableFnRegistry::new();
    registry.register(&add);
    registry.register(&mult);
    registry
}

#[tarpc::service]
trait PortableFnService {
    async fn call(call: PortableFnCall) -> Result<Vec<u8>, String>;
}

#[derive(Clone)]
struct PortableFnServer {
    registry_tx: mpsc::Sender<(mpsc::Sender<anyhow::Result<Vec<u8>>>, PortableFnCall)>,
}

impl PortableFnService for PortableFnServer {
    async fn call(
        self,
        _context: ::tarpc::context::Context,
        call: PortableFnCall,
    ) -> Result<Vec<u8>, String> {
        let (tx, rx) = mpsc::channel();
        self.registry_tx
            .send((tx, call))
            .map_err(|e| e.to_string())?;

        let resp_opt = rx.recv();

        match resp_opt {
            Ok(resp) => resp.map_err(|e| e.to_string()),
            Err(e) => Err(e.to_string()),
        }
    }
}

async fn run_server() -> anyhow::Result<()> {
    let server_addr = ("0.0.0.0", 50051);
    println!("Running server: {:?}", server_addr);
    let mut listener = tarpc::serde_transport::tcp::listen(server_addr, Json::default).await?;

    listener.config_mut().max_frame_length(usize::MAX);

    async fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
        tokio::spawn(fut);
    }

    let (registry_tx, registry_rx) = mpsc::channel();

    // registry runs as a dedicated thread.
    std::thread::spawn(move || {
        let registry = create_registry();
        loop {
            let (tx, call): (mpsc::Sender<anyhow::Result<Vec<u8>>>, PortableFnCall) = registry_rx
                .recv()
                .expect("registry_rx channel closed unexpectedly");
            println!("Received call: {:?}", call);
            let resp = registry.call_raw(call);
            println!("Response: {:?}", resp);
            tx.send(resp).expect("tx channel closed unexpectedly");
        }
    });

    listener
        .filter_map(|r| future::ready(r.ok()))
        .map(server::BaseChannel::with_defaults)
        // limit channels to 1 per IP
        .max_channels_per_key(1, |t| t.as_ref().peer_addr().unwrap().ip())
        // serve is generated by the service attribute
        .map(|channel| {
            let pdserver = PortableFnServer {
                registry_tx: registry_tx.clone(),
            };
            channel.execute(pdserver.serve()).for_each(spawn)
        })
        .buffer_unordered(16)
        .for_each(|_| async {})
        .await;

    Ok(())
}

async fn run_client() -> anyhow::Result<()> {
    let server_addr = ("127.0.0.1", 50051);
    println!("Running client: {:?}", server_addr);

    let mut transport = tarpc::serde_transport::tcp::connect(server_addr, Json::default);
    transport.config_mut().max_frame_length(usize::MAX);

    let registry = create_registry();
    let client = PortableFnServiceClient::new(client::Config::default(), transport.await?).spawn();
    let portable_fn = registry.get(&add).expect("function not found");
    let portable_call = portable_fn.portable_call(&(2, 3))?;
    println!("Portable Call: {:?}", portable_call);
    let result_data = client
        .call(context::current(), portable_call)
        .await
        .expect("could not complete client call")
        .expect("server returned error");
    println!("Result Data: {:?}", result_data);
    let result = portable_fn
        .decode_output::<i32>(&result_data)
        .expect("could not decode result");
    println!("Result: {}", result);

    Ok(())
}

#[derive(clap::Parser)]
struct Args {
    #[clap(short, long, default_value_t = false)]
    server: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    if args.server {
        run_server().await?;
    } else {
        run_client().await?;
    }

    Ok(())
}
