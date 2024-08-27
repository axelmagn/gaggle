use futures::prelude::*;
use tarpc::{
    client, context,
    server::{self, Channel},
};

#[tarpc::service]
pub trait GaggleWorker {
    async fn call(thunk_id: uuid::Bytes, request: Vec<u8>) -> Vec<u8>;
}

#[tarpc::service]
trait World {
    async fn hello(name: String) -> String;
}

#[derive(Clone)]
struct HelloServer;

impl World for HelloServer {
    async fn hello(self, _: context::Context, name: String) -> String {
        format!("Hello, {}!", name)
    }
}

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    let (client_transport, server_transport) = tarpc::transport::channel::unbounded();

    let server = server::BaseChannel::with_defaults(server_transport);
    tokio::spawn(
        server
            .execute(HelloServer.serve())
            // Handle all requests concurrently.
            .for_each(|response| async move {
                tokio::spawn(response);
            }),
    );

    // WorldClient is generated by the #[tarpc::service] attribute. It has a constructor `new`
    // that takes a config and any Transport as input.
    let mut client = WorldClient::new(client::Config::default(), client_transport).spawn();

    // The client has an RPC method for each RPC defined in the annotated trait. It takes the same
    // args as defined, with the addition of a Context, which is always the first arg. The Context
    // specifies a deadline and trace information which can be helpful in debugging requests.
    let hello = client.hello(context::current(), "Stim".to_string()).await?;

    println!("{hello}");

    Ok(())
}
