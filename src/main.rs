use lambda_runtime::{run, service_fn, tracing, Error};

mod event_handler;
use event_handler::{function_handler, init_aws};

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing::init_default_subscriber();

    // Per-server init tasks
    init_aws().await;

    // Run the service
    run(service_fn(function_handler)).await
}
