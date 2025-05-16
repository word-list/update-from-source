use aws_config::BehaviorVersion;
use aws_lambda_events::event::sqs::SqsEvent;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Client as DynamoClient;
use aws_sdk_sqs::Client as SqsClient;
use futures::future::join_all;
use futures::TryStreamExt;
use lambda_runtime::{Error, LambdaEvent};
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use std::env;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_util::io::StreamReader;

#[derive(Deserialize)]
struct UpdateFromSourceMessage {
    id: String,
    force: bool,
}

#[derive(Serialize)]
struct QueryWordMessage {
    word: String,
    force: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    #[error("DynamoDB service error: {0}")]
    ServiceError(String),

    #[error("URL not found in DynamoDB")]
    NotFound,
}

static CONFIG: OnceCell<aws_config::SdkConfig> = OnceCell::new();
static DYNAMO_CLIENT: OnceCell<DynamoClient> = OnceCell::new();
static SQS_CLIENT: OnceCell<SqsClient> = OnceCell::new();

pub(crate) async fn init_aws() {
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    CONFIG.set(config).expect("Failed to set AWS config");

    let dynamo_client = DynamoClient::new(CONFIG.get().unwrap());
    DYNAMO_CLIENT
        .set(dynamo_client)
        .expect("Failed to set DynamoDB client");

    let sqs_client = SqsClient::new(CONFIG.get().unwrap());
    SQS_CLIENT
        .set(sqs_client)
        .expect("Failed to set SQS client");
}

async fn fetch_url_from_dynamodb(id: &str) -> Result<String, FetchError> {
    let table_name = env::var("DYNAMODB_TABLE_NAME").expect("DYNAMODB_TABLE_NAME must be set");

    let response = DYNAMO_CLIENT
        .get()
        .unwrap()
        .get_item()
        .table_name(table_name)
        .key("id", AttributeValue::S(id.to_string()))
        .send()
        .await
        .map_err(|e| FetchError::ServiceError(format!("AWS SDK error: {}", e)))?;

    let url = response
        .item
        .as_ref()
        .and_then(|item| item.get("url"))
        .and_then(|url_attr| match url_attr {
            AttributeValue::S(url) => Some(url.clone()),
            _ => None,
        });

    url.ok_or(FetchError::NotFound)
}

async fn process_text_file(url: String, force: bool) -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let response = client.get(&url).send().await?;

    let stream = response
        .bytes_stream()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));
    let async_reader = StreamReader::new(stream);
    let reader = BufReader::new(async_reader);

    let mut lines = reader.lines();

    let queue_url = env::var("SQS_QUEUE_URL")?;

    while let Some(line) = lines.next_line().await? {
        if !line.contains(' ') && !line.contains('\t') {
            let msg = QueryWordMessage {
                word: line,
                force: force,
            };

            SQS_CLIENT
                .get()
                .unwrap()
                .send_message()
                .queue_url(&queue_url)
                .message_body(serde_json::to_string(&msg)?)
                .send()
                .await?;
        }
    }

    Ok(())
}

pub(crate) async fn function_handler(event: LambdaEvent<SqsEvent>) -> Result<(), Error> {
    let futures = event
        .payload
        .records
        .iter()
        .filter_map(|record| record.body.as_deref())
        .filter_map(|body| serde_json::from_str::<UpdateFromSourceMessage>(body).ok())
        .map(|msg| async move {
            println!("updating from source: {:?}", msg.id);
            let url = fetch_url_from_dynamodb(&msg.id).await?;
            process_text_file(url, msg.force).await?;
            Ok::<(), Box<dyn std::error::Error>>(())
        });

    let results = join_all(futures).await;
    for result in results {
        if let Err(err) = result {
            eprintln!("Error processing message: {:?}", err);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use lambda_runtime::{Context, LambdaEvent};

    #[tokio::test]
    async fn test_event_handler() {
        let event = LambdaEvent::new(SqsEvent::default(), Context::default());
        let response = function_handler(event).await.unwrap();
        assert_eq!((), response);
    }
}
