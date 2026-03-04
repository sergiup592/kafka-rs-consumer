use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use kafka_rs_consumer::{
    error::HandlerError,
    handler::{JsonHandler, JsonMessageHandlerAdapter, Message, MessageHandler, MessageMetadata},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TestPayload {
    id: u64,
    name: String,
}

type RecordedCall = (Option<Vec<u8>>, TestPayload, MessageMetadata);

#[derive(Debug, Error)]
#[error("json handler error")]
struct JsonHandlerError;

#[derive(Clone, Default)]
struct RecordingJsonHandler {
    calls: Arc<Mutex<Vec<RecordedCall>>>,
}

#[async_trait]
impl JsonHandler<TestPayload> for RecordingJsonHandler {
    type Error = JsonHandlerError;

    async fn handle(
        &self,
        key: Option<Vec<u8>>,
        value: TestPayload,
        metadata: MessageMetadata,
    ) -> Result<(), Self::Error> {
        self.calls
            .lock()
            .expect("mutex poisoned")
            .push((key, value, metadata));
        Ok(())
    }
}

#[tokio::test]
async fn json_handler_adapter_deserializes_payload_and_passes_metadata() {
    let handler = RecordingJsonHandler::default();
    let calls_handle = handler.calls.clone();
    let adapter = JsonMessageHandlerAdapter::<RecordingJsonHandler, TestPayload>::new(handler);

    let payload = serde_json::to_vec(&TestPayload {
        id: 42,
        name: "order-created".to_string(),
    })
    .expect("json should serialize");

    let mut headers = HashMap::new();
    headers.insert("source".to_string(), b"test".to_vec());

    let message = Message {
        key: Some(b"k1".to_vec()),
        payload: Some(payload),
        topic: "events".to_string(),
        partition: 1,
        offset: 9,
        timestamp: Some(1700000000),
        headers,
    };

    adapter
        .handle(message)
        .await
        .expect("handler should succeed");

    let calls = calls_handle.lock().expect("mutex poisoned");

    assert_eq!(calls.len(), 1);
    let (key, value, metadata) = &calls[0];

    assert_eq!(key.as_ref().expect("key must exist"), b"k1");
    assert_eq!(
        value,
        &TestPayload {
            id: 42,
            name: "order-created".to_string()
        }
    );
    assert_eq!(metadata.topic, "events");
    assert_eq!(metadata.partition, 1);
    assert_eq!(metadata.offset, 9);
}

#[tokio::test]
async fn json_handler_adapter_errors_on_missing_payload() {
    let handler = RecordingJsonHandler::default();
    let adapter = JsonMessageHandlerAdapter::<RecordingJsonHandler, TestPayload>::new(handler);

    let message = Message {
        key: None,
        payload: None,
        topic: "events".to_string(),
        partition: 0,
        offset: 0,
        timestamp: None,
        headers: HashMap::new(),
    };

    let error = adapter
        .handle(message)
        .await
        .expect_err("missing payload should fail");

    assert!(matches!(error, HandlerError::MissingPayload));
}
