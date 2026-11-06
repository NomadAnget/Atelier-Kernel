//! 传输层：命名端点往返、跨语言打开方式、出口有界与背压、超大响应。

#[path = "cases/transport/endpoint.rs"]
mod endpoint;
#[path = "cases/transport/pipe_client.rs"]
mod pipe_client;
#[path = "cases/transport/outbox.rs"]
mod outbox;
#[path = "cases/transport/large_response.rs"]
mod large_response;
