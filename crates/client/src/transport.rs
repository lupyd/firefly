//! The WebSocket stream the connection loop runs on.
//!
//! Native builds use tokio-tungstenite. In the browser the same split sink/stream of
//! [`Message`]s is built on the DOM `WebSocket`, so the connection loop is shared. WebTransport
//! (quinn) is native-only.

#[cfg(not(target_arch = "wasm32"))]
pub use tokio_tungstenite::tungstenite::Message;

#[cfg(not(target_arch = "wasm32"))]
pub type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

#[cfg(target_arch = "wasm32")]
pub use browser::{Message, WsError, WsStream, connect};

#[cfg(target_arch = "wasm32")]
mod browser {
    use std::{
        pin::Pin,
        sync::Arc,
        task::{Context, Poll},
    };

    use bytes::Bytes;
    use firefly_core::utils::SendWrapper;
    use futures::{Sink, Stream};
    use tokio::sync::{mpsc, oneshot};
    use wasm_bindgen::{JsCast, closure::Closure};
    use web_sys::{BinaryType, CloseEvent, Event, MessageEvent, WebSocket};

    /// Mirrors the tungstenite variants the connection loop handles.
    #[derive(Debug, Clone)]
    pub enum Message {
        Text(String),
        Binary(Bytes),
        /// Browsers cannot send WebSocket pings; sending one is a no-op.
        Ping(Bytes),
        Pong(Bytes),
        Close(Option<String>),
    }

    #[derive(Debug)]
    pub struct WsError(String);

    impl std::fmt::Display for WsError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(&self.0)
        }
    }

    impl std::error::Error for WsError {}

    type Handlers = (
        Closure<dyn FnMut()>,
        Closure<dyn FnMut(MessageEvent)>,
        Closure<dyn FnMut(Event)>,
        Closure<dyn FnMut(CloseEvent)>,
    );

    /// Owns the socket and its event handlers; closing it when the last half is dropped.
    struct Socket {
        ws: SendWrapper<WebSocket>,
        _handlers: SendWrapper<Handlers>,
    }

    impl Drop for Socket {
        fn drop(&mut self) {
            let ws = &self.ws.0;
            ws.set_onopen(None);
            ws.set_onmessage(None);
            ws.set_onerror(None);
            ws.set_onclose(None);
            let _ = ws.close();
        }
    }

    pub struct WsStream {
        socket: Arc<Socket>,
        incoming: mpsc::UnboundedReceiver<Result<Message, WsError>>,
    }

    /// Opens a socket and resolves once it is connected.
    pub async fn connect(url: &str) -> Result<WsStream, WsError> {
        let ws = WebSocket::new(url).map_err(|err| WsError(format!("{err:?}")))?;
        ws.set_binary_type(BinaryType::Arraybuffer);

        let (tx, incoming) = mpsc::unbounded_channel();
        let (open_tx, open_rx) = oneshot::channel::<Result<(), WsError>>();
        let open_tx = Arc::new(std::sync::Mutex::new(Some(open_tx)));

        let on_open = {
            let open_tx = open_tx.clone();
            Closure::<dyn FnMut()>::new(move || {
                if let Some(open_tx) = open_tx.lock().unwrap().take() {
                    let _ = open_tx.send(Ok(()));
                }
            })
        };
        let on_message = {
            let tx = tx.clone();
            Closure::<dyn FnMut(MessageEvent)>::new(move |event: MessageEvent| {
                let data = event.data();
                let message = if let Ok(buffer) = data.clone().dyn_into::<js_sys::ArrayBuffer>() {
                    Message::Binary(Bytes::from(js_sys::Uint8Array::new(&buffer).to_vec()))
                } else if let Some(text) = data.as_string() {
                    Message::Text(text)
                } else {
                    return;
                };
                let _ = tx.send(Ok(message));
            })
        };
        let on_error = {
            let tx = tx.clone();
            let open_tx = open_tx.clone();
            // A WebSocket `error` is a plain Event with no detail; the following `close` event
            // carries the code.
            Closure::<dyn FnMut(Event)>::new(move |_event: Event| {
                let message = "websocket error".to_string();
                match open_tx.lock().unwrap().take() {
                    Some(open_tx) => {
                        let _ = open_tx.send(Err(WsError(message)));
                    }
                    None => {
                        let _ = tx.send(Err(WsError(message)));
                    }
                }
            })
        };
        let on_close = {
            let tx = tx.clone();
            let open_tx = open_tx.clone();
            Closure::<dyn FnMut(CloseEvent)>::new(move |event: CloseEvent| {
                let reason = format!("code {} {}", event.code(), event.reason());
                if let Some(open_tx) = open_tx.lock().unwrap().take() {
                    let _ = open_tx.send(Err(WsError(format!("websocket closed before opening: {reason}"))));
                }
                let _ = tx.send(Ok(Message::Close(Some(reason))));
            })
        };
        ws.set_onopen(Some(on_open.as_ref().unchecked_ref()));
        ws.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
        ws.set_onerror(Some(on_error.as_ref().unchecked_ref()));
        ws.set_onclose(Some(on_close.as_ref().unchecked_ref()));

        let socket = Arc::new(Socket {
            ws: SendWrapper(ws),
            _handlers: SendWrapper((on_open, on_message, on_error, on_close)),
        });

        match open_rx.await {
            Ok(Ok(())) => Ok(WsStream { socket, incoming }),
            Ok(Err(err)) => Err(err),
            Err(_) => Err(WsError("websocket dropped before opening".into())),
        }
    }

    impl WsStream {
        pub fn split(self) -> (WsSink, WsReceiver) {
            (
                WsSink { socket: self.socket.clone() },
                WsReceiver { _socket: self.socket, incoming: self.incoming },
            )
        }
    }

    pub struct WsSink {
        socket: Arc<Socket>,
    }

    impl Sink<Message> for WsSink {
        type Error = WsError;

        fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, item: Message) -> Result<(), WsError> {
            let ws = &self.socket.ws.0;
            if ws.ready_state() != WebSocket::OPEN {
                return Err(WsError("websocket is not open".into()));
            }
            let sent = match item {
                Message::Binary(bytes) => ws.send_with_u8_array(&bytes),
                Message::Text(text) => ws.send_with_str(&text),
                Message::Close(_) => ws.close(),
                Message::Ping(_) | Message::Pong(_) => Ok(()),
            };
            sent.map_err(|err| WsError(format!("{err:?}")))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
            let _ = self.socket.ws.0.close();
            Poll::Ready(Ok(()))
        }
    }

    pub struct WsReceiver {
        _socket: Arc<Socket>,
        incoming: mpsc::UnboundedReceiver<Result<Message, WsError>>,
    }

    impl Stream for WsReceiver {
        type Item = Result<Message, WsError>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.incoming.poll_recv(cx)
        }
    }
}
