use anyhow::{Result, anyhow, bail};

use futures_util::{SinkExt, StreamExt};

use crate::BroadcastMessage;
use crate::stop::Stop;
use log::{debug, info, warn};
use pipeweaver_ipc::commands::DaemonRequest::GetStatus;
use pipeweaver_ipc::commands::{
    DaemonRequest, DaemonResponse, DaemonStatus, WebsocketRequest, WebsocketResponse,
};
use serde_json::Value;
use std::io::ErrorKind;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::{broadcast, mpsc};
use tokio_tungstenite::tungstenite::{Message, Utf8Bytes};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite};

type WebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub(crate) struct PipeweaverHandler {
    message_receiver: mpsc::Receiver<DaemonRequest>,
    change_broadcast: broadcast::Sender<BroadcastMessage>,

    stopper: Stop,
    has_connected: bool,

    command_index: u64,
    raw_status: Value,
    status: DaemonStatus,
}

impl PipeweaverHandler {
    pub fn new(
        broadcast: broadcast::Sender<BroadcastMessage>,
        receiver: mpsc::Receiver<DaemonRequest>,
        stopper: Stop,
    ) -> Self {
        Self {
            message_receiver: receiver,
            change_broadcast: broadcast,

            has_connected: false,
            stopper,

            command_index: 0,
            raw_status: Value::Null,
            status: DaemonStatus::default(),
        }
    }

    pub async fn run_handler(&mut self) {
        info!("Starting Pipeweaver Manager");
        let url = "ws://localhost:14565/api/websocket";

        // We need to handle this in a loop, if something goes bad just make sure we're disconnencted
        // and try again after 5 seconds,
        'connect: while let Err(e) = self.handle_connection(url).await {
            if self.has_connected {
                // We were connected, now we're not connected, broadcast this.
                self.has_connected = false;
                let _ = self.change_broadcast.send(BroadcastMessage::Offline);
            }

            // We only suppress 'Connection Refused' errors, as they're expected to happen
            let is_connection_refused = e
                .downcast_ref::<tungstenite::Error>()
                .and_then(|e| {
                    if let tungstenite::Error::Io(io) = e {
                        Some(io)
                    } else {
                        None
                    }
                })
                .map(|io| io.kind() == ErrorKind::ConnectionRefused)
                .unwrap_or(false);

            if !is_connection_refused {
                warn!("Pipeweaver Error: {}", e);
            }

            let mut ticker = tokio::time::interval(Duration::from_secs(5));

            // Create a loop which handles things like incoming messages and stopping
            loop {
                select! {
                    Some(message) = self.message_receiver.recv() => {
                        warn!("Message received while disconnected: {:?}", message);
                    }
                    _ = self.stopper.recv() => {
                        warn!("Stopping Pipeweaver Manager");
                        break 'connect;
                    }
                    _ = ticker.tick() => {
                        // 5 Seconds have elapsed, break this loop to reconnect
                        continue 'connect;
                    }
                }
            }
        }
    }

    async fn handle_connection(&mut self, url: &str) -> Result<()> {
        let (mut stream, _) = connect_async(url).await?;
        info!("Successfully connected to Pipeweaver");

        if !self.has_connected {
            self.has_connected = true;
            let _ = self.change_broadcast.send(BroadcastMessage::Online);
        }

        self.load_status(&mut stream).await?;
        self.run_message_loop(&mut stream).await?;

        Ok(())
    }

    async fn load_status(&mut self, stream: &mut WebSocket) -> Result<()> {
        // Perform the Initial Status Fetch
        let status_id = self.get_command_index();

        let status_request = serde_json::to_string(&WebsocketRequest {
            id: status_id,
            data: GetStatus,
        })?;

        let message = Message::Text(Utf8Bytes::from(status_request));
        if let Err(e) = stream.send(message).await {
            bail!("Failed to fetch Status: {}", e)
        }

        // There are occasionally patch messages which could occur before the status response,
        // so we'll loop here until we get the answer we're looking for
        loop {
            let message = stream.next().await;
            match message {
                None => {
                    bail!("Pipeweaver closed the connection");
                }
                Some(Err(e)) => {
                    bail!("Failed to read message from Pipeweaver: {}", e);
                }
                Some(Ok(message)) => {
                    if message.is_close() {
                        bail!("Pipeweaver closed the connection");
                    }

                    if let Message::Text(msg) = message {
                        let value = serde_json::from_str::<Value>(msg.as_str())?;

                        // This should be a WebSocketResponse object
                        let object = value.as_object().ok_or(anyhow!("Failed to Read Object"))?;

                        // Check the ID (should always be present)
                        let id = object.get("id").ok_or(anyhow!("Failed to Read ID"))?;

                        // We can occasionally get patches before the Status response, so verify the ID...
                        if id.as_u64().ok_or(anyhow!("Unable to Parse id"))? == status_id {
                            // This is our DaemonStatus response
                            let error = anyhow!("Failed to Read Data");
                            let data = object.get("data").ok_or(error)?.clone();

                            let error = anyhow!("Failed to Read Status");
                            self.raw_status = data.get("Status").ok_or(error)?.clone();

                            let raw = self.raw_status.clone();
                            self.status = serde_json::from_value::<DaemonStatus>(raw)?;
                            let _ = self
                                .change_broadcast
                                .send(BroadcastMessage::Status(Box::new(self.status.clone())));

                            break;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn run_message_loop(&mut self, stream: &mut WebSocket) -> Result<()> {
        info!("Starting Pipeweaver Message Loop");
        loop {
            select! {
                // Check for messages from Pipeweaver
                message = stream.next() => {
                    match message {
                        None => {
                            bail!("Pipeweaver closed the connection");
                        }
                        Some(Err(e)) => {
                            bail!("Failed to read message from Pipeweaver: {}", e);
                        }

                        Some(Ok(message)) => {
                            if message.is_close() {
                                bail!("Pipeweaver closed the connection");
                            }

                            if let Message::Text(message) = message {
                                let result = serde_json::from_str::<WebsocketResponse>(message.as_str())?;
                                match result.data {
                                    DaemonResponse::Err(e) => {
                                        warn!("Error from Pipeweaver: {}", e);
                                    }
                                    DaemonResponse::Patch(patch) => {
                                        // Update the raw status for the change
                                        json_patch::patch(&mut self.raw_status, &patch)?;
                                        self.status = serde_json::from_value::<DaemonStatus>(self.raw_status.clone())?;
                                        let _ = self.change_broadcast.send(BroadcastMessage::Status(Box::new(self.status.clone())));
                                    }
                                    _ => {}
                                }
                            } else {
                                bail!("Received non-text message from Websocket!")
                            }
                        }
                    }
                },

                // Check whether a message has been sent to the handler
                Some(message) = self.message_receiver.recv() => {
                    let command = serde_json::to_string(&WebsocketRequest {
                        id: self.get_command_index(),
                        data: message,
                    })?;
                    stream.send(Message::Text(Utf8Bytes::from(command))).await?;
                }

                // Check whether we should stop
                _ = self.stopper.recv() => {
                    debug!("Stopping Pipeweaver Message Loop");
                    return Ok(());
                }
            }
        }
    }

    fn get_command_index(&mut self) -> u64 {
        let result = self.command_index;
        self.command_index += 1;
        result
    }
}
