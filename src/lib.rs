use crate::socket_handler::PipeweaverHandler;
use crate::stop::Stop;
use log::warn;
use pipeweaver_ipc::client::Client;
use pipeweaver_ipc::clients::web::web_client::WebClient;
use pipeweaver_ipc::commands::{DaemonCommand, DaemonRequest, DaemonResponse, DaemonStatus};
use tokio::runtime::Runtime;
use tokio::sync::{broadcast, mpsc};
use tokio::task;

mod socket_handler;
mod stop;

#[derive(Clone)]
pub enum BroadcastMessage {
    Online,
    Offline,
    Status(Box<DaemonStatus>),
}

pub async fn spawn_pipeweaver_handler(
    receiver: mpsc::Receiver<DaemonRequest>,
    broadcast: broadcast::Sender<BroadcastMessage>,
) -> Stop {
    let stop = Stop::new();

    let mut handler = PipeweaverHandler::new(broadcast.clone(), receiver, stop.clone());
    task::spawn(async move {
        handler.run_handler().await;
    });

    stop
}

// Simple method that checks whether pipeweaver is running, and if so, launches the UI
pub fn launch_pipeweaver_ui() -> bool {
    let rt = Runtime::new().unwrap();
    rt.block_on(async move {
        let mut client = WebClient::new(String::from("http://localhost:14565/api/command"));

        let message = DaemonRequest::Daemon(DaemonCommand::OpenInterface);
        if let Ok(result) = client.send(&message).await {
            return match result {
                DaemonResponse::Ok => true,
                DaemonResponse::Err(e) => {
                    warn!("Failed to Connect to Pipeweaver: {}", e);
                    false
                }
                _ => false,
            };
        }
        false
    })
}
