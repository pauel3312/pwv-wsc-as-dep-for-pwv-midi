use anyhow::Result;
use pipeweaver_ipc::commands::{APICommand, DaemonRequest, DaemonStatus};
use pipeweaver_shared::Mix;
use pipeweaver_websocket_client::{BroadcastMessage, spawn_pipeweaver_handler};
use rand::RngExt;
use rand::seq::IteratorRandom;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use tokio::{signal, time};
use ulid::Ulid;

/// A small app which will grab a random virtual source, and adjust its volume every 10 seconds

#[tokio::main]
async fn main() -> Result<()> {
    // Create a channel for broadcasting changes
    let (broadcast, _) = broadcast::channel(10);

    // Create a subscription for the broadcast channel
    let mut subscription = broadcast.subscribe();

    // Create a channel for sending commands
    let (tx, rx) = mpsc::channel(10);

    // Spawn up the Pipeweaver handler, which will return a way to stop it.
    let stopper = spawn_pipeweaver_handler(rx, broadcast.clone()).await;

    // --------
    // At this point, the pipeweaver handler is spawned and running, all that's needed is to
    // handle monitoring the broadcast channel for changes.
    // --------

    // Toggle for whether we can send messages to Pipeweaver
    let mut can_send_message = false;

    // Some variables to keep track of the current state
    let mut channel: Option<Ulid> = None;
    let mut daemon_status: Option<DaemonStatus> = None;
    let mut last_volume = 0;

    // A 5 second timer
    let mut tick = time::interval(Duration::from_secs(5));

    loop {
        tokio::select! {
            Ok(message) = subscription.recv() => {
                match message {
                    BroadcastMessage::Online => {
                        println!("Connected to Pipeweaver");
                        can_send_message = true;
                    },
                    BroadcastMessage::Offline => {
                        println!("Connection to Pipeweaver lost, reconnecting in 5 seconds...");
                        can_send_message = false;
                        daemon_status = None;
                    }
                    BroadcastMessage::Status(status) => {
                        // Is this the first time we've seen the status since connecting?
                        if daemon_status.is_none() {
                            // Lets find a random virtual source
                            let devs = &status.audio.profile.devices.sources.virtual_devices;
                            let dev = devs.iter().choose(&mut rand::rng()).unwrap();

                            // This is the channel we're changing
                            println!("Using Virtual Device: {}", dev.description.name);
                            channel = Some(dev.description.id);
                            last_volume = dev.volumes.volume[Mix::A];
                        }

                        // Status has updated, check if our monitored volume has changed
                        if let Some(id) = channel {
                            if let Some(dev) = status.audio.profile.devices.sources.virtual_devices.iter().find(|d| d.description.id == id) {
                                if dev.volumes.volume[Mix::A] != last_volume {
                                    let new_volume = dev.volumes.volume[Mix::A];
                                    println!("Channel Volume Changed, Old: {}, New: {}", last_volume, new_volume);
                                    last_volume = new_volume;
                                }
                            } else {
                                eprintln!("Virtual Device is gone! Picking a new one..");
                                let devs = &status.audio.profile.devices.sources.virtual_devices;
                                let dev = devs.iter().choose(&mut rand::rng()).unwrap();

                                // This is the channel we're changing
                                println!("Using Virtual Device: {}", dev.description.name);
                                channel = Some(dev.description.id);
                                last_volume = dev.volumes.volume[Mix::A];
                            }
                        }

                        daemon_status = Some(*status);
                    }
                }
            }

            _ = tick.tick() => {
                if can_send_message {
                    if let Some(id) = channel {
                        let n: u8 = rand::rng().random_range(0..=100);
                        println!("Setting Volume to: {}", n);

                        let message = DaemonRequest::Pipewire(APICommand::SetSourceVolume(id, Mix::A, n));
                        let _ = tx.send(message).await;
                    }
                }
            }

            _ = signal::ctrl_c() => {
                println!("Stopping Pipeweaver Manager");
                stopper.trigger();
                break;
            }
        }
    }

    Ok(())
}
