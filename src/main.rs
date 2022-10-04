use std::env;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use log::{debug, error, info, trace, warn};
use rumqttc::ConnectionError::MqttState;
use rumqttc::{Event, EventLoop, Incoming, MqttOptions, Outgoing, QoS, StateError};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tokio::time::{sleep, timeout};

#[derive(Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
struct Device {
    actual_temp: String,
    device_id: i64,
    heat_on: bool,
    set_temp: String,
    zone_name: String,
}

#[derive(Deserialize)]
struct LiveData {
    #[serde(rename = "HUB_TIME", with = "time::serde::timestamp")]
    hub_time: OffsetDateTime,
    devices: Vec<Device>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MqttMessage<'s> {
    /// the date/time I believe to be true
    #[serde(with = "time::serde::rfc3339")]
    publish_time: OffsetDateTime,

    /// the date/time the hub believes to be true
    #[serde(with = "time::serde::rfc3339")]
    hub_time: OffsetDateTime,

    /// identifier for the hub (it's mac address)
    hub_id: &'s str,

    /// index of the device we're reporting data for
    device_idx: i64,

    /// where the device thinks it is
    zone_name: &'s str,

    /// what the device thinks the temperature is
    temp_actual: f64,

    /// what temperature the device is targeting
    temp_set: f64,

    /// whether the device is calling for heat
    heat_on: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    pretty_env_logger::init_timed();

    let mut neohub = neohub::Client::from_env().context("loading neohub client")?;
    debug!("connecting to hub");
    let hub_id = neohub
        .identify()
        .await
        .context("identifying neohub")?
        .device_id;
    info!("found hub with id {:?}", hub_id);
    let hub_id_topic = hub_id
        .chars()
        .filter(|c: &char| c.is_ascii_alphanumeric())
        .collect::<String>();

    let mut mqtt_opts = MqttOptions::new("neohub-mqtt", env_var("MQTT_HOST")?, 1883);
    mqtt_opts.set_keep_alive(Duration::from_secs(15));
    let (mqtt, mqtt_loop) = rumqttc::AsyncClient::new(
        mqtt_opts,
        // how much data to buffer in-memory, when the broker is down, before we crash out
        (10 /* minutes */ * 60) / 5 /* seconds per publish */ * 10, /* devices */
    );

    let mqtt_loop = tokio::spawn(log_for_loop(mqtt_loop));

    let err = loop {
        let data: LiveData = match neohub
            .command_void(neohub::commands::GET_LIVE_DATA)
            .await
            .context("fetching live data from neohub")
        {
            Ok(data) => data,
            Err(e) => break e,
        };
        let now = OffsetDateTime::now_utc();

        for device in data.devices {
            let topic = format!("neohub/{hub_id_topic}/SENSOR/{}", device.device_id);
            let payload = MqttMessage {
                publish_time: now,
                hub_time: data.hub_time,
                hub_id: &hub_id,
                device_idx: device.device_id,
                zone_name: &device.zone_name,
                temp_actual: parse_temp(&device.actual_temp)
                    .with_context(|| anyhow!("actual temp on device {}", device.device_id))?,
                temp_set: parse_temp(&device.set_temp)
                    .with_context(|| anyhow!("set temp on device {}", device.device_id))?,
                heat_on: device.heat_on,
            };
            let payload = serde_json::to_string(&payload)?;
            debug!("publishing: {topic} {payload}");

            mqtt.try_publish(&topic, QoS::AtLeastOnce, true, payload)
                .with_context(|| anyhow!("sending mqtt message to {}", topic))?;
        }

        sleep(Duration::from_secs(5)).await;
    };
    error!("neohub failed {err:?}, attempting to cleanup...");
    info!("neohub disconnection: {:?}", neohub.disconnect().await);

    info!("pausing to allow mqtt to flush...");
    sleep(Duration::from_secs(5)).await;

    info!("attempting to initiate a disconnect from the broker...");
    // I *believe* this should queue behind real data but have not tried it.
    mqtt.try_disconnect()
        .context("asking mqtt to disconnect after an error")?;

    info!("waiting for broker disconnect...");
    timeout(Duration::from_secs(60), mqtt_loop)
        .await
        .context("waiting for mqtt to flush")?
        .context("joining mqtt")?;

    Err(err.context("exiting as there was an error talking to the hub"))
}

fn env_var(name: &'static str) -> Result<String> {
    env::var(name).with_context(|| anyhow!("reading env var {name:?}"))
}

fn parse_temp(temp: &str) -> Result<f64> {
    temp.parse()
        .with_context(|| anyhow!("parsing a temperature: {:?}", temp))
}

async fn log_for_loop(mut ev: EventLoop) {
    #[derive(Copy, Clone, PartialEq)]
    enum State {
        Unknown,
        AckPrinted,
        ErrPrinted,
    }

    let mut disconnecting = false;
    let mut state = State::Unknown;
    loop {
        let res = ev.poll().await;
        trace!("mqtt event: {:?}", res);
        match res {
            Ok(Event::Incoming(Incoming::PubAck(_))) if state != State::AckPrinted => {
                info!("mqtt broker acknowledged a publication (we're all good)");
                state = State::AckPrinted;
            }
            Ok(Event::Outgoing(Outgoing::Disconnect)) if !disconnecting => {
                info!("asked mqtt broker to disconnect us");
                disconnecting = true;
            }
            Ok(Event::Incoming(Incoming::Disconnect)) if disconnecting => break,
            // mosquitto appears to just drop the connection, generating a:
            // MqttState(Io(Custom { kind: ConnectionAborted, error: "connection closed by peer" }))
            Err(MqttState(StateError::Io(e))) if disconnecting => {
                warn!("connection error during broker disconnect, assuming intention was to disconnect: {e:?}");
                break;
            }
            Ok(some) if state == State::ErrPrinted => {
                warn!(
                    "mqtt client recovered from error: {:?} (hopefully we'll flush)",
                    some
                );
                state = State::Unknown;
            }
            Err(e) if state != State::ErrPrinted => {
                warn!(
                    "mqtt client in an error state (unable to reach broker?): {:?}",
                    e
                );
                state = State::ErrPrinted;
            }
            _ => (),
        }
    }
}
