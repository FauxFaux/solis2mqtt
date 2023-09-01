use std::env;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use log::{debug, info, trace, warn};
use reqwest::Client;
use rumqttc::ConnectionError::MqttState;
use rumqttc::{Event, EventLoop, Incoming, MqttOptions, Outgoing, QoS, StateError};
use serde::Serialize;
use time::OffsetDateTime;
use tokio::time::{sleep, timeout};

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MqttMessage {
    /// the date/time I believe to be true
    #[serde(with = "time::serde::rfc3339")]
    publish_time: OffsetDateTime,

    /// identifier for the inverter (it's serial number)
    inverter_id: String,

    /// what the device thinks the temperature is
    temp: f64,

    /// current power from the solar panels (watts)
    current_power_watts: f64,

    /// yield today (kWh)
    yield_today_kwh: f64,

    /// total yield (kWh) (broken)
    yield_total_kwh: Option<f64>,
}

struct Req {
    endpoint: String,
    username: String,
    password: String,
}

async fn fetch(client: &Client, req: &Req) -> Result<MqttMessage> {
    let data = client
        .get(&req.endpoint)
        .basic_auth(&req.username, Some(&req.password))
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;

    let data = data
        .trim_matches('\0')
        .trim()
        .split(';')
        .collect::<Vec<&str>>();
    if data.len() < 7 {
        return Err(anyhow!("unexpected data: {:?}", data));
    }

    let serial = data[0];
    let _firmware = data[1];
    let _model = data[2];
    let temp = parse_float(data[3])?;
    let current_power_watts = parse_float(data[4])?;
    let yield_today_kwh = parse_float(data[5])?;
    let yield_total_kwh = data[6];

    let yield_total_kwh = match yield_total_kwh {
        "d" => None,
        _ => Some(parse_float(yield_total_kwh)?),
    };

    if serial.len() != 16 || serial.starts_with('0') {
        return Err(anyhow!("unexpected serial: {:?}", serial));
    }

    Ok(MqttMessage {
        publish_time: OffsetDateTime::now_utc(),
        inverter_id: serial.to_string(),
        temp,
        current_power_watts,
        yield_today_kwh,
        yield_total_kwh,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    pretty_env_logger::init_timed();

    let client = reqwest::Client::builder()
        .http1_title_case_headers()
        .build()?;
    let solis_addr = env_var("SOLIS_ADDR")?;
    let req = Req {
        endpoint: format!("http://{}/inverter.cgi", solis_addr),
        username: "admin".to_string(),
        password: "123456789".to_string(),
    };

    let mut mqtt_opts = MqttOptions::new("neohub-mqtt", env_var("MQTT_HOST")?, 1883);
    mqtt_opts.set_keep_alive(Duration::from_secs(15));
    let (mqtt, mqtt_loop) = rumqttc::AsyncClient::new(
        mqtt_opts,
        // how much data to buffer in-memory, when the broker is down, before we crash out
        (10 /* minutes */ * 60) / 5, /* seconds per publish */
    );

    let mqtt_loop = tokio::spawn(log_for_loop(mqtt_loop));

    let err = loop {
        let payload = match fetch(&client, &req).await {
            Ok(payload) => payload,
            Err(e) => break e,
        };

        let topic = format!("solis/{}/SENSOR/scrape", payload.inverter_id);
        let payload = serde_json::to_string(&payload)?;
        debug!("publishing: {topic} {payload}");

        mqtt.try_publish(&topic, QoS::AtLeastOnce, true, payload)
            .with_context(|| anyhow!("sending mqtt message to {}", topic))?;

        sleep(Duration::from_secs(5)).await;
    };

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

fn parse_float(s: &str) -> Result<f64> {
    s.parse()
        .with_context(|| anyhow!("parsing a float: {:?}", s))
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
