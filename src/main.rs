#[macro_use]
extern crate log;


use std::env;
use std::io::Write;
use std::net::TcpStream;
use std::str;
use std::collections::HashMap;

use clap::{App, Arg};

use uuid::Uuid;

use mqtt::control::variable_header::ConnectReturnCode;
use mqtt::packet::*;
use mqtt::{TopicFilter, TopicName};
use mqtt::{Decodable, Encodable, QualityOfService};

use std::thread;
use std::time::Duration;

use serde::{Serialize, Deserialize};


fn generate_client_id() -> String {
    format!("MQTT_rust_{}", Uuid::new_v4())
}


#[derive(Deserialize, Serialize, Debug)]
struct Msg {
    pub MSGID: String,
    pub TYPE: String,
    pub DEVICE_ID: String,
    pub IC_ID: String,
    pub TIME: String,
    pub MSG: String,
}





fn main() {
    // configure logging
    env::set_var("RUST_LOG", env::var_os("RUST_LOG").unwrap_or_else(|| "info".into()));
    env_logger::init();

    let server_addr = "qeh4ra6.mqtt.iot.gz.baidubce.com:1883".to_string();
    let client_id = generate_client_id();
    let user_name = "qeh4ra6/ruanchuang".to_string();
    let password = "oV0UQ33kJIKJzYFb".to_string();
    let channel_filters = vec![(TopicFilter::new("/siheyuan/cloud/meeting/sub".to_string()).unwrap(), QualityOfService::Level0)];
    let keep_alive = 10;

    info!("Connecting to {:?} ... ", server_addr);
    let mut stream = TcpStream::connect(server_addr).unwrap();
    info!("Connected!");

    info!("Client identifier {:?}", client_id);
    let mut conn = ConnectPacket::new("MQTT", client_id);
    conn.set_clean_session(true);
    conn.set_keep_alive(keep_alive);
    conn.set_user_name(Some(user_name));
    conn.set_password(Some(password));
    let mut buf = Vec::new();
    conn.encode(&mut buf).unwrap();
    stream.write_all(&buf[..]).unwrap();

    

    let connack = ConnackPacket::decode(&mut stream).unwrap();
    trace!("CONNACK {:?}", connack);

    if connack.connect_return_code() != ConnectReturnCode::ConnectionAccepted {
        panic!(
            "Failed to connect to server, return code {:?}",
            connack.connect_return_code()
        );
    }

    info!("Applying channel filters {:?} ...", channel_filters);
    let sub = SubscribePacket::new(10, channel_filters);
    let mut buf = Vec::new();
    sub.encode(&mut buf).unwrap();
    stream.write_all(&buf[..]).unwrap();

    loop {
        let packet = match VariablePacket::decode(&mut stream) {
            Ok(pk) => pk,
            Err(err) => {
                error!("1. Error in receiving packet {:?}", err);
                thread::sleep(Duration::from_millis(1000));
                continue;
            }
        };
        trace!("PACKET {:?}", packet);

        if let VariablePacket::SubackPacket(ref ack) = packet {
            if ack.packet_identifier() != 10 {
                panic!("SUBACK packet identifier not match");
            }

            info!("Subscribed!");
            break;
        }
    }

    let mut stream_clone = stream.try_clone().unwrap();
    thread::spawn(move || {
        let mut last_ping_time = 0;
        let mut next_ping_time = last_ping_time + (keep_alive as f32 * 0.9) as i64;
        loop {
            let current_timestamp = time::get_time().sec;
            if keep_alive > 0 && current_timestamp >= next_ping_time {
                info!("Sending PINGREQ to broker");

                let pingreq_packet = PingreqPacket::new();

                let mut buf = Vec::new();
                pingreq_packet.encode(&mut buf).unwrap();
                stream_clone.write_all(&buf[..]).unwrap();

                last_ping_time = current_timestamp;
                next_ping_time = last_ping_time + (keep_alive as f32 * 0.9) as i64;
                thread::sleep(Duration::new((keep_alive / 2) as u64, 0));
            }
        }
    });

    loop {
        let packet = match VariablePacket::decode(&mut stream) {
            Ok(pk) => pk,
            Err(err) => {
                error!("2. Error in receiving packet {}", err);
                thread::sleep(Duration::from_millis(1000));
                continue;
            }
        };
        trace!("PACKET {:?}", packet);

        match packet {
            VariablePacket::PingrespPacket(..) => {
                info!("Receiving PINGRESP from broker ..");
            },
            VariablePacket::PublishPacket(ref publ) => {
                let msg = match str::from_utf8(&publ.payload_ref()[..]) {
                    Ok(msg) => msg,
                    Err(err) => {
                        error!("Failed to decode publish message {:?}", err);
                        continue;
                    }
                };
                //info!("PUBLISH ({}): {}", publ.topic_name(), msg);

                // 收到msg消息了，现在需要解析成 JSON 数据
                let p: Result<Msg, _> = serde_json::from_str(&msg);
                match p {
                    Ok(p) => {
                        info!("{:?}", p);
                        // 先判断是不是心跳包，是的话，返回心跳响应
                        if &p.MSG == "HEART" {
                            println!("{}", "in heartbeat");
                            // 如果是心跳包
                            
                            // 获取当前系统时间，并用于产生新的msg
                            let msg = Msg {
                                MSGID: p.MSGID.to_string(),
                                TYPE: p.TYPE.to_string(),
                                DEVICE_ID: p.DEVICE_ID.to_string(),
                                IC_ID: p.IC_ID.to_string(),
                                TIME: time::now().to_timespec().sec.to_string(),
                                MSG: "HEART_OK".to_string()
                            };
                            let j = serde_json::to_string(&msg).unwrap();

                            pub_msg(p.DEVICE_ID.to_string(), j);

                        } 
                        else if &p.MSG == "UPDATA" {
                            // 这是考勤数据，在这里制造一个 http request，发送到考勤服务API去，
                            // 并且接收请求返回结果，并通过pub返回给mqtt那端
                            let mut map = HashMap::new();
                            map.insert("user_id", p.IC_ID.to_string());
                            map.insert("device_id", p.DEVICE_ID.to_string());
                            map.insert("source", "kq_device".to_string());
                            map.insert("event_type", "0".to_string());
                            map.insert("event_time", p.TIME.to_string());

                            // 同步发起，内网，操作很快，不需要担心
                            let client = reqwest::Client::new();
                            let res = client.post("http://127.0.0.1:1337/api/attendance/v1/add")
                                .json(&map)
                                .send().unwrap();
                            info!("res {:?}", res);

                            if res.status().is_success() {
                                // 考勤数据添加成功，就返回 UPDATA_OK
                                // 不成功，不返回
                                let msg = Msg {
                                    MSGID: p.MSGID.to_string(),
                                    TYPE: p.TYPE.to_string(),
                                    DEVICE_ID: p.DEVICE_ID.to_string(),
                                    IC_ID: p.IC_ID.to_string(),
                                    TIME: p.TIME.to_string(),
                                    MSG: "UPDATA_OK".to_string()
                                };
                                let j = serde_json::to_string(&msg).unwrap();

                                pub_msg(p.DEVICE_ID.to_string(), j);
                            }

                        }
                    },
                    Err(_) => {
                        info!("json parsing error");
                    }
                }
            }
            _ => {}
        }
    }


}



fn pub_msg (channel: String, msg: String) {
    thread::spawn(move || {

        let server_addr = "qeh4ra6.mqtt.iot.gz.baidubce.com:1883".to_string();
        let client_id = generate_client_id();
        let user_name = "qeh4ra6/ruanchuang".to_string();
        let password = "oV0UQ33kJIKJzYFb".to_string();
        let channel_filters = vec![(TopicFilter::new("/siheyuan/cloud/meeting/pub/".to_string() + &channel).unwrap(), QualityOfService::Level0)];
        let keep_alive = 10;

        info!("Connecting to {:?} ... ", server_addr);
        let mut stream = TcpStream::connect(server_addr).unwrap();
        info!("Connected!");

        info!("Client identifier {:?}", client_id);
        let mut conn = ConnectPacket::new("MQTT", client_id);
        conn.set_clean_session(true);
        conn.set_keep_alive(keep_alive);
        conn.set_user_name(Some(user_name));
        conn.set_password(Some(password));

        let mut buf = Vec::new();
        conn.encode(&mut buf).unwrap();
        stream.write_all(&buf[..]).unwrap();

        let connack = ConnackPacket::decode(&mut stream).unwrap();
        trace!("CONNACK {:?}", connack);

        if connack.connect_return_code() != ConnectReturnCode::ConnectionAccepted {
            panic!(
                "Failed to connect to server, return code {:?}",
                connack.connect_return_code()
            );
        }

        info!("Applying channel filters {:?} ...", channel_filters);
        let sub = SubscribePacket::new(10, channel_filters);
        let mut buf = Vec::new();
        sub.encode(&mut buf).unwrap();
        stream.write_all(&buf[..]).unwrap();

        let channels = vec![TopicName::new("/siheyuan/cloud/meeting/pub/".to_string() + &channel).unwrap()];

        info!("channel {}, msg {} ", channel, msg);
        for chan in &channels {
            let publish_packet = PublishPacket::new(chan.clone(), QoSWithPacketIdentifier::Level0, msg.clone());
            let mut buf = Vec::new();
            publish_packet.encode(&mut buf).unwrap();
            stream.write_all(&buf[..]).unwrap();
        }
    });

}