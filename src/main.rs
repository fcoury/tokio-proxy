use std::error::Error;
use std::time::SystemTime;
use std::{env, time::UNIX_EPOCH};

use futures::{
    stream::{self, StreamExt},
    FutureExt,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let listen_addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:27018".to_string());
    let server_addrs = env::args().skip(2).collect::<Vec<_>>();

    println!("Listening on: {}", listen_addr);
    println!("Proxying to: {:?}", server_addrs);

    let listener = TcpListener::bind(listen_addr).await?;

    while let Ok((inbound, _)) = listener.accept().await {
        let transfer = transfer(inbound, server_addrs.clone()).map(|r| {
            if let Err(e) = r {
                println!("Failed to transfer; error={}", e);
            }
        });

        tokio::spawn(transfer);
    }

    Ok(())
}

async fn transfer(mut inbound: TcpStream, backends: Vec<String>) -> Result<(), Box<dyn Error>> {
    // connects to all backends
    let tcp_futures = backends
        .iter()
        .map(|addr| TcpStream::connect(addr))
        .collect::<Vec<_>>();
    let mut outbounds_stream = stream::iter(tcp_futures).buffer_unordered(10);

    let mut outbounds = Vec::new();
    while let Some(outbound) = outbounds_stream.next().await {
        let outbound = outbound?;
        outbounds.push(outbound);
    }

    let mut outbound_halves = outbounds
        .into_iter()
        .map(|outbound| outbound.into_split())
        .collect::<Vec<_>>();

    // inbound connection halves
    let (mut ir, mut iw) = inbound.split();

    loop {
        // request id
        let req_id = SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_millis()
            .to_string();

        // read from inbound
        let mut buf = [0u8; 4096];
        let n = ir.read(&mut buf).await?;
        if n == 0 {
            break;
        }

        tokio::fs::write(format!("logs/{}-request.bin", req_id), &buf[..n]).await?;

        let mut responses = Vec::new();
        for (or, ow) in &mut outbound_halves {
            // write to outbound
            ow.write_all(&buf[..n]).await?;
            ow.flush().await?;

            // read from outbound
            let mut buf = [0u8; 4096];
            let n = or.read(&mut buf).await?;
            if n == 0 {
                continue;
            }

            responses.push(buf[..n].to_vec());
        }

        println!(
            "{} outgoing {:?} responses",
            responses.len(),
            responses.iter().map(|r| r.len()).collect::<Vec<_>>()
        );

        for (i, responses) in responses.iter().enumerate() {
            tokio::fs::write(format!("logs/{}-response-{}.bin", req_id, i), responses).await?;
        }

        // write to inbound
        let Some(buf) = responses.get(0) else {
            break;
        };
        let buf = buf.as_slice();
        iw.write_all(&buf).await?;
        iw.flush().await?;
    }

    Ok(())
}
