use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use futures::FutureExt;
use std::env;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let listen_addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:27018".to_string());
    let server_addr = env::args()
        .nth(2)
        .unwrap_or_else(|| "127.0.0.1:27017".to_string());

    println!("Listening on: {}", listen_addr);
    println!("Proxying to: {}", server_addr);

    let listener = TcpListener::bind(listen_addr).await?;

    while let Ok((inbound, _)) = listener.accept().await {
        let transfer = transfer(inbound, server_addr.clone()).map(|r| {
            if let Err(e) = r {
                println!("Failed to transfer; error={}", e);
            }
        });

        tokio::spawn(transfer);
    }

    Ok(())
}

async fn transfer(mut inbound: TcpStream, proxy_addr: String) -> Result<(), Box<dyn Error>> {
    let mut outbound = TcpStream::connect(proxy_addr).await?;

    let (mut ir, mut iw) = inbound.split();
    let (mut or, mut ow) = outbound.split();

    loop {
        // read from inbound
        let mut buf = [0u8; 4096];
        let n = ir.read(&mut buf).await?;
        if n == 0 {
            break;
        }

        // write to outbound
        ow.write_all(&buf[..n]).await?;
        ow.flush().await?;

        // read from outbound
        let mut buf = [0u8; 4096];
        let n = or.read(&mut buf).await?;
        if n == 0 {
            break;
        }

        // write to inbound
        iw.write_all(&buf[..n]).await?;
        iw.flush().await?;
    }

    Ok(())
}
