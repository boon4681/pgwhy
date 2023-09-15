mod crafter;
mod interface;
mod packet;

use duckdb::types::ValueRef;
use tokio::io;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

use crate::crafter::authentication_ok;
use crate::crafter::backend_key_data;
use crate::crafter::command_complete;
use crate::crafter::data_row;
use crate::crafter::empty_query_response;
use crate::crafter::error_response;
use crate::crafter::ready_for_query;
use crate::crafter::row_description;
use crate::interface::Data;
use crate::interface::Table;
use crate::packet::read_header;
use crate::packet::read_query;
use crate::packet::read_start_up_packet;
use crate::packet::writesock;

use crate::packet::PacketReader;
use duckdb::{Connection, Result};

macro_rules! bytes {
    ($typ:expr,$stream:expr) => {{
        Data {
            typ: $typ,
            data: Vec::from($stream.to_string().as_bytes()),
        }
    }};
}

async fn handle_connection(mut stream: TcpStream) {
    let conn = Connection::open_in_memory().ok().unwrap();
    let mut buffer = [0; 1024]; // A buffer to read data into
    let mut startup: bool = false;
    loop {
        match stream.read(&mut buffer).await {
            Ok(n) if n > 0 => {
                let received_data = &buffer[..n];
                println!("Received: {:?}", received_data);
                println!(
                    "String: {}",
                    String::from_iter(
                        received_data
                            .iter()
                            .map(|i: &u8| -> char { *i as char })
                            .collect::<Vec<char>>()
                    )
                );
                let mut reader = PacketReader::new(received_data);
                if !startup {
                    read_start_up_packet(&mut reader);
                    writesock!(stream, authentication_ok());
                    writesock!(stream, backend_key_data());
                    writesock!(stream, ready_for_query());
                    startup = true;
                } else {
                    // https://www.postgresql.org/docs/current/protocol-flow.html
                    // 55.2.2. Simple Query
                    match read_header(&mut reader).as_str() {
                        "Q" => {
                            let query = read_query(&mut reader);
                            if query.trim().len() == 0 {
                                writesock!(stream, empty_query_response());
                                writesock!(stream, ready_for_query());
                            } else {
                                let first = query.split(";").next().unwrap().trim();
                                let qtype = first.split_whitespace().next().unwrap().to_owned();

                                let result: Result<Table, String>;
                                {
                                    let stmt = conn.prepare(first);
                                    if stmt.is_ok() {
                                        let mut statement = stmt.unwrap();
                                        let mut rows = statement.query([]).unwrap();
                                        let names: Vec<String> =
                                            rows.as_ref().unwrap().column_names().clone();
                                        let mut table: Table = Table {
                                            names,
                                            rows: Vec::new(),
                                        };
                                        loop {
                                            let row = match rows.next().unwrap() {
                                                Some(e) => e,
                                                None => break,
                                            };
                                            let mut i = 0;
                                            let mut result_row: Vec<Data> = Vec::new();
                                            loop {
                                                let wrap_data = match row.get_ref(i) {
                                                    Ok(e) => e,
                                                    Err(_) => break,
                                                };
                                                let data: Data = match wrap_data {
                                                    ValueRef::Null => bytes!(0, ""),
                                                    ValueRef::Boolean(e) => bytes!(16, e),
                                                    ValueRef::TinyInt(e) => bytes!(21, e),
                                                    ValueRef::SmallInt(e) => bytes!(21, e),
                                                    ValueRef::Int(e) => bytes!(23, e),
                                                    ValueRef::BigInt(e) => bytes!(20, e),
                                                    ValueRef::HugeInt(e) => bytes!(20, e),
                                                    ValueRef::UTinyInt(e) => bytes!(21, e),
                                                    ValueRef::USmallInt(e) => bytes!(21, e),
                                                    ValueRef::UInt(e) => bytes!(23, e),
                                                    ValueRef::UBigInt(e) => bytes!(20, e),
                                                    ValueRef::Float(e) => bytes!(700, e),
                                                    ValueRef::Double(e) => bytes!(701, e),
                                                    ValueRef::Decimal(e) => bytes!(701, e),
                                                    ValueRef::Timestamp(_, _) => todo!(),
                                                    ValueRef::Text(e) => Data {
                                                        typ: 25,
                                                        data: Vec::from(e),
                                                    },
                                                    ValueRef::Blob(e) => Data {
                                                        typ: 17,
                                                        data: Vec::from(e),
                                                    },
                                                    ValueRef::Date32(e) => bytes!(1083, e),
                                                    ValueRef::Time64(_, _) => todo!(),
                                                };
                                                i += 1;
                                                result_row.push(data);
                                            }
                                            table.rows.push(result_row);
                                        }
                                        result = Ok(table);
                                    } else {
                                        result = Err(stmt.unwrap_err().to_string());
                                    }
                                };
                                match result {
                                    Ok(table) => {
                                        writesock!(stream, row_description(&table));
                                        for row in &table.rows {
                                            writesock!(stream, data_row(row));
                                        }
                                        writesock!(
                                            stream,
                                            command_complete(
                                                &(qtype + " " + &table.rows.len().to_string())
                                            )
                                        );
                                        writesock!(stream, ready_for_query());
                                    }
                                    Err(e) => {
                                        // println!("{}", e);
                                        writesock!(
                                            stream,
                                            error_response(query.to_string(), e.clone())
                                        );
                                        writesock!(stream, ready_for_query());
                                        // writesock!(stream, ready_for_query());
                                    }
                                }
                            }
                            println!("{}", query);
                        }
                        _ => {
                            println!("HUH")
                        }
                    }
                }
            }
            Ok(_) => {
                println!("Connection closed by remote.");
                break;
            }
            Err(e) => {
                eprintln!("Error reading from socket: {}", e);
                break;
            }
        }
    }
}

async fn proxy(client: &str) -> io::Result<()> {
    let listener = TcpListener::bind(client).await?;
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(handle_connection(stream));
            }
            Err(e) => eprintln!("Error accepting connection: {}", e),
        }
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let client = "::1:6000";
    proxy(client).await
}
