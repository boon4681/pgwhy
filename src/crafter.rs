use crate::{
    interface::{Row, Table},
    packet::PacketBuilder,
};

pub fn authentication_ok() -> Box<[u8]> {
    PacketBuilder::new(&'R').add_i32(0).build()
}

pub fn backend_key_data() -> Box<[u8]> {
    PacketBuilder::new(&'K').add_i32(1234).add_i32(5678).build()
}

pub fn ready_for_query() -> Box<[u8]> {
    PacketBuilder::new(&'Z').add_string("I").build()
}

pub fn empty_query_response() -> Box<[u8]> {
    PacketBuilder::new(&'I').build()
}

pub fn error_response(code: String, msg: String) -> Box<[u8]> {
    PacketBuilder::new(&'E')
        .add_string("S")
        .add_string_zero("ERROR")
        .add_string("C")
        .add_string_zero(&code)
        .add_string("D")
        .add_string_zero(&msg)
        .add_i1(0)
        .build()
}

pub fn row_description(table: &Table) -> Box<[u8]> {
    let mut builder = PacketBuilder::new(&'T');
    builder.add_i16(table.names.len() as i16);
    let typs = table.rows.get(0).unwrap();
    let mut i = 0;
    for name in &table.names {
        builder.add_string_zero(&name);
        builder.add_i32(0);
        builder.add_i16(0);
        builder.add_i32(typs.get(i).unwrap().typ);
        builder.add_i16(4);
        builder.add_i32(-1);
        builder.add_i16(0);
        i += 1;
    }
    builder.build()
}

pub fn data_row(row: &Row) -> Box<[u8]> {
    let mut builder = PacketBuilder::new(&'D');
    builder.add_i16(row.len() as i16);
    for col in row {
        builder.add_i32(col.data.len() as i32);
        for byte in &col.data {
            builder.add_i1(*byte);
        }
    }
    builder.build()
}

pub fn command_complete(msg: &str) -> Box<[u8]> {
    PacketBuilder::new(&'C').add_string_zero(msg).build()
}
