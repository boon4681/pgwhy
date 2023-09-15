macro_rules! ensureSize {
    ($packet:expr,$length:expr) => {{
        let result: bool = ($packet.index + $length) < $packet.buffer.len() as i32;
        if result == false {
            panic!("Error: PGWHY length not suit");
        }
    }};
}

macro_rules! writesock {
    ($stream:ident,$packet:expr) => {
        match $stream.write_all(&$packet).await {
            Ok(_) => {
                // println!("SENDING {:?}", $packet);
            }
            Err(e) => {
                eprintln!("Error: PGWHY {}", e);
            }
        }
    };
}

pub(crate) use writesock;

fn i32_to_4u8(i: i32) -> [u8; 4] {
    let mut buf: [u8; 4] = [0; 4];
    buf[0] = (i >> 24) as u8 & 0xff;
    buf[1] = (i >> 16) as u8 & 0xff;
    buf[2] = (i >> 8) as u8 & 0xff;
    buf[3] = (i >> 0) as u8 & 0xff;
    return buf;
}

fn i16_to_2u8(i: i16) -> [u8; 2] {
    let mut buf: [u8; 2] = [0; 2];
    buf[0] = (i >> 8) as u8 & 0xff;
    buf[1] = (i >> 0) as u8 & 0xff;
    return buf;
}

pub struct PacketBuilder {
    prefix: char,
    length: i32,
    buffer: Vec<u8>,
}

pub struct PacketReader {
    index: i32,
    length: i32,
    buffer: Vec<u8>,
}

impl PacketReader {
    pub fn new(buf: &[u8]) -> PacketReader {
        return PacketReader {
            index: 0,
            length: buf.len() as i32,
            buffer: Vec::from(buf),
        };
    }
    pub fn shift(&mut self, i: i32) {
        self.index += i;
    }
    pub fn read(&mut self, length: i32) -> String {
        ensureSize!(self, length);
        let mut str: String = "".to_owned();
        for i in (self.index as usize)..(self.index + length) as usize {
            str.push(self.buffer[i] as char)
        }
        str
    }
    pub fn read_shift(&mut self, length: i32) -> String {
        let result = self.read(length);
        self.shift(length);
        result
    }
    pub fn read_i32_be(&mut self) -> i32 {
        ensureSize!(self, 4);
        let i: usize = self.index as usize;
        let mut result: i32 = 0;
        result += (self.buffer[i] as i32) << 24;
        result += (self.buffer[i + 1] as i32) << 16;
        result += (self.buffer[i + 2] as i32) << 8;
        result += (self.buffer[i + 3] as i32) << 0;
        result
    }
    pub fn read_i16_be(&mut self) -> i16 {
        ensureSize!(self, 2);
        let i: usize = self.index as usize;
        let mut result: i16 = 0;
        result += (self.buffer[i] as i16) << 8;
        result += (self.buffer[i + 1] as i16) << 0;
        result
    }
    pub fn read_i32_shift(&mut self) -> i32 {
        let result = self.read_i32_be();
        self.shift(4);
        result
    }
    pub fn read_i16_shift(&mut self) -> i16 {
        let result = self.read_i16_be();
        self.shift(2);
        result
    }
    pub fn len(&mut self) -> i32 {
        self.buffer.len() as i32
    }
}

impl PacketBuilder {
    pub fn new(prefix: &char) -> PacketBuilder {
        return PacketBuilder {
            prefix: *prefix,
            length: 0,
            buffer: Vec::new(),
        };
    }
    pub fn add_i32(&mut self, i: i32) -> &mut PacketBuilder {
        self.length += 4;
        self.buffer.append(&mut Vec::from(i32_to_4u8(i)));
        self
    }
    pub fn add_i16(&mut self, i: i16) -> &mut PacketBuilder {
        self.length += 2;
        self.buffer.append(&mut Vec::from(i16_to_2u8(i)));
        self
    }
    pub fn add_i1(&mut self, i: u8) -> &mut PacketBuilder {
        self.length += 1;
        self.buffer.push(i);
        self
    }
    pub fn add_string(&mut self, string: &str) -> &mut PacketBuilder {
        let bytes = string.as_bytes();
        self.length += bytes.len() as i32;
        self.buffer.append(&mut Vec::from(bytes));
        self
    }
    pub fn add_string_zero(&mut self, string: &str) -> &mut PacketBuilder {
        let bytes = string.as_bytes();
        self.length += bytes.len() as i32 + 1;
        self.buffer.append(&mut Vec::from(bytes));
        self.buffer.push(0);
        self
    }
    pub fn build(&mut self) -> Box<[u8]> {
        let mut buf: Vec<u8> = Vec::new();
        buf.push(self.prefix as u8);
        buf.append(&mut Vec::from(i32_to_4u8(self.length + 4)));
        buf.append(&mut self.buffer);
        buf.into_boxed_slice()
    }
}

pub fn read_start_up_packet(reader: &mut PacketReader) -> bool {
    if reader.len() < 4 {
        panic!("Error: PGWHY is not OK with length of startup packet")
    }
    let len = reader.read_i32_shift();
    let protocol = reader.read_i16_shift();
    reader.read_i16_shift();
    if reader.len() != len {
        panic!("Error: PGWHY is not OK with length of startup packet");
    }
    if protocol != 3 {
        panic!("Error: PGWHY does not support postgres wire protocol that is not 3");
    }
    return true;
}

pub fn read_header(reader: &mut PacketReader) -> String {
    reader.read_shift(1)
}

pub fn read_query(reader: &mut PacketReader) -> String {
    let length = reader.read_i32_shift() - 5;
    reader.read(length)
}
