pub struct Data {
    pub typ: i32,
    pub data: Vec<u8>,
}

pub type Row = Vec<Data>;

pub struct Table {
    pub names: Vec<String>,
    pub rows: Vec<Row>,
}
