// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! 'Value' used in local.

use ceresdbproto::storage::Value as ValuePb;

pub type TimestampMs = i64;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Value {
    Timestamp(TimestampMs),
    Double(f64),
    Float(f32),
    Varbinary(Vec<u8>),
    String(String),
    UInt64(u64),
    UInt32(u32),
    UInt16(u16),
    UInt8(u8),
    Int64(i64),
    Int32(i32),
    Int16(i16),
    Int8(i8),
    Boolean(bool),
}

impl Value {
    pub fn as_bytes(&self) -> Vec<u8> {
        match &self {
            Value::Timestamp(v) => (*v).to_le_bytes().to_vec(),
            Value::Double(v) => (*v).to_le_bytes().to_vec(),
            Value::Float(v) => (*v).to_le_bytes().to_vec(),
            Value::Varbinary(v) => v.clone(),
            Value::String(v) => v.as_bytes().to_vec(),
            Value::UInt64(v) => (*v).to_le_bytes().to_vec(),
            Value::UInt32(v) => (*v).to_le_bytes().to_vec(),
            Value::UInt16(v) => (*v).to_le_bytes().to_vec(),
            Value::UInt8(v) => (*v).to_le_bytes().to_vec(),
            Value::Int64(v) => (*v).to_le_bytes().to_vec(),
            Value::Int32(v) => (*v).to_le_bytes().to_vec(),
            Value::Int16(v) => (*v).to_le_bytes().to_vec(),
            Value::Int8(v) => (*v).to_le_bytes().to_vec(),
            Value::Boolean(v) => (*v as u8).to_le_bytes().to_vec(),
        }
    }
}

impl From<Value> for ValuePb {
    fn from(val: Value) -> Self {
        let mut val_pb = ValuePb::default();
        match val {
            Value::Timestamp(v) => val_pb.set_timestamp_value(v),
            Value::Double(v) => val_pb.set_float64_value(v),
            Value::Float(v) => val_pb.set_float32_value(v),
            Value::Varbinary(v) => val_pb.set_varbinary_value(v),
            Value::String(v) => val_pb.set_string_value(v),
            Value::UInt64(v) => val_pb.set_uint64_value(v),
            Value::UInt32(v) => val_pb.set_uint32_value(v),
            Value::UInt16(v) => val_pb.set_uint16_value(v as u32),
            Value::UInt8(v) => val_pb.set_uint8_value(v as u32),
            Value::Int64(v) => val_pb.set_int64_value(v),
            Value::Int32(v) => val_pb.set_int32_value(v),
            Value::Int16(v) => val_pb.set_int16_value(v as i32),
            Value::Int8(v) => val_pb.set_int8_value(v as i32),
            Value::Boolean(v) => val_pb.set_bool_value(v),
        };

        val_pb
    }
}
