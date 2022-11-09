// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! 'Value' used in local.

use ceresdbproto::storage::{Value as ValuePb, value};

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
            Value::Timestamp(v) => val_pb.value = Some(value::Value::TimestampValue(v)),
            Value::Double(v) => val_pb.value = Some(value::Value::Float64Value(v)),
            Value::Float(v) => val_pb.value = Some(value::Value::Float32Value(v)),
            Value::Varbinary(v) => val_pb.value = Some(value::Value::VarbinaryValue(v)),
            Value::String(v) => val_pb.value = Some(value::Value::StringValue(v)),
            Value::UInt64(v) => val_pb.value = Some(value::Value::Uint64Value(v)),
            Value::UInt32(v) => val_pb.value = Some(value::Value::Uint32Value(v)),
            Value::UInt16(v) => val_pb.value = Some(value::Value::Uint16Value(v.into())),
            Value::UInt8(v) => val_pb.value = Some(value::Value::Uint8Value(v.into())),
            Value::Int64(v) => val_pb.value = Some(value::Value::Int64Value(v)),
            Value::Int32(v) => val_pb.value = Some(value::Value::Int32Value(v)),
            Value::Int16(v) => val_pb.value = Some(value::Value::Int16Value(v.into())),
            Value::Int8(v) => val_pb.value = Some(value::Value::Int8Value(v.into())),
            Value::Boolean(v) => val_pb.value = Some(value::Value::BoolValue(v)),
        };

        val_pb
    }
}
