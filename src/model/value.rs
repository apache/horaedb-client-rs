// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::any::Any;

use horaedbproto::storage::{value, Value as ValuePb};

pub type TimestampMs = i64;

/// The value enum to express the data in HoraeDB.
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Value {
    Null,
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
    pub fn data_type(&self) -> DataType {
        match self {
            Value::Null => DataType::Null,
            Value::Timestamp(_) => DataType::Timestamp,
            Value::Double(_) => DataType::Double,
            Value::Float(_) => DataType::Float,
            Value::Varbinary(_) => DataType::Varbinary,
            Value::String(_) => DataType::String,
            Value::UInt64(_) => DataType::UInt64,
            Value::UInt32(_) => DataType::UInt32,
            Value::UInt16(_) => DataType::UInt16,
            Value::UInt8(_) => DataType::UInt8,
            Value::Int64(_) => DataType::Int64,
            Value::Int32(_) => DataType::Int32,
            Value::Int16(_) => DataType::Int16,
            Value::Int8(_) => DataType::Int8,
            Value::Boolean(_) => DataType::Boolean,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn as_any(&self) -> &dyn Any {
        self
    }

    pub fn as_i8(&self) -> Option<i8> {
        match self {
            Value::Boolean(v) => Some(*v as i8),
            Value::Int8(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_u8(&self) -> Option<u8> {
        match self {
            Value::Boolean(v) => Some(*v as u8),
            Value::Int8(v) => Some(*v as u8),
            Value::UInt8(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_i16(&self) -> Option<i16> {
        match self {
            Value::Boolean(v) => Some(*v as i16),
            Value::Int8(v) => Some(*v as i16),
            Value::UInt8(v) => Some(*v as i16),
            Value::Int16(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_u16(&self) -> Option<u16> {
        match self {
            Value::Boolean(v) => Some(*v as u16),
            Value::Int8(v) => Some(*v as u16),
            Value::UInt8(v) => Some(*v as u16),
            Value::Int16(v) => Some(*v as u16),
            Value::UInt16(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_i32(&self) -> Option<i32> {
        match self {
            Value::Boolean(v) => Some(*v as i32),
            Value::Int8(v) => Some(*v as i32),
            Value::UInt8(v) => Some(*v as i32),
            Value::Int16(v) => Some(*v as i32),
            Value::UInt16(v) => Some(*v as i32),
            Value::Int32(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_u32(&self) -> Option<u32> {
        match self {
            Value::Boolean(v) => Some(*v as u32),
            Value::Int8(v) => Some(*v as u32),
            Value::UInt8(v) => Some(*v as u32),
            Value::Int16(v) => Some(*v as u32),
            Value::UInt16(v) => Some(*v as u32),
            Value::Int32(v) => Some(*v as u32),
            Value::UInt32(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Boolean(v) => Some(*v as i64),
            Value::Int8(v) => Some(*v as i64),
            Value::UInt8(v) => Some(*v as i64),
            Value::Int16(v) => Some(*v as i64),
            Value::UInt16(v) => Some(*v as i64),
            Value::Int32(v) => Some(*v as i64),
            Value::UInt32(v) => Some(*v as i64),
            Value::Int64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Value::Boolean(v) => Some(*v as u64),
            Value::Int8(v) => Some(*v as u64),
            Value::UInt8(v) => Some(*v as u64),
            Value::Int16(v) => Some(*v as u64),
            Value::UInt16(v) => Some(*v as u64),
            Value::Int32(v) => Some(*v as u64),
            Value::UInt32(v) => Some(*v as u64),
            Value::Int64(v) => Some(*v as u64),
            Value::UInt64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_f32(&self) -> Option<f32> {
        match self {
            Value::Float(v) => Some(*v),
            Value::UInt64(v) => Some(*v as f32),
            Value::UInt32(v) => Some(*v as f32),
            Value::UInt16(v) => Some(*v as f32),
            Value::UInt8(v) => Some(*v as f32),
            Value::Int64(v) => Some(*v as f32),
            Value::Int32(v) => Some(*v as f32),
            Value::Int16(v) => Some(*v as f32),
            Value::Int8(v) => Some(*v as f32),
            Value::Boolean(_)
            | Value::Double(_)
            | Value::Null
            | Value::Timestamp(_)
            | Value::Varbinary(_)
            | Value::String(_) => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Double(v) => Some(*v),
            Value::Float(v) => Some(*v as f64),
            Value::UInt64(v) => Some(*v as f64),
            Value::UInt32(v) => Some(*v as f64),
            Value::UInt16(v) => Some(*v as f64),
            Value::UInt8(v) => Some(*v as f64),
            Value::Int64(v) => Some(*v as f64),
            Value::Int32(v) => Some(*v as f64),
            Value::Int16(v) => Some(*v as f64),
            Value::Int8(v) => Some(*v as f64),
            Value::Boolean(_)
            | Value::Null
            | Value::Timestamp(_)
            | Value::Varbinary(_)
            | Value::String(_) => None,
        }
    }

    pub fn as_varbinary(&self) -> Option<Vec<u8>> {
        match &self {
            Value::Varbinary(v) => Some(v.clone()),
            _ => None,
        }
    }

    /// Cast datum to &str.
    pub fn as_str(&self) -> Option<String> {
        match self {
            Value::String(v) => Some(v.clone()),
            _ => None,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Value::Null => b"".to_vec(),
            Value::Timestamp(v) => v.to_le_bytes().to_vec(),
            Value::Double(v) => v.to_le_bytes().to_vec(),
            Value::Float(v) => v.to_le_bytes().to_vec(),
            Value::Varbinary(v) => v.clone(),
            Value::String(v) => v.as_bytes().to_vec(),
            Value::UInt64(v) => v.to_le_bytes().to_vec(),
            Value::UInt32(v) => v.to_le_bytes().to_vec(),
            Value::UInt16(v) => v.to_le_bytes().to_vec(),
            Value::UInt8(v) => v.to_le_bytes().to_vec(),
            Value::Int64(v) => v.to_le_bytes().to_vec(),
            Value::Int32(v) => v.to_le_bytes().to_vec(),
            Value::Int16(v) => v.to_le_bytes().to_vec(),
            Value::Int8(v) => v.to_le_bytes().to_vec(),
            Value::Boolean(v) => (*v as u8).to_le_bytes().to_vec(),
        }
    }
}

impl Default for Value {
    fn default() -> Self {
        Self::Null
    }
}

impl From<Value> for ValuePb {
    fn from(val: Value) -> Self {
        let value = match val {
            Value::Null => None,
            Value::Timestamp(v) => Some(value::Value::TimestampValue(v)),
            Value::Double(v) => Some(value::Value::Float64Value(v)),
            Value::Float(v) => Some(value::Value::Float32Value(v)),
            Value::Varbinary(v) => Some(value::Value::VarbinaryValue(v)),
            Value::String(v) => Some(value::Value::StringValue(v)),
            Value::UInt64(v) => Some(value::Value::Uint64Value(v)),
            Value::UInt32(v) => Some(value::Value::Uint32Value(v)),
            Value::UInt16(v) => Some(value::Value::Uint16Value(v.into())),
            Value::UInt8(v) => Some(value::Value::Uint8Value(v.into())),
            Value::Int64(v) => Some(value::Value::Int64Value(v)),
            Value::Int32(v) => Some(value::Value::Int32Value(v)),
            Value::Int16(v) => Some(value::Value::Int16Value(v.into())),
            Value::Int8(v) => Some(value::Value::Int8Value(v.into())),
            Value::Boolean(v) => Some(value::Value::BoolValue(v)),
        };

        ValuePb { value }
    }
}

impl From<ValuePb> for Value {
    fn from(value_pb: ValuePb) -> Self {
        if value_pb.value.is_none() {
            return Value::Null;
        }

        let value = value_pb.value.unwrap();
        match value {
            value::Value::Float64Value(v) => Value::Double(v),
            value::Value::StringValue(v) => Value::String(v),
            value::Value::Int64Value(v) => Value::Int64(v),
            value::Value::Float32Value(v) => Value::Float(v),
            value::Value::Int32Value(v) => Value::Int32(v),
            value::Value::Int16Value(v) => Value::Int16(v as i16),
            value::Value::Int8Value(v) => Value::Int8(v as i8),
            value::Value::BoolValue(v) => Value::Boolean(v),
            value::Value::Uint64Value(v) => Value::UInt64(v),
            value::Value::Uint32Value(v) => Value::UInt32(v),
            value::Value::Uint16Value(v) => Value::UInt16(v as u16),
            value::Value::Uint8Value(v) => Value::UInt8(v as u8),
            value::Value::TimestampValue(v) => Value::Timestamp(v),
            value::Value::VarbinaryValue(v) => Value::Varbinary(v),
        }
    }
}

/// The data type supported by HoraeDB.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Null = 0,
    Timestamp,
    Double,
    Float,
    Varbinary,
    String,
    UInt64,
    UInt32,
    UInt16,
    UInt8,
    Int64,
    Int32,
    Int16,
    Int8,
    Boolean,
}
