// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! [Point] and its builder

use std::collections::BTreeMap;

use crate::model::value::Value;

const TSID: &str = "tsid";
const TIMESTAMP: &str = "timestamp";

#[inline]
pub fn is_reserved_column_name(name: &str) -> bool {
    name.eq_ignore_ascii_case(TSID) || name.eq_ignore_ascii_case(TIMESTAMP)
}

/// One point in the [`WriteRequest`](crate::WriteRequest).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Point {
    pub table: String,
    pub timestamp: i64,
    pub tags: BTreeMap<String, Value>,
    pub fields: BTreeMap<String, Value>,
}

/// Builder for building a point.
#[derive(Debug)]
pub struct PointBuilder {
    table: String,
    timestamp: Option<i64>,
    // tags' traversing should have definite order
    tags: BTreeMap<String, Value>,
    fields: BTreeMap<String, Value>,
    contains_reserved_column_name: bool,
}

impl PointBuilder {
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            timestamp: None,
            tags: BTreeMap::new(),
            fields: BTreeMap::new(),
            contains_reserved_column_name: false,
        }
    }

    /// Set the table name for the point.
    pub fn table(mut self, table: impl Into<String>) -> Self {
        self.table = table.into();
        self
    }

    /// Set the timestamp for the point.
    pub fn timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Set tag name and value of the write entry.
    ///
    /// You cannot set tag with name like 'timestamp' or 'tsid',
    /// because they are keywords in horaedb.
    pub fn tag(mut self, name: impl Into<String>, value: Value) -> Self {
        let name = name.into();
        if is_reserved_column_name(&name) {
            self.contains_reserved_column_name = true;
        }

        let _ = self.tags.insert(name, value);
        self
    }

    /// Set the name and value of a field specified by its `name`.
    pub fn field(mut self, name: impl Into<String>, value: Value) -> Self {
        let name = name.into();
        if is_reserved_column_name(&name) {
            self.contains_reserved_column_name = true;
        }

        let _ = self.fields.insert(name, value);
        self
    }

    /// Build the final point.
    pub fn build(self) -> Result<Point, String> {
        if self.contains_reserved_column_name {
            return Err("Tag or field name reserved column name in horaedb".to_string());
        }

        if self.fields.is_empty() {
            return Err("Fields should not be empty".to_string());
        }

        let timestamp = self
            .timestamp
            .ok_or_else(|| "Timestamp must be set".to_string())?;

        Ok(Point {
            table: self.table,
            timestamp,
            tags: self.tags,
            fields: self.fields,
        })
    }
}
