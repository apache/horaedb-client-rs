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

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Point {
    pub table: String,
    pub timestamp: i64,
    pub tags: BTreeMap<String, Value>,
    pub fields: BTreeMap<String, Value>,
}

#[derive(Debug)]
pub struct PointBuilder {
    timestamp: Option<i64>,
    // tags' traversing should have definite order
    tags: BTreeMap<String, Value>,
    fields: BTreeMap<String, Value>,
    points_builder: PointGroupBuilder,
    contains_reserved_column_name: bool,
}

impl PointBuilder {
    pub fn new(points_builder: PointGroupBuilder) -> Self {
        Self {
            timestamp: None,
            tags: BTreeMap::new(),
            fields: BTreeMap::new(),
            points_builder,
            contains_reserved_column_name: false,
        }
    }

    #[must_use]
    pub fn timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Set tag name and value of the write entry.
    ///
    /// You cannot set tag with name like 'timestamp' or 'tsid',
    /// because they are keywords in ceresdb.
    #[allow(clippy::return_self_not_must_use)]
    pub fn tag(mut self, name: String, value: Value) -> Self {
        if is_reserved_column_name(&name) {
            self.contains_reserved_column_name = true;
        }

        let _ = self.tags.insert(name, value);
        self
    }

    #[must_use]
    pub fn field(mut self, name: String, value: Value) -> Self {
        if is_reserved_column_name(&name) {
            self.contains_reserved_column_name = true;
        }

        let _ = self.fields.insert(name, value);
        self
    }

    /// Finish building this row and append this row into the
    /// [`WriteRequestBuilder`].
    pub fn finish(self) -> Result<PointGroupBuilder, String> {
        if self.contains_reserved_column_name {
            return Err("Tag or field name reserved column name in ceresdb".to_string());
        }

        if self.fields.is_empty() {
            return Err("Fields should not be empty".to_string());
        }

        let timestamp = self
            .timestamp
            .ok_or_else(|| "Timestamp must be set".to_string())?;

        // Build [PointContext] and push it into [PointGroupBuilder].
        let mut points_builder = self.points_builder;
        let table = points_builder.table.clone();
        let point = Point {
            table,
            timestamp,
            tags: self.tags,
            fields: self.fields,
        };
        points_builder.points.push(point);

        Ok(points_builder)
    }
}

/// Points in specific table.
#[derive(Debug)]
pub struct PointGroup {
    pub table: String,
    pub points: Vec<Point>,
}

/// Points(in specific table) builder
#[derive(Debug)]
pub struct PointGroupBuilder {
    table: String,
    points: Vec<Point>,
}

impl PointGroupBuilder {
    pub fn new(table: String) -> Self {
        Self {
            table,
            points: Vec::new(),
        }
    }

    #[must_use]
    pub fn point_builder(self) -> PointBuilder {
        PointBuilder::new(self)
    }

    /// Finish building this row and append this row into the
    /// [`WriteRequestBuilder`].
    pub fn build(self) -> PointGroup {
        PointGroup {
            table: self.table,
            points: self.points,
        }
    }
}
