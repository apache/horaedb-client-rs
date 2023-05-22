// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Write request and some useful tools for it.

use std::collections::HashMap;

use crate::model::write::point::Point;

/// Write request
#[derive(Clone, Debug, Default)]
pub struct Request {
    pub point_groups: HashMap<String, Vec<Point>>,
}

impl Request {
    pub fn add_point(&mut self, point: Point) -> &mut Self {
        let points = self
            .point_groups
            .entry(point.table.clone())
            .or_insert_with(Vec::new);
        points.push(point);

        self
    }

    pub fn add_points(&mut self, points: Vec<Point>) -> &mut Self {
        for point in points {
            self.add_point(point);
        }

        self
    }
}

pub mod pb_builder {
    use std::collections::{BTreeMap, HashMap};

    use ceresdbproto::storage::{
        Field, FieldGroup as FieldGroupPb, Tag as TagPb, WriteSeriesEntry as WriteSeriesEntryPb,
        WriteTableRequest as WriteTableRequestPb,
    };

    use crate::model::{
        value::{TimestampMs, Value},
        write::{point::Point, Request},
    };

    type TagsKey = Vec<u8>;

    /// Used to build [`WriteRequestPb`](WriteTableRequestPb) from [Request].
    pub struct WriteTableRequestPbsBuilder(pub Request);

    impl WriteTableRequestPbsBuilder {
        pub fn build(self) -> Vec<WriteTableRequestPb> {
            // Partition points by table.
            let point_group = self.0.point_groups;

            // Build pb.
            let mut table_request_pbs = Vec::with_capacity(point_group.len());
            for (table, points) in point_group {
                let write_table_request_pb_builder = TableRequestPbBuilder::new(table, points);
                let write_table_request_pb = write_table_request_pb_builder.build();
                table_request_pbs.push(write_table_request_pb);
            }

            table_request_pbs
        }
    }

    struct TableRequestPbBuilder {
        table: String,
        series_entires: Vec<SeriesEntry>,
    }

    impl TableRequestPbBuilder {
        pub fn new(table: String, points: Vec<Point>) -> Self {
            // Partition points according to tags and build [WriteSeriesEntry].
            let mut series_entries_by_tags = HashMap::new();
            for point in points {
                assert_eq!(point.table, table);
                let tags_key = make_tags_key(&point.tags);
                let series_entry =
                    series_entries_by_tags
                        .entry(tags_key)
                        .or_insert_with(|| SeriesEntry {
                            tags: point.tags,
                            ts_fields: BTreeMap::new(),
                        });
                series_entry.ts_fields.insert(point.timestamp, point.fields);
            }

            // Flatten the write series entires.
            let series_entires = series_entries_by_tags.into_values().collect();

            Self {
                table,
                series_entires,
            }
        }

        pub fn build(self) -> WriteTableRequestPb {
            let mut tags_dict = NameDict::new();
            let mut fields_dict = NameDict::new();
            let mut wirte_entries_pb = Vec::with_capacity(self.series_entires.len());
            for entry in self.series_entires {
                wirte_entries_pb.push(Self::build_series_entry(
                    &mut tags_dict,
                    &mut fields_dict,
                    entry,
                ));
            }

            WriteTableRequestPb {
                table: self.table,
                tag_names: tags_dict.convert_ordered(),
                field_names: fields_dict.convert_ordered(),
                entries: wirte_entries_pb,
            }
        }

        fn build_series_entry(
            tags_dict: &mut NameDict,
            fields_dict: &mut NameDict,
            entry: SeriesEntry,
        ) -> WriteSeriesEntryPb {
            let tags = Self::build_tags(tags_dict, entry.tags);
            let field_groups = Self::build_ts_fields(fields_dict, entry.ts_fields);

            WriteSeriesEntryPb { tags, field_groups }
        }

        fn build_tags(tags_dict: &mut NameDict, tags: BTreeMap<String, Value>) -> Vec<TagPb> {
            if tags.is_empty() {
                return Vec::new();
            }

            let mut tag_pbs = Vec::with_capacity(tags.len());
            for (name, val) in tags {
                let tag_pb = TagPb {
                    name_index: tags_dict.insert(name),
                    value: Some(val.into()),
                };
                tag_pbs.push(tag_pb);
            }

            tag_pbs
        }

        fn build_ts_fields(
            fields_dict: &mut NameDict,
            ts_fields: BTreeMap<TimestampMs, Fields>,
        ) -> Vec<FieldGroupPb> {
            if ts_fields.is_empty() {
                return Vec::new();
            }

            let mut field_group_pbs = Vec::with_capacity(ts_fields.len());
            for (ts, fields) in ts_fields {
                // Ts + fields will be converted to field group in pb.
                let mut field_pbs = Vec::with_capacity(fields.len());
                for (name, val) in fields {
                    let field_pb = Field {
                        name_index: fields_dict.insert(name),
                        value: Some(val.into()),
                    };
                    field_pbs.push(field_pb);
                }
                let field_group_pb = FieldGroupPb {
                    timestamp: ts,
                    fields: field_pbs,
                };

                // Collect field group.
                field_group_pbs.push(field_group_pb);
            }

            field_group_pbs
        }
    }

    #[derive(Clone, Default, Debug)]
    pub struct SeriesEntry {
        tags: BTreeMap<String, Value>,
        ts_fields: BTreeMap<TimestampMs, Fields>,
    }

    type Fields = BTreeMap<String, Value>;

    /// Struct helps to convert [`WriteRequest`] to [`WriteRequestPb`].
    struct NameDict {
        dict: HashMap<String, u32>,
        name_idx: u32,
    }

    impl NameDict {
        fn new() -> Self {
            NameDict {
                dict: HashMap::new(),
                name_idx: 0,
            }
        }

        fn insert(&mut self, name: String) -> u32 {
            *self.dict.entry(name).or_insert_with(|| {
                let old_name_idx = self.name_idx;
                self.name_idx += 1;
                old_name_idx
            })
        }

        fn convert_ordered(self) -> Vec<String> {
            let mut ordered = vec![String::new(); self.dict.len()];
            self.dict
                .into_iter()
                .for_each(|(name, idx)| ordered[idx as usize] = name);
            ordered
        }
    }

    pub fn make_tags_key(tags: &BTreeMap<String, Value>) -> TagsKey {
        let mut series_key = Vec::default();
        for (name, val) in tags {
            series_key.extend(name.as_bytes());
            series_key.extend_from_slice(&val.to_bytes());
        }

        series_key
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use chrono::Local;

    use super::pb_builder::make_tags_key;
    use crate::model::{
        value::Value,
        write::{
            point::{Point, PointBuilder},
            request::pb_builder::WriteTableRequestPbsBuilder,
            Request,
        },
    };

    #[test]
    fn test_build_write_table() {
        let ts1 = Local::now().timestamp_millis();
        let ts2 = ts1 + 50;
        // With same table and tags.
        let test_table = "test_table";
        let test_tag1 = ("test_tag1", 42);
        let test_tag2 = ("test_tag2", "test_tag_val");
        let test_field1 = ("test_field1", 42);
        let test_field2 = ("test_field2", "test_field_val");
        let test_field3 = ("test_field3", 0.42);
        // With same table but different tags.
        let test_tag3 = ("test_tag1", b"binarybinary");
        // With different table.
        let test_table2 = "test_table2";

        // Build write request.
        let mut write_req = Request::default();

        let points = vec![
            PointBuilder::new(test_table.to_string())
                .timestamp(ts1)
                .tag(test_tag1.0.to_owned(), Value::Int32(test_tag1.1))
                .tag(
                    test_tag2.0.to_owned(),
                    Value::String(test_tag2.1.to_owned()),
                )
                .field(test_field1.0.to_owned(), Value::Int32(test_field1.1))
                .build()
                .unwrap(),
            PointBuilder::new(test_table.to_string())
                .timestamp(ts1)
                .tag(test_tag1.0.to_owned(), Value::Int32(test_tag1.1))
                .tag(
                    test_tag2.0.to_owned(),
                    Value::String(test_tag2.1.to_owned()),
                )
                .field(
                    test_field2.0.to_owned(),
                    Value::String(test_field2.1.to_owned()),
                )
                .build()
                .unwrap(),
            PointBuilder::new(test_table.to_string())
                .timestamp(ts2)
                .tag(test_tag1.0.to_owned(), Value::Int32(test_tag1.1))
                .tag(
                    test_tag2.0.to_owned(),
                    Value::String(test_tag2.1.to_owned()),
                )
                .field(test_field3.0.to_owned(), Value::Double(test_field3.1))
                .build()
                .unwrap(),
            PointBuilder::new(test_table.to_string())
                .timestamp(ts1)
                .tag(test_tag1.0.to_owned(), Value::Int32(test_tag1.1))
                .tag(
                    test_tag2.0.to_owned(),
                    Value::String(test_tag2.1.to_owned()),
                )
                .tag(
                    test_tag3.0.to_owned(),
                    Value::Varbinary(test_tag3.1.to_vec()),
                )
                .field(test_field1.0.to_owned(), Value::Int32(test_field1.1))
                .build()
                .unwrap(),
        ];

        let points2 = vec![PointBuilder::new(test_table2.to_string())
            .timestamp(ts1)
            .tag(test_tag1.0.to_owned(), Value::Int32(test_tag1.1))
            .tag(
                test_tag2.0.to_owned(),
                Value::String(test_tag2.1.to_owned()),
            )
            .field(test_field1.0.to_owned(), Value::Int32(test_field1.1))
            .build()
            .unwrap()];

        write_req.add_points(points).add_points(points2);

        // Build pb.
        let table_requests = WriteTableRequestPbsBuilder(write_req.clone()).build();
        // Recover points from pb and compare.
        let mut points = Vec::new();
        for table_request in table_requests {
            let tag_names = table_request.tag_names;
            let field_names = table_request.field_names;
            for entry in table_request.entries {
                let tags = entry
                    .tags
                    .into_iter()
                    .map(|tag| {
                        let tag_name = tag_names[tag.name_index as usize].clone();
                        let tag_value = Value::from(tag.value.unwrap());
                        (tag_name, tag_value)
                    })
                    .collect::<BTreeMap<_, _>>();

                for ts_field in entry.field_groups {
                    let timestamp = ts_field.timestamp;
                    let fields = ts_field
                        .fields
                        .into_iter()
                        .map(|field| {
                            let field_name = field_names[field.name_index as usize].clone();
                            let field_value = Value::from(field.value.unwrap());
                            (field_name, field_value)
                        })
                        .collect::<BTreeMap<_, _>>();

                    let point = Point {
                        table: table_request.table.clone(),
                        timestamp,
                        tags: tags.clone(),
                        fields,
                    };

                    points.push(point);
                }
            }
        }

        // Compare original and recovered.
        let mut expected_points = BTreeMap::new();
        for (_, points) in write_req.point_groups {
            let points = points.into_iter().map(|point| {
                let cmp_key = make_cmp_key(&point);
                (cmp_key, point)
            });
            expected_points.extend(points);
        }
        let expected_points = expected_points.into_values().collect::<Vec<_>>();

        make_ordered(&mut points);

        assert_eq!(points, expected_points);
    }

    fn make_cmp_key(point: &Point) -> (Vec<u8>, i64) {
        let mut series_key = point.table.as_bytes().to_vec();
        let tagks_key = make_tags_key(&point.tags);
        series_key.extend(tagks_key);

        (series_key, point.timestamp)
    }

    fn make_ordered(points: &mut [Point]) {
        points.sort_by(|point1, point2| {
            let mut series_key1 = point1.table.as_bytes().to_vec();
            let mut series_key2 = point2.table.as_bytes().to_vec();
            let tagks_key1 = make_tags_key(&point1.tags);
            let tagks_key2 = make_tags_key(&point2.tags);
            series_key1.extend(tagks_key1);
            series_key2.extend(tagks_key2);
            let cmp_key1 = (series_key1, point1.timestamp);
            let cmp_key2 = (series_key2, point2.timestamp);

            cmp_key1.cmp(&cmp_key2)
        });
    }
}
