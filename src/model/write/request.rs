// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Write request and some useful tools for it.

use std::collections::{BTreeMap, HashMap};

use ceresdbproto::storage::{
    Field, FieldGroup as FieldGroupPb, Tag as TagPb, WriteEntry as WriteEntryPb,
    WriteMetric as WriteMetricPb, WriteRequest as WriteRequestPb,
};

use crate::model::value::{TimestampMs, Value};

#[inline]
fn is_reserved_column_name(name: &str) -> bool {
    name.eq_ignore_ascii_case("tsid") || name.eq_ignore_ascii_case("timestamp")
}

/// Builder for [`WriteRequest`].
///
/// You should call [`row_builder`] to build and insert the row you want to write.
/// And after all rows inserted, call [`build`] to get [`WriteRequest`].
///
/// [`row_builder`]: WriteRequestBuilder::row_builder
/// [`build`]: WriteRequestBuilder::build
#[derive(Debug, Default)]
pub struct WriteRequestBuilder {
    data_in_metrics: HashMap<Vec<u8>, WriteEntry>,
}

impl WriteRequestBuilder {
    pub fn row_builder(&mut self) -> RowBuilder {
        RowBuilder {
            timestamp: None,
            metric: None,
            tags: BTreeMap::new(),
            fields: HashMap::new(),
            write_req_builder: self,
            contains_keyword: false,
        }
    }

    pub fn build(self) -> WriteRequest {
        WriteRequest {
            write_entries: self
                .data_in_metrics
                .into_iter()
                .map(|(_, entry)| entry)
                .collect(),
        }
    }
}

#[derive(Debug)]
pub struct RowBuilder<'a> {
    timestamp: Option<i64>,
    metric: Option<String>,
    // tags' traversing should have definite order
    tags: BTreeMap<String, Value>,
    fields: HashMap<String, Value>,
    write_req_builder: &'a mut WriteRequestBuilder,
    contains_keyword: bool,
}

impl<'a> RowBuilder<'a> {
    #[must_use]
    pub fn timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    #[must_use]
    pub fn metric(mut self, metric: String) -> Self {
        self.metric = Some(metric);
        self
    }

    /// Set tag name and value of the write entry.
    ///
    /// You cannot set tag with name like 'timestamp' or 'tsid',
    /// because they are keywords in ceresdb.
    #[allow(clippy::return_self_not_must_use)]
    pub fn tag(mut self, name: String, value: Value) -> Self {
        if is_reserved_column_name(&name) {
            self.contains_keyword = true;
        }

        let _ = self.tags.insert(name, value);
        self
    }

    #[must_use]
    pub fn field(mut self, name: String, value: Value) -> Self {
        if is_reserved_column_name(&name) {
            self.contains_keyword = true;
        }

        let _ = self.fields.insert(name, value);
        self
    }

    /// Finish building this row and append this row into the [`WriteRequestBuilder`].
    pub fn finish(self) -> Result<(), String> {
        // valid check
        if self.metric.is_none() {
            return Err("Metric must be set".to_owned());
        }

        if self.timestamp.is_none() {
            return Err("Timestamp must be set".to_owned());
        }

        if self.contains_keyword {
            return Err("Tag or field name contains keyword in ceresdb".to_owned());
        }

        // make series key
        let metric = self.metric.unwrap();
        let series_key = make_series_key(metric.as_str(), &self.tags);

        // insert to write req builder
        let tags = self.tags;
        let data = self
            .write_req_builder
            .data_in_metrics
            .entry(series_key)
            .or_insert_with(|| {
                let series = Series { metric, tags };

                WriteEntry {
                    series,
                    points: BTreeMap::new(),
                }
            });

        let point = data
            .points
            .entry(self.timestamp.unwrap())
            .or_insert_with(Point::default);
        point.fields.extend(self.fields.into_iter());

        Ok(())
    }
}

fn make_series_key(metric: &str, tags: &BTreeMap<String, Value>) -> Vec<u8> {
    let mut series_key = metric.as_bytes().to_vec();
    for (name, val) in tags {
        series_key.extend_from_slice(name.as_bytes());
        series_key.extend_from_slice(&val.as_bytes());
    }

    series_key
}

#[derive(Clone, Debug)]
pub struct WriteRequest {
    write_entries: Vec<WriteEntry>,
}

impl WriteRequest {
    pub fn entry(&mut self, entry: WriteEntry) {
        self.write_entries.push(entry);
    }
}

#[derive(Clone, Default, Debug)]
pub struct WriteEntry {
    series: Series,
    points: BTreeMap<TimestampMs, Point>,
}

#[derive(Clone, Default, Debug)]
struct Series {
    metric: String,
    tags: BTreeMap<String, Value>,
}

#[derive(Clone, Default, Debug)]
struct Point {
    fields: HashMap<String, Value>,
}

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

impl From<WriteRequest> for WriteRequestPb {
    fn from(req: WriteRequest) -> Self {
        let mut req_pb = WriteRequestPb::default();
        // partition the write entries first
        let mut partitions_by_metric: HashMap<_, Vec<_>> = HashMap::new();
        for (idx, entry) in req.write_entries.iter().enumerate() {
            let parition = partitions_by_metric
                .entry(entry.series.metric.clone())
                .or_insert(Vec::new());
            parition.push(idx);
        }

        let mut write_metrics = Vec::with_capacity(partitions_by_metric.len());
        for partition in partitions_by_metric {
            write_metrics.push(convert_one_write_metric(
                partition.0,
                &partition.1,
                &req.write_entries,
            ));
        }
        req_pb.set_metrics(write_metrics.into());

        req_pb
    }
}

fn convert_one_write_metric(
    metric: String,
    idxs: &[usize],
    entries: &[WriteEntry],
) -> WriteMetricPb {
    let mut write_metric_pb = WriteMetricPb::default();

    let mut tags_dic = NameDict::new();
    let mut fields_dic = NameDict::new();
    let mut wirte_entries = Vec::with_capacity(idxs.len());
    for idx in idxs {
        assert!(*idx < entries.len());
        wirte_entries.push(convert_entry(
            &mut tags_dic,
            &mut fields_dic,
            &entries[*idx],
        ));
    }

    write_metric_pb.set_metric(metric);
    write_metric_pb.set_tag_names(tags_dic.convert_ordered().into());
    write_metric_pb.set_field_names(fields_dic.convert_ordered().into());
    write_metric_pb.set_entries(wirte_entries.into());

    write_metric_pb
}

fn convert_entry(
    tags_dic: &mut NameDict,
    fields_dic: &mut NameDict,
    entry: &WriteEntry,
) -> WriteEntryPb {
    let mut entry_pb = WriteEntryPb::default();
    entry_pb.set_tags(convert_tags(tags_dic, &entry.series.tags).into());
    entry_pb.set_field_groups(convert_points(fields_dic, &entry.points).into());

    entry_pb
}

// TODO(kamille) reduce cloning from tags.
fn convert_tags(tags_dic: &mut NameDict, tags: &BTreeMap<String, Value>) -> Vec<TagPb> {
    if tags.is_empty() {
        return Vec::new();
    }

    let mut tag_pbs = Vec::with_capacity(tags.len());
    for (name, val) in tags {
        let mut tag_pb = TagPb::default();
        tag_pb.set_name_index(tags_dic.insert(name.clone()));
        tag_pb.set_value(val.clone().into());
        tag_pbs.push(tag_pb);
    }

    tag_pbs
}

fn convert_points(fields_dic: &mut NameDict, points: &BTreeMap<i64, Point>) -> Vec<FieldGroupPb> {
    if points.is_empty() {
        return Vec::new();
    }

    let mut field_group_pbs = Vec::with_capacity(points.len());
    for (ts, point) in points {
        // ts + point will be converted to file group in pb
        let mut file_group_pb = FieldGroupPb::default();
        file_group_pb.set_timestamp(*ts);

        let mut field_pbs = Vec::with_capacity(point.fields.len());
        for (name, val) in point.fields.iter() {
            let mut field_pb = Field::default();
            field_pb.set_name_index(fields_dic.insert(name.clone()));
            field_pb.set_value(val.clone().into());
            field_pbs.push(field_pb);
        }
        file_group_pb.set_fields(field_pbs.into());

        // collect field group
        field_group_pbs.push(file_group_pb);
    }

    field_group_pbs
}

#[cfg(test)]
mod test {
    use std::collections::{BTreeMap, HashMap};

    use ceresdbproto::storage::Value as ValuePb;
    use chrono::Local;

    use super::{convert_points, convert_tags, NameDict, Point};
    use crate::model::{
        value::Value,
        write::request::{make_series_key, WriteRequestBuilder},
    };

    #[test]
    fn test_build_wirte_metric() {
        let ts1 = Local::now().timestamp_millis();
        let ts2 = ts1 + 50;
        // with same metric and tags
        let test_metric = "test_metric";
        let test_tag1 = ("test_tag1", 42);
        let test_tag2 = ("test_tag2", "test_tag_val");
        let test_field1 = ("test_field1", 42);
        let test_field2 = ("test_field2", "test_field_val");
        let test_field3 = ("test_field3", 0.42);
        // with same metric but different tags
        let test_tag3 = ("test_tag1", b"binarybinary");
        // with different metric
        let test_metric2 = "test_metric2";

        // test write request with just one row
        let mut wreq_builder = WriteRequestBuilder::default();
        wreq_builder
            .row_builder()
            .metric(test_metric.to_owned())
            .timestamp(ts1)
            .tag(test_tag1.0.to_owned(), Value::Int32(test_tag1.1))
            .tag(
                test_tag2.0.to_owned(),
                Value::String(test_tag2.1.to_owned()),
            )
            .field(test_field1.0.to_owned(), Value::Int32(test_field1.1))
            .finish()
            .unwrap();
        wreq_builder
            .row_builder()
            .metric(test_metric.to_owned())
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
            .finish()
            .unwrap();
        wreq_builder
            .row_builder()
            .metric(test_metric.to_owned())
            .timestamp(ts2)
            .tag(test_tag1.0.to_owned(), Value::Int32(test_tag1.1))
            .tag(
                test_tag2.0.to_owned(),
                Value::String(test_tag2.1.to_owned()),
            )
            .field(test_field3.0.to_owned(), Value::Double(test_field3.1))
            .finish()
            .unwrap();

        wreq_builder
            .row_builder()
            .metric(test_metric.to_owned())
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
            .finish()
            .unwrap();

        wreq_builder
            .row_builder()
            .metric(test_metric2.to_owned())
            .timestamp(ts1)
            .tag(test_tag1.0.to_owned(), Value::Int32(test_tag1.1))
            .tag(
                test_tag2.0.to_owned(),
                Value::String(test_tag2.1.to_owned()),
            )
            .field(test_field1.0.to_owned(), Value::Int32(test_field1.1))
            .finish()
            .unwrap();
        let wreq = wreq_builder.build();

        // check build result
        assert_eq!(wreq.write_entries.len(), 3);
        let tmp_tags1 = vec![
            (test_tag1.0.to_owned(), Value::Int32(test_tag1.1)),
            (
                test_tag2.0.to_owned(),
                Value::String(test_tag2.1.to_owned()),
            ),
        ];
        let tmp_tags2 = vec![
            (test_tag1.0.to_owned(), Value::Int32(test_tag1.1)),
            (
                test_tag2.0.to_owned(),
                Value::String(test_tag2.1.to_owned()),
            ),
            (
                test_tag3.0.to_owned(),
                Value::Varbinary(test_tag3.1.to_vec()),
            ),
        ];
        let mut tags1 = BTreeMap::new();
        let mut tags2 = BTreeMap::new();
        tags1.extend(tmp_tags1.into_iter());
        tags2.extend(tmp_tags2.into_iter());
        let series_key1 = make_series_key(test_metric, &tags1);
        let series_key2 = make_series_key(test_metric, &tags2);
        let series_key3 = make_series_key(test_metric2, &tags1);
        for entry in &wreq.write_entries {
            let series_key = make_series_key(entry.series.metric.as_str(), &entry.series.tags);
            if series_key == series_key1 {
                assert_eq!(
                    *entry
                        .points
                        .get(&ts1)
                        .unwrap()
                        .fields
                        .get(test_field1.0)
                        .unwrap(),
                    Value::Int32(test_field1.1)
                );
                assert_eq!(
                    *entry
                        .points
                        .get(&ts1)
                        .unwrap()
                        .fields
                        .get(test_field2.0)
                        .unwrap(),
                    Value::String(test_field2.1.to_owned())
                );
                assert_eq!(
                    *entry
                        .points
                        .get(&ts2)
                        .unwrap()
                        .fields
                        .get(test_field3.0)
                        .unwrap(),
                    Value::Double(test_field3.1)
                );
            } else if series_key == series_key2 || series_key == series_key3 {
                assert_eq!(
                    *entry
                        .points
                        .get(&ts1)
                        .unwrap()
                        .fields
                        .get(test_field1.0)
                        .unwrap(),
                    Value::Int32(test_field1.1)
                );
            }
        }
    }

    #[test]
    fn test_convert_tags() {
        let test_tag1 = ("tag_name1", "tag_val1");
        let test_tag2 = ("tag_name2", 42);
        let test_tag3 = ("tag_name3", b"wewraewfsfjkldsafjkdlsa");
        let mut test_tags = BTreeMap::new();
        test_tags.insert(
            test_tag1.0.to_owned(),
            Value::String(test_tag1.1.to_owned()),
        );
        test_tags.insert(test_tag2.0.to_owned(), Value::Int32(test_tag2.1));
        test_tags.insert(
            test_tag3.0.to_owned(),
            Value::Varbinary(test_tag3.1.to_vec()),
        );

        let mut tag_dic = NameDict::new();

        let tags_pb = convert_tags(&mut tag_dic, &test_tags);
        let tag_names = tag_dic.convert_ordered();

        for tag_pb in tags_pb {
            let name_idx = tag_pb.get_name_index() as usize;
            let value_in_map: ValuePb = test_tags.get(&tag_names[name_idx]).unwrap().clone().into();
            assert_eq!(value_in_map, *tag_pb.get_value());
        }
    }

    #[test]
    fn test_convert_points() {
        // test points
        let mut test_points = BTreeMap::new();

        let ts1 = Local::now().timestamp_millis();
        let ts1_test_field1 = ("field_name1", "field_val1");
        let ts1_test_field2 = ("field_name2", 42);
        let ts1_test_field3 = ("field_name3", b"wewraewfsfjkldsafjkdlsa");
        let mut test_fields = HashMap::new();
        test_fields.insert(
            ts1_test_field1.0.to_owned(),
            Value::String(ts1_test_field1.1.to_owned()),
        );
        test_fields.insert(
            ts1_test_field2.0.to_owned(),
            Value::Int32(ts1_test_field2.1),
        );
        test_fields.insert(
            ts1_test_field3.0.to_owned(),
            Value::Varbinary(ts1_test_field3.1.to_vec()),
        );

        let ts2 = ts1 + 42;
        let ts2_test_field1 = ("field_name4", "field_val4");
        let ts2_test_field2 = ("field_name5", 4242);
        let ts2_test_field3 = ("field_name6", b"afewlfmewlkfmdksmf");
        let mut test_fields2 = HashMap::new();
        test_fields2.insert(
            ts2_test_field1.0.to_owned(),
            Value::String(ts2_test_field1.1.to_owned()),
        );
        test_fields2.insert(
            ts2_test_field2.0.to_owned(),
            Value::Int32(ts2_test_field2.1),
        );
        test_fields2.insert(
            ts2_test_field3.0.to_owned(),
            Value::Varbinary(ts2_test_field3.1.to_vec()),
        );

        test_points.insert(
            ts1,
            Point {
                fields: test_fields,
            },
        );
        test_points.insert(
            ts2,
            Point {
                fields: test_fields2,
            },
        );

        // convert and check
        let mut field_dic = NameDict::new();
        let field_groups_pb = convert_points(&mut field_dic, &test_points);
        let field_names = field_dic.convert_ordered();

        for f_group in field_groups_pb {
            let fields_map = &test_points.get(&f_group.get_timestamp()).unwrap().fields;
            for field_pb in f_group.fields {
                let key_in_map = field_names[field_pb.get_name_index() as usize].as_str();
                let val_in_map: ValuePb = fields_map.get(key_in_map).unwrap().clone().into();

                assert_eq!(val_in_map, *field_pb.get_value());
            }
        }
    }
}
