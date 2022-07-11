// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

use std::collections::{BTreeMap, HashMap};

use ceresdbproto::storage::{
    Field, FieldGroup as FieldGroupPb, Tag as TagPb, WriteEntry as WriteEntryPb,
    WriteMetric as WriteMetricPb, WriteRequest as WriteRequestPb,
};

use crate::model::value::{TimestampMs, Value};

#[inline]
fn is_key_word(name: &str) -> bool {
    name.eq_ignore_ascii_case("tsid") && name.eq_ignore_ascii_case("timestamp")
}

/// Write request
/// You can write into multiple metrics once
#[derive(Clone, Debug, Default)]
pub struct WriteRequest {
    write_entries: Vec<WriteEntry>,
}

impl WriteRequest {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn entry(&mut self, entry: WriteEntry) {
        self.write_entries.push(entry);
    }
}

#[derive(Clone, Debug)]
pub struct WriteEntry {
    series: Series,
    points: BTreeMap<i64, Point>,
}

#[derive(Clone, Default, Debug)]
struct Series {
    metric: String,
    tags: HashMap<String, Value>,
}

#[derive(Clone, Default, Debug)]
struct Point {
    filed_kvs: HashMap<String, Value>,
}

/// One time builder for `WriteMetric`
/// Should follow this order to build:
/// `WriteEntryBuilder` -> `SeriesBuilder` -> `PointsBuilder` -> `WriteEntry`
#[derive(Clone, Default)]
pub struct WriteEntryBuilder;

impl WriteEntryBuilder {
    pub fn new() -> Self {
        WriteEntryBuilder
    }

    pub fn to_series_builder(self) -> SeriesBuilder {
        SeriesBuilder::new()
    }
}

#[derive(Clone, Default)]
pub struct SeriesBuilder {
    metric: String,
    tags: HashMap<String, Value>,
    contains_keyword: bool,
}

impl SeriesBuilder {
    fn new() -> Self {
        SeriesBuilder {
            metric: String::new(),
            tags: HashMap::new(),
            contains_keyword: false,
        }
    }

    #[must_use]
    pub fn metric(mut self, metric: String) -> Self {
        self.metric = metric;
        self
    }

    /// You cannot set tag with name like 'timestamp' or 'tsid',
    /// because they are keywords in ceresdb.
    /// Normally, you can unwrap the result directly.
    #[allow(clippy::return_self_not_must_use)]
    pub fn tag(mut self, name: String, value: Value) -> Self {
        if is_key_word(&name) {
            self.contains_keyword = true;
        }

        let _ = self.tags.insert(name, value);
        self
    }

    pub fn to_points_builder(self) -> Result<PointsBuilder, String> {
        if self.metric.is_empty() {
            return Err("Metric is not set".to_owned());
        }

        if self.contains_keyword {
            return Err("Contains ceresdb keyword in tag names".to_owned());
        }

        Ok(PointsBuilder::new(Series {
            metric: self.metric,
            tags: self.tags,
        }))
    }
}

#[derive(Clone, Default)]
pub struct PointsBuilder {
    series: Series,
    points: BTreeMap<i64, Point>,
    contains_keyword: bool,
}

impl PointsBuilder {
    fn new(series: Series) -> Self {
        PointsBuilder {
            series,
            points: BTreeMap::new(),
            contains_keyword: false,
        }
    }

    /// Point represents fileds in one timestamp.
    /// Field name cannot be keyword in ceresdb (same as tag name).
    #[must_use]
    pub fn field_in_point(mut self, timestamp: TimestampMs, name: String, value: Value) -> Self {
        if is_key_word(&name) {
            self.contains_keyword = true;
        }

        let point = self.points.entry(timestamp).or_insert_with(Point::default);
        let _ = point.filed_kvs.insert(name, value);
        self
    }

    pub fn build(self) -> Result<WriteEntry, String> {
        if self.points.is_empty() {
            return Err("Write nothing into metric".to_owned());
        }

        if self.contains_keyword {
            return Err("Contains ceresdb keyword in field names".to_owned());
        }

        Ok(WriteEntry {
            series: self.series,
            points: self.points,
        })
    }
}

/// Struct help to convert `WriteRequest` to `WriteRequestPb`
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

/// TODO(kamille) reduce cloning from tag_kvs
fn convert_tags(tags_dic: &mut NameDict, tags: &HashMap<String, Value>) -> Vec<TagPb> {
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

        let mut field_pbs = Vec::with_capacity(point.filed_kvs.len());
        for (name, val) in point.filed_kvs.iter() {
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

    use super::{convert_points, convert_tags, NameDict, Point, WriteEntryBuilder};
    use crate::model::value::Value;

    #[test]
    fn test_build_wirte_metric() {
        let test_metric = "test_metric";
        let test_tag1 = ("test_tag1", 42);
        let test_tag2 = ("test_tag2", "test_tag_val");
        let test_field1 = ("test_field1", 42);
        let test_field2 = ("test_field2", "test_field_val");

        let now = Local::now().timestamp_millis();
        let test_entry = WriteEntryBuilder::new()
            .to_series_builder()
            .metric(test_metric.to_owned())
            .tag(test_tag1.0.to_owned(), Value::Int32(test_tag1.1))
            .unwrap()
            .tag(
                test_tag2.0.to_owned(),
                Value::String(test_tag2.1.to_owned()),
            )
            .unwrap()
            .to_points_builder()
            .expect("to points builder failed")
            .field_in_point(now, test_field1.0.to_owned(), Value::Int32(test_field1.1))
            .unwrap()
            .field_in_point(
                now,
                test_field2.0.to_owned(),
                Value::String(test_field2.1.to_owned()),
            )
            .unwrap()
            .build()
            .expect("build write metric failed");

        assert_eq!(test_entry.series.metric, test_metric);
        assert_eq!(
            *test_entry.series.tag_kvs.get(test_tag1.0).unwrap(),
            Value::Int32(test_tag1.1)
        );
        assert_eq!(
            *test_entry.series.tag_kvs.get(test_tag2.0).unwrap(),
            Value::String(test_tag2.1.to_owned())
        );
        assert_eq!(
            *test_entry
                .points
                .get(&now)
                .unwrap()
                .filed_kvs
                .get(test_field1.0)
                .unwrap(),
            Value::Int32(test_field1.1)
        );
        assert_eq!(
            *test_entry
                .points
                .get(&now)
                .unwrap()
                .filed_kvs
                .get(test_field2.0)
                .unwrap(),
            Value::String(test_field2.1.to_owned())
        );
    }

    #[test]
    fn test_convert_tags() {
        let test_tag1 = ("tag_name1", "tag_val1");
        let test_tag2 = ("tag_name2", 42);
        let test_tag3 = ("tag_name3", b"wewraewfsfjkldsafjkdlsa");
        let mut test_tags = HashMap::new();
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
                filed_kvs: test_fields,
            },
        );
        test_points.insert(
            ts2,
            Point {
                filed_kvs: test_fields2,
            },
        );

        // convert and check
        let mut field_dic = NameDict::new();
        let field_groups_pb = convert_points(&mut field_dic, &test_points);
        let field_names = field_dic.convert_ordered();

        for f_group in field_groups_pb {
            let fields_map = &test_points.get(&f_group.get_timestamp()).unwrap().filed_kvs;
            for field_pb in f_group.fields {
                let key_in_map = field_names[field_pb.get_name_index() as usize].as_str();
                let val_in_map: ValuePb = fields_map.get(key_in_map).unwrap().clone().into();

                assert_eq!(val_in_map, *field_pb.get_value());
            }
        }
    }
}
