// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Write request and some useful tools for it.

use std::collections::{BTreeMap, HashMap};

use ceresdbproto::storage::{
    Field, FieldGroup as FieldGroupPb, Tag as TagPb, WriteRequest as WriteRequestPb,
    WriteSeriesEntry as WriteSeriesEntryPb, WriteTableRequest as WriteTableRequestPb,
};

use crate::model::value::{TimestampMs, Value};

type SeriesKey = Vec<u8>;

const TSID: &str = "tsid";
const TIMESTAMP: &str = "timestamp";

#[inline]
pub fn is_reserved_column_name(name: &str) -> bool {
    name.eq_ignore_ascii_case(TSID) || name.eq_ignore_ascii_case(TIMESTAMP)
}

/// Builder for [`WriteRequest`].
///
/// You should call [`row_builder`] to build and insert the row you want to
/// write. And after all rows inserted, call [`build`] to get [`WriteRequest`].
///
/// [`row_builder`]: WriteRequestBuilder::row_builder
/// [`build`]: WriteRequestBuilder::build
#[derive(Clone, Debug, Default)]
pub struct WriteRequestBuilder {
    write_entries: HashMap<SeriesKey, WriteEntry>,
}

impl WriteRequestBuilder {
    pub fn row_builder(&mut self) -> RowBuilder {
        RowBuilder {
            timestamp: None,
            metric: None,
            tags: BTreeMap::new(),
            fields: HashMap::new(),
            write_req_builder: self,
            contains_reserved_column_name: false,
        }
    }

    pub fn build(self) -> Request {
        let mut partitions_by_metric: HashMap<_, Vec<_>> = HashMap::new();
        for (_, entry) in self.write_entries.into_iter() {
            let partition = match partitions_by_metric.get_mut(&entry.series.metric) {
                Some(p) => p,
                None => partitions_by_metric
                    .entry(entry.series.metric.clone())
                    .or_insert_with(Vec::new),
            };
            partition.push(entry);
        }

        Request {
            write_entries: partitions_by_metric,
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
    contains_reserved_column_name: bool,
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
    pub fn finish(self) -> Result<(), String> {
        if self.contains_reserved_column_name {
            return Err("Tag or field name reserved column name in ceresdb".to_string());
        }

        if self.fields.is_empty() {
            return Err("Fields should not be empty".to_string());
        }

        // make series key
        let metric = self
            .metric
            .ok_or_else(|| "Metric must be set".to_string())?;
        let series_key = make_series_key(metric.as_str(), &self.tags);

        // insert to write req builder
        let tags = self.tags;
        let write_entry = self
            .write_req_builder
            .write_entries
            .entry(series_key)
            .or_insert_with(|| {
                let series = Series { metric, tags };

                WriteEntry {
                    series,
                    ts_fields: BTreeMap::new(),
                }
            });

        let fields = write_entry
            .ts_fields
            .entry(
                self.timestamp
                    .ok_or_else(|| "Timestamp must be set".to_string())?,
            )
            .or_insert_with(Fields::default);
        fields.extend(self.fields.into_iter());

        Ok(())
    }
}

fn make_series_key(metric: &str, tags: &BTreeMap<String, Value>) -> SeriesKey {
    let mut series_key = metric.as_bytes().to_vec();
    for (name, val) in tags {
        series_key.extend_from_slice(name.as_bytes());
        series_key.extend_from_slice(&val.to_bytes());
    }

    series_key
}

#[derive(Clone, Debug, Default)]
pub struct Request {
    pub write_entries: HashMap<String, Vec<WriteEntry>>,
}

#[derive(Clone, Default, Debug)]
pub struct WriteEntry {
    pub series: Series,
    ts_fields: BTreeMap<TimestampMs, Fields>,
}

#[derive(Clone, Default, Debug)]
pub struct Series {
    pub(crate) metric: String,
    tags: BTreeMap<String, Value>,
}

type Fields = HashMap<String, Value>;

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

impl From<Request> for WriteRequestPb {
    fn from(req: Request) -> Self {
        let mut req_pb = WriteRequestPb::default();
        // partition the write entries first
        let mut table_requests_pb = Vec::with_capacity(req.write_entries.len());
        for (metric, entries) in req.write_entries {
            table_requests_pb.push(TableRequestPbBuilder::build(metric, entries));
        }
        req_pb.table_requests = table_requests_pb;

        req_pb
    }
}

struct TableRequestPbBuilder;

impl TableRequestPbBuilder {
    pub fn build(table: String, entries: Vec<WriteEntry>) -> WriteTableRequestPb {
        let mut tags_dict = NameDict::new();
        let mut fields_dict = NameDict::new();
        let mut wirte_entries_pb = Vec::with_capacity(entries.len());
        for entry in entries {
            wirte_entries_pb.push(Self::build_series_entry(
                &mut tags_dict,
                &mut fields_dict,
                entry,
            ));
        }

        WriteTableRequestPb {
            table,
            tag_names: tags_dict.convert_ordered(),
            field_names: fields_dict.convert_ordered(),
            entries: wirte_entries_pb,
        }
    }

    fn build_series_entry(
        tags_dict: &mut NameDict,
        fields_dict: &mut NameDict,
        entry: WriteEntry,
    ) -> WriteSeriesEntryPb {
        let tags = Self::build_tags(tags_dict, entry.series.tags);
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
            // ts + fields will be converted to field group in pb
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

            // collect field group
            field_group_pbs.push(field_group_pb);
        }

        field_group_pbs
    }
}

#[cfg(test)]
mod test {
    use std::collections::{BTreeMap, HashMap};

    use ceresdbproto::storage::Value as ValuePb;
    use chrono::Local;

    use super::{make_series_key, NameDict, TableRequestPbBuilder, WriteRequestBuilder};
    use crate::model::value::Value;

    #[test]
    fn test_build_write_metric() {
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
        assert_eq!(wreq.write_entries.len(), 2);
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
        let write_entries: Vec<_> = wreq
            .write_entries
            .iter()
            .flat_map(|(_, entries)| entries.iter())
            .collect();
        for entry in write_entries {
            let series_key = make_series_key(entry.series.metric.as_str(), &entry.series.tags);
            if series_key == series_key1 {
                assert_eq!(
                    *entry
                        .ts_fields
                        .get(&ts1)
                        .unwrap()
                        .get(test_field1.0)
                        .unwrap(),
                    Value::Int32(test_field1.1)
                );
                assert_eq!(
                    *entry
                        .ts_fields
                        .get(&ts1)
                        .unwrap()
                        .get(test_field2.0)
                        .unwrap(),
                    Value::String(test_field2.1.to_owned())
                );
                assert_eq!(
                    *entry
                        .ts_fields
                        .get(&ts2)
                        .unwrap()
                        .get(test_field3.0)
                        .unwrap(),
                    Value::Double(test_field3.1)
                );
            } else if series_key == series_key2 || series_key == series_key3 {
                assert_eq!(
                    *entry
                        .ts_fields
                        .get(&ts1)
                        .unwrap()
                        .get(test_field1.0)
                        .unwrap(),
                    Value::Int32(test_field1.1)
                );
            }
        }
    }

    #[test]
    fn test_build_tags() {
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

        let mut tags_dict = NameDict::new();

        let tags_pb = TableRequestPbBuilder::build_tags(&mut tags_dict, test_tags.clone());
        let tag_names = tags_dict.convert_ordered();

        for tag_pb in tags_pb {
            let name_idx = tag_pb.name_index as usize;
            let value_in_map: ValuePb = test_tags.get(&tag_names[name_idx]).unwrap().clone().into();
            assert_eq!(value_in_map, tag_pb.value.unwrap());
        }
    }

    #[test]
    fn test_convert_ts_fields() {
        let mut test_ts_fields = BTreeMap::new();

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

        test_ts_fields.insert(ts1, test_fields);
        test_ts_fields.insert(ts2, test_fields2);
        // convert and check
        let mut fields_dict = NameDict::new();
        let field_groups_pb =
            TableRequestPbBuilder::build_ts_fields(&mut fields_dict, test_ts_fields.clone());
        let field_names = fields_dict.convert_ordered();

        for f_group in field_groups_pb {
            let fields_map = test_ts_fields.get(&f_group.timestamp).unwrap();
            for field_pb in f_group.fields {
                let key_in_map = field_names[field_pb.name_index as usize].as_str();
                let val_in_map: ValuePb = fields_map.get(key_in_map).unwrap().clone().into();

                assert_eq!(val_in_map, field_pb.value.unwrap());
            }
        }
    }
}
