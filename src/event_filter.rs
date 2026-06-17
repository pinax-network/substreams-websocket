use std::collections::{BTreeMap, HashMap};

use serde_json::{Map, Value};

#[derive(Debug, thiserror::Error)]
pub enum EventFilterError {
    #[error("filter must be a JSON object")]
    NotAnObject,

    #[error("filter field {field:?} has invalid value: must be a string or array of strings")]
    InvalidFieldValue { field: String },

    #[error("filter exceeds max fields: {actual} > {max}")]
    TooManyFields { actual: usize, max: usize },

    #[error("filter exceeds max values (total across all fields): {actual} > {max}")]
    TooManyValues { actual: usize, max: usize },

    #[error("failed to parse filter JSON: {0}")]
    Parse(String),
}

/// Per-subscription event filter. `{ "protocol": "raydium_cpmm", "user": ["a","b"] }`
/// matches events where `protocol == "raydium_cpmm" AND user IN ("a","b")`.
///
/// Fields are AND'd; values within a field are OR'd. Events missing the
/// filtered field are dropped (conservative — operators do not receive rows
/// they cannot inspect for the filter key).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EventFilter {
    /// Ordered for stable `list()` output. BTreeMap by field name.
    fields: BTreeMap<String, Vec<String>>,
}

impl EventFilter {
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Parse a filter from a JSON value. Caller enforces field / value caps.
    pub fn from_json(
        value: &Value,
        max_fields: usize,
        max_values: usize,
    ) -> Result<Self, EventFilterError> {
        let obj = value.as_object().ok_or(EventFilterError::NotAnObject)?;
        if obj.len() > max_fields {
            return Err(EventFilterError::TooManyFields {
                actual: obj.len(),
                max: max_fields,
            });
        }

        let mut fields = BTreeMap::new();
        let mut total_values: usize = 0;
        for (key, raw) in obj {
            let values = match raw {
                Value::String(s) => vec![s.clone()],
                Value::Array(items) => {
                    let mut out = Vec::with_capacity(items.len());
                    for item in items {
                        let Some(s) = item.as_str() else {
                            return Err(EventFilterError::InvalidFieldValue { field: key.clone() });
                        };
                        out.push(s.to_owned());
                    }
                    out
                }
                _ => {
                    return Err(EventFilterError::InvalidFieldValue { field: key.clone() });
                }
            };
            total_values += values.len();
            if total_values > max_values {
                return Err(EventFilterError::TooManyValues {
                    actual: total_values,
                    max: max_values,
                });
            }
            fields.insert(key.clone(), values);
        }

        Ok(Self { fields })
    }

    pub fn from_str(
        raw: &str,
        max_fields: usize,
        max_values: usize,
    ) -> Result<Self, EventFilterError> {
        let value: Value =
            serde_json::from_str(raw).map_err(|e| EventFilterError::Parse(e.to_string()))?;
        Self::from_json(&value, max_fields, max_values)
    }

    /// Check whether a single event row matches every field in the filter.
    /// An empty filter matches every event.
    pub fn matches_event(&self, event: &Map<String, Value>) -> bool {
        if self.fields.is_empty() {
            return true;
        }
        for (key, allowed) in &self.fields {
            let Some(actual) = event.get(key).and_then(Value::as_str) else {
                return false;
            };
            if !allowed.iter().any(|v| v == actual) {
                return false;
            }
        }
        true
    }

    pub fn to_json(&self) -> Value {
        let mut obj = Map::new();
        for (key, values) in &self.fields {
            let v = if values.len() == 1 {
                Value::String(values[0].clone())
            } else {
                Value::Array(values.iter().map(|s| Value::String(s.clone())).collect())
            };
            obj.insert(key.clone(), v);
        }
        Value::Object(obj)
    }
}

/// Per-connection map of `network@table` selector → filter. Wildcards on
/// either side of `@` are supported: `*@*`, `<network>@*`, `*@<table>`.
/// At broadcast time every stored filter whose selector matches the
/// outgoing `(network, table)` contributes — all must pass.
#[derive(Debug, Clone, Default)]
pub struct EventFilterSet {
    by_selector: HashMap<String, EventFilter>,
}

impl EventFilterSet {
    pub fn is_empty(&self) -> bool {
        self.by_selector.is_empty()
    }

    pub fn set(&mut self, selector: String, filter: EventFilter) {
        self.by_selector.insert(selector, filter);
    }

    pub fn remove(&mut self, selector: &str) {
        self.by_selector.remove(selector);
    }

    pub fn get(&self, selector: &str) -> Option<&EventFilter> {
        self.by_selector.get(selector)
    }

    /// Every stored filter whose selector matches `(network, table)` —
    /// exact match plus the three wildcard variants.
    pub fn matching(&self, network: &str, table: &str) -> Vec<&EventFilter> {
        let candidates = [
            format!("{network}@{table}"),
            format!("{network}@*"),
            format!("*@{table}"),
            "*@*".to_owned(),
        ];
        candidates
            .iter()
            .filter_map(|k| self.by_selector.get(k))
            .filter(|f| !f.is_empty())
            .collect()
    }

    pub fn list(&self) -> Map<String, Value> {
        let mut keys: Vec<&String> = self.by_selector.keys().collect();
        keys.sort();
        let mut out = Map::new();
        for key in keys {
            out.insert(key.clone(), self.by_selector[key].to_json());
        }
        out
    }
}

/// Apply a filter to a fully-decoded block JSON. Mutates `events[]` in place,
/// retaining only events that match. Returns the surviving event count.
pub fn apply_filter_in_place(block: &mut Value, filter: &EventFilter) -> usize {
    if filter.is_empty() {
        return block
            .get("events")
            .and_then(Value::as_array)
            .map(Vec::len)
            .unwrap_or(0);
    }
    let Some(events) = block.get_mut("events").and_then(Value::as_array_mut) else {
        return 0;
    };
    events.retain(|event| {
        let Some(obj) = event.as_object() else {
            return false;
        };
        filter.matches_event(obj)
    });
    events.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(pairs: &[(&str, &str)]) -> Map<String, Value> {
        let mut m = Map::new();
        for (k, v) in pairs {
            m.insert((*k).to_owned(), Value::String((*v).to_owned()));
        }
        m
    }

    #[test]
    fn empty_filter_matches_every_event() {
        let filter = EventFilter::default();
        let evt = event(&[("protocol", "raydium_cpmm")]);
        assert!(filter.matches_event(&evt));
    }

    #[test]
    fn single_field_string_equality_matches() {
        let filter = EventFilter::from_str(r#"{"protocol":"raydium_cpmm"}"#, 16, 64).unwrap();
        assert!(filter.matches_event(&event(&[("protocol", "raydium_cpmm")])));
        assert!(!filter.matches_event(&event(&[("protocol", "pump_fun")])));
    }

    #[test]
    fn array_value_matches_any() {
        let filter = EventFilter::from_str(r#"{"user":["a","b"]}"#, 16, 64).unwrap();
        assert!(filter.matches_event(&event(&[("user", "a")])));
        assert!(filter.matches_event(&event(&[("user", "b")])));
        assert!(!filter.matches_event(&event(&[("user", "c")])));
    }

    #[test]
    fn missing_field_is_a_miss() {
        let filter = EventFilter::from_str(r#"{"protocol":"raydium_cpmm"}"#, 16, 64).unwrap();
        assert!(!filter.matches_event(&event(&[("user", "abc")])));
    }

    #[test]
    fn multi_field_is_and() {
        let filter =
            EventFilter::from_str(r#"{"protocol":"raydium_cpmm","user":"abc"}"#, 16, 64).unwrap();
        assert!(filter.matches_event(&event(&[("protocol", "raydium_cpmm"), ("user", "abc")])));
        assert!(!filter.matches_event(&event(&[("protocol", "raydium_cpmm"), ("user", "xyz")])));
        assert!(!filter.matches_event(&event(&[("protocol", "pump_fun"), ("user", "abc")])));
    }

    #[test]
    fn empty_array_matches_nothing() {
        let filter = EventFilter::from_str(r#"{"user":[]}"#, 16, 64).unwrap();
        assert!(!filter.matches_event(&event(&[("user", "abc")])));
    }

    #[test]
    fn rejects_non_object() {
        let err = EventFilter::from_str(r#"["protocol"]"#, 16, 64).unwrap_err();
        assert!(matches!(err, EventFilterError::NotAnObject));
    }

    #[test]
    fn rejects_non_string_value() {
        let err = EventFilter::from_str(r#"{"protocol":42}"#, 16, 64).unwrap_err();
        assert!(matches!(err, EventFilterError::InvalidFieldValue { .. }));
    }

    #[test]
    fn rejects_non_string_array_element() {
        let err = EventFilter::from_str(r#"{"user":["a",42]}"#, 16, 64).unwrap_err();
        assert!(matches!(err, EventFilterError::InvalidFieldValue { .. }));
    }

    #[test]
    fn enforces_max_fields() {
        let err = EventFilter::from_str(r#"{"a":"1","b":"2","c":"3"}"#, 2, 64).unwrap_err();
        assert!(matches!(err, EventFilterError::TooManyFields { .. }));
    }

    #[test]
    fn enforces_max_values() {
        let err = EventFilter::from_str(r#"{"user":["a","b","c","d"]}"#, 16, 3).unwrap_err();
        assert!(matches!(err, EventFilterError::TooManyValues { .. }));
    }

    #[test]
    fn apply_filter_in_place_retains_matching_events() {
        let mut block = serde_json::json!({
            "stream": "swaps",
            "events": [
                { "@table": "swaps", "protocol": "raydium_cpmm", "user": "a" },
                { "@table": "swaps", "protocol": "pump_fun", "user": "b" },
                { "@table": "swaps", "protocol": "raydium_cpmm", "user": "c" }
            ]
        });
        let filter = EventFilter::from_str(r#"{"protocol":"raydium_cpmm"}"#, 16, 64).unwrap();
        let remaining = apply_filter_in_place(&mut block, &filter);
        assert_eq!(remaining, 2);
        let events = block["events"].as_array().unwrap();
        assert_eq!(events[0]["user"], "a");
        assert_eq!(events[1]["user"], "c");
    }

    #[test]
    fn to_json_roundtrips_through_from_json() {
        let original =
            EventFilter::from_str(r#"{"protocol":"raydium_cpmm","user":["a","b"]}"#, 16, 64)
                .unwrap();
        let roundtripped = EventFilter::from_json(&original.to_json(), 16, 64).unwrap();
        assert_eq!(original, roundtripped);
    }

    #[test]
    fn filter_set_list_returns_sorted_selectors() {
        let mut set = EventFilterSet::default();
        set.set(
            "solana-mainnet@swaps".to_owned(),
            EventFilter::from_str(r#"{"protocol":"raydium_cpmm"}"#, 16, 64).unwrap(),
        );
        set.set(
            "ethereum-mainnet@transfers".to_owned(),
            EventFilter::from_str(r#"{"mint":"abc"}"#, 16, 64).unwrap(),
        );
        let listed = set.list();
        let keys: Vec<&String> = listed.keys().collect();
        assert_eq!(
            keys,
            vec!["ethereum-mainnet@transfers", "solana-mainnet@swaps"]
        );
    }
}
