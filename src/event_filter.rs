use std::collections::{BTreeSet, HashMap};

use serde_json::{Map, Value};

#[derive(Debug, thiserror::Error)]
pub enum EventFilterError {
    #[error("filter expression must be a string")]
    NotAString,

    #[error("filter parse error at position {pos}: {msg}")]
    Parse { pos: usize, msg: String },

    #[error("filter exceeds max terms (total across the expression): {actual} > {max}")]
    TooManyTerms { actual: usize, max: usize },

    #[error("filter references more fields than allowed: {actual} > {max}")]
    TooManyFields { actual: usize, max: usize },

    #[error("filter nests parentheses deeper than {max} levels")]
    TooDeep { max: usize },
}

/// One node of a parsed filter expression (a [StreamingFast SQE]-style boolean
/// query). A term is either `field:value` (exact equality on that event column)
/// or a bare `value` (matches when *any* string column of the event equals it).
///
/// [StreamingFast SQE]: https://github.com/streamingfast/substreams-rs
#[derive(Debug, Clone, PartialEq, Eq)]
enum Expr {
    /// `field:value` when `field` is `Some`; a bare `value` when `None`.
    Term {
        field: Option<String>,
        value: String,
    },
    Not(Box<Expr>),
    And(Vec<Expr>),
    Or(Vec<Expr>),
}

impl Expr {
    fn eval(&self, event: &Map<String, Value>, bare_values: Option<&[&str]>) -> bool {
        match self {
            Expr::Term {
                field: Some(field),
                value,
            } => event
                .get(field)
                .and_then(Value::as_str)
                .is_some_and(|actual| actual.eq_ignore_ascii_case(value)),
            Expr::Term { field: None, value } => {
                bare_values.is_some_and(|vs| vs.iter().any(|v| v.eq_ignore_ascii_case(value)))
            }
            Expr::Not(inner) => !inner.eval(event, bare_values),
            Expr::And(children) => children.iter().all(|c| c.eval(event, bare_values)),
            Expr::Or(children) => children.iter().any(|c| c.eval(event, bare_values)),
        }
    }

    /// Accumulate cap inputs: total terms, the set of named fields, and whether
    /// any bare (field-less) term exists.
    fn walk(&self, terms: &mut usize, fields: &mut BTreeSet<String>, has_bare: &mut bool) {
        match self {
            Expr::Term { field, .. } => {
                *terms += 1;
                match field {
                    Some(f) => {
                        fields.insert(f.clone());
                    }
                    None => *has_bare = true,
                }
            }
            Expr::Not(inner) => inner.walk(terms, fields, has_bare),
            Expr::And(children) | Expr::Or(children) => {
                children
                    .iter()
                    .for_each(|c| c.walk(terms, fields, has_bare));
            }
        }
    }
}

/// Per-subscription event filter — a [StreamingFast SQE]-style boolean
/// expression over `events[*]` columns. Examples:
///
/// ```text
/// protocol:raydium_cpmm                       single field equality
/// maker:0xabc || taker:0xabc                  OR across columns
/// protocol:raydium_cpmm && user:0xabc         AND (also implied by whitespace)
/// (maker:0xabc || taker:0xabc) && !amm:0xdead  grouping + negation
/// 0xabc                                       bare term: any column == 0xabc
/// ```
///
/// `field:value` is **ASCII-case-insensitive** string equality; a bare `value`
/// (no field) matches when any string column of the event equals it. Operators:
/// `||` (or), `&&` or whitespace (and), `!` (not), `( )` (grouping). Values
/// containing whitespace or `() | & ' "` must be quoted (`'…'` or `"…"`). An
/// empty expression matches every event. Events missing a referenced `field`
/// are a miss (conservative).
///
/// [StreamingFast SQE]: https://github.com/streamingfast/substreams-rs
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EventFilter {
    /// `None` = empty expression = matches every event.
    root: Option<Expr>,
    /// Original expression text, echoed verbatim by `LIST_FILTERS`.
    source: String,
    /// Whether any bare (field-less) term exists. When false, matching skips
    /// building the per-event value set.
    has_bare: bool,
}

impl EventFilter {
    /// Max parenthesis nesting depth — guards against pathological payloads.
    const MAX_DEPTH: usize = 16;

    /// `true` when the filter imposes no constraint (empty expression). Used to
    /// short-circuit broadcast-time filtering.
    pub fn is_empty(&self) -> bool {
        self.root.is_none()
    }

    /// Parse a filter expression string. `max_values` caps the total number of
    /// terms; `max_fields` caps the number of distinct named fields. An empty /
    /// whitespace-only string is a valid match-everything filter.
    pub fn parse(
        raw: &str,
        max_fields: usize,
        max_values: usize,
    ) -> Result<Self, EventFilterError> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Ok(Self::default());
        }
        let mut parser = Parser::new(trimmed);
        let expr = parser.parse_or(0)?;
        parser.skip_ws();
        if !parser.at_end() {
            return Err(EventFilterError::Parse {
                pos: parser.pos,
                msg: "unexpected trailing input (use && / || between terms)".to_owned(),
            });
        }

        let mut terms = 0usize;
        let mut fields = BTreeSet::new();
        let mut has_bare = false;
        expr.walk(&mut terms, &mut fields, &mut has_bare);
        if terms > max_values {
            return Err(EventFilterError::TooManyTerms {
                actual: terms,
                max: max_values,
            });
        }
        if fields.len() > max_fields {
            return Err(EventFilterError::TooManyFields {
                actual: fields.len(),
                max: max_fields,
            });
        }

        Ok(Self {
            root: Some(expr),
            source: trimmed.to_owned(),
            has_bare,
        })
    }

    /// Parse from a JSON value — accepts a string expression (the wire form for
    /// `SET_FILTER` / `?filter=`). Non-strings are rejected.
    pub fn from_json(
        value: &Value,
        max_fields: usize,
        max_values: usize,
    ) -> Result<Self, EventFilterError> {
        let raw = value.as_str().ok_or(EventFilterError::NotAString)?;
        Self::parse(raw, max_fields, max_values)
    }

    /// Convenience wrapper over [`EventFilter::parse`] for `&str` callers.
    pub fn from_str(
        raw: &str,
        max_fields: usize,
        max_values: usize,
    ) -> Result<Self, EventFilterError> {
        Self::parse(raw, max_fields, max_values)
    }

    /// Check whether a single event row matches the expression. An empty filter
    /// matches every event.
    pub fn matches_event(&self, event: &Map<String, Value>) -> bool {
        let Some(root) = &self.root else {
            return true;
        };
        if self.has_bare {
            let values: Vec<&str> = event.values().filter_map(Value::as_str).collect();
            root.eval(event, Some(&values))
        } else {
            root.eval(event, None)
        }
    }

    /// The filter's wire representation: its source expression as a JSON string
    /// (round-trips through [`EventFilter::from_json`]).
    pub fn to_json(&self) -> Value {
        Value::String(self.source.clone())
    }
}

/// Recursive-descent parser for the SQE-style grammar:
///
/// ```text
/// or      := and ( '||' and )*
/// and     := unary ( ('&&')? unary )*      // adjacency = implicit AND
/// unary   := '!' unary | primary
/// primary := '(' or ')' | term
/// term    := (field ':')? value
/// value   := '"' … '"' | '\'' … '\'' | unquoted
/// ```
struct Parser<'a> {
    src: &'a str,
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(src: &'a str) -> Self {
        Self {
            src,
            bytes: src.as_bytes(),
            pos: 0,
        }
    }

    fn at_end(&self) -> bool {
        self.pos >= self.bytes.len()
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(b) if b.is_ascii_whitespace()) {
            self.pos += 1;
        }
    }

    /// True if the next two bytes are `op`.
    fn peek2(&self, op: &[u8; 2]) -> bool {
        self.bytes.get(self.pos..self.pos + 2) == Some(&op[..])
    }

    fn parse_or(&mut self, depth: usize) -> Result<Expr, EventFilterError> {
        let mut children = vec![self.parse_and(depth)?];
        loop {
            self.skip_ws();
            if self.peek2(b"||") {
                self.pos += 2;
                children.push(self.parse_and(depth)?);
            } else {
                break;
            }
        }
        Ok(if children.len() == 1 {
            children.pop().expect("len 1")
        } else {
            Expr::Or(children)
        })
    }

    fn parse_and(&mut self, depth: usize) -> Result<Expr, EventFilterError> {
        let mut children = vec![self.parse_unary(depth)?];
        loop {
            self.skip_ws();
            if self.peek2(b"&&") {
                self.pos += 2;
                children.push(self.parse_unary(depth)?);
            } else if self.peek2(b"||") || matches!(self.peek(), Some(b')') | None) {
                break;
            } else {
                // Adjacency without an operator is an implicit AND.
                children.push(self.parse_unary(depth)?);
            }
        }
        Ok(if children.len() == 1 {
            children.pop().expect("len 1")
        } else {
            Expr::And(children)
        })
    }

    fn parse_unary(&mut self, depth: usize) -> Result<Expr, EventFilterError> {
        self.skip_ws();
        if self.peek() == Some(b'!') {
            self.pos += 1;
            return Ok(Expr::Not(Box::new(self.parse_unary(depth)?)));
        }
        self.parse_primary(depth)
    }

    fn parse_primary(&mut self, depth: usize) -> Result<Expr, EventFilterError> {
        self.skip_ws();
        if self.peek() == Some(b'(') {
            if depth + 1 > EventFilter::MAX_DEPTH {
                return Err(EventFilterError::TooDeep {
                    max: EventFilter::MAX_DEPTH,
                });
            }
            self.pos += 1;
            let inner = self.parse_or(depth + 1)?;
            self.skip_ws();
            if self.peek() != Some(b')') {
                return Err(EventFilterError::Parse {
                    pos: self.pos,
                    msg: "expected ')'".to_owned(),
                });
            }
            self.pos += 1;
            return Ok(inner);
        }
        self.parse_term()
    }

    fn parse_term(&mut self) -> Result<Expr, EventFilterError> {
        self.skip_ws();
        // Optional `field:` prefix — a run of field chars immediately followed
        // by ':'. Otherwise the run is part of a bare value.
        let mut field = None;
        let ident_end = {
            let mut i = self.pos;
            while matches!(self.bytes.get(i), Some(b) if is_field_byte(*b)) {
                i += 1;
            }
            i
        };
        if ident_end > self.pos && self.bytes.get(ident_end) == Some(&b':') {
            field = Some(self.src[self.pos..ident_end].to_owned());
            self.pos = ident_end + 1; // consume field and ':'
        }
        let value = self.parse_value()?;
        Ok(Expr::Term { field, value })
    }

    fn parse_value(&mut self) -> Result<String, EventFilterError> {
        match self.peek() {
            Some(q @ (b'"' | b'\'')) => {
                self.pos += 1;
                let start = self.pos;
                while let Some(b) = self.peek() {
                    if b == q {
                        let value = self.src[start..self.pos].to_owned();
                        self.pos += 1;
                        return Ok(value);
                    }
                    self.pos += 1;
                }
                Err(EventFilterError::Parse {
                    pos: start,
                    msg: "unterminated quoted value".to_owned(),
                })
            }
            _ => {
                let start = self.pos;
                while matches!(self.peek(), Some(b) if is_value_byte(b)) {
                    self.pos += 1;
                }
                if self.pos == start {
                    return Err(EventFilterError::Parse {
                        pos: self.pos,
                        msg: "expected a term".to_owned(),
                    });
                }
                Ok(self.src[start..self.pos].to_owned())
            }
        }
    }
}

/// Bytes allowed in a `field` identifier (before the `:`).
fn is_field_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'_' | b'@' | b'.' | b'-')
}

/// Bytes allowed in an unquoted value — anything except whitespace, the
/// grouping/operator characters, and quotes.
fn is_value_byte(b: u8) -> bool {
    !b.is_ascii_whitespace() && !matches!(b, b'(' | b')' | b'|' | b'&' | b'"' | b'\'')
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

    fn parse(expr: &str) -> EventFilter {
        EventFilter::parse(expr, 16, 64).expect("parses")
    }

    #[test]
    fn empty_filter_matches_every_event() {
        assert!(EventFilter::default().matches_event(&event(&[("protocol", "raydium_cpmm")])));
        assert!(parse("   ").is_empty());
        assert!(parse("").matches_event(&event(&[("a", "b")])));
    }

    #[test]
    fn single_field_equality() {
        let f = parse("protocol:raydium_cpmm");
        assert!(f.matches_event(&event(&[("protocol", "raydium_cpmm")])));
        assert!(!f.matches_event(&event(&[("protocol", "pump_fun")])));
    }

    #[test]
    fn missing_field_is_a_miss() {
        assert!(!parse("protocol:raydium_cpmm").matches_event(&event(&[("user", "abc")])));
    }

    #[test]
    fn field_equality_is_ascii_case_insensitive() {
        // EVM addresses are lowercase on the wire; a checksummed query still
        // matches so users don't have to normalize casing.
        let f = parse("tx_from:0xABC");
        assert!(f.matches_event(&event(&[("tx_from", "0xabc")])));
        assert!(f.matches_event(&event(&[("tx_from", "0xABC")])));
        // Bare terms are case-insensitive too.
        assert!(parse("0xDEAD").matches_event(&event(&[("maker", "0xdead")])));
    }

    #[test]
    fn or_across_fields() {
        // The headline use case: a wallet as maker OR taker OR tx_from.
        let f = parse("tx_from:0xW || maker:0xW || taker:0xW");
        assert!(f.matches_event(&event(&[("maker", "0xW"), ("taker", "0xZ")])));
        assert!(f.matches_event(&event(&[("taker", "0xW")])));
        assert!(f.matches_event(&event(&[("tx_from", "0xW")])));
        assert!(!f.matches_event(&event(&[("maker", "0xA"), ("taker", "0xB")])));
    }

    #[test]
    fn explicit_and() {
        let f = parse("protocol:raydium_cpmm && user:0xabc");
        assert!(f.matches_event(&event(&[("protocol", "raydium_cpmm"), ("user", "0xabc")])));
        assert!(!f.matches_event(&event(&[("protocol", "raydium_cpmm"), ("user", "0xZ")])));
    }

    #[test]
    fn implicit_and_by_whitespace() {
        let f = parse("protocol:raydium_cpmm user:0xabc");
        assert!(f.matches_event(&event(&[("protocol", "raydium_cpmm"), ("user", "0xabc")])));
        assert!(!f.matches_event(&event(&[("protocol", "raydium_cpmm")])));
    }

    #[test]
    fn grouping_and_precedence() {
        // (a || b) && c
        let f = parse("(maker:0xW || taker:0xW) && protocol:clob");
        assert!(f.matches_event(&event(&[("maker", "0xW"), ("protocol", "clob")])));
        assert!(!f.matches_event(&event(&[("maker", "0xW"), ("protocol", "amm")])));
        // Without grouping, && binds tighter than ||.
        let g = parse("maker:0xW || taker:0xW && protocol:clob");
        assert!(g.matches_event(&event(&[("maker", "0xW"), ("protocol", "amm")])));
    }

    #[test]
    fn negation() {
        let f = parse("maker:0xW && !amm:0xdead");
        assert!(f.matches_event(&event(&[("maker", "0xW"), ("amm", "0xlive")])));
        assert!(!f.matches_event(&event(&[("maker", "0xW"), ("amm", "0xdead")])));
    }

    #[test]
    fn bare_term_matches_any_field() {
        let f = parse("0xWALLET");
        assert!(f.matches_event(&event(&[("maker", "0xWALLET")])));
        assert!(f.matches_event(&event(&[("tx_from", "0xWALLET")])));
        assert!(!f.matches_event(&event(&[("maker", "0xother")])));
    }

    #[test]
    fn quoted_value_with_spaces() {
        let f = parse(r#"label:"hello world""#);
        assert!(f.matches_event(&event(&[("label", "hello world")])));
        let bare = parse("'some value'");
        assert!(bare.matches_event(&event(&[("note", "some value")])));
    }

    #[test]
    fn to_json_roundtrips() {
        let src = "maker:0xW || taker:0xW";
        let f = parse(src);
        assert_eq!(f.to_json(), Value::String(src.to_owned()));
        let again = EventFilter::from_json(&f.to_json(), 16, 64).unwrap();
        assert_eq!(f, again);
    }

    #[test]
    fn rejects_non_string_json() {
        assert!(matches!(
            EventFilter::from_json(&serde_json::json!({"a": 1}), 16, 64),
            Err(EventFilterError::NotAString)
        ));
    }

    #[test]
    fn rejects_unterminated_quote_and_trailing_garbage() {
        assert!(matches!(
            EventFilter::parse(r#"label:"oops"#, 16, 64),
            Err(EventFilterError::Parse { .. })
        ));
        assert!(matches!(
            EventFilter::parse(")", 16, 64),
            Err(EventFilterError::Parse { .. })
        ));
    }

    #[test]
    fn enforces_max_terms() {
        let err = EventFilter::parse("a:1 || b:2 || c:3 || d:4", 16, 3).unwrap_err();
        assert!(matches!(
            err,
            EventFilterError::TooManyTerms { actual: 4, max: 3 }
        ));
    }

    #[test]
    fn enforces_max_fields() {
        let err = EventFilter::parse("a:1 && b:2 && c:3", 2, 64).unwrap_err();
        assert!(matches!(
            err,
            EventFilterError::TooManyFields { actual: 3, max: 2 }
        ));
    }

    #[test]
    fn apply_filter_in_place_retains_matching_events() {
        let mut block = serde_json::json!({
            "stream": "swaps",
            "events": [
                { "@table": "swaps", "maker": "0xW", "taker": "0xA" },
                { "@table": "swaps", "maker": "0xB", "taker": "0xW" },
                { "@table": "swaps", "maker": "0xC", "taker": "0xD" }
            ]
        });
        let f = parse("maker:0xW || taker:0xW");
        assert_eq!(apply_filter_in_place(&mut block, &f), 2);
    }

    #[test]
    fn filter_set_list_returns_sorted_selectors() {
        let mut set = EventFilterSet::default();
        set.set(
            "solana-mainnet@swaps".to_owned(),
            parse("protocol:raydium_cpmm"),
        );
        set.set("ethereum-mainnet@transfers".to_owned(), parse("mint:abc"));
        let listed = set.list();
        let keys: Vec<&String> = listed.keys().collect();
        assert_eq!(
            keys,
            vec!["ethereum-mainnet@transfers", "solana-mainnet@swaps"]
        );
        assert_eq!(
            listed["solana-mainnet@swaps"],
            Value::String("protocol:raydium_cpmm".to_owned())
        );
    }
}
