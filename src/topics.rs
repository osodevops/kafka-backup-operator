//! Topic selection helpers shared by `KafkaBackup` validation and the core
//! config adapter.
//!
//! Core selects a topic when it matches any `include` pattern and no `exclude`
//! pattern (`*` matches any run of characters, `?` exactly one; names are
//! case-sensitive). Two things core does *not* do are handled here:
//!
//! - it checks literal `include` entries against the cluster *before* applying
//!   `exclude`, so a literal that is also excluded fails the backup as
//!   "topic not found" (or is reported as missing) even though the user asked
//!   to leave it out — [`effective_selection`] drops such entries;
//! - an `include` list that `exclude` cancels completely only logs
//!   "No topics matched" and the run completes having archived nothing —
//!   [`validate_exclude_topics`] rejects it up front.

use kafka_backup_core::config::TopicSelection;

use crate::error::{Error, Result};

/// Same semantics as core's private `glob_match`: `*` matches any run of
/// characters (including none), `?` exactly one, anything else itself.
pub fn glob_match(pattern: &str, text: &str) -> bool {
    fn go(pattern: &[char], text: &[char]) -> bool {
        match pattern.split_first() {
            None => text.is_empty(),
            Some(('*', rest)) => (0..=text.len()).any(|i| go(rest, &text[i..])),
            Some(('?', rest)) => !text.is_empty() && go(rest, &text[1..]),
            Some((c, rest)) => text.first() == Some(c) && go(rest, &text[1..]),
        }
    }
    let pattern: Vec<char> = pattern.chars().collect();
    let text: Vec<char> = text.chars().collect();
    go(&pattern, &text)
}

/// A plain topic name rather than a glob.
pub fn is_literal(entry: &str) -> bool {
    !entry.contains(['*', '?'])
}

/// True when `exclude` guarantees that `include` can never select a topic: an
/// exclude entry is `*`, is identical to the include entry, or is a glob that
/// matches a literal include entry.
pub fn is_cancelled(include: &str, exclude: &[String]) -> bool {
    exclude
        .iter()
        .any(|x| x == "*" || x == include || (is_literal(include) && glob_match(x, include)))
}

/// Reject an `excludeTopics` list that cannot do what the user meant: a blank
/// entry, or a list that cancels every entry of `topics`.
pub fn validate_exclude_topics(topics: &[String], exclude: &[String]) -> Result<()> {
    if let Some(entry) = exclude.iter().find(|e| e.trim().is_empty()) {
        return Err(Error::validation(format!(
            "excludeTopics contains a blank entry {entry:?}: use a topic name or a glob"
        )));
    }
    if !topics.is_empty() && topics.iter().all(|t| is_cancelled(t, exclude)) {
        return Err(Error::validation(
            "excludeTopics cancels every entry of topics, so the backup could never select a topic",
        ));
    }
    Ok(())
}

/// The selection handed to core. Literal `topics` entries that `exclude`
/// cancels are dropped from `include`: they could never be selected, and
/// leaving them in makes core's missing-topic check fail (or report them as
/// missing) for a topic the user deliberately left out. `include` is never
/// emptied — an empty include means "everything" to core — so a fully
/// cancelled list (which [`validate_exclude_topics`] rejects) is passed
/// through unchanged if it is reached anyway.
pub fn effective_selection(topics: &[String], exclude: &[String]) -> TopicSelection {
    let live: Vec<String> = topics
        .iter()
        .filter(|t| !(is_literal(t) && is_cancelled(t, exclude)))
        .cloned()
        .collect();
    TopicSelection {
        include: if live.is_empty() {
            topics.to_vec()
        } else {
            live
        },
        exclude: exclude.to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn glob_matches_like_core() {
        assert!(glob_match("*", ""));
        assert!(glob_match("*", "orders"));
        assert!(glob_match("syslog-*", "syslog-"));
        assert!(glob_match("syslog-*", "syslog-1"));
        assert!(!glob_match("syslog-*", "syslog"));
        assert!(glob_match("a?c", "abc"));
        assert!(!glob_match("a?c", "ac"));
        assert!(glob_match("orders", "orders"));
        assert!(
            !glob_match("orders", "Orders"),
            "topic names are case-sensitive"
        );
        assert!(!glob_match("", "orders"));
        assert!(glob_match("", ""));
    }

    #[test]
    fn literal_vs_glob() {
        assert!(is_literal("orders"));
        assert!(!is_literal("orders-*"));
        assert!(!is_literal("order?"));
    }

    #[test]
    fn cancellation_rules() {
        assert!(is_cancelled("orders", &v(&["orders"])));
        assert!(is_cancelled("orders", &v(&["ord*"])));
        assert!(is_cancelled("orders-*", &v(&["orders-*"])));
        assert!(is_cancelled("orders-*", &v(&["*"])));
        assert!(!is_cancelled("orders", &v(&["metrics", "syslog-*"])));
        // A glob include is not cancelled by a narrower exclude.
        assert!(!is_cancelled("orders-*", &v(&["orders-eu"])));
        assert!(!is_cancelled("orders", &[]));
    }

    #[test]
    fn validation_rejects_blank_and_fully_cancelled_lists() {
        assert!(validate_exclude_topics(&v(&["*"]), &v(&[" "]))
            .unwrap_err()
            .to_string()
            .contains("blank"));
        assert!(validate_exclude_topics(&v(&["*"]), &v(&[""])).is_err());
        for (topics, exclude) in [
            (v(&["*"]), v(&["*"])),
            (v(&["orders"]), v(&["orders"])),
            (v(&["orders", "orders-eu"]), v(&["orders*"])),
        ] {
            let err = validate_exclude_topics(&topics, &exclude).unwrap_err();
            assert!(err.to_string().contains("cancels every entry"), "{err}");
        }
    }

    #[test]
    fn validation_accepts_partial_or_unmatched_excludes() {
        assert!(validate_exclude_topics(&v(&["*"]), &[]).is_ok());
        assert!(validate_exclude_topics(&v(&["*"]), &v(&["metrics", "syslog-*"])).is_ok());
        assert!(validate_exclude_topics(&v(&["orders", "events"]), &v(&["orders"])).is_ok());
        assert!(validate_exclude_topics(&v(&["orders-*"]), &v(&["orders-eu"])).is_ok());
        assert!(validate_exclude_topics(&v(&["orders"]), &v(&["nothing-like-this"])).is_ok());
        // An empty include list is reported by the caller, not here.
        assert!(validate_exclude_topics(&[], &v(&["*"])).is_ok());
    }

    #[test]
    fn effective_selection_drops_cancelled_literals_only() {
        let sel = effective_selection(&v(&["orders", "ghost", "logs-*"]), &v(&["ghost", "logs-*"]));
        assert_eq!(sel.include, v(&["orders", "logs-*"]));
        assert_eq!(sel.exclude, v(&["ghost", "logs-*"]));

        let sel = effective_selection(&v(&["orders"]), &v(&["metrics"]));
        assert_eq!(sel.include, v(&["orders"]));
    }

    #[test]
    fn effective_selection_never_empties_include() {
        let sel = effective_selection(&v(&["orders"]), &v(&["orders"]));
        assert_eq!(sel.include, v(&["orders"]));
        let sel = effective_selection(&v(&["*"]), &v(&["*"]));
        assert_eq!(sel.include, v(&["*"]));
    }
}
