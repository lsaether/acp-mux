//! Parser for Hermes ACP child-process stderr lines.
//!
//! The Hermes Agent emits human-readable log lines (with a tracing
//! prefix like `2026-05-27 21:27:04 [INFO] agent.conversation_compression:`)
//! that describe context-compaction lifecycle. We don't control the format,
//! so the parser is pragmatic: it walks well-known substrings, treats every
//! field as optional, and falls back to `None` rather than failing loudly.
//!
//! See `docs/design/rooms.md` and issue #63 for the line shapes we observe.

/// Structured view of one recognized Hermes stderr line. Lines we don't
/// understand resolve to `None` — the caller still mirrors them into mux
/// logs unchanged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HermesStderrSignal {
    CompactionStarted {
        hermes_session_id: Option<String>,
        messages_before: Option<u64>,
        tokens_approx_before: Option<u64>,
        model: Option<String>,
        focus: Option<String>,
    },
    /// Status line emitted via `agent._emit_status(...)` — useful for
    /// transient UI state, but carries no structured fields.
    CompactionStatus,
    /// Auxiliary-model selection log, emitted right before the compression
    /// call when Hermes routes through a non-default provider.
    AuxiliaryCompression {
        provider: Option<String>,
        model: Option<String>,
        base_url: Option<String>,
    },
    RepeatedCompressionWarning {
        count: Option<u64>,
    },
    CompactionDone {
        hermes_session_id: Option<String>,
        messages_before: Option<u64>,
        messages_after: Option<u64>,
        tokens_approx_after: Option<u64>,
    },
}

const MARKER_STARTED: &str = "context compression started:";
const MARKER_DONE: &str = "context compression done:";
const MARKER_STATUS: &str = "Compacting context";
const MARKER_AUXILIARY: &str = "Auxiliary compression:";
const MARKER_REPEATED: &str = "Session compressed";

pub fn parse(line: &str) -> Option<HermesStderrSignal> {
    if let Some(rest) = find_after(line, MARKER_STARTED) {
        return Some(parse_started(rest));
    }
    if let Some(rest) = find_after(line, MARKER_DONE) {
        return Some(parse_done(rest));
    }
    if let Some(rest) = find_after(line, MARKER_AUXILIARY) {
        return Some(parse_auxiliary(rest));
    }
    if let Some(rest) = find_after(line, MARKER_REPEATED) {
        return Some(parse_repeated(rest));
    }
    if line.contains(MARKER_STATUS) {
        return Some(HermesStderrSignal::CompactionStatus);
    }
    None
}

fn find_after<'a>(haystack: &'a str, needle: &str) -> Option<&'a str> {
    let idx = haystack.find(needle)?;
    Some(haystack[idx + needle.len()..].trim_start())
}

fn parse_started(rest: &str) -> HermesStderrSignal {
    let fields = key_value_fields(rest);
    HermesStderrSignal::CompactionStarted {
        hermes_session_id: fields.get("session").cloned(),
        messages_before: fields.get("messages").and_then(|v| parse_number(v)),
        tokens_approx_before: fields.get("tokens").and_then(|v| parse_number(v)),
        model: fields.get("model").cloned(),
        focus: fields
            .get("focus")
            .filter(|v| v.as_str() != "None")
            .cloned(),
    }
}

fn parse_done(rest: &str) -> HermesStderrSignal {
    let fields = key_value_fields(rest);
    let (before, after) = fields
        .get("messages")
        .map(|v| parse_arrow_pair(v))
        .unwrap_or((None, None));
    HermesStderrSignal::CompactionDone {
        hermes_session_id: fields.get("session").cloned(),
        messages_before: before,
        messages_after: after,
        tokens_approx_after: fields.get("tokens").and_then(|v| parse_number(v)),
    }
}

fn parse_auxiliary(rest: &str) -> HermesStderrSignal {
    // Format: `using <provider> (<model>) at <base-url>`
    let trimmed = rest.strip_prefix("using ").unwrap_or(rest);

    let (provider, after_provider) = match trimmed.split_once(' ') {
        Some((p, r)) => (Some(p.to_string()), r),
        None => (Some(trimmed.to_string()), ""),
    };
    let (model, after_model) = match after_provider.strip_prefix('(') {
        Some(remaining) => match remaining.split_once(')') {
            Some((m, r)) => (Some(m.to_string()), r.trim_start()),
            None => (None, after_provider),
        },
        None => (None, after_provider),
    };
    let base_url = after_model
        .strip_prefix("at ")
        .map(|u| u.trim().to_string())
        .filter(|u| !u.is_empty());

    HermesStderrSignal::AuxiliaryCompression {
        provider,
        model,
        base_url,
    }
}

fn parse_repeated(rest: &str) -> HermesStderrSignal {
    // Format: `<count> times — accuracy may degrade...`
    let count = rest
        .split_whitespace()
        .next()
        .and_then(|tok| tok.parse::<u64>().ok());
    HermesStderrSignal::RepeatedCompressionWarning { count }
}

/// Split a key=value sequence (whitespace-separated, terminator-tolerant)
/// into a map. Values stop at the next whitespace; we don't handle quoted
/// strings because Hermes doesn't emit any.
fn key_value_fields(rest: &str) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    for tok in rest.split_whitespace() {
        if let Some((k, v)) = tok.split_once('=') {
            map.insert(k.to_string(), v.to_string());
        }
    }
    map
}

fn parse_number(raw: &str) -> Option<u64> {
    let cleaned: String = raw
        .chars()
        .filter(|c| c.is_ascii_digit())
        .collect();
    if cleaned.is_empty() {
        return None;
    }
    cleaned.parse::<u64>().ok()
}

fn parse_arrow_pair(raw: &str) -> (Option<u64>, Option<u64>) {
    // Tolerate both ASCII `->` and unicode `→` arrows.
    let split = raw.split_once("->").or_else(|| raw.split_once('\u{2192}'));
    let Some((before, after)) = split else {
        return (parse_number(raw), None);
    };
    (parse_number(before), parse_number(after))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_started_line() {
        let line = "2026-05-27 21:27:04 [INFO] agent.conversation_compression: context compression started: session=20260527_202251_df54bf messages=50 tokens=~72,908 model=gpt-5.5 focus=None";
        match parse(line).expect("recognized") {
            HermesStderrSignal::CompactionStarted {
                hermes_session_id,
                messages_before,
                tokens_approx_before,
                model,
                focus,
            } => {
                assert_eq!(hermes_session_id.as_deref(), Some("20260527_202251_df54bf"));
                assert_eq!(messages_before, Some(50));
                assert_eq!(tokens_approx_before, Some(72_908));
                assert_eq!(model.as_deref(), Some("gpt-5.5"));
                assert!(focus.is_none(), "focus=None should normalize to None");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parses_done_line_ascii_arrow() {
        let line = "2026-05-27 21:27:08 [INFO] agent.conversation_compression: context compression done: session=20260527_202251_df54bf messages=50->9 tokens=~54,700";
        match parse(line).expect("recognized") {
            HermesStderrSignal::CompactionDone {
                hermes_session_id,
                messages_before,
                messages_after,
                tokens_approx_after,
            } => {
                assert_eq!(hermes_session_id.as_deref(), Some("20260527_202251_df54bf"));
                assert_eq!(messages_before, Some(50));
                assert_eq!(messages_after, Some(9));
                assert_eq!(tokens_approx_after, Some(54_700));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parses_done_line_unicode_arrow() {
        let line = "agent.conversation_compression: context compression done: session=abc messages=50\u{2192}9 tokens=~1234";
        match parse(line).expect("recognized") {
            HermesStderrSignal::CompactionDone {
                messages_before,
                messages_after,
                tokens_approx_after,
                ..
            } => {
                assert_eq!(messages_before, Some(50));
                assert_eq!(messages_after, Some(9));
                assert_eq!(tokens_approx_after, Some(1234));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parses_status_line() {
        let line = "\u{1f5dc}\u{fe0f} Compacting context \u{2014} summarizing earlier conversation so I can continue...";
        assert_eq!(parse(line), Some(HermesStderrSignal::CompactionStatus));
    }

    #[test]
    fn parses_auxiliary_line() {
        let line = "2026-05-27 21:27:04 [INFO] agent.auxiliary_client: Auxiliary compression: using openai-codex (gpt-5.3-codex-spark) at https://chatgpt.com/backend-api/codex/";
        match parse(line).expect("recognized") {
            HermesStderrSignal::AuxiliaryCompression {
                provider,
                model,
                base_url,
            } => {
                assert_eq!(provider.as_deref(), Some("openai-codex"));
                assert_eq!(model.as_deref(), Some("gpt-5.3-codex-spark"));
                assert_eq!(
                    base_url.as_deref(),
                    Some("https://chatgpt.com/backend-api/codex/")
                );
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn parses_repeated_warning() {
        let line = "\u{26a0}\u{fe0f}  Session compressed 3 times \u{2014} accuracy may degrade. Consider /new to start fresh.";
        assert_eq!(
            parse(line),
            Some(HermesStderrSignal::RepeatedCompressionWarning { count: Some(3) })
        );
    }

    #[test]
    fn unrecognized_line_returns_none() {
        assert_eq!(parse("nothing interesting here"), None);
        assert_eq!(parse(""), None);
    }

    #[test]
    fn tolerates_missing_fields() {
        let line = "agent.conversation_compression: context compression started: messages=12";
        match parse(line).expect("recognized") {
            HermesStderrSignal::CompactionStarted {
                hermes_session_id,
                messages_before,
                model,
                focus,
                ..
            } => {
                assert!(hermes_session_id.is_none());
                assert_eq!(messages_before, Some(12));
                assert!(model.is_none());
                assert!(focus.is_none());
            }
            other => panic!("unexpected: {other:?}"),
        }
    }
}
