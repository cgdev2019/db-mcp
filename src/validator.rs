use anyhow::{bail, Result};
use regex::Regex;
use std::sync::LazyLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DangerLevel {
    Safe,
    Warning,
    Dangerous,
}

impl DangerLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Safe => "SAFE",
            Self::Warning => "WARNING",
            Self::Dangerous => "DANGEROUS",
        }
    }
}

static READ_ONLY_KEYWORDS: &[&str] = &[
    "SELECT", "WITH", "EXPLAIN", "SHOW", "DESCRIBE", "DESC", "PRAGMA", "VALUES",
];

static DANGEROUS_KEYWORDS: &[&str] = &["DROP", "TRUNCATE", "ALTER"];

static BLOCK_COMMENT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?s)/\*.*?\*/").unwrap());
static LINE_COMMENT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"--[^\n]*").unwrap());
static STRING_LITERAL: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"'[^']*'").unwrap());

fn strip_comments_and_literals(sql: &str) -> String {
    let result = BLOCK_COMMENT.replace_all(sql, " ");
    let result = LINE_COMMENT.replace_all(&result, " ");
    let result = STRING_LITERAL.replace_all(&result, "''");
    result.to_string()
}

fn contains_multiple_statements(cleaned: &str) -> bool {
    let without_trailing = cleaned.trim_end().trim_end_matches(';').trim_end();
    without_trailing.contains(';')
}

pub fn validate_read_only(sql: &str) -> Result<()> {
    let cleaned = strip_comments_and_literals(sql).trim().to_string();
    if cleaned.is_empty() {
        bail!("Empty SQL statement");
    }

    let first_token = cleaned
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_uppercase();

    if !READ_ONLY_KEYWORDS.contains(&first_token.as_str()) {
        bail!(
            "Only read-only statements are allowed (SELECT, WITH, EXPLAIN, SHOW, DESCRIBE). \
             Got: {}. Use the 'execute' tool for data modifications.",
            first_token
        );
    }

    if contains_multiple_statements(&cleaned) {
        bail!("Multiple statements are not allowed. Remove extra semicolons.");
    }

    Ok(())
}

pub fn assess_danger(sql: &str) -> DangerLevel {
    let cleaned = strip_comments_and_literals(sql).trim().to_string();
    if cleaned.is_empty() {
        return DangerLevel::Safe;
    }

    let upper = cleaned.to_uppercase();
    let first_token = upper.split_whitespace().next().unwrap_or("");

    if DANGEROUS_KEYWORDS.contains(&first_token) {
        return DangerLevel::Dangerous;
    }

    if (first_token == "DELETE" || first_token == "UPDATE") && !upper.contains("WHERE") {
        return DangerLevel::Warning;
    }

    DangerLevel::Safe
}
