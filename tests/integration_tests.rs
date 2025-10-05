use assert_cmd::Command;
use std::fs;

fn normalize_csv(csv: &str) -> Vec<Vec<String>> {
    let mut rows: Vec<Vec<String>> = csv
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| line.split(',').map(|s| s.trim().to_string()).collect())
        .collect();

    // Sort by client ID (skip header)
    if rows.len() > 1 {
        rows[1..].sort_by_key(|row| row[0].parse::<u16>().unwrap_or(0));
    }

    rows
}

fn compare_csv_output(fixture: &str, expected: &str) {
    let mut cmd = Command::cargo_bin("payments_engine").unwrap();
    let output = cmd
        .arg(fixture)
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success(), "Command failed");

    let actual = String::from_utf8(output.stdout).unwrap();
    let expected_content = fs::read_to_string(expected).unwrap();

    let actual_rows = normalize_csv(&actual);
    let expected_rows = normalize_csv(&expected_content);

    assert_eq!(
        actual_rows.len(),
        expected_rows.len(),
        "Number of rows mismatch. Actual:\n{}\nExpected:\n{}",
        actual,
        expected_content
    );

    for (i, (actual_row, expected_row)) in actual_rows.iter().zip(expected_rows.iter()).enumerate() {
        assert_eq!(
            actual_row, expected_row,
            "Row {} mismatch.\nActual: {:?}\nExpected: {:?}",
            i, actual_row, expected_row
        );
    }
}

#[test]
fn test_basic_scenario() {
    compare_csv_output(
        "tests/fixtures/basic.csv",
        "tests/expected/basic_expected.csv",
    );
}

#[test]
fn test_dispute_resolve_scenario() {
    compare_csv_output(
        "tests/fixtures/dispute_resolve.csv",
        "tests/expected/dispute_resolve_expected.csv",
    );
}

#[test]
fn test_chargeback_scenario() {
    compare_csv_output(
        "tests/fixtures/chargeback.csv",
        "tests/expected/chargeback_expected.csv",
    );
}

#[test]
fn test_invalid_ops_scenario() {
    compare_csv_output(
        "tests/fixtures/invalid_ops.csv",
        "tests/expected/invalid_ops_expected.csv",
    );
}

#[test]
fn test_precision_scenario() {
    compare_csv_output(
        "tests/fixtures/precision.csv",
        "tests/expected/precision_expected.csv",
    );
}

#[test]
fn test_comprehensive_all_types() {
    compare_csv_output(
        "tests/fixtures/comprehensive_all_types.csv",
        "tests/expected/comprehensive_all_types_expected.csv",
    );
}
