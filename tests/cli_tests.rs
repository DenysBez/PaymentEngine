use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn test_missing_argument() {
    let mut cmd = Command::cargo_bin("payments_engine").unwrap();
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("Usage:"));
}
#[test]
fn test_nonexistent_file() {
    let mut cmd = Command::cargo_bin("payments_engine").unwrap();
    cmd.arg("nonexistent_file.csv")
        .assert()
        .failure()
        .stderr(predicate::str::contains("not found"));
}
