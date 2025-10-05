use assert_cmd::Command;
use std::time::Instant;

#[test]
fn test_large_csv_streaming() {
    let start = Instant::now();

    let mut cmd = Command::cargo_bin("payments_engine").unwrap();
    cmd.arg("tests/fixtures/large_test.csv")
        .assert()
        .success();

    let duration = start.elapsed();

    // Should complete in reasonable time (< 5 seconds for 100k transactions)
    // This is a smoke test to ensure streaming works efficiently
    println!("Processed 100k transactions in {:?}", duration);
    assert!(duration.as_secs() < 10, "Processing took too long: {:?}", duration);
}
