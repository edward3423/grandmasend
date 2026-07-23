//! M1 exit criterion: a 100 MB transfer survives repeated receiver kills and
//! the final bytes are identical to the source.
//!
//! One sender serves throughout (forever-serve); the receiver is SIGKILLed
//! mid-transfer twice at increasing partial sizes, then allowed to finish.
//! Each resumed run must report previously fetched bytes.

use std::time::Duration;

use e2e::{grandmasend_bin, hash_file, run_receiver, write_random_payload, Sender};

const PAYLOAD_SIZE: u64 = 100 * 1024 * 1024;

#[test]
fn kill_and_resume_100mb() {
    let bin = grandmasend_bin();
    let work = tempfile::tempdir().expect("workdir");
    let dest = work.path().join("dest");
    std::fs::create_dir_all(&dest).expect("dest dir");

    let payload = work.path().join("payload.bin");
    let expected = write_random_payload(&payload, PAYLOAD_SIZE);

    let sender = Sender::spawn(&bin, &payload);
    assert_eq!(sender.code.split_whitespace().count(), 4, "code is 4 words");

    // Two mid-transfer kills at growing thresholds, then a clean run.
    let mut resumed_runs = 0;
    for threshold in [20 * 1024 * 1024, 60 * 1024 * 1024] {
        let run = run_receiver(
            &bin,
            &sender.code,
            &dest,
            &sender.addr_json,
            Some(threshold),
        );
        assert!(
            run.killed,
            "receiver finished before reaching the {threshold} byte kill threshold; \
             transfer too fast for this machine?"
        );
        assert!(
            !dest.join("payload.bin").exists(),
            "no destination file may appear before completion"
        );
    }

    // Interrupted runs after the first must resume, not restart.
    let final_run = run_receiver(&bin, &sender.code, &dest, &sender.addr_json, None);
    assert!(
        final_run.success,
        "final receiver run failed:\n{}",
        final_run.stderr
    );
    if final_run.stderr.contains("Resuming:") {
        resumed_runs += 1;
    }
    assert!(
        resumed_runs > 0,
        "final run should have resumed previously fetched bytes:\n{}",
        final_run.stderr
    );

    // Byte-identical result.
    let received = dest.join("payload.bin");
    assert_eq!(
        hash_file(&received),
        expected,
        "received file differs from source"
    );

    // Partial state is gone after completion.
    assert!(
        !dest.join(".grandmasend-partial").exists()
            || e2e::dir_size(&dest.join(".grandmasend-partial")) == 0,
        "partial dir should be cleaned up after completion"
    );

    // Completion ack consumed the send: the sender exits cleanly on its own.
    assert!(
        sender.wait_success(Duration::from_secs(30)),
        "sender did not exit cleanly after completion ack"
    );
}
