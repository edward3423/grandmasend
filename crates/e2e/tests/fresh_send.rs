//! `send --fresh` abandons a bound-but-unfinished send: a new code is
//! generated with no binding, so the same payload can go to a different
//! person; the old code stops working.

#![cfg(unix)]

use std::time::Duration;

use e2e::{
    grandmasend_bin, hash_file, interrupt, run_receiver, write_random_payload, ReceiverMode, Sender,
};

const PAYLOAD_SIZE: u64 = 20 * 1024 * 1024;

#[test]
fn fresh_send_rebinds_to_a_new_receiver() {
    let bin = grandmasend_bin();
    let work = tempfile::tempdir().expect("workdir");
    let sender_data = work.path().join("sd");
    let receiver_a_data = work.path().join("rd-a");
    let receiver_b_data = work.path().join("rd-b");
    let dest_a = work.path().join("dest-a");
    let dest_b = work.path().join("dest-b");
    for dir in [
        &sender_data,
        &receiver_a_data,
        &receiver_b_data,
        &dest_a,
        &dest_b,
    ] {
        std::fs::create_dir_all(dir).expect("mkdir");
    }
    let payload = work.path().join("payload.bin");
    let expected = write_random_payload(&payload, PAYLOAD_SIZE);

    // Receiver A binds the first code but never completes.
    let mut sender = Sender::spawn(&bin, &payload, &sender_data);
    let old_code = sender.code.clone();
    let run_a = run_receiver(
        &bin,
        &old_code,
        &dest_a,
        Some(&sender.addr_json),
        &receiver_a_data,
        ReceiverMode::KillAtBytes(3 * 1024 * 1024),
    );
    assert!(
        run_a.killed,
        "receiver A should have been killed mid-transfer"
    );
    interrupt(&sender.child);
    assert!(
        sender.wait_exit(Duration::from_secs(15)).success(),
        "interrupted sender should exit cleanly"
    );
    drop(sender);

    // --fresh: new code, no binding.
    let fresh = Sender::spawn_with(&bin, &payload, &sender_data, &["--fresh"]);
    assert_ne!(fresh.code, old_code, "--fresh must generate a new code");

    // The old code is dead: a receiver dialing it gets nothing.
    let run_old = run_receiver(
        &bin,
        &old_code,
        &dest_a,
        None,
        &receiver_a_data,
        ReceiverMode::KillAfter(Duration::from_secs(10)),
    );
    assert!(
        !run_old.stderr.contains("Receiving"),
        "the abandoned code must not be served:\n{}",
        run_old.stderr
    );

    // A different person completes under the new code.
    let run_b = run_receiver(
        &bin,
        &fresh.code,
        &dest_b,
        Some(&fresh.addr_json),
        &receiver_b_data,
        ReceiverMode::ToCompletion,
    );
    assert!(run_b.success, "receiver B failed:\n{}", run_b.stderr);
    assert_eq!(hash_file(&dest_b.join("payload.bin")), expected);
    assert!(
        fresh.wait_success(Duration::from_secs(30)),
        "fresh sender did not exit after completion"
    );
}
