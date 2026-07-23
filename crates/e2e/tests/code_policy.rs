//! M2 policies end to end: binding rejection of a second receiver identity,
//! revival of an interrupted send with the same code, and single-use
//! consumption on completion.

#![cfg(unix)]

use std::time::Duration;

use e2e::{
    dir_size, grandmasend_bin, hash_file, interrupt, run_receiver, write_random_payload,
    ReceiverMode, Sender,
};

const PAYLOAD_SIZE: u64 = 30 * 1024 * 1024;

#[test]
fn binding_revival_and_single_use() {
    let bin = grandmasend_bin();
    let work = tempfile::tempdir().expect("workdir");
    let sender_data = work.path().join("sender-data");
    let receiver_a_data = work.path().join("receiver-a-data");
    let receiver_b_data = work.path().join("receiver-b-data");
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

    // Receiver A binds and is killed mid-transfer.
    let mut sender = Sender::spawn(&bin, &payload, &sender_data);
    let code = sender.code.clone();
    let run_a = run_receiver(
        &bin,
        &code,
        &dest_a,
        &sender.addr_json,
        &receiver_a_data,
        ReceiverMode::KillAtBytes(5 * 1024 * 1024),
    );
    assert!(
        run_a.killed,
        "receiver A should have been killed mid-transfer"
    );

    // Receiver B has a different identity: binding must reject it with
    // nothing served - to B this is indistinguishable from an offline sender.
    let run_b = run_receiver(
        &bin,
        &code,
        &dest_b,
        &sender.addr_json,
        &receiver_b_data,
        ReceiverMode::KillAfter(Duration::from_secs(12)),
    );
    assert!(
        run_b.killed,
        "receiver B should hang until the test kills it"
    );
    assert!(
        !run_b.stderr.contains("Receiving"),
        "receiver B must never learn the offer:\n{}",
        run_b.stderr
    );
    assert_eq!(
        dir_size(&dest_b.join(".grandmasend-partial")),
        0,
        "receiver B must not fetch a single byte"
    );

    // Ctrl-c the sender: state persists, code revivable.
    interrupt(&sender.child);
    let status = sender.wait_exit(Duration::from_secs(15));
    assert!(status.success(), "interrupted sender should exit cleanly");
    drop(sender);

    // Revival: same payload, same data dir -> same code, binding intact.
    let revived = Sender::spawn(&bin, &payload, &sender_data);
    assert_eq!(revived.code, code, "revived send must reuse the same code");

    // Receiver B still rejected after revival (binding was persisted).
    let run_b2 = run_receiver(
        &bin,
        &code,
        &dest_b,
        &revived.addr_json,
        &receiver_b_data,
        ReceiverMode::KillAfter(Duration::from_secs(12)),
    );
    assert!(
        !run_b2.stderr.contains("Receiving"),
        "receiver B must stay rejected after revival:\n{}",
        run_b2.stderr
    );

    // Receiver A resumes and completes.
    let run_a2 = run_receiver(
        &bin,
        &code,
        &dest_a,
        &revived.addr_json,
        &receiver_a_data,
        ReceiverMode::ToCompletion,
    );
    assert!(
        run_a2.success,
        "receiver A resume failed:\n{}",
        run_a2.stderr
    );
    // No "Resuming:" assertion here: iroh-blobs commits resume metadata in
    // 500 ms batches, and this test's early kill (5 MB on loopback) races
    // that window. kill_resume covers resume efficiency; correctness here is
    // the hash check.
    assert_eq!(hash_file(&dest_a.join("payload.bin")), expected);

    // Completion consumed the code: sender exits and the send state is gone.
    assert!(
        revived.wait_success(Duration::from_secs(30)),
        "sender did not exit after completion"
    );
    assert!(
        !sender_data.join("sends").exists()
            || std::fs::read_dir(sender_data.join("sends"))
                .map(|entries| entries.count() == 0)
                .unwrap_or(true),
        "send state must be consumed on completion"
    );
}
