//! Relay-forced completion: with every direct address stripped, the
//! transfer must still complete through the n0 public relay.
//!
//! Needs internet; small payload keeps relay traffic polite.

use std::time::Duration;

use e2e::{
    addr_relay_only, grandmasend_bin, hash_file, run_receiver, write_random_payload, ReceiverMode,
    Sender,
};

#[test]
fn relay_only_completes() {
    let bin = grandmasend_bin();
    let work = tempfile::tempdir().expect("workdir");
    let dest = work.path().join("dest");
    let sender_data = work.path().join("sd");
    let receiver_data = work.path().join("rd");
    for dir in [&dest, &sender_data, &receiver_data] {
        std::fs::create_dir_all(dir).expect("mkdir");
    }
    let payload = work.path().join("payload.bin");
    let expected = write_random_payload(&payload, 3 * 1024 * 1024);

    let sender = Sender::spawn(&bin, &payload, &sender_data);
    let relay_only = addr_relay_only(&sender.addr_json);

    let run = run_receiver(
        &bin,
        &sender.code,
        &dest,
        Some(&relay_only),
        &receiver_data,
        ReceiverMode::ToCompletion,
    );
    assert!(run.success, "relay-forced transfer failed:\n{}", run.stderr);
    assert_eq!(hash_file(&dest.join("payload.bin")), expected);
    assert!(
        sender.wait_success(Duration::from_secs(30)),
        "sender did not exit after completion"
    );
}
