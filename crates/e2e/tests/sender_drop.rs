//! A sender crash mid-transfer must not kill the receiver: it waits, and
//! when the sender revives the same send, the transfer resumes and
//! completes in the same receiver run.
//!
//! Uses real discovery (no address injection): a revived sender binds a new
//! port, so a pinned address can never reconnect.

use std::time::{Duration, Instant};

use e2e::{
    dir_size, grandmasend_bin, hash_file, run_receiver, write_random_payload, ReceiverMode, Sender,
};

const PAYLOAD_SIZE: u64 = 50 * 1024 * 1024;

#[test]
fn sender_killed_and_revived_mid_transfer() {
    let bin = grandmasend_bin();
    let work = tempfile::tempdir().expect("workdir");
    let dest = work.path().join("dest");
    let sender_data = work.path().join("sd");
    let receiver_data = work.path().join("rd");
    for dir in [&dest, &sender_data, &receiver_data] {
        std::fs::create_dir_all(dir).expect("mkdir");
    }
    let payload = work.path().join("payload.bin");
    let expected = write_random_payload(&payload, PAYLOAD_SIZE);

    let mut sender = Sender::spawn(&bin, &payload, &sender_data);
    let code = sender.code.clone();

    // Receiver runs with discovery in a background thread.
    let receiver = {
        let bin = bin.clone();
        let code = code.clone();
        let dest = dest.clone();
        let receiver_data = receiver_data.clone();
        std::thread::spawn(move || {
            run_receiver(
                &bin,
                &code,
                &dest,
                None,
                &receiver_data,
                ReceiverMode::ToCompletion,
            )
        })
    };

    // Kill the sender once ~10 MB flowed.
    let partial = dest.join(".grandmasend-partial");
    let deadline = Instant::now() + Duration::from_secs(120);
    while dir_size(&partial) < 10 * 1024 * 1024 {
        assert!(
            Instant::now() < deadline,
            "transfer never reached the kill threshold"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    sender.kill();

    // Let the receiver notice the loss, then revive the send (same data
    // dir -> same code, same binding).
    std::thread::sleep(Duration::from_secs(5));
    let revived = Sender::spawn(&bin, &payload, &sender_data);
    assert_eq!(revived.code, code, "revival must reuse the code");

    // The ORIGINAL receiver run completes without restart.
    let run = receiver.join().expect("receiver thread");
    assert!(
        run.success,
        "receiver should survive the sender crash and finish:\n{}",
        run.stderr
    );
    assert_eq!(hash_file(&dest.join("payload.bin")), expected);
    assert!(
        revived.wait_success(Duration::from_secs(30)),
        "revived sender did not exit after completion"
    );
}
