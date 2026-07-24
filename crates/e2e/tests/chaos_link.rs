//! Transfers through the chaos proxy: lossy/laggy links and a mid-transfer
//! link cut must not break a transfer or corrupt a byte.

use std::time::Duration;

use e2e::{
    addr_via, chaos::ChaosProxy, dir_size, first_ipv4, grandmasend_bin, hash_file, run_receiver,
    write_random_payload, ReceiverMode, Sender,
};

#[test]
fn lossy_link_completes() {
    let bin = grandmasend_bin();
    let work = tempfile::tempdir().expect("workdir");
    let dest = work.path().join("dest");
    let sender_data = work.path().join("sd");
    let receiver_data = work.path().join("rd");
    for dir in [&dest, &sender_data, &receiver_data] {
        std::fs::create_dir_all(dir).expect("mkdir");
    }
    let payload = work.path().join("payload.bin");
    let expected = write_random_payload(&payload, 10 * 1024 * 1024);

    let sender = Sender::spawn(&bin, &payload, &sender_data);
    let proxy = ChaosProxy::start(first_ipv4(&sender.addr_json)).expect("proxy");
    proxy.set_loss_per_mille(30);
    proxy.set_latency(Duration::from_millis(20), Duration::from_millis(10));

    let via = addr_via(&sender.addr_json, proxy.listen_addr);
    let run = run_receiver(
        &bin,
        &sender.code,
        &dest,
        Some(&via),
        &receiver_data,
        ReceiverMode::ToCompletion,
    );
    assert!(
        run.success,
        "transfer over lossy link failed:\n{}",
        run.stderr
    );
    assert_eq!(hash_file(&dest.join("payload.bin")), expected);
    assert!(
        sender.wait_success(Duration::from_secs(30)),
        "sender did not exit after completion"
    );
}

#[test]
fn link_cut_mid_transfer_recovers() {
    let bin = grandmasend_bin();
    let work = tempfile::tempdir().expect("workdir");
    let dest = work.path().join("dest");
    let sender_data = work.path().join("sd");
    let receiver_data = work.path().join("rd");
    for dir in [&dest, &sender_data, &receiver_data] {
        std::fs::create_dir_all(dir).expect("mkdir");
    }
    let payload = work.path().join("payload.bin");
    let expected = write_random_payload(&payload, 30 * 1024 * 1024);

    let sender = Sender::spawn(&bin, &payload, &sender_data);
    let proxy = ChaosProxy::start(first_ipv4(&sender.addr_json)).expect("proxy");
    // Slow the link enough that the cut lands mid-transfer.
    proxy.set_latency(Duration::from_millis(15), Duration::from_millis(5));

    let via = addr_via(&sender.addr_json, proxy.listen_addr);

    // Cut the link for 3 s once ~5 MB flowed, from a watcher thread.
    let partial = dest.join(".grandmasend-partial");
    let control = proxy.control.clone();
    let watcher = std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + Duration::from_secs(120);
        while dir_size(&partial) < 5 * 1024 * 1024 {
            if std::time::Instant::now() > deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        control
            .cut
            .store(true, std::sync::atomic::Ordering::Relaxed);
        std::thread::sleep(Duration::from_secs(3));
        control
            .cut
            .store(false, std::sync::atomic::Ordering::Relaxed);
        true
    });

    let run = run_receiver(
        &bin,
        &sender.code,
        &dest,
        Some(&via),
        &receiver_data,
        ReceiverMode::ToCompletion,
    );
    assert!(
        watcher.join().expect("watcher thread"),
        "cut never triggered: transfer finished below the threshold"
    );
    assert!(
        run.success,
        "transfer with mid-stream link cut failed:\n{}",
        run.stderr
    );
    assert_eq!(hash_file(&dest.join("payload.bin")), expected);
    assert!(
        sender.wait_success(Duration::from_secs(30)),
        "sender did not exit after completion"
    );
}
