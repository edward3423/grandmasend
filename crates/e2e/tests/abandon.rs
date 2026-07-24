//! `grandmasend abandon <code>` kills a waiting send without re-sending:
//! the code disappears from status, cannot be revived, and a rerun of the
//! same path gets a brand-new code.

#![cfg(unix)]

use std::{process::Command, time::Duration};

use e2e::{grandmasend_bin, interrupt, write_random_payload, Sender};

#[test]
fn abandon_kills_a_waiting_send() {
    let bin = grandmasend_bin();
    let work = tempfile::tempdir().expect("workdir");
    let sender_data = work.path().join("sd");
    std::fs::create_dir_all(&sender_data).expect("mkdir");
    let payload = work.path().join("payload.bin");
    write_random_payload(&payload, 1024 * 1024);

    // Create a waiting send, then interrupt it so only its state remains.
    let mut sender = Sender::spawn(&bin, &payload, &sender_data);
    let code = sender.code.clone();
    interrupt(&sender.child);
    assert!(sender.wait_exit(Duration::from_secs(15)).success());
    drop(sender);

    // Abandon by code.
    let out = Command::new(&bin)
        .arg("abandon")
        .args(code.split_whitespace())
        .env("GRANDMASEND_DATA_DIR", &sender_data)
        .env("GRANDMASEND_NO_UPDATE_CHECK", "1")
        .output()
        .expect("run abandon");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "abandon failed:\n{stderr}");
    assert!(stderr.contains("Abandoned"), "unexpected output:\n{stderr}");

    // Status shows nothing; abandoning again reports the code as unknown.
    let status = Command::new(&bin)
        .arg("status")
        .env("GRANDMASEND_DATA_DIR", &sender_data)
        .env("GRANDMASEND_NO_UPDATE_CHECK", "1")
        .output()
        .expect("run status");
    assert!(String::from_utf8_lossy(&status.stderr).contains("No sends waiting"));

    let again = Command::new(&bin)
        .arg("abandon")
        .args(code.split_whitespace())
        .env("GRANDMASEND_DATA_DIR", &sender_data)
        .env("GRANDMASEND_NO_UPDATE_CHECK", "1")
        .output()
        .expect("run abandon again");
    assert!(String::from_utf8_lossy(&again.stderr).contains("No waiting send"));

    // A rerun of the same path is a fresh send, not a revival.
    let reborn = Sender::spawn(&bin, &payload, &sender_data);
    assert_ne!(reborn.code, code, "abandoned code must not be revived");
}
