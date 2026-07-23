//! Folder payloads arrive with structure intact, and an existing entry at
//! the destination - even an empty folder - forces a " (1)" suffix instead
//! of an overwrite or a prompt.

use std::time::Duration;

use e2e::{grandmasend_bin, hash_file, run_receiver, write_random_payload, ReceiverMode, Sender};

#[test]
fn folder_transfer_with_collision() {
    let bin = grandmasend_bin();
    let work = tempfile::tempdir().expect("workdir");
    let dest = work.path().join("dest");
    let sender_data = work.path().join("sd");
    let receiver_data = work.path().join("rd");
    for dir in [&dest, &sender_data, &receiver_data] {
        std::fs::create_dir_all(dir).expect("mkdir");
    }

    // A folder payload with nesting.
    let folder = work.path().join("album");
    std::fs::create_dir_all(folder.join("inner")).expect("payload dirs");
    let h1 = write_random_payload(&folder.join("one.bin"), 512 * 1024);
    let h2 = write_random_payload(&folder.join("inner/two.bin"), 512 * 1024);

    // The strict collision rule counts ANY entry, including an empty folder.
    std::fs::create_dir(dest.join("album")).expect("collision decoy");

    let sender = Sender::spawn(&bin, &folder, &sender_data);
    let run = run_receiver(
        &bin,
        &sender.code,
        &dest,
        &sender.addr_json,
        &receiver_data,
        ReceiverMode::ToCompletion,
    );
    assert!(run.success, "folder transfer failed:\n{}", run.stderr);

    // The decoy is untouched; the payload landed under the suffixed name.
    assert!(dest
        .join("album")
        .read_dir()
        .expect("decoy")
        .next()
        .is_none());
    assert_eq!(hash_file(&dest.join("album (1)/one.bin")), h1);
    assert_eq!(hash_file(&dest.join("album (1)/inner/two.bin")), h2);

    assert!(
        sender.wait_success(Duration::from_secs(30)),
        "sender did not exit after completion"
    );
}
