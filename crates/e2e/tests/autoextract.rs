//! Autoextract end to end: a zip sent with --autoextract arrives extracted
//! next to the archive; passwords travel over the encrypted channel; a
//! wrong password still delivers the archive; archives inside the archive
//! stay unextracted.

use std::{io::Write, path::Path, time::Duration};

use e2e::{grandmasend_bin, run_receiver, write_random_payload, ReceiverMode, Sender};

fn make_zip(path: &Path, password: Option<&str>, entries: &[(&str, &[u8])]) {
    let file = std::fs::File::create(path).expect("create zip");
    let mut writer = zip::ZipWriter::new(file);
    for (name, data) in entries {
        let options = zip::write::SimpleFileOptions::default();
        let options = match password {
            Some(pw) => options.with_aes_encryption(zip::AesMode::Aes256, pw),
            None => options,
        };
        writer.start_file(*name, options).expect("start entry");
        writer.write_all(data).expect("write entry");
    }
    writer.finish().expect("finish zip");
}

fn transfer(
    work: &Path,
    archive: &Path,
    send_flags: &[&str],
) -> (e2e::ReceiverRun, std::path::PathBuf) {
    let bin = grandmasend_bin();
    let dest = work.join("dest");
    let sender_data = work.join("sd");
    let receiver_data = work.join("rd");
    for dir in [&dest, &sender_data, &receiver_data] {
        std::fs::create_dir_all(dir).expect("mkdir");
    }
    let sender = Sender::spawn_with(&bin, archive, &sender_data, send_flags);
    let run = run_receiver(
        &bin,
        &sender.code,
        &dest,
        Some(&sender.addr_json),
        &receiver_data,
        ReceiverMode::ToCompletion,
    );
    assert!(
        sender.wait_success(Duration::from_secs(30)),
        "sender did not exit after completion"
    );
    (run, dest)
}

#[test]
fn autoextract_zip_end_to_end() {
    let work = tempfile::tempdir().expect("workdir");
    let payload = work.path().join("inner-payload.bin");
    let inner = write_random_payload(&payload, 512 * 1024);
    let inner_bytes = std::fs::read(&payload).expect("read payload");
    let archive = work.path().join("bundle.zip");
    make_zip(
        &archive,
        None,
        &[
            ("docs/readme.txt", b"hello"),
            ("data/blob.bin", &inner_bytes),
        ],
    );

    let (run, dest) = transfer(work.path(), &archive, &["--autoextract"]);
    assert!(run.success, "receive failed:\n{}", run.stderr);
    assert!(
        run.stderr.contains("Extracted"),
        "no extraction:\n{}",
        run.stderr
    );

    // Both the archive and the extracted folder are delivered.
    assert!(dest.join("bundle.zip").exists());
    assert_eq!(
        std::fs::read(dest.join("bundle/docs/readme.txt")).expect("extracted file"),
        b"hello"
    );
    assert_eq!(
        blake3::hash(&std::fs::read(dest.join("bundle/data/blob.bin")).expect("blob")),
        inner
    );
}

#[test]
fn autoextract_password_and_wrong_password() {
    // Right password: extracted.
    let work = tempfile::tempdir().expect("workdir");
    let archive = work.path().join("secret.zip");
    make_zip(&archive, Some("hunter2"), &[("note.txt", b"classified")]);
    let (run, dest) = transfer(
        work.path(),
        &archive,
        &["--autoextract", "--password", "hunter2"],
    );
    assert!(run.success, "receive failed:\n{}", run.stderr);
    assert_eq!(
        std::fs::read(dest.join("secret/note.txt")).expect("extracted"),
        b"classified"
    );

    // Wrong password: archive delivered, extraction reported failed,
    // receive still succeeds.
    let work2 = tempfile::tempdir().expect("workdir");
    let archive2 = work2.path().join("secret.zip");
    make_zip(&archive2, Some("hunter2"), &[("note.txt", b"classified")]);
    let (run2, dest2) = transfer(
        work2.path(),
        &archive2,
        &["--autoextract", "--password", "wrong"],
    );
    assert!(run2.success, "receive must not fail:\n{}", run2.stderr);
    assert!(
        run2.stderr.contains("Could not extract"),
        "missing failure notice:\n{}",
        run2.stderr
    );
    assert!(dest2.join("secret.zip").exists());
    assert!(!dest2.join("secret/note.txt").exists());
}

#[test]
fn autoextract_is_top_level_only() {
    let work = tempfile::tempdir().expect("workdir");
    let inner_zip = work.path().join("inner.zip");
    make_zip(&inner_zip, None, &[("nested.txt", b"deep")]);
    let inner_bytes = std::fs::read(&inner_zip).expect("read inner");
    let archive = work.path().join("outer.zip");
    make_zip(&archive, None, &[("payload/inner.zip", &inner_bytes)]);

    let (run, dest) = transfer(work.path(), &archive, &["--autoextract"]);
    assert!(run.success, "receive failed:\n{}", run.stderr);
    // The inner archive arrives as a file, byte-identical, unextracted.
    assert_eq!(
        std::fs::read(dest.join("outer/payload/inner.zip")).expect("inner delivered"),
        inner_bytes
    );
    assert!(!dest.join("outer/payload/nested.txt").exists());
}
