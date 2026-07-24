//! Archive extraction for autoextract sends: zip, 7z, and rar, top level
//! only. Runs strictly after the archive is fully verified on disk; the
//! caller decides where results land and how failures surface.
//!
//! Safety rules for attacker-supplied archives: every entry path goes
//! through [`crate::sanitize::safe_join`] (traversal rejected, components
//! sanitized), symlink and other non-regular entries are skipped, and the
//! declared uncompressed size is checked against free disk space before a
//! byte is written.

use std::{io::Read, path::Path};

use anyhow::{Context, Result};

/// Formats autoextract understands, decided by file extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveKind {
    Zip,
    SevenZ,
    Rar,
}

impl ArchiveKind {
    pub fn from_path(path: &Path) -> Option<Self> {
        match path
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_ascii_lowercase())
            .as_deref()
        {
            Some("zip") => Some(Self::Zip),
            Some("7z") => Some(Self::SevenZ),
            Some("rar") => Some(Self::Rar),
            _ => None,
        }
    }
}

/// What extraction accomplished; skipped counts hostile or non-regular
/// entries that were deliberately not written.
#[derive(Debug, Default)]
pub struct Extracted {
    pub files: u64,
    pub skipped: u64,
}

/// Extract `archive` into `out_dir` (created if missing), top level only:
/// archives inside the archive are written as plain files, never recursed.
pub fn extract_archive(
    archive: &Path,
    out_dir: &Path,
    password: Option<&str>,
) -> Result<Extracted> {
    let kind = ArchiveKind::from_path(archive)
        .with_context(|| format!("{} is not a supported archive", archive.display()))?;
    std::fs::create_dir_all(out_dir)?;
    match kind {
        ArchiveKind::Zip => extract_zip(archive, out_dir, password),
        ArchiveKind::SevenZ => extract_7z(archive, out_dir, password),
        ArchiveKind::Rar => extract_rar(archive, out_dir, password),
    }
}

fn ensure_free_space(out_dir: &Path, declared: u64) -> Result<()> {
    if let Ok(free) = fs4::available_space(out_dir) {
        anyhow::ensure!(
            free > declared,
            "not enough disk space to extract: needs {declared} bytes free"
        );
    }
    Ok(())
}

fn write_entry(target: &Path, reader: &mut dyn Read) -> Result<()> {
    if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = std::fs::File::create(target)?;
    std::io::copy(reader, &mut file)?;
    Ok(())
}

fn extract_zip(archive: &Path, out_dir: &Path, password: Option<&str>) -> Result<Extracted> {
    let file = std::fs::File::open(archive)?;
    let mut zip = zip::ZipArchive::new(file).context("not a readable zip archive")?;

    let declared: u64 = (0..zip.len())
        .filter_map(|i| zip.by_index_raw(i).ok().map(|e| e.size()))
        .sum();
    ensure_free_space(out_dir, declared)?;

    let mut result = Extracted::default();
    for i in 0..zip.len() {
        let mut entry = match password {
            Some(pw) => zip
                .by_index_decrypt(i, pw.as_bytes())
                .context("wrong password or unreadable zip entry")?,
            None => zip.by_index(i).context("unreadable zip entry")?,
        };
        if entry.is_symlink() {
            result.skipped += 1;
            continue;
        }
        let name = entry.name().to_string();
        if entry.is_dir() {
            if let Ok(dir) = crate::sanitize::safe_join(out_dir, &name) {
                std::fs::create_dir_all(dir)?;
            }
            continue;
        }
        match crate::sanitize::safe_join(out_dir, &name) {
            Ok(target) => {
                write_entry(&target, &mut entry).with_context(|| format!("extracting {name}"))?;
                result.files += 1;
            }
            Err(_) => result.skipped += 1,
        }
    }
    Ok(result)
}

fn extract_7z(archive: &Path, out_dir: &Path, password: Option<&str>) -> Result<Extracted> {
    use sevenz_rust2::{ArchiveReader, Password};

    let password = password.map(Password::from).unwrap_or_else(Password::empty);
    let mut reader = ArchiveReader::open(archive, password)
        .context("wrong password or not a readable 7z archive")?;

    let declared: u64 = reader.archive().files.iter().map(|f| f.size).sum();
    ensure_free_space(out_dir, declared)?;

    let mut result = Extracted::default();
    let out_dir = out_dir.to_path_buf();
    reader
        .for_each_entries(|entry, entry_reader| {
            if entry.is_directory() {
                if let Ok(dir) = crate::sanitize::safe_join(&out_dir, entry.name()) {
                    std::fs::create_dir_all(dir)
                        .map_err(|cause| sevenz_rust2::Error::Io(cause, "creating dir".into()))?;
                }
                return Ok(true);
            }
            match crate::sanitize::safe_join(&out_dir, entry.name()) {
                Ok(target) => {
                    write_entry(&target, entry_reader).map_err(|cause| {
                        sevenz_rust2::Error::Other(
                            format!("extracting {}: {cause}", entry.name()).into(),
                        )
                    })?;
                    result.files += 1;
                }
                Err(_) => result.skipped += 1,
            }
            Ok(true)
        })
        .context("wrong password or corrupt 7z archive")?;
    Ok(result)
}

fn extract_rar(archive: &Path, out_dir: &Path, password: Option<&str>) -> Result<Extracted> {
    let open = match password {
        Some(pw) => unrar::Archive::with_password(archive, pw),
        None => unrar::Archive::new(archive),
    }
    .open_for_processing()
    .context("not a readable rar archive")?;

    let mut result = Extracted::default();
    let mut cursor = Some(open);
    while let Some(open) = cursor.take() {
        let Some(header) = open
            .read_header()
            .context("wrong password or corrupt rar archive")?
        else {
            break;
        };
        let entry_name = header.entry().filename.to_string_lossy().into_owned();
        let is_file = header.entry().is_file();
        let next = if is_file {
            match crate::sanitize::safe_join(out_dir, &entry_name) {
                Ok(target) => {
                    if let Some(parent) = target.parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    result.files += 1;
                    header
                        .extract_to(&target)
                        .context("wrong password or corrupt rar entry")?
                }
                Err(_) => {
                    result.skipped += 1;
                    header.skip().context("skipping rar entry")?
                }
            }
        } else {
            if let Ok(dir) = crate::sanitize::safe_join(out_dir, &entry_name) {
                std::fs::create_dir_all(dir)?;
            }
            header.skip().context("skipping rar entry")?
        };
        cursor = Some(next);
    }
    Ok(result)
}

/// The folder name extraction lands in: the archive's file stem.
pub fn extraction_dir_name(archive_name: &str) -> String {
    let stem = match archive_name.rsplit_once('.') {
        Some((stem, _)) if !stem.is_empty() => stem,
        _ => archive_name,
    };
    crate::sanitize::sanitize_component(stem)
}

/// Best-effort declared uncompressed size, for preflight sizing. Zero when
/// the format or archive does not declare it cheaply.
pub fn declared_size(archive: &Path, password: Option<&str>) -> u64 {
    match ArchiveKind::from_path(archive) {
        Some(ArchiveKind::Zip) => std::fs::File::open(archive)
            .ok()
            .and_then(|f| zip::ZipArchive::new(f).ok())
            .map(|mut zip| {
                (0..zip.len())
                    .filter_map(|i| zip.by_index_raw(i).ok().map(|e| e.size()))
                    .sum()
            })
            .unwrap_or(0),
        Some(ArchiveKind::SevenZ) => {
            use sevenz_rust2::{ArchiveReader, Password};
            let password = password.map(Password::from).unwrap_or_else(Password::empty);
            ArchiveReader::open(archive, password)
                .map(|r| r.archive().files.iter().map(|f| f.size).sum())
                .unwrap_or(0)
        }
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{io::Write, path::PathBuf};

    fn make_zip(entries: &[(&str, &[u8])]) -> PathBuf {
        let dir = tempfile::tempdir().unwrap().keep();
        let path = dir.join("test.zip");
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = zip::ZipWriter::new(file);
        let options = zip::write::SimpleFileOptions::default();
        for (name, data) in entries {
            writer.start_file(*name, options).unwrap();
            writer.write_all(data).unwrap();
        }
        writer.finish().unwrap();
        path
    }

    #[test]
    fn zip_roundtrip_with_nesting() {
        let archive = make_zip(&[
            ("top.txt", b"hello"),
            ("dir/inner.txt", b"world"),
            ("dir/deep/leaf.bin", b"\x00\x01"),
        ]);
        let out = tempfile::tempdir().unwrap();
        let result = extract_archive(&archive, out.path(), None).unwrap();
        assert_eq!(result.files, 3);
        assert_eq!(result.skipped, 0);
        assert_eq!(std::fs::read(out.path().join("top.txt")).unwrap(), b"hello");
        assert_eq!(
            std::fs::read(out.path().join("dir/deep/leaf.bin")).unwrap(),
            b"\x00\x01"
        );
    }

    #[test]
    fn zip_hostile_paths_are_skipped_or_sanitized() {
        let archive = make_zip(&[
            ("../escape.txt", b"evil"),
            ("/abs/olute.txt", b"abs"),
            ("ok/CON.txt", b"reserved"),
        ]);
        let out = tempfile::tempdir().unwrap();
        let result = extract_archive(&archive, out.path(), None).unwrap();
        // Traversal skipped; absolute and reserved names land sanitized.
        assert_eq!(result.skipped, 1);
        assert_eq!(result.files, 2);
        assert!(!out.path().parent().unwrap().join("escape.txt").exists());
        assert!(out.path().join("abs/olute.txt").exists());
        assert!(out.path().join("ok/_CON.txt").exists());
    }

    #[test]
    fn zip_inner_archive_stays_unextracted() {
        let inner = make_zip(&[("secret.txt", b"nested")]);
        let inner_bytes = std::fs::read(&inner).unwrap();
        let archive = make_zip(&[("bundle/inner.zip", &inner_bytes)]);
        let out = tempfile::tempdir().unwrap();
        extract_archive(&archive, out.path(), None).unwrap();
        // Top level only: the inner zip arrives as a file, bytes intact.
        assert_eq!(
            std::fs::read(out.path().join("bundle/inner.zip")).unwrap(),
            inner_bytes
        );
        assert!(!out.path().join("bundle/secret.txt").exists());
    }

    #[test]
    fn unsupported_extension_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("notanarchive.txt");
        std::fs::write(&path, b"plain").unwrap();
        assert!(extract_archive(&path, dir.path(), None).is_err());
    }

    #[test]
    fn extraction_dir_names() {
        assert_eq!(extraction_dir_name("photos.zip"), "photos");
        assert_eq!(extraction_dir_name("archive.tar.7z"), "archive.tar");
        assert_eq!(extraction_dir_name("noext"), "noext");
    }
}
