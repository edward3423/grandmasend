//! Filesystem-name sanitization shared by collection export and archive
//! extraction: everything written to disk from remote input goes through
//! here.

use std::path::{Path, PathBuf};

use anyhow::Result;

/// Windows reserved device names; also refused on other platforms so a
/// payload lands identically everywhere.
const RESERVED_NAMES: [&str; 22] = [
    "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
    "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
];

/// Make one path component safe on every supported filesystem: control and
/// Windows-illegal characters become '_', trailing dots/spaces are trimmed,
/// reserved device names are prefixed. Never returns an empty string.
pub fn sanitize_component(part: &str) -> String {
    let mut cleaned: String = part
        .chars()
        .map(|c| match c {
            '\0'..='\x1f' | '<' | '>' | ':' | '"' | '|' | '?' | '*' | '/' | '\\' => '_',
            c => c,
        })
        .collect();
    while cleaned.ends_with('.') || cleaned.ends_with(' ') {
        cleaned.pop();
    }
    if cleaned.is_empty() {
        return "_".to_string();
    }
    let stem = cleaned.split('.').next().unwrap_or("").to_ascii_uppercase();
    if RESERVED_NAMES.contains(&stem.as_str()) {
        cleaned.insert(0, '_');
    }
    cleaned
}

/// Resolve a slash-separated remote entry name to a path under `root`:
/// traversal is rejected outright, every component is sanitized.
pub fn safe_join(root: &Path, name: &str) -> Result<PathBuf> {
    let mut path = root.to_path_buf();
    let mut any = false;
    for part in name.split(['/', '\\']) {
        if part.is_empty() {
            continue;
        }
        anyhow::ensure!(
            part != "." && part != "..",
            "invalid path component {part:?}"
        );
        path.push(sanitize_component(part));
        any = true;
    }
    anyhow::ensure!(any, "empty entry name");
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_component_cases() {
        assert_eq!(sanitize_component("normal.txt"), "normal.txt");
        assert_eq!(sanitize_component("a<b>c:d.txt"), "a_b_c_d.txt");
        assert_eq!(sanitize_component("trailing. . "), "trailing");
        assert_eq!(sanitize_component("..."), "_");
        assert_eq!(sanitize_component("CON"), "_CON");
        assert_eq!(sanitize_component("con.txt"), "_con.txt");
        assert_eq!(sanitize_component("console.txt"), "console.txt");
        assert_eq!(sanitize_component("tab\there"), "tab_here");
    }

    #[test]
    fn safe_join_rejects_traversal() {
        let root = Path::new("/tmp/x");
        assert!(safe_join(root, "a/../b").is_err());
        assert!(safe_join(root, "..").is_err());
        assert!(safe_join(root, "").is_err());
        assert_eq!(safe_join(root, "a//b").unwrap(), root.join("a").join("b"));
        assert_eq!(
            safe_join(root, "a\\b").unwrap(),
            root.join("a").join("b"),
            "backslash-separated names split into components"
        );
        assert!(safe_join(root, "ok/name.txt").is_ok());
    }

    #[test]
    fn safe_join_sanitizes_absolute_paths() {
        // A leading separator only produces empty components, which are
        // skipped: the result stays under root.
        let root = Path::new("/tmp/x");
        assert_eq!(
            safe_join(root, "/etc/passwd").unwrap(),
            root.join("etc").join("passwd")
        );
    }
}
