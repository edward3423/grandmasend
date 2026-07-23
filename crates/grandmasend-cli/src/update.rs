//! Launch-time update check against GitHub Releases.
//!
//! Policy: outdated is a nag, never a refusal - refusing would strand the
//! offline LAN scenario. A failed check (including offline) is one explicit
//! warning line, never silent, never blocking.

use std::time::Duration;

/// Where releases live; the latest-tag redirect is the version oracle, so no
/// GitHub API quota is involved.
const LATEST_URL: &str = "https://github.com/edward3423/grandmasend/releases/latest";

pub const INSTALL_HINT: &str =
    "curl -fsSL https://github.com/edward3423/grandmasend/releases/latest/download/install.sh | sh";

/// Compare the running version against the latest release and print the
/// result: a nag when outdated, one warning line when the check fails,
/// nothing when current. Bounded to `timeout`; never blocks longer.
pub async fn check_and_nag(current: &str, timeout: Duration) {
    if std::env::var_os("GRANDMASEND_NO_UPDATE_CHECK").is_some() {
        return;
    }
    match tokio::time::timeout(timeout, latest_version()).await {
        Ok(Ok(latest)) => {
            if is_older(current, &latest) {
                eprintln!(
                    "A newer version is available ({latest}, you run {current}). Update with:"
                );
                eprintln!("  {INSTALL_HINT}");
            }
        }
        _ => {
            eprintln!("Could not check for updates. This copy may be outdated.");
        }
    }
}

/// Resolve the latest release version from the releases/latest redirect
/// (".../releases/tag/vX.Y.Z").
async fn latest_version() -> anyhow::Result<String> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let response = client.head(LATEST_URL).send().await?;
    let location = response
        .headers()
        .get("location")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| anyhow::anyhow!("no redirect from releases/latest"))?;
    let tag = location
        .rsplit('/')
        .next()
        .ok_or_else(|| anyhow::anyhow!("unparseable redirect {location}"))?;
    Ok(tag.trim_start_matches('v').to_string())
}

/// Numeric semver comparison; malformed versions are treated as not older
/// so a bad tag never triggers a false nag.
pub fn is_older(current: &str, latest: &str) -> bool {
    match (parse(current), parse(latest)) {
        (Some(c), Some(l)) => c < l,
        _ => false,
    }
}

fn parse(v: &str) -> Option<(u64, u64, u64)> {
    let mut parts = v.trim().trim_start_matches('v').splitn(3, '.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    let patch = parts
        .next()
        .map(|p| {
            // Tolerate pre-release suffixes like "3-rc.1".
            p.split(|c: char| !c.is_ascii_digit())
                .next()
                .unwrap_or("0")
                .parse()
                .unwrap_or(0)
        })
        .unwrap_or(0);
    Some((major, minor, patch))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_ordering() {
        assert!(is_older("0.1.0", "0.2.0"));
        assert!(is_older("0.1.0", "1.0.0"));
        assert!(is_older("1.2.3", "1.2.4"));
        assert!(!is_older("1.2.3", "1.2.3"));
        assert!(!is_older("2.0.0", "1.9.9"));
        assert!(!is_older("garbage", "1.0.0"));
        assert!(!is_older("1.0.0", "garbage"));
        assert!(is_older("v0.1.0", "v0.1.1"));
        assert!(is_older("1.2.3", "1.2.4-rc.1"));
    }
}
