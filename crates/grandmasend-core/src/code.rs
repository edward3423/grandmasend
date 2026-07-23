//! Transfer codes: four words from the EFF large wordlist.
//!
//! The code is the sole per-transfer secret. It is generated on the sender,
//! spoken or typed to the receiver, and fed through a KDF to derive the
//! transfer identity on both sides (see [`crate::identity`]).

use std::{fmt::Display, str::FromStr, sync::OnceLock};

use anyhow::bail;
use rand::RngExt;

const WORDLIST_RAW: &str = include_str!("eff_large_wordlist.txt");
pub const WORD_COUNT: usize = 4;

fn wordlist() -> &'static Vec<&'static str> {
    static WORDS: OnceLock<Vec<&'static str>> = OnceLock::new();
    WORDS.get_or_init(|| {
        WORDLIST_RAW
            .lines()
            .filter_map(|line| line.split_whitespace().nth(1))
            .collect()
    })
}

/// A validated four-word transfer code.
///
/// Canonical form is lowercase words joined by single spaces; that exact
/// string is the KDF input, so it must never change once released.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Code {
    words: [&'static str; WORD_COUNT],
}

impl Code {
    /// Generate a fresh random code using the OS RNG.
    pub fn generate() -> Self {
        let list = wordlist();
        let mut rng = rand::rng();
        let words = std::array::from_fn(|_| list[rng.random_range(0..list.len())]);
        Self { words }
    }

    /// The canonical string: lowercase words separated by single spaces.
    pub fn canonical(&self) -> String {
        self.words.join(" ")
    }
}

impl Display for Code {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.canonical())
    }
}

impl FromStr for Code {
    type Err = anyhow::Error;

    /// Accepts any mix of case, spaces, and hyphens; surrounding junk
    /// (quotes, punctuation) is trimmed per word. Every word must be on the
    /// EFF large wordlist, which catches misspellings without a checksum.
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let list = wordlist();
        let candidates: Vec<String> = input
            .split(|c: char| c.is_whitespace() || c == '-')
            .map(|w| {
                w.trim_matches(|c: char| !c.is_ascii_alphabetic())
                    .to_ascii_lowercase()
            })
            .filter(|w| !w.is_empty())
            .collect();
        if candidates.len() != WORD_COUNT {
            bail!(
                "a code is exactly {WORD_COUNT} words, got {}",
                candidates.len()
            );
        }
        let mut words = [""; WORD_COUNT];
        for (i, candidate) in candidates.iter().enumerate() {
            match list.iter().find(|w| **w == candidate.as_str()) {
                Some(word) => words[i] = word,
                None => bail!("\"{candidate}\" is not a code word - check the spelling"),
            }
        }
        Ok(Self { words })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wordlist_is_complete() {
        assert_eq!(wordlist().len(), 7776);
    }

    #[test]
    fn generate_roundtrips() {
        let code = Code::generate();
        let parsed: Code = code.canonical().parse().unwrap();
        assert_eq!(code, parsed);
    }

    #[test]
    fn parse_is_forgiving() {
        let code: Code = "abacus abdomen abdominal abide".parse().unwrap();
        for input in [
            "Abacus Abdomen Abdominal Abide",
            "abacus-abdomen-abdominal-abide",
            "  abacus   abdomen-abdominal abide  ",
            "\"abacus\" abdomen, abdominal. abide!",
            "ABACUS-Abdomen abdominal-ABIDE",
        ] {
            assert_eq!(input.parse::<Code>().unwrap(), code, "input: {input:?}");
        }
    }

    #[test]
    fn parse_rejects_bad_input() {
        assert!("abacus abdomen abdominal".parse::<Code>().is_err());
        assert!("abacus abdomen abdominal abide extra".parse::<Code>().is_err());
        assert!("abacus abdomen abdominal zzzznotaword".parse::<Code>().is_err());
        assert!("".parse::<Code>().is_err());
    }

    #[test]
    fn display_uses_spaces() {
        let code: Code = "abacus-abdomen-abdominal-abide".parse().unwrap();
        assert_eq!(code.to_string(), "abacus abdomen abdominal abide");
    }
}
