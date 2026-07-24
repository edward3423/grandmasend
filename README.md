# grandmasend

Send any file or folder of any size to anyone anywhere with an internet
connection who can type one command. Resumes if interrupted. No cloud
storage, accounts, network setup, or installation for the receiver.

## Receiving a file

To receive, paste this into your terminal, press Enter, and type the four
words the sender gave you when asked:

macOS or Linux:

```
curl -fsSL https://edward3423.github.io/grandma.sh | sh
```

Windows (PowerShell):

```
irm https://edward3423.github.io/grandma.ps1 | iex
```

The sender may instead give you a full command with the four words already
in it. Then there is nothing to type, just paste:

```
curl -fsSL https://edward3423.github.io/grandma.sh | sh -s -- maple-canyon-lantern-thirty
```

The file lands in your Downloads folder. Nothing is installed on your
machine. The two commands at the top never change, share them freely.

Interruptions lose nothing. A running receiver survives lost wifi, a
closed laptop, even the sender going offline mid transfer, and continues
automatically when the sender returns. Rerunning the command with the same
words resumes from the bytes already transferred.

Only receive files from people you trust.

## Sending a file

Install grandmasend (macOS/Linux):

```
curl -fsSL https://github.com/edward3423/grandmasend/releases/latest/download/install.sh | sh
```

Windows (PowerShell):

```
irm https://github.com/edward3423/grandmasend/releases/latest/download/install.ps1 | iex
```

Then:

```
grandmasend send path/to/file-or-folder
```

This prints a four word code to read to the receiver over the phone, plus
ready to paste receive commands for macOS/Linux and Windows. Press `c` to
copy the macOS/Linux command or `w` to copy the Windows one. Keep the
window open until it says Done. The code works exactly once, for the first
receiver that redeems it. Rerunning the same send after an interruption
revives the same code.

Useful companions:

- `grandmasend status` lists sends still waiting for a receiver
- `grandmasend send --fresh <path>` abandons a previous send of this path
  and starts over with a new code. Use this to hand the same file to a
  different person. The old code stops working.
- `grandmasend tidy` removes all waiting sends and interrupted receive
  leftovers. Interrupted transfers can no longer resume afterwards.

## Properties

- Works over the internet: the two machines connect to each other across
  networks, home routers, and NATs with no port forwarding or setup. When
  no direct path exists the transfer falls back to an encrypted relay, so
  it works from anywhere to anywhere.
- Any size: transfers stream directly between the two machines with no size
  limit or cloud storage in the middle.
- Resumable: either side can lose power or network mid transfer. The
  transfer continues from the verified bytes already there.
- Verified: every byte is BLAKE3 verified against the hash exchanged over
  the code authenticated channel.
- Private: the connection is end to end encrypted (QUIC/TLS). The four
  word code is the entire secret and anyone who has it can receive the
  file, so share it directly with the person you mean. It works exactly
  once, for the first receiver that redeems it.
- Local: two machines on the same LAN transfer directly, even with no
  internet (mDNS discovery, sending requires the installed edition).
- No leftovers: completed transfers clean up after themselves on both
  sides. `grandmasend tidy` clears anything abandoned.

Built on [iroh](https://github.com/n0-computer/iroh) and
[iroh-blobs](https://github.com/n0-computer/iroh-blobs).
Inspired by [sendme](https://github.com/n0-computer/sendme).

Status: prerelease, under active development.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
