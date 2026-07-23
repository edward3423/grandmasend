# grandmasend bootstrap: transient receiver for Windows.
#
# The one constant command a receiver ever pastes:
#   irm https://github.com/edward3423/grandmasend/releases/latest/download/bootstrap.ps1 | iex
#
# Fetches the latest release binary to a temp dir, runs it once in transient
# receive mode (prompts for the four-word code), then deletes it. Installs
# nothing, needs no admin rights.

$ErrorActionPreference = "Stop"

$baseUrl = if ($env:GRANDMASEND_BASE_URL) {
    $env:GRANDMASEND_BASE_URL
} else {
    "https://github.com/edward3423/grandmasend/releases/latest/download"
}

$arch = if ([System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture -eq "Arm64") {
    "aarch64-pc-windows-msvc"
} else {
    "x86_64-pc-windows-msvc"
}

$tmp = Join-Path ([System.IO.Path]::GetTempPath()) ("grandmasend-" + [System.Guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $tmp | Out-Null

try {
    Write-Host "Fetching grandmasend..."
    $zip = Join-Path $tmp "grandmasend.zip"
    Invoke-WebRequest -Uri "$baseUrl/grandmasend-$arch.zip" -OutFile $zip
    Expand-Archive -Path $zip -DestinationPath $tmp
    $exe = Join-Path $tmp "grandmasend.exe"
    if ($env:GRANDMASEND_CODE) {
        # Test hook: CI drives the paste-path without a terminal.
        $cliArgs = @("receive", "--transient") + ($env:GRANDMASEND_CODE -split " ")
        if ($env:GRANDMASEND_DEST) { $cliArgs += @("--dest", $env:GRANDMASEND_DEST) }
        if ($env:GRANDMASEND_SENDER_ADDR) { $cliArgs += @("--sender-addr", $env:GRANDMASEND_SENDER_ADDR) }
        & $exe @cliArgs
    } else {
        & $exe receive --transient
    }
} finally {
    Remove-Item -Recurse -Force $tmp -ErrorAction SilentlyContinue
}
