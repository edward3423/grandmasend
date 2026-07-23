# grandmasend installer and updater for Windows.
#
#   irm https://github.com/edward3423/grandmasend/releases/latest/download/install.ps1 | iex
#
# Installs the latest release to %LOCALAPPDATA%\grandmasend\bin and adds it
# to the user PATH. No admin rights. Running it again updates in place.

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

$binDir = Join-Path $env:LOCALAPPDATA "grandmasend\bin"
New-Item -ItemType Directory -Force -Path $binDir | Out-Null

$tmp = Join-Path ([System.IO.Path]::GetTempPath()) ("grandmasend-" + [System.Guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $tmp | Out-Null

try {
    Write-Host "Fetching grandmasend..."
    $zip = Join-Path $tmp "grandmasend.zip"
    Invoke-WebRequest -Uri "$baseUrl/grandmasend-$arch.zip" -OutFile $zip
    Expand-Archive -Path $zip -DestinationPath $tmp
    Move-Item -Force (Join-Path $tmp "grandmasend.exe") (Join-Path $binDir "grandmasend.exe")
} finally {
    Remove-Item -Recurse -Force $tmp -ErrorAction SilentlyContinue
}

$userPath = [Environment]::GetEnvironmentVariable("Path", "User")
if ($userPath -notlike "*$binDir*") {
    [Environment]::SetEnvironmentVariable("Path", "$userPath;$binDir", "User")
    Write-Host "Added $binDir to your PATH - open a new terminal to use it."
}

& (Join-Path $binDir "grandmasend.exe") --version
Write-Host "Installed to $binDir\grandmasend.exe"
