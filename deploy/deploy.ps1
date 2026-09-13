<#
.SYNOPSIS
    Deploy polyperps to an EC2 box over PuTTY (plink), or bootstrap a
    fresh box for the first time. Windows PowerShell 5.1 compatible.

.DESCRIPTION
    First deploy:   deploy.ps1 -Session <s> -Bootstrap -RepoUrl <url> [-Ref <ref>]
    Later deploys:  deploy.ps1 -Session <s> [-Ref <ref>]

    Never runs a network call from this machine other than plink
    against the named PuTTY saved session, and never sets
    POLYMARKET_LIVE_TRADING or passes --executor live anywhere.
#>
param(
    [Parameter(Mandatory = $true)]
    [string]$Session,

    [switch]$Bootstrap,

    [string]$RepoUrl,

    [string]$Ref = "master",

    [string]$Plink = "C:\Program Files\PuTTY\plink.exe"
)

$ErrorActionPreference = "Stop"

if ($Bootstrap -and (-not $RepoUrl)) {
    Write-Error "-RepoUrl is required when -Bootstrap is set"
    exit 1
}

# RepoUrl and Ref get spliced into a remote shell command string (see
# Invoke-Plink below) - allowlist their characters so nothing can break out
# of the quoting, regardless of where the values came from.
$RepoUrlPattern = '^[A-Za-z0-9._:/@+-]+$'
$RefPattern = '^[A-Za-z0-9._/-]+$'

if ($Ref -notmatch $RefPattern) {
    Write-Error "-Ref '$Ref' contains characters outside the allowed set ($RefPattern)"
    exit 1
}

if ($Bootstrap -and ($RepoUrl -notmatch $RepoUrlPattern)) {
    Write-Error "-RepoUrl '$RepoUrl' contains characters outside the allowed set ($RepoUrlPattern)"
    exit 1
}

# Refuse to deploy a dirty working tree - the box would end up running code
# that isn't on any commit.
$porcelain = git status --porcelain
if ($porcelain) {
    Write-Error "working tree is dirty; commit or stash before deploying"
    exit 1
}

# Refuse to deploy a commit that hasn't been pushed, unless this is the
# very first bootstrap of a repo with no origin (or no origin/Ref) yet.
# `--quiet` makes git suppress its own stderr message on a miss, so no
# output redirection is needed - just check $LASTEXITCODE (redirecting a
# native command's stderr in PS 5.1 wraps it as a terminating error under
# $ErrorActionPreference = "Stop", which a try/catch here would not reliably
# catch).
$localHead = (git rev-parse HEAD).Trim()

$originHead = git rev-parse --verify --quiet "origin/$Ref"
$hasOrigin = ($LASTEXITCODE -eq 0)

if (-not $hasOrigin) {
    if ($Bootstrap) {
        Write-Warning "no origin/$Ref found yet (no origin remote, or ref not pushed) - proceeding because -Bootstrap was given"
    } else {
        Write-Error "could not resolve origin/$Ref; push first"
        exit 1
    }
} else {
    $originHead = $originHead.Trim()
    if ($localHead -ne $originHead) {
        Write-Error "local HEAD ($localHead) differs from origin/$Ref ($originHead); push first"
        exit 1
    }
}

function Invoke-Plink {
    param([string[]]$RemoteArgs)

    $plinkArgs = @("-load", $Session, "-batch") + $RemoteArgs
    & $Plink @plinkArgs
    if ($LASTEXITCODE -ne 0) {
        Write-Error "plink exited with code $LASTEXITCODE"
        exit $LASTEXITCODE
    }
}

if ($Bootstrap) {
    $BootstrapScript = Join-Path $PSScriptRoot "bootstrap.sh"

    Write-Host "== copying deploy/bootstrap.sh to the box =="
    # Stream the script over plink's stdin instead of pscp: pscp cannot take
    # a saved-session name as the remote host. The remote sed strips any CR
    # the Windows pipe may add so bash never sees CRLF.
    $bootstrapText = [System.IO.File]::ReadAllText($BootstrapScript)
    $bootstrapText | & $Plink -load $Session -batch "cat > /tmp/bootstrap.sh && sed -i 's/$//' /tmp/bootstrap.sh"
    if ($LASTEXITCODE -ne 0) {
        Write-Error "plink (copy bootstrap.sh) exited with code $LASTEXITCODE"
        exit $LASTEXITCODE
    }

    Write-Host "== running bootstrap.sh =="
    Invoke-Plink -RemoteArgs @("sudo bash /tmp/bootstrap.sh '$RepoUrl' '$Ref'")
} else {
    Write-Host "== running update.sh =="
    Invoke-Plink -RemoteArgs @("sudo bash /opt/polyperps/deploy/update.sh '$Ref'")
}

Write-Host "== recent logs =="
Invoke-Plink -RemoteArgs @("sudo journalctl -u polyperps-feed -u polyperps-paper -n 40 --no-pager")
