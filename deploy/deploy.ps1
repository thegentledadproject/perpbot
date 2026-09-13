<#
.SYNOPSIS
    Deploy polyperps to an EC2 box over PuTTY (plink/pscp), or bootstrap a
    fresh box for the first time. Windows PowerShell 5.1 compatible.

.DESCRIPTION
    First deploy:   deploy.ps1 -Session <s> -Bootstrap -RepoUrl <url> [-Ref <ref>]
    Later deploys:  deploy.ps1 -Session <s> [-Ref <ref>]

    Never runs a network call from this machine other than plink/pscp
    against the named PuTTY saved session, and never sets
    POLYMARKET_LIVE_TRADING or passes --executor live anywhere.
#>
param(
    [Parameter(Mandatory = $true)]
    [string]$Session,

    [switch]$Bootstrap,

    [string]$RepoUrl,

    [string]$Ref = "master",

    [string]$Plink = "C:\Program Files\PuTTY\plink.exe",

    [string]$Pscp = "C:\Program Files\PuTTY\pscp.exe"
)

$ErrorActionPreference = "Stop"

if ($Bootstrap -and (-not $RepoUrl)) {
    Write-Error "-RepoUrl is required when -Bootstrap is set"
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
# very first bootstrap of a repo with no origin configured yet.
$localHead = (git rev-parse HEAD).Trim()
$originHead = $null
$originError = $null
try {
    $originHead = (git rev-parse "origin/$Ref" 2>$null).Trim()
} catch {
    $originError = $_
}

$hasOrigin = $true
try {
    git rev-parse --verify --quiet origin | Out-Null
} catch {
    $hasOrigin = $false
}

if (-not $originHead) {
    if ($Bootstrap -and (-not $hasOrigin)) {
        Write-Warning "no origin/$Ref found yet (no origin remote) - proceeding because -Bootstrap was given"
    } else {
        Write-Error "could not resolve origin/$Ref; push first"
        exit 1
    }
} elseif ($localHead -ne $originHead) {
    Write-Error "local HEAD ($localHead) differs from origin/$Ref ($originHead); push first"
    exit 1
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
    Write-Host "== copying deploy/bootstrap.sh to the box =="
    & $Pscp -load $Session "deploy/bootstrap.sh" "/tmp/bootstrap.sh"
    if ($LASTEXITCODE -ne 0) {
        Write-Error "pscp exited with code $LASTEXITCODE"
        exit $LASTEXITCODE
    }

    Write-Host "== running bootstrap.sh =="
    Invoke-Plink -RemoteArgs @("sudo bash /tmp/bootstrap.sh $RepoUrl $Ref")
} else {
    Write-Host "== running update.sh =="
    Invoke-Plink -RemoteArgs @("sudo bash /opt/polyperps/deploy/update.sh $Ref")
}

Write-Host "== recent logs =="
Invoke-Plink -RemoteArgs @("sudo journalctl -u polyperps-feed -u polyperps-paper -n 40 --no-pager")
