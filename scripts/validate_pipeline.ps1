param(
    [string]$RunId = "run_20261013",
    [string]$CleanHdfsPath = "/civicpulse/processed/pig_clean_20261013_v2",
    [string]$PatternsHdfsPath = "/civicpulse/analytics/persistent_hotspots_20261013_v2",
    [string]$DashboardUrl = "http://127.0.0.1:8088",
    [string]$WslDistro = "Ubuntu",
    [string]$HiveUrl = "jdbc:hive2://localhost:10000/default",
    [string]$HiveUser = "monica",
    [int]$TimeoutSeconds = 300
)

$ErrorActionPreference = "Stop"

foreach ($path in @($CleanHdfsPath, $PatternsHdfsPath)) {
    if ($path -notmatch "^/civicpulse/[A-Za-z0-9_/-]+$") {
        throw "HDFS path must be a safe path under /civicpulse/: $path"
    }
}

function Invoke-WslCommand {
    param([string[]]$Arguments)

    $previousErrorAction = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        $result = @(& wsl.exe --distribution $WslDistro -- @Arguments 2>&1)
    } finally {
        $ErrorActionPreference = $previousErrorAction
    }
    if ($LASTEXITCODE -ne 0) {
        throw "WSL command failed (exit $LASTEXITCODE): $($result -join [Environment]::NewLine)"
    }
    return $result
}

function Get-HdfsLineCount {
    param([string]$Path)

    $command = "set -o pipefail; hdfs dfs -cat '$Path/part-*' | wc -l"
    $result = Invoke-WslCommand -Arguments @("bash", "-lc", $command)
    $countLines = @(
        $result |
            ForEach-Object { "$_".Trim() } |
            Where-Object { $_ -match "^\d+$" }
    )
    if ($countLines.Count -ne 1) {
        throw "Could not parse one HDFS line count for $Path. Output: $($result -join ' ')"
    }
    return [long]$countLines[0]
}

function Get-HiveScalar {
    param([string]$Query)

    $result = Invoke-WslCommand -Arguments @(
        "/opt/hive/bin/beeline",
        "-u", $HiveUrl,
        "-n", $HiveUser,
        "--silent=true",
        "--showHeader=true",
        "--outputformat=tsv2",
        "--hiveconf", "mapreduce.task.io.sort.mb=32",
        "-e", $Query
    )
    $numberLines = @(
        $result |
            ForEach-Object { "$_".Trim() } |
            Where-Object { $_ -match "^\d+$" }
    )
    if ($numberLines.Count -ne 1) {
        throw "Could not parse one numeric result for Hive query '$Query'. Output: $($result -join ' ')"
    }
    return [long]$numberLines[0]
}

$hdfsCleanRows = Get-HdfsLineCount -Path $CleanHdfsPath
$hdfsPatternRows = Get-HdfsLineCount -Path $PatternsHdfsPath

$hiveCleanRows = Get-HiveScalar `
    -Query "SELECT COUNT(*) FROM civicpulse.complaint_clean"
$hivePatternRows = Get-HiveScalar `
    -Query "SELECT COUNT(*) FROM civicpulse.persistent_hotspots"
$hiveMinPatternMonths = Get-HiveScalar `
    -Query "SELECT MIN(months_observed) FROM civicpulse.persistent_hotspots"
$hiveCategories = Get-HiveScalar `
    -Query "SELECT COUNT(DISTINCT complaint_type) FROM civicpulse.complaint_clean"
$hiveMonths = Get-HiveScalar `
    -Query "SELECT COUNT(DISTINCT SUBSTR(created_date,1,7)) FROM civicpulse.complaint_clean WHERE created_date IS NOT NULL AND LENGTH(created_date) >= 7"

if ($hdfsCleanRows -lt 1 -or $hdfsPatternRows -lt 1) {
    throw "Expected non-empty clean and pattern HDFS outputs."
}
if ($hiveCleanRows -ne $hdfsCleanRows) {
    throw "Clean row mismatch: HDFS=$hdfsCleanRows, Hive=$hiveCleanRows."
}
if ($hivePatternRows -ne $hdfsPatternRows) {
    throw "Pattern row mismatch: HDFS=$hdfsPatternRows, Hive=$hivePatternRows."
}
if ($hiveMinPatternMonths -lt 2) {
    throw "Recurrence rule failed: Hive minimum months_observed=$hiveMinPatternMonths."
}

$dashboardResponse = Invoke-WebRequest `
    -Uri "$DashboardUrl/api/dashboard" `
    -UseBasicParsing `
    -TimeoutSec $TimeoutSeconds
if ($dashboardResponse.StatusCode -ne 200) {
    throw "Dashboard returned HTTP $($dashboardResponse.StatusCode)."
}
$dashboard = $dashboardResponse.Content | ConvertFrom-Json
if ($dashboard.runId -ne $RunId) {
    throw "Run ID mismatch: requested '$RunId', dashboard reports '$($dashboard.runId)'."
}
if ([long]$dashboard.sourceRecordCount -ne $hdfsCleanRows) {
    throw "Dashboard/HDFS row mismatch: dashboard=$($dashboard.sourceRecordCount), HDFS=$hdfsCleanRows."
}
if ([long]$dashboard.categoryCount -ne $hiveCategories) {
    throw "Dashboard/Hive category mismatch: dashboard=$($dashboard.categoryCount), Hive=$hiveCategories."
}
if ($dashboard.monthly.Count -ne $hiveMonths) {
    throw "Dashboard/Hive month mismatch: dashboard=$($dashboard.monthly.Count), Hive=$hiveMonths."
}
$hasNoDashboardData = [long]$dashboard.patternCount -lt 1 -or [long]$dashboard.storedPatternCount -lt 1 -or [long]$dashboard.evidenceCount -lt 1
if ($hasNoDashboardData) {
    throw "Dashboard reports no MongoDB pattern or evidence documents."
}
if ($dashboard.patterns.Count -lt 1) {
    throw "Dashboard returned no pattern summaries for evidence lookup."
}

$patternId = [string]$dashboard.patterns[0]._id
$encodedPatternId = [uri]::EscapeDataString($patternId)
$evidenceResponse = Invoke-WebRequest `
    -Uri "$DashboardUrl/api/patterns/$encodedPatternId/evidence" `
    -UseBasicParsing `
    -TimeoutSec $TimeoutSeconds
if ($evidenceResponse.StatusCode -ne 200) {
    throw "Pattern evidence endpoint returned HTTP $($evidenceResponse.StatusCode)."
}
$evidenceResult = $evidenceResponse.Content | ConvertFrom-Json
if ($evidenceResult.pattern._id -ne $patternId -or $evidenceResult.evidence.Count -lt 1) {
    throw "Pattern evidence endpoint did not return linked evidence."
}

Write-Output "CivicPulse pipeline validation passed."
Write-Output "Run ID: $RunId"
Write-Output "Clean rows: HDFS=$hdfsCleanRows, Hive=$hiveCleanRows, Dashboard=$($dashboard.sourceRecordCount)"
Write-Output "Patterns: HDFS=$hdfsPatternRows, Hive=$hivePatternRows; minimum months=$hiveMinPatternMonths"
Write-Output "Hive categories=$hiveCategories; months=$hiveMonths"
Write-Output "Hive persistent patterns=$($dashboard.patternCount)"
Write-Output "MongoDB stored patterns=$($dashboard.storedPatternCount); matching evidence=$($dashboard.evidenceCount)"
Write-Output "Evidence lookup returned $($evidenceResult.evidence.Count) records for $patternId"
