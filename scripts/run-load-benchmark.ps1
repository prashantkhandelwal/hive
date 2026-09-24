[CmdletBinding()]
param(
    [ValidateRange(1, [int]::MaxValue)]
    [int]$Requests = 100000,

    [ValidateNotNullOrEmpty()]
    [int[]]$Concurrency = @(1, 4, 16, 64, 128, 256, 512, 1000, 2000),

    [ValidateRange(1, [int]::MaxValue)]
    [int]$Runs = 3,

    [ValidateNotNullOrEmpty()]
    [string]$OutputDirectory = "benchmark-results\sweep"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Assert-Command {
    param([Parameter(Mandatory)][string]$Name)

    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' was not found on PATH."
    }
}

function ConvertTo-GnuplotPath {
    param([Parameter(Mandatory)][string]$Path)

    return [System.IO.Path]::GetFullPath($Path).Replace("\", "/").Replace("'", "''")
}

Assert-Command "cargo"
Assert-Command "gnuplot"

if ($Concurrency.Count -eq 0 -or ($Concurrency | Where-Object { $_ -le 0 })) {
    throw "Every concurrency value must be greater than zero."
}

$repositoryRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$resolvedOutputDirectory = if ([System.IO.Path]::IsPathRooted($OutputDirectory)) {
    [System.IO.Path]::GetFullPath($OutputDirectory)
} else {
    [System.IO.Path]::GetFullPath((Join-Path $repositoryRoot $OutputDirectory))
}
$summaryDataPath = Join-Path $resolvedOutputDirectory "summary.dat"
$summaryScriptPath = Join-Path $resolvedOutputDirectory "summary.gnuplot"
$summaryImagePath = Join-Path $resolvedOutputDirectory "summary.png"

New-Item -ItemType Directory -Force -Path $resolvedOutputDirectory | Out-Null
@"
# Hive concurrency sweep
# run requests concurrency successful failed throughput_rps latency_ms cpu_percent memory_mib
"@ | Set-Content -Encoding ascii -Path $summaryDataPath

$savedEnvironment = @{
    HIVE_BENCH_REQUESTS = $env:HIVE_BENCH_REQUESTS
    HIVE_BENCH_CONCURRENCY = $env:HIVE_BENCH_CONCURRENCY
    HIVE_BENCH_OUTPUT_DIRECTORY = $env:HIVE_BENCH_OUTPUT_DIRECTORY
}

Push-Location $repositoryRoot
try {
    foreach ($concurrencyValue in $Concurrency) {
        for ($run = 1; $run -le $Runs; $run++) {
            $runName = "concurrency-$concurrencyValue-run-$run"
            $runDirectory = Join-Path $resolvedOutputDirectory $runName
            New-Item -ItemType Directory -Force -Path $runDirectory | Out-Null

            $env:HIVE_BENCH_REQUESTS = "$Requests"
            $env:HIVE_BENCH_CONCURRENCY = "$concurrencyValue"
            $env:HIVE_BENCH_OUTPUT_DIRECTORY = $runDirectory

            Write-Host ""
            Write-Host "Running $runName ($Requests requests)..." -ForegroundColor Cyan
            & cargo test --locked --release --test application_load -- --ignored --nocapture
            if ($LASTEXITCODE -ne 0) {
                throw "Benchmark $runName failed with exit code $LASTEXITCODE."
            }

            $runDataPath = Join-Path $runDirectory "benchmark-results.dat"
            $resultLines = @(
                Get-Content -Path $runDataPath |
                    Where-Object { $_.Trim() -and -not $_.TrimStart().StartsWith("#") }
            )
            if ($resultLines.Count -ne 1) {
                throw "Expected one result row in '$runDataPath'; found $($resultLines.Count)."
            }
            Add-Content -Encoding ascii -Path $summaryDataPath -Value "$run $($resultLines[0])"

            $runPlotScript = Join-Path $runDirectory "benchmark-results.gnuplot"
            & gnuplot $runPlotScript
            if ($LASTEXITCODE -ne 0) {
                throw "GNUplot failed for $runName with exit code $LASTEXITCODE."
            }
        }
    }
} finally {
    Pop-Location
    $env:HIVE_BENCH_REQUESTS = $savedEnvironment.HIVE_BENCH_REQUESTS
    $env:HIVE_BENCH_CONCURRENCY = $savedEnvironment.HIVE_BENCH_CONCURRENCY
    $env:HIVE_BENCH_OUTPUT_DIRECTORY = $savedEnvironment.HIVE_BENCH_OUTPUT_DIRECTORY
}

$gnuplotDataPath = ConvertTo-GnuplotPath $summaryDataPath
$gnuplotImagePath = ConvertTo-GnuplotPath $summaryImagePath
@"
set terminal pngcairo size 1800,1200 enhanced font 'Arial,10'
set output '$gnuplotImagePath'
set datafile separator whitespace
set grid
set key outside right
set xlabel 'Concurrency'
set logscale x 2
set multiplot layout 3,2 title 'Hive load benchmark concurrency sweep'

set ylabel 'Throughput (requests/second)'
plot '$gnuplotDataPath' using 3:6 smooth unique with linespoints title 'Mean throughput', \
     '$gnuplotDataPath' using 3:6 with points pointtype 7 title 'Runs'

set ylabel 'Average latency (ms)'
plot '$gnuplotDataPath' using 3:7 smooth unique with linespoints title 'Mean latency', \
     '$gnuplotDataPath' using 3:7 with points pointtype 7 title 'Runs'

set ylabel 'Server CPU (%)'
plot '$gnuplotDataPath' using 3:8 smooth unique with linespoints title 'Mean CPU', \
     '$gnuplotDataPath' using 3:8 with points pointtype 7 title 'Runs'

set ylabel 'Peak server memory (MiB)'
plot '$gnuplotDataPath' using 3:9 smooth unique with linespoints title 'Mean peak memory', \
     '$gnuplotDataPath' using 3:9 with points pointtype 7 title 'Runs'

set ylabel 'RPS per 100% CPU'
plot '$gnuplotDataPath' using 3:(`$8 > 0 ? `$6 * 100 / `$8 : 0) smooth unique with linespoints title 'Mean efficiency', \
     '$gnuplotDataPath' using 3:(`$8 > 0 ? `$6 * 100 / `$8 : 0) with points pointtype 7 title 'Runs'

set ylabel 'Failed requests'
plot '$gnuplotDataPath' using 3:5 smooth unique with linespoints title 'Mean failures', \
     '$gnuplotDataPath' using 3:5 with points pointtype 7 title 'Runs'

unset multiplot
"@ | Set-Content -Encoding ascii -Path $summaryScriptPath

& gnuplot $summaryScriptPath
if ($LASTEXITCODE -ne 0) {
    throw "GNUplot failed to generate the summary chart with exit code $LASTEXITCODE."
}

Write-Host ""
Write-Host "Benchmark sweep completed." -ForegroundColor Green
Write-Host "Summary data:  $summaryDataPath"
Write-Host "Summary chart: $summaryImagePath"
