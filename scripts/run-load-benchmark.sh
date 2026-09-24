#!/usr/bin/env bash

set -euo pipefail

REQUESTS=100000
CONCURRENCY_VALUES="1,4,16,64,128,256,512,1000,2000"
RUNS=3
OUTPUT_DIRECTORY="benchmark-results/sweep"

usage() {
    cat <<'EOF'
Usage: run-load-benchmark.sh [options]

Options:
  -r, --requests NUMBER       Requests per run (default: 100000)
  -c, --concurrency VALUES    Comma-separated concurrency values
  -n, --runs NUMBER           Runs per concurrency value (default: 3)
  -o, --output DIRECTORY      Output directory
  -h, --help                  Show this help
EOF
}

require_positive_integer() {
    local name="$1"
    local value="$2"
    if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
        echo "$name must be a positive integer; got '$value'." >&2
        exit 2
    fi
}

while (($# > 0)); do
    case "$1" in
        -r|--requests)
            [[ $# -ge 2 ]] || { echo "$1 requires a value." >&2; exit 2; }
            REQUESTS="$2"
            shift 2
            ;;
        -c|--concurrency)
            [[ $# -ge 2 ]] || { echo "$1 requires a value." >&2; exit 2; }
            CONCURRENCY_VALUES="$2"
            shift 2
            ;;
        -n|--runs)
            [[ $# -ge 2 ]] || { echo "$1 requires a value." >&2; exit 2; }
            RUNS="$2"
            shift 2
            ;;
        -o|--output)
            [[ $# -ge 2 ]] || { echo "$1 requires a value." >&2; exit 2; }
            OUTPUT_DIRECTORY="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

require_positive_integer "requests" "$REQUESTS"
require_positive_integer "runs" "$RUNS"

IFS=',' read -r -a CONCURRENCY <<< "$CONCURRENCY_VALUES"
if ((${#CONCURRENCY[@]} == 0)); then
    echo "At least one concurrency value is required." >&2
    exit 2
fi
for value in "${CONCURRENCY[@]}"; do
    require_positive_integer "concurrency" "$value"
done

for command_name in cargo gnuplot; do
    if ! command -v "$command_name" >/dev/null 2>&1; then
        echo "Required command '$command_name' was not found on PATH." >&2
        exit 1
    fi
done

REPOSITORY_ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
if [[ "$OUTPUT_DIRECTORY" = /* ]]; then
    RESOLVED_OUTPUT_DIRECTORY="$OUTPUT_DIRECTORY"
else
    RESOLVED_OUTPUT_DIRECTORY="$REPOSITORY_ROOT/$OUTPUT_DIRECTORY"
fi
mkdir -p "$RESOLVED_OUTPUT_DIRECTORY"
RESOLVED_OUTPUT_DIRECTORY="$(CDPATH= cd -- "$RESOLVED_OUTPUT_DIRECTORY" && pwd)"

SUMMARY_DATA="$RESOLVED_OUTPUT_DIRECTORY/summary.dat"
SUMMARY_SCRIPT="$RESOLVED_OUTPUT_DIRECTORY/summary.gnuplot"
SUMMARY_IMAGE="$RESOLVED_OUTPUT_DIRECTORY/summary.png"

cat > "$SUMMARY_DATA" <<'EOF'
# Hive concurrency sweep
# run requests concurrency successful failed throughput_rps latency_ms cpu_percent memory_mib
EOF

cd "$REPOSITORY_ROOT"
for concurrency_value in "${CONCURRENCY[@]}"; do
    for ((run = 1; run <= RUNS; run++)); do
        run_name="concurrency-$concurrency_value-run-$run"
        run_directory="$RESOLVED_OUTPUT_DIRECTORY/$run_name"
        mkdir -p "$run_directory"

        echo
        echo "Running $run_name ($REQUESTS requests)..."
        HIVE_BENCH_REQUESTS="$REQUESTS" \
        HIVE_BENCH_CONCURRENCY="$concurrency_value" \
        HIVE_BENCH_OUTPUT_DIRECTORY="$run_directory" \
            cargo test --locked --release --test application_load -- --ignored --nocapture

        run_data="$run_directory/benchmark-results.dat"
        result_count="$(awk '!/^#/ && NF { count++ } END { print count + 0 }' "$run_data")"
        if [[ "$result_count" -ne 1 ]]; then
            echo "Expected one result row in '$run_data'; found $result_count." >&2
            exit 1
        fi
        awk -v run="$run" '!/^#/ && NF { print run, $0 }' "$run_data" >> "$SUMMARY_DATA"
        gnuplot "$run_directory/benchmark-results.gnuplot"
    done
done

gnuplot_escape() {
    printf '%s' "$1" | sed "s/'/''/g"
}

gnuplot_data="$(gnuplot_escape "$SUMMARY_DATA")"
gnuplot_image="$(gnuplot_escape "$SUMMARY_IMAGE")"
cat > "$SUMMARY_SCRIPT" <<EOF
set terminal pngcairo size 1800,1200 enhanced font 'Arial,10'
set output '$gnuplot_image'
set datafile separator whitespace
set grid
set key outside right
set xlabel 'Concurrency'
set logscale x 2
set multiplot layout 3,2 title 'Hive load benchmark concurrency sweep'

set ylabel 'Throughput (requests/second)'
plot '$gnuplot_data' using 3:6 smooth unique with linespoints title 'Mean throughput', \\
     '$gnuplot_data' using 3:6 with points pointtype 7 title 'Runs'

set ylabel 'Average latency (ms)'
plot '$gnuplot_data' using 3:7 smooth unique with linespoints title 'Mean latency', \\
     '$gnuplot_data' using 3:7 with points pointtype 7 title 'Runs'

set ylabel 'Server CPU (%)'
plot '$gnuplot_data' using 3:8 smooth unique with linespoints title 'Mean CPU', \\
     '$gnuplot_data' using 3:8 with points pointtype 7 title 'Runs'

set ylabel 'Peak server memory (MiB)'
plot '$gnuplot_data' using 3:9 smooth unique with linespoints title 'Mean peak memory', \\
     '$gnuplot_data' using 3:9 with points pointtype 7 title 'Runs'

set ylabel 'RPS per 100% CPU'
plot '$gnuplot_data' using 3:(\$8 > 0 ? \$6 * 100 / \$8 : 0) smooth unique with linespoints title 'Mean efficiency', \\
     '$gnuplot_data' using 3:(\$8 > 0 ? \$6 * 100 / \$8 : 0) with points pointtype 7 title 'Runs'

set ylabel 'Failed requests'
plot '$gnuplot_data' using 3:5 smooth unique with linespoints title 'Mean failures', \\
     '$gnuplot_data' using 3:5 with points pointtype 7 title 'Runs'

unset multiplot
EOF

gnuplot "$SUMMARY_SCRIPT"

echo
echo "Benchmark sweep completed."
echo "Summary data:  $SUMMARY_DATA"
echo "Summary chart: $SUMMARY_IMAGE"
