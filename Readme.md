# Big Data Spark Analysis – Deucalion HPC

## Author 

Srey Pheak Sang, Helena Cunha, Margarida Ferreira  
University of Minho – Big Data Analysis 2025/2026

## Overview

This repository contains the code used to analyse and optimise a PySpark workload processing SLURM job accounting data from the **Deucalion HPC cluster (University of Minho)**.

The workload processes **557,488 job records** stored across **12 monthly CSV files**, and computes aggregated statistics across compute partition types (**ARM, AMD, GPU**) and funding agencies (**FCT, EuroHPC, LOCAL**).

The project includes:

- Baseline Spark implementation
- Multiple optimisation experiments
- Event log analysis scripts
- Final performance report

---

## Repository Structure

```
│
├── README.md
├── .gitignore
│
├── baseline/
│   ├── statsEHPC_v2_init.py        # Original unoptimised script
│   ├── execution_plan_base.txt     # Spark explain() output for baseline
│   ├── baseline_run1_pidstat.txt   # CPU/RAM measurements run 1
│   ├── baseline_run2_pidstat.txt   # CPU/RAM measurements run 2
│   └── baseline_run3_pidstat.txt   # CPU/RAM measurements run 3
│
├── optimizations/
│   ├── opt1_cache.py               # Opt 1 — DataFrame caching
│   ├── opt2_repartition.py         # Opt 2 — Repartitioning
│   ├── opt3_cache_repartition.py   # Opt 3 — Cache + Repartition combined
│   ├── opt4_wildcard_pruning.py    # Opt 4 — Wildcard read + column pruning
│   └── opt5_final.py               # Opt 5 — All optimisations combined
│
├── analysis/
│   ├── parse_event_log.py          # Spark event log parser
│   └── metrics.csv                 # All run results (baseline + optimisations)
│
└── report/
    └── report.pdf                  # Final submitted report
```

---

## Baseline

```bash
# Run 1
pidstat -u -r 5 > baseline/baseline_run1_pidstat.txt &
PIDSTAT_PID=$!
time python baseline/statsEHPC_v2_init.py -m Jan > output_with_execution_plan.txt 2>&1
kill $PIDSTAT_PID

# Run 2
pidstat -u -r 5 > baseline/baseline_run2_pidstat.txt &
PIDSTAT_PID=$!
time python baseline/statsEHPC_v2_init.py -m Jan > output_with_execution_plan.txt 2>&1
kill $PIDSTAT_PID

# Run 3
pidstat -u -r 5 > baseline/baseline_run3_pidstat.txt &
PIDSTAT_PID=$!
time python baseline/statsEHPC_v2_init.py -m Jan > output_with_execution_plan.txt 2>&1
kill $PIDSTAT_PID
```

---

## Optimisations

Use the same protocol as the baseline. Example for **opt1** (repeat for opt2 through opt5):

```bash
# Run 1
pidstat -u -r 5 > analysis/opt1_run1_pidstat.txt &
PIDSTAT_PID=$!
time python optimizations/opt1_cache.py -m Jan > output_with_execution_plan_opt1.txt 2>&1
kill $PIDSTAT_PID
```

Example commands for running each optimisation:

```bash
time python optimizations/opt1_cache.py -m Jan > output_with_execution_plan_opt1.txt 2>&1
time python optimizations/opt2_repartition.py -m Jan > output_with_execution_plan_opt2.txt 2>&1
time python optimizations/opt3_cache_repartition.py -m Jan > output_with_execution_plan_opt3.txt 2>&1
time python optimizations/opt4_wildcard_pruning.py -m Jan > output_with_execution_plan_opt4.txt 2>&1
time python optimizations/opt5_final.py -m Jan > output_with_execution_plan_opt5.txt 2>&1
```

---

## Event Log Analysis

Metrics were extracted from Spark event logs using the script  
`analysis/parse_event_log.py`.

Spark event logs on HPC systems may be stored in compressed format  
(e.g., `.zstd`). If necessary, decompress the log file first to obtain  
the JSON event log before running the parser.

Example:

```bash
# decompress (example for .zstd)
zstd -d eventlog.zstd -o eventlog.json

# run parser
python analysis/parse_event_log.py eventlog.json baseline_run1

# change the run label (e.g., baseline_run1, opt1_run1, opt2_run1)
# to match the corresponding experiment
```

The parser reads the JSON event log and appends the extracted metrics to  
`analysis/metrics.csv`.

Note: Spark event log file names and locations depend on the cluster  
configuration. You may need to adjust the file path accordingly.
