# Big Data Spark Analysis – Deucalion HPC

#Author 

Srey Pheak Sang , Helena Cunha, Magarida Ferreira
University of Minho – Big Data Analysis 2025/2026

## Overview

This repository contains the code used to analyse and optimise a PySpark workload processing SLURM job accounting data from the **Deucalion HPC cluster (University of Minho)**.

The workload processes **557,488 job records** stored in **12 monthly CSV files** and computes aggregated statistics across compute partition types (ARM, AMD, GPU) and funding agencies (FCT, EuroHPC, LOCAL).

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
    
## How to Run 


### Baseline
```bash
time python baseline/statsEHPC_v2_init.py -m Jan > output_with_execution_plan.txt 2>&1
```

### Optimisations
```bash
time python optimizations/opt1_cache.py -m Jan > output_with_execution_plan_opt1.txt 2>&1
time python optimizations/opt2_repartition.py -m Jan > output_with_execution_plan_opt2.txt 2>&1
time python optimizations/opt3_cache_repartition.py -m Jan > output_with_execution_plan_opt3.txt 2>&1
time python optimizations/opt4_wildcard_pruning.py -m Jan > output_with_execution_plan_opt4.txt 2>&1
time python optimizations/opt5_final.py -m Jan > output_with_execution_plan_opt5.txt 2>&1

```


##Event Log from Spark Event 

All run metrics are available in `analysis/metrics.csv`. Metrics were extracted from
Spark event logs generated during each run and include: application duration, task
execution span, driver time, stages submitted and completed, total tasks, input bytes
read, shuffle read/write volume, executor run time, and GC overhead.







