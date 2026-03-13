"""
Spark Event Log Parser
======================
Extracts all required project metrics from a Spark JSON event log file.

Usage:
    python parse_event_log.py <event_log_file> [run_label]

Examples:
    python parse_event_log.py events_baseline.json        baseline_run1
    python parse_event_log.py events_opt1.json            opt1_cache_run1

Output:
    - Prints a summary table to stdout
    - Appends one CSV row to metrics.csv (creates it if it doesn't exist)
"""

import json
import sys
import csv
import os
from datetime import datetime

# ── arguments ─────────────────────────────────────────────────────────────────
file_path = sys.argv[1] if len(sys.argv) > 1 else "events_baseline.json"
run_label = sys.argv[2] if len(sys.argv) > 2 else os.path.basename(file_path).replace(".json", "")

# ── accumulators ──────────────────────────────────────────────────────────────
stages_submitted  = 0
stages_completed  = 0
stages_failed     = 0
total_tasks       = 0
completed_tasks   = 0
failed_tasks      = 0

shuffle_read_bytes  = 0
shuffle_write_bytes = 0
executor_run_time_ms  = 0
executor_gc_time_ms   = 0
input_bytes_read      = 0

task_start_times = []
task_end_times   = []
executors_registered = set()
app_start_time = None
app_end_time   = None
stage_info = {}
parse_errors = 0
line_count   = 0

# ── parse ─────────────────────────────────────────────────────────────────────
with open(file_path, "r") as f:
    for line in f:
        line_count += 1
        line = line.strip()
        if not line:
            continue
        try:
            ev = json.loads(line)
        except json.JSONDecodeError:
            parse_errors += 1
            continue

        etype = ev.get("Event", "")

        if etype == "SparkListenerApplicationStart":
            app_start_time = ev.get("Timestamp")

        elif etype == "SparkListenerApplicationEnd":
            app_end_time = ev.get("Timestamp")

        elif etype == "SparkListenerExecutorAdded":
            eid = ev.get("Executor ID")
            if eid is not None:
                executors_registered.add(eid)

        elif etype == "SparkListenerStageSubmitted":
            si = ev.get("Stage Info", {})
            sid = si.get("Stage ID")
            stages_submitted += 1
            num_tasks = si.get("Number of Tasks", 0)
            total_tasks += num_tasks
            stage_info[sid] = {
                "name":            si.get("Stage Name", ""),
                "num_tasks":       num_tasks,
                "submission_time": si.get("Submission Time"),
                "completion_time": None,
                "failure_reason":  None,
            }

        elif etype == "SparkListenerStageCompleted":
            si = ev.get("Stage Info", {})
            sid = si.get("Stage ID")
            failure = si.get("Failure Reason")
            if failure:
                stages_failed += 1
            else:
                stages_completed += 1
            if sid in stage_info:
                stage_info[sid]["completion_time"] = si.get("Completion Time")
                stage_info[sid]["failure_reason"]  = failure

        elif etype == "SparkListenerTaskEnd":
            task_info    = ev.get("Task Info", {})
            task_metrics = ev.get("Task Metrics", {})
            reason = ev.get("Task End Reason", {}).get("Reason", "")

            if reason == "Success":
                completed_tasks += 1
            else:
                failed_tasks += 1

            launch_time = task_info.get("Launch Time")
            finish_time = task_info.get("Finish Time")
            if launch_time:
                task_start_times.append(launch_time)
            if finish_time:
                task_end_times.append(finish_time)

            if task_metrics:
                sr = task_metrics.get("Shuffle Read Metrics", {})
                shuffle_read_bytes += sr.get("Remote Bytes Read", 0)
                shuffle_read_bytes += sr.get("Local Bytes Read", 0)

                sw = task_metrics.get("Shuffle Write Metrics", {})
                shuffle_write_bytes += sw.get("Shuffle Bytes Written", 0)

                executor_run_time_ms += task_metrics.get("Executor Run Time", 0)
                executor_gc_time_ms  += task_metrics.get("JVM GC Time", 0)

                inp = task_metrics.get("Input Metrics", {})
                input_bytes_read += inp.get("Bytes Read", 0)

# ── derived metrics ────────────────────────────────────────────────────────────
def mb(b):
    return round(b / (1024 * 1024), 2)

def sec(ms):
    return round(ms / 1000, 2) if ms else None

shuffle_read_mb  = mb(shuffle_read_bytes)
shuffle_write_mb = mb(shuffle_write_bytes)
input_read_mb    = mb(input_bytes_read)

app_duration_ms = None
if app_start_time and app_end_time:
    app_duration_ms = app_end_time - app_start_time

driver_time_ms = None
task_span_ms   = None
if task_start_times and task_end_times and app_duration_ms is not None:
    task_span_ms   = max(task_end_times) - min(task_start_times)
    driver_time_ms = app_duration_ms - task_span_ms

executor_run_s = sec(executor_run_time_ms)
executor_gc_s  = sec(executor_gc_time_ms)
driver_time_s  = sec(driver_time_ms) if driver_time_ms else None
task_span_s    = sec(task_span_ms)   if task_span_ms   else None
app_duration_s = sec(app_duration_ms) if app_duration_ms else None

# ── print summary ──────────────────────────────────────────────────────────────
SEP = "─" * 60
print(f"\n{SEP}")
print(f"  Spark Event Log Parser — {run_label}")
print(f"  File : {file_path}")
print(f"  Lines: {line_count}   Parse errors: {parse_errors}")
print(SEP)

print(f"\n  APP TIMELINE")
print(f"    App duration (from log)   : {app_duration_s} s")
print(f"    Task execution span       : {task_span_s} s")
print(f"    Driver time (estimated)   : {driver_time_s} s")

print(f"\n  STAGES & TASKS")
print(f"    Stages submitted          : {stages_submitted}")
print(f"    Stages completed (OK)     : {stages_completed}")
print(f"    Stages failed             : {stages_failed}")
print(f"    Total tasks (from stages) : {total_tasks}")
print(f"    Tasks completed (OK)      : {completed_tasks}")
print(f"    Tasks failed              : {failed_tasks}")
print(f"    Executors registered      : {len(executors_registered)}  {sorted(executors_registered)}")

print(f"\n  DATA VOLUME")
print(f"    Input bytes read (CSV)    : {input_read_mb} MB")
print(f"    Shuffle write             : {shuffle_write_mb} MB")
print(f"    Shuffle read              : {shuffle_read_mb} MB")

print(f"\n  EXECUTOR CPU TIME")
print(f"    Total executor run time   : {executor_run_s} s  (sum across all tasks)")
print(f"    Total GC time             : {executor_gc_s} s")
if executor_run_time_ms and executor_gc_time_ms:
    gc_pct = round(executor_gc_time_ms / executor_run_time_ms * 100, 1)
    print(f"    GC overhead               : {gc_pct}%")

print(f"\n  TOP 10 STAGES BY TASK COUNT")
sorted_stages = sorted(stage_info.items(), key=lambda x: x[1]["num_tasks"], reverse=True)
for sid, s in sorted_stages[:10]:
    dur = ""
    if s["completion_time"] and s["submission_time"]:
        dur = f"  duration={round((s['completion_time']-s['submission_time'])/1000,1)}s"
    failed_marker = "  [FAILED]" if s["failure_reason"] else ""
    print(f"    Stage {sid:>3}: {s['num_tasks']:>4} tasks  {s['name'][:40]}{dur}{failed_marker}")

print(f"\n{SEP}\n")

# ── write CSV row ──────────────────────────────────────────────────────────────
csv_path = "metrics.csv"
fieldnames = [
    "run_label", "parsed_at",
    "app_duration_s", "task_span_s", "driver_time_s",
    "stages_submitted", "stages_completed", "stages_failed",
    "total_tasks", "completed_tasks", "failed_tasks",
    "executors_registered",
    "input_read_mb", "shuffle_write_mb", "shuffle_read_mb",
    "executor_run_s", "executor_gc_s",
    "parse_errors"
]

write_header = not os.path.exists(csv_path)
with open(csv_path, "a", newline="") as csvf:
    writer = csv.DictWriter(csvf, fieldnames=fieldnames)
    if write_header:
        writer.writeheader()
    writer.writerow({
        "run_label":            run_label,
        "parsed_at":            datetime.now().isoformat(timespec="seconds"),
        "app_duration_s":       app_duration_s,
        "task_span_s":          task_span_s,
        "driver_time_s":        driver_time_s,
        "stages_submitted":     stages_submitted,
        "stages_completed":     stages_completed,
        "stages_failed":        stages_failed,
        "total_tasks":          total_tasks,
        "completed_tasks":      completed_tasks,
        "failed_tasks":         failed_tasks,
        "executors_registered": len(executors_registered),
        "input_read_mb":        input_read_mb,
        "shuffle_write_mb":     shuffle_write_mb,
        "shuffle_read_mb":      shuffle_read_mb,
        "executor_run_s":       executor_run_s,
        "executor_gc_s":        executor_gc_s,
        "parse_errors":         parse_errors,
    })

print(f"  CSV row appended to: {csv_path}")
print(f"  Run label: {run_label}\n")