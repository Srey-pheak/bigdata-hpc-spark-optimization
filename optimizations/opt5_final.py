#!/usr/bin/env python
# coding: utf-8
"""
Optimisation 5 (v2) — All Optimisations Combined
=================================================
CHANGES FROM BASELINE:

    1. CACHING (Opt 1)
       nd_tagged.persist(MEMORY_AND_DISK) after broadcast join.
       Materialises the tagged DataFrame once — all aggregations reuse it.

    2. REPARTITIONING (Opt 2)
       nd.repartition(N) where N = defaultParallelism * 2.
       Dynamically uses all available cores (48 on the ARM node = 96 partitions).
       Balances data across cores before the expensive aggregation step.

    3. BROADCAST JOIN (Opt 3 — Shuffle Reduction)
       Small Period→tag mapping table (<20 rows) broadcast to all partitions.
       Avoids shuffling the large nd DataFrame during the join.
       Replaces the iterative Python loop over tag_month with a single join.

    4. WILDCARD READ + COLUMN PRUNING (Opt 4 — Query Simplification)
       Single sc.read.csv(wildcard) replaces the os.walk loop + iterative union.
       Period derived from input_file_name() — 1 FileScan node instead of 12.
       Unused columns dropped early with select() — smaller cache footprint.
       try_cast used for numeric columns — handles malformed values gracefully.
       OldVNodes removed — column was computed but never used downstream.
       spark.sql.shuffle.partitions=8 — avoids AQE over-coalescing on small shuffles.

    5. SINGLE collect() (Opt 5 — Output Path Efficiency)
       All metrics computed in one groupBy.agg() with conditional aggregations.
       Replaces 24 separate Spark actions with 1 job + 1 collect().
       params.tex written in a single writelines() call.

Usage:
    python opt5_v2_final.py -m Jan

Configuration (optional — falls back to cluster defaults):
    export HPC_DATADIR=/path/to/csv/files
    export HPC_OUTDIR=/path/to/output/folder
    export HPC_EVTDIR=/path/to/spark-events/Opt5_1_log
"""

import os
import findspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark import StorageLevel
import argparse
import calendar
from datetime import date, datetime
from dateutil.relativedelta import relativedelta

# ── CONFIGURATION ──────────────────────────────────────────────────────────────
DATADIR = os.environ.get('HPC_DATADIR', '/projects/F202500010HPCVLABUMINHO/DataSets/Reports/2025')
OUTDIR  = os.environ.get('HPC_OUTDIR',  '/projects/F202500010HPCVLABUMINHO/uminhocp150/big_data')
EVTDIR  = os.environ.get('HPC_EVTDIR',  '/projects/F202500010HPCVLABUMINHO/uminhocp150/spark-events')
# ───────────────────────────────────────────────────────────────────────────────

findspark.init()

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--month",   nargs='?', help="month abbreviation e.g. Jan")
    parser.add_argument("-y", "--year",    nargs='?', help="year e.g. 2025")
    parser.add_argument("-s", "--start",   nargs='?', help="start day YYYY-MM-DD")
    parser.add_argument("-o", "--outfile", nargs='?', help="output filename (default: params.tex)")
    args = parser.parse_args()

    params = {
        'reportPeriod': 0, 'reportPeriodTrimester': 0, 'reportPeriodYear': 0,
        'reportMonth': 0, 'reportYear': 0,
        'armnodes': 1632, 'amdnodes': 500, 'gpunodes': 132,
        'percentaviail': 0.8, 'eurohpcavail': 0.35, 'ndays': 0,
        'armusedhours': 0, 'amdusedhours': 0, 'gpuusedhours': 0,
        'gpuusedhoursEuroHPC': 0, 'amdusedhoursEuroHPC': 0, 'armusedhoursEuroHPC': 0,
        'armJobs': 0, 'amdJobs': 0, 'gpuJobs': 0,
        'gpuCompletedJobs': 0, 'gpuFailedJobs': 0,
        'armCompletedJobs': 0, 'amdCompletedJobs': 0,
        'amdFailedJobs': 0, 'armFailedJobs': 0,
        'gpuJobsEuroHPC': 0, 'amdJobsEuroHPC': 0, 'armJobsEuroHPC': 0,
        'ndaysTrimester': 0,
        'gpuCompletedJobsTrimester': 0, 'gpuFailedJobsTrimester': 0,
        'armCompletedJobsTrimester': 0, 'amdCompletedJobsTrimester': 0,
        'amdFailedJobsTrimester': 0, 'armFailedJobsTrimester': 0,
        'armusedhoursTrimester': 0, 'amdusedhoursTrimester': 0, 'gpuusedhoursTrimester': 0,
        'armJobsTrimester': 0, 'amdJobsTrimester': 0, 'gpuJobsTrimester': 0,
        'gpuusedhoursEuroHPCTrimester': 0, 'amdusedhoursEuroHPCTrimester': 0,
        'armusedhoursEuroHPCTrimester': 0,
        'gpuJobsEuroHPCTrimester': 0, 'amdJobsEuroHPCTrimester': 0, 'armJobsEuroHPCTrimester': 0,
        'ndaysYear': 0,
        'gpuCompletedJobsYear': 0, 'gpuFailedJobsYear': 0,
        'armCompletedJobsYear': 0, 'amdCompletedJobsYear': 0,
        'amdFailedJobsYear': 0, 'armFailedJobsYear': 0,
        'armusedhoursYear': 0, 'amdusedhoursYear': 0, 'gpuusedhoursYear': 0,
        'armJobsYear': 0, 'amdJobsYear': 0, 'gpuJobsYear': 0,
        'gpuJobsEuroHPCYear': 0, 'amdJobsEuroHPCYear': 0, 'armJobsEuroHPCYear': 0,
        'gpuusedhoursEuroHPCYear': 0, 'amdusedhoursEuroHPCYear': 0, 'armusedhoursEuroHPCYear': 0,
        'monthhours': '{\inteval{\\ndays * 24}}',
        'hoursTrimester': '{\inteval{\\ndaysTrimester * 24}}',
        'hoursYear': '{\inteval{\\ndaysYear * 24}}'
    }

    list_of_Months     = list(calendar.month_name)[1:]
    list_of_months_abr = list(calendar.month_abbr)[1:]

    today     = datetime.now().date()
    year      = today.year
    month_int = today.month - 2
    month     = list_of_months_abr[month_int]
    syear     = date(year, 1, 1)

    if args.month is not None:
        month_int = list_of_months_abr.index(args.month)
        month     = list_of_months_abr[month_int]
    if args.year is not None:
        year  = int(args.year)
        syear = date(year, 1, 1)
    if args.start is not None:
        syear = datetime.strptime(args.start, "%Y-%m-%d").date()

    params['reportMonth'] = list_of_Months[month_int]
    params['reportYear']  = year

    smonth  = date(year, month_int + 1, 1)
    emonthd = smonth + relativedelta(months=1) + relativedelta(days=-1)
    emonth  = smonth + relativedelta(months=1)
    tmonth  = emonth - relativedelta(months=month_int + 1) if month_int < 3 \
              else emonth - relativedelta(months=3)

    params['reportPeriod'] = f"{smonth.strftime('%d/%m/%Y')} - {emonthd.strftime('%d/%m/%Y')}"
    params['reportPeriodTrimester'] = (
        f"{syear.strftime('%d/%m/%Y')} - {emonthd.strftime('%d/%m/%Y')}" if month_int < 3
        else f"{tmonth.strftime('%d/%m/%Y')} - {emonthd.strftime('%d/%m/%Y')}"
    )
    params['reportPeriodYear'] = f"{syear.strftime('%d/%m/%Y')} - {emonthd.strftime('%d/%m/%Y')}"
    params['ndays']          = (emonth - smonth).days
    params['ndaysTrimester'] = (emonth - tmonth).days
    params['ndaysYear']      = (emonth - syear).days

    tag_month = {
        '':          [month],
        'Trimester': list_of_months_abr[:month_int + 1] if month_int < 3
                     else list_of_months_abr[month_int - 2:month_int + 1],
        'Year':      list_of_months_abr[:month_int + 1]
    }

    outfilename = args.outfile if args.outfile else "params.tex"

    # ── OPT 4: Set shuffle partitions — avoids AQE over-coalescing ───────────
    sc = (SparkSession.builder
          .config("spark.eventLog.enabled", "true")
          .config("spark.executor.memory", "4g")
          .config("spark.executor.instances", "4")
          .config("spark.sql.shuffle.partitions", "8")
          .config("spark.eventLog.dir", f"file://{EVTDIR}")
          .getOrCreate())

    # ── OPT 4: Wildcard read — single FileScan replaces 12 separate reads ────
    nd = sc.read.option("delimiter", "|").csv(
        f'{DATADIR}/jobs_*.txt', inferSchema=False, header=True)

    # ── OPT 4: Period derived from filename — no Python loop needed ───────────
    nd = nd.withColumn('EState',
            F.regexp_replace(F.col('State'), "CANCELLED(.*)", "CANCELLED")) \
           .withColumn('COMPLETED',
            F.when(F.col('State') == 'COMPLETED', "COMPLETED").otherwise("FAILED")) \
           .withColumn('Period',
            F.regexp_extract(F.input_file_name(), r'jobs_(\w+)\.txt', 1))

    # ── OPT 4: Safe numeric cast — handles malformed values (e.g. '1K') ──────
    nd = nd.withColumn("ElapsedRaw", F.expr("try_cast(ElapsedRaw as BIGINT)")) \
           .withColumn("NNodes",     F.expr("try_cast(NNodes as BIGINT)")) \
           .withColumn("AllocCPUS",  F.expr("try_cast(AllocCPUS as BIGINT)"))

    # ── OPT 4: Column pruning — drop unused columns early ────────────────────
    # Smaller DataFrame = more compact cache + less data shuffled
    nd = nd.select("State", "EState", "COMPLETED", "Period", "Account",
                   "Partition", "ElapsedRaw", "NNodes", "AllocCPUS", "AllocTRES")

    # ── Derived columns ───────────────────────────────────────────────────────
    nd = nd.withColumn("cluster",
        F.when(F.col('Partition').contains("arm"), "ARM")
         .otherwise(F.when(F.col('Partition').contains("a100"), "GPU").otherwise("AMD"))) \
       .withColumn("Agency",
        F.when(F.col('Account').startswith("f"), "FCT")
         .otherwise(F.when(F.col('Account').startswith("ee"), "EHPC").otherwise("LOCAL")))

    # OldVNodes removed — was computed in baseline but never used downstream

    # VNodes: for GPU partitions, use gres/gpu count from AllocTRES if available
    nd = nd.withColumn("VNodes",
        F.when(F.col("Partition").contains("a100"),
            F.when(F.col("AllocTRES").isNull(), F.col("NNodes"))
             .otherwise(
                F.when(F.col("AllocTRES").rlike(r"gres/gpu=(\d+)"),
                       F.regexp_extract(F.col("AllocTRES"), r"gres/gpu=(\d+)", 1))
                 .otherwise(F.col("NNodes") * 4))
        ).otherwise(F.col("NNodes"))) \
       .withColumn("totalJobSeconds", F.col('ElapsedRaw') * F.col('VNodes'))

    # ── OPT 2: Repartition — distribute evenly across all available cores ─────
    # N = defaultParallelism * 2 ensures all cores are active (96 on ARM node)
    num_cores = sc.sparkContext.defaultParallelism
    N = num_cores * 2
    nd = nd.repartition(N)

    # ── OPT 3: Broadcast join — avoid shuffling the large DataFrame ───────────
    # Build a small Period→tag mapping table (<20 rows) and broadcast it.
    # The large nd DataFrame stays in place; only the tiny table is sent to workers.
    period_rows = []
    for tag, months in tag_month.items():
        tag_label = tag if tag != '' else 'Month'
        for m in months:
            period_rows.append((m, tag_label))

    period_df = sc.createDataFrame(period_rows, ["Period", "tag"])
    nd_tagged = nd.join(F.broadcast(period_df), on="Period", how="inner")

    # ── OPT 1: Persist — materialise once, reuse across all aggregations ──────
    # MEMORY_AND_DISK spills to disk if memory is insufficient
    nd_tagged = nd_tagged.persist(StorageLevel.MEMORY_AND_DISK)

    print("===== EXECUTION PLAN: FILTER + GROUPBY =====")
    nd_tagged.filter(F.col("Agency") != "LOCAL").groupBy("cluster").count().explain("extended")

    # ── OPT 5: Single collect() — all metrics in one groupBy.agg() ───────────
    # Baseline had 24 separate Spark actions (collect() calls).
    # This computes completed/failed jobs, hours, job counts, and EuroHPC
    # metrics for all tags and clusters in a single Spark job.
    all_agg_rows = (
        nd_tagged
        .groupby("tag", "cluster")
        .agg(
            # completed and failed job counts per cluster
            F.count(F.when(F.col("COMPLETED") == "COMPLETED", 1)).alias("completedJobs"),
            F.count(F.when(F.col("COMPLETED") == "FAILED",    1)).alias("failedJobs"),

            # total hours and job count excluding LOCAL agency
            F.sum(F.when(F.col("Agency") != "LOCAL", F.col("totalJobSeconds"))).alias("totalSecs"),
            F.count(F.when(F.col("Agency") != "LOCAL", 1)).alias("jobCount"),

            # EuroHPC hours and job count
            F.sum(F.when(F.col("Agency") == "EHPC", F.col("totalJobSeconds"))).alias("ehpcSecs"),
            F.count(F.when(F.col("Agency") == "EHPC", 1)).alias("ehpcJobCount"),
        )
        .collect()  # single collect() for all metrics across all tags
    )

    # Populate params dictionary from aggregation results
    for row in all_agg_rows:
        r   = row.asDict()
        tag = '' if r['tag'] == 'Month' else r['tag']
        cl  = r['cluster'].lower()

        params[f"{cl}CompletedJobs{tag}"]    = r['completedJobs']
        params[f"{cl}FailedJobs{tag}"]       = r['failedJobs']
        params[f"{cl}usedhours{tag}"]        = (r['totalSecs'] or 0) / 3600
        params[f"{cl}Jobs{tag}"]             = r['jobCount']
        params[f"{cl}JobsEuroHPC{tag}"]      = r['ehpcJobCount']
        params[f"{cl}usedhoursEuroHPC{tag}"] = (r['ehpcSecs'] or 0) / 3600

    # ── OPT 1: Release persisted cache after all aggregations ─────────────────
    nd_tagged.unpersist()

    # ── OPT 5: Write params.tex in a single batch I/O call ───────────────────
    lines = [f"\\def\\{k}{{{v}}}\n" for k, v in params.items()]
    with open(f"{OUTDIR}/{outfilename}", "w") as wfile:
        wfile.writelines(lines)