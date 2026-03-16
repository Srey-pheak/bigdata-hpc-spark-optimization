#!/usr/bin/env python
# coding: utf-8
import sys

import findspark
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark import StorageLevel
import argparse
import calendar
import os
from datetime import date, timedelta, datetime, time
from dateutil.relativedelta import relativedelta

findspark.init()

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--month", nargs='?', help="month")
    parser.add_argument("-y", "--year", nargs='?', help="year")
    parser.add_argument("-s", "--start", nargs='?', help="start day")
    parser.add_argument("-o", "--outfile", nargs='?', help="outfile")
    args = parser.parse_args()

    DATADIR = '/projects/F202500010HPCVLABUMINHO/DataSets/Reports/2025'
    OUTDIR  = '/projects/F202500010HPCVLABUMINHO/uminhocp150/big_data'

    params = {
        'reportPeriod': 0,
        'reportPeriodTrimester': 0,
        'reportPeriodYear': 0,
        'reportMonth': 0,
        'reportYear': 0,

        'armnodes': 1632,
        'amdnodes': 500,
        'gpunodes': 132,

        'percentaviail': 0.8,
        'eurohpcavail': 0.35,

        'ndays': 0,

        'armusedhours': 0,
        'amdusedhours': 0,
        'gpuusedhours': 0,

        'gpuusedhoursEuroHPC': 0,
        'amdusedhoursEuroHPC': 0,
        'armusedhoursEuroHPC': 0,

        'armJobs': 0,
        'amdJobs': 0,
        'gpuJobs': 0,

        'gpuCompletedJobs': 0,
        'gpuFailedJobs': 0,
        'armCompletedJobs': 0,
        'amdCompletedJobs': 0,
        'amdFailedJobs': 0,
        'armFailedJobs': 0,

        'gpuJobsEuroHPC': 0,
        'amdJobsEuroHPC': 0,
        'armJobsEuroHPC': 0,

        'ndaysTrimester': 0,

        'gpuCompletedJobsTrimester': 0,
        'gpuFailedJobsTrimester': 0,
        'armCompletedJobsTrimester': 0,
        'amdCompletedJobsTrimester': 0,
        'amdFailedJobsTrimester': 0,
        'armFailedJobsTrimester': 0,

        'armusedhoursTrimester': 0,
        'amdusedhoursTrimester': 0,
        'gpuusedhoursTrimester': 0,
        'armJobsTrimester': 0,
        'amdJobsTrimester': 0,
        'gpuJobsTrimester': 0,

        'gpuusedhoursEuroHPCTrimester': 0,
        'amdusedhoursEuroHPCTrimester': 0,
        'armusedhoursEuroHPCTrimester': 0,

        'gpuJobsEuroHPCTrimester': 0,
        'amdJobsEuroHPCTrimester': 0,
        'armJobsEuroHPCTrimester': 0,

        'ndaysYear': 0,

        'gpuCompletedJobsYear': 0,
        'gpuFailedJobsYear': 0,
        'armCompletedJobsYear': 0,
        'amdCompletedJobsYear': 0,
        'amdFailedJobsYear': 0,
        'armFailedJobsYear': 0,

        'armusedhoursYear': 0,
        'amdusedhoursYear': 0,
        'gpuusedhoursYear': 0,
        'armJobsYear': 0,
        'amdJobsYear': 0,
        'gpuJobsYear': 0,

        'gpuJobsEuroHPCYear': 0,
        'amdJobsEuroHPCYear': 0,
        'armJobsEuroHPCYear': 0,

        'gpuusedhoursEuroHPCYear': 0,
        'amdusedhoursEuroHPCYear': 0,
        'armusedhoursEuroHPCYear': 0,

        'monthhours': '{\inteval{\\ndays * 24}}',
        'hoursTrimester': '{\inteval{\\ndaysTrimester * 24}}',
        'hoursYear': '{\inteval{\\ndaysYear * 24}}'
    }

    list_of_Months = list(calendar.month_name)[1:]
    list_of_months_abr = list(calendar.month_abbr)[1:]

    today = datetime.now().date()
    year = today.year
    month_int = today.month - 2
    month = list_of_months_abr[month_int]
    syear = date(year, 1, 1)
    print(f"MONTH : {month} {month_int} \n {list_of_months_abr}")

    if args.month != None:
        month_int = list_of_months_abr.index(args.month)
        month = list_of_months_abr[month_int]
        print(f"MONTH2 : {month} {month_int} ")

    if args.year != None:
        year = int(args.year)
        syear = date(year, 1, 1)

    if args.start != None:
        syear = datetime.strptime(args.start, "%Y-%m-%d").date()

    params['reportMonth'] = list_of_Months[month_int]
    params['reportYear'] = year

    smonth = date(year, month_int+1, 1)
    emonthd = smonth + relativedelta(months=1) + relativedelta(days=-1)
    emonth = smonth + relativedelta(months=1)
    if month_int < 3:
        tmonth = emonth - relativedelta(months=month_int+1)
    else:
        tmonth = emonth - relativedelta(months=3)

    print(f"smonth {smonth} -- emonthd {emonthd} -- emonth {emonth} -- tmonth {tmonth}")

    params['reportPeriod'] = f"{smonth.strftime('%d/%m/%Y')} - {emonthd.strftime('%d/%m/%Y')}"
    if month_int < 3:
        params['reportPeriodTrimester'] = f"{syear.strftime('%d/%m/%Y')} - {emonthd.strftime('%d/%m/%Y')}"
    else:
        params['reportPeriodTrimester'] = f"{tmonth.strftime('%d/%m/%Y')} - {emonthd.strftime('%d/%m/%Y')}"

    params['reportPeriodYear'] = f"{syear.strftime('%d/%m/%Y')} - {emonthd.strftime('%d/%m/%Y')}"

    params['ndays'] = (emonth - smonth).days
    params['ndaysTrimester'] = (emonth - tmonth).days
    params['ndaysYear'] = (emonth - syear).days

    tag_month = {
        '': [month],
        'Trimester': None,
        'Year': list_of_months_abr[:month_int+1]
    }

    if month_int < 3:
        tag_month['Trimester'] = list_of_months_abr[:month_int+1]
    else:
        tag_month['Trimester'] = list_of_months_abr[month_int-2:month_int+1]

    outfilename = "params.tex"
    if args.outfile != None:
        outfilename = args.outfile
    wfile = open(f"{OUTDIR}/{outfilename}", "w+")

    print("FIND SPARK")
    print(findspark.find())

    sc = (SparkSession.builder
          .config("spark.eventLog.enabled", "true")
          .config("executor.memory", "4g")
          .config("num.executors", "4")
          .config("spark.eventLog.dir", f"file:///projects/F202500010HPCVLABUMINHO/uminhocp150/spark-events")
          .getOrCreate()
          )

    # ── LEITURA: todos os ficheiros jobs_* ────────────────────────────────
    nd = None
    for root, dirs, files in os.walk(DATADIR):
        for f in files:
            print(f)
            if f.startswith('jobs'):
                month = "_".join(f.split("_")[1:]).split(".")[0]
                print(f"Process: {month} {DATADIR}/{f}")
                # inferSchema=False evita o crash com valores como '1K'
                data = sc.read.option("delimiter", "|").csv(f'{DATADIR}/{f}', inferSchema=False, header=True)
                data = data \
                    .withColumn('EState', F.regexp_replace(F.col('State'), "CANCELLED(.*)", "CANCELLED")) \
                    .withColumn('COMPLETED', F.when(F.col('State') == 'COMPLETED', "COMPLETED").otherwise("FAILED"))
                data = data.withColumn('Period', F.lit(month))
                if nd == None:
                    nd = data
                else:
                    nd = nd.union(data)

    # ── CONVERSÃO SEGURA de colunas numéricas (try_cast ignora valores como '1K') ──
    nd = nd.withColumn("ElapsedRaw", F.expr("try_cast(ElapsedRaw as BIGINT)"))
    nd = nd.withColumn("NNodes",     F.expr("try_cast(NNodes as BIGINT)"))
    nd = nd.withColumn("AllocCPUS",  F.expr("try_cast(AllocCPUS as BIGINT)"))

    # ── TRANSFORMAÇÕES: adicionar colunas derivadas ───────────────────────
    nd = nd.withColumn("cluster",
        F.when(F.col('Partition').contains("arm"), "ARM")
         .otherwise(F.when(F.col('Partition').contains("a100"), "GPU")
         .otherwise("AMD"))
    )

    nd = nd.withColumn("Agency",
        F.when(F.col('Account').startswith("f"), "FCT")
         .otherwise(F.when(F.col('Account').startswith("ee"), "EHPC")
         .otherwise("LOCAL"))
    )

    nd = nd.withColumn("OldVNodes", F.when(
        F.col("Partition").contains("a100"),
            F.when(F.col('AllocCPUS') % 32 == 0,
                   F.cast(int, F.col('AllocCPUS') / 32))
             .otherwise(F.cast(int, F.col('AllocCPUS') / 32) + 1)
    ).otherwise(F.col("NNodes")))

    nd = nd.withColumn("VNodes", F.when(
        F.col("Partition").contains("a100"),
            F.when(F.col("AllocTRES").isNull(), F.col("NNodes"))
             .otherwise(
                F.when(F.col("AllocTRES").rlike(r"gres/gpu=(\d+)"),
                       F.regexp_extract(F.col("AllocTRES"), r"gres/gpu=(\d+)", 1))
                 .otherwise(F.col("NNodes") * 4)
             )
    ).otherwise(F.col("NNodes")))

    nd = nd.withColumn("totalJobSeconds",
                       F.col('ElapsedRaw') * F.col('VNodes'))

    # ── REPARTIÇÃO para equilibrar paralelismo ────────────────────────────
    num_cores = sc.sparkContext.defaultParallelism
    N = num_cores * 2
    print(f"[OTIM] Repartitioning to {N} partitions (cores={num_cores})")
    nd = nd.repartition(N)

    # ── OTIMIZAÇÃO 3: JOIN/SHUFFLE REDUCTION ─────────────────────────────
    # Construir tabela pequena que mapeia cada mês (Period) para o seu tag
    # Exemplo: 'Jan' -> 'Month', 'Jan' -> 'Trimester', 'Jan' -> 'Year'
    # Um mesmo mês pode pertencer a múltiplos tags, por isso pode aparecer
    # várias vezes na tabela.
    period_rows = []
    for tag, months in tag_month.items():
        tag_label = tag if tag != '' else 'Month'  # tag '' passa a ser 'Month'
        for m in months:
            period_rows.append((m, tag_label))

    # Criar DataFrame com as linhas Period -> tag (é muito pequeno, < 20 linhas)
    period_df = sc.createDataFrame(period_rows, ["Period", "tag"])

    # Fazer broadcast join: o Spark envia period_df para todos os executores
    # em vez de fazer shuffle do nd grande. Muito mais eficiente.
    nd_tagged = nd.join(F.broadcast(period_df), on="Period", how="inner")

    # Persistir nd_tagged (já com a coluna tag) para reutilizar nas 3 queries abaixo
    nd_tagged = nd_tagged.persist(StorageLevel.MEMORY_AND_DISK)

    # ── QUERY 1: Jobs concluídos e falhados por tag + cluster (1 único job Spark) ──
    # Antes: ~6 queries separadas (2 por iteração do loop)
    # Agora: 1 query só
    completed_rows = (
        nd_tagged
        .groupby("tag", "cluster", "COMPLETED")
        .count()
        .collect()
    )
    for row in completed_rows:
        r = row.asDict()
        tag = '' if r['tag'] == 'Month' else r['tag']
        if r['COMPLETED'] == 'COMPLETED':
            params[f"{r['cluster'].lower()}CompletedJobs{tag}"] = r['count']
        else:
            params[f"{r['cluster'].lower()}FailedJobs{tag}"] = r['count']

    # ── QUERY 2: Horas usadas e nº de jobs (excluindo LOCAL) por tag + cluster ──
    # Antes: ~6 queries separadas (loop por cluster dentro do loop por tag)
    # Agora: 1 query só com sum + count em simultâneo
    hours_jobs_rows = (
        nd_tagged
        .filter(F.col("Agency") != 'LOCAL')
        .groupby("tag", "cluster")
        .agg(
            F.sum("totalJobSeconds").alias("totalSecs"),
            F.count("*").alias("jobCount")
        )
        .collect()
    )
    for row in hours_jobs_rows:
        r = row.asDict()
        tag = '' if r['tag'] == 'Month' else r['tag']
        params[f"{r['cluster'].lower()}usedhours{tag}"] = r['totalSecs'] / 3600
        params[f"{r['cluster'].lower()}Jobs{tag}"] = r['jobCount']

    # ── QUERY 3: Jobs e horas EuroHPC por tag + cluster ──────────────────
    # Antes: ~6 queries separadas (2 por iteração do loop)
    # Agora: 1 query só com count + sum em simultâneo
    ehpc_rows = (
        nd_tagged
        .filter(F.col("Agency") == 'EHPC')
        .groupby("tag", "cluster")
        .agg(
            F.count("*").alias("jobCount"),
            F.sum("totalJobSeconds").alias("totalSecs")
        )
        .collect()
    )
    for row in ehpc_rows:
        r = row.asDict()
        tag = '' if r['tag'] == 'Month' else r['tag']
        params[f"{r['cluster'].lower()}JobsEuroHPC{tag}"] = r['jobCount']
        params[f"{r['cluster'].lower()}usedhoursEuroHPC{tag}"] = r['totalSecs'] / 3600

    # ── ESCRITA DO FICHEIRO params.tex ────────────────────────────────────
    for k, v in params.items():
        msg = f"\def\{k}{{{v}}}\n"
        wfile.write(msg)

    nd_tagged.unpersist()  # libertar cache Spark
    wfile.close()