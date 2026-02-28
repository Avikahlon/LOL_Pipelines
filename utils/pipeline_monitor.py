from datetime import datetime

def log_pipeline_run(spark, pipeline_name, status, records_processed=None, error=None):
    log = [{
        "pipeline_name": pipeline_name,
        "run_timestamp": datetime.utcnow().isoformat(),
        "status": status,
        "records_processed": records_processed,
        "error": str(error) if error else None
    }]

    df = spark.createDataFrame(log)
    df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable("lol_monitoring.pipeline_runs")

    print(f"[{status}] {pipeline_name} — {records_processed} records processed")