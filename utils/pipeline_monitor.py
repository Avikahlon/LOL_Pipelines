from datetime import datetime

def log_pipeline_run(spark, pipeline_name, status, records_processed=None, error=None):
    spark.sql("CREATE SCHEMA IF NOT EXISTS lol_monitoring")
    log = [{
        "pipeline_name": str(pipeline_name),
        "run_timestamp": datetime.utcnow().isoformat(),
        "status": str(status),
        "records_processed": str(records_processed) if records_processed is not None else "",
        "error": str(error) if error is not None else ""
    }]

    df = spark.createDataFrame(log)
    df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable("lol_monitoring.pipeline_runs")

    print(f"[{status}] {pipeline_name} — {records_processed} records processed")