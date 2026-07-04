"""
Embeddings & Cosine Similarity - Brute Force Vector Search

This script:
1. Generates 1 million random vectors
2. Stores them in a Delta table
3. Creates a query vector
4. Runs brute-force cosine similarity search
5. Captures Spark job and stage metrics
"""

from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
import math
import numpy as np
import time
import requests
import pandas as pd


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

NUM_VECTORS = 1_000_000
DIM = 64

VECTOR_TABLE_NAME = "vector_practice.embeddings_1m_64d"
JOB_GROUP_ID = "vector_cosine_1m_64d_run_001"
JOB_DESCRIPTION = "Vector cosine similarity benchmark"

spark.sql("CREATE DATABASE IF NOT EXISTS vector_practice")
spark.sql("USE vector_practice")

# ---------------------------------------------------------
# Generate 1 Million Random Vectors
# ---------------------------------------------------------

df = (
    spark.range(NUM_VECTORS)
    .withColumn(
        "embedding",
        F.array(*[
            F.rand(seed=i).cast("float")
            for i in range(DIM)
        ])
    )
)

df.printSchema()
df.show(5, truncate=False)


# ---------------------------------------------------------
# Save Vectors to Delta Table
# ---------------------------------------------------------

start = time.time()

(
    df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(VECTOR_TABLE_NAME)
)

end = time.time()

print(f"Vector table write time: {end - start:.2f} seconds")


# ---------------------------------------------------------
# Create Query Vector
# ---------------------------------------------------------

query_vector = np.random.rand(DIM).astype("float32").tolist()
query_norm = float(np.linalg.norm(query_vector))

broadcast_query = spark.sparkContext.broadcast(query_vector)
broadcast_query_norm = spark.sparkContext.broadcast(query_norm)


# ---------------------------------------------------------
# Define Cosine Similarity UDF
# ---------------------------------------------------------

def cosine_similarity(vec):
    q = broadcast_query.value
    q_norm = broadcast_query_norm.value

    dot = 0.0
    vec_norm_sq = 0.0

    for i in range(len(vec)):
        dot += float(vec[i]) * float(q[i])
        vec_norm_sq += float(vec[i]) * float(vec[i])

    vec_norm = math.sqrt(vec_norm_sq)

    if vec_norm == 0 or q_norm == 0:
        return 0.0

    return float(dot / (vec_norm * q_norm))


cosine_udf = F.udf(cosine_similarity, FloatType())


# ---------------------------------------------------------
# Run Brute Force Cosine Similarity Search
# ---------------------------------------------------------

spark.sparkContext.setJobGroup(
    JOB_GROUP_ID,
    JOB_DESCRIPTION
)

source_df = spark.table(VECTOR_TABLE_NAME)

start = time.time()

top10 = (
    source_df
    .withColumn("similarity", cosine_udf("embedding"))
    .select("id", "similarity")
    .orderBy(F.desc("similarity"))
    .limit(10)
    .collect()
)

display(top10)

end = time.time()

print(f"Brute-force search time: {end - start:.2f} seconds")


# ---------------------------------------------------------
# Spark REST API Setup
# ---------------------------------------------------------

app_id = spark.sparkContext.applicationId
ui_url = spark.sparkContext.uiWebUrl

print("Application ID:", app_id)
print("Spark UI URL:", ui_url)

base_url = f"{ui_url}/api/v1/applications/{app_id}"


def spark_api_get(path):
    url = f"{base_url}/{path}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


# ---------------------------------------------------------
# Collect Job Metrics for Benchmark Job Group
# ---------------------------------------------------------

tracker = spark.sparkContext.statusTracker()

job_ids = tracker.getJobIdsForGroup(JOB_GROUP_ID)

print("Benchmark Job IDs:", job_ids)

job_rows = []

for job_id in job_ids:
    job_detail = spark_api_get(f"jobs/{job_id}")
    stage_ids = job_detail.get("stageIds", [])

    job_rows.append({
        "job_id": job_id,
        "status": job_detail.get("status"),
        "num_stages": len(stage_ids),
        "num_tasks": job_detail.get("numTasks"),
        "num_completed_tasks": job_detail.get("numCompletedTasks"),
        "num_failed_tasks": job_detail.get("numFailedTasks")
    })

job_metrics_df = spark.createDataFrame(pd.DataFrame(job_rows))

job_metrics_df.show(truncate=False)


# ---------------------------------------------------------
# Collect Stage Metrics
# ---------------------------------------------------------

stage_rows = []

for job_id in job_ids:
    job_detail = spark_api_get(f"jobs/{job_id}")

    for stage_id in job_detail.get("stageIds", []):
        stage_detail = spark_api_get(f"stages/{stage_id}")

        attempts = stage_detail if isinstance(stage_detail, list) else [stage_detail]

        for stage in attempts:
            stage_rows.append({
                "job_id": job_id,
                "stage_id": stage.get("stageId"),
                "num_tasks": stage.get("numTasks"),
                "executor_run_time_ms": stage.get("executorRunTime"),
                "executor_cpu_time_sec": (
                    (stage.get("executorCpuTime") or 0) / 1_000_000_000
                ),
                "jvm_gc_time_ms": stage.get("jvmGCTime"),
                "input_bytes": stage.get("inputBytes"),
                "input_records": stage.get("inputRecords"),
                "shuffle_read_bytes": stage.get("shuffleReadBytes"),
                "shuffle_write_bytes": stage.get("shuffleWriteBytes"),
                "memory_bytes_spilled": stage.get("memoryBytesSpilled"),
                "disk_bytes_spilled": stage.get("diskBytesSpilled")
            })

stage_metrics_df = spark.createDataFrame(pd.DataFrame(stage_rows))

stage_metrics_df.show(truncate=False)