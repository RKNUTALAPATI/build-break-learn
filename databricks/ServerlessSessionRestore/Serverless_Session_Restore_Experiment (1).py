# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Serverless Job Session Restore Experiment
# MAGIC
# MAGIC Test Python variables, functions, Spark DataFrames,temporary views, and Spark configuration across a failed serverless job restore.
# MAGIC
# MAGIC After the intentional failure, use **Debug in new notebook** and run only
# MAGIC the validation cells below the failure cell. Do not rerun setup cells.

# COMMAND ----------

from datetime import datetime
import sys
from pyspark.sql import functions as F
from datetime import datetime

experiment_name = "serverless_session_restore"
experiment_started_at = datetime.now()

print("=" * 70)
print("SERVERLESS SESSION RESTORE EXPERIMENT")
print("=" * 70)
print(f"Experiment : {experiment_name}")
print(f"Started    : {experiment_started_at}")
print(f"Python     : {sys.version.split()[0]}")
print(f"Spark      : {spark.version}")

# COMMAND ----------

print(f"Variables Executed Time - {datetime.now()}")
threshold = 500
region = "US"
experiment_config = {
    "experiment": experiment_name,
    "threshold": threshold,
    "region": region,
    "purpose": "Test Databricks Serverless Job Session Restore",
}
test_regions = ["EAST", "WEST", "CENTRAL"]
print(experiment_config)
print(test_regions)

# COMMAND ----------

print(f"Function Test: Executed Time - {datetime.now()}")
def classify_order(amount):
    if amount >= 750:
        return "HIGH"
    elif amount >= 500:
        return "MEDIUM"
    return "STANDARD"

print("Function test:", classify_order(800))

# COMMAND ----------

print(f"DataFrame: Executed Time - {datetime.now()}")
TOTAL_ORDERS = 1_000_000
orders_df = (
    spark.range(0, TOTAL_ORDERS)
    .withColumn("customer_id", (F.col("id") % 10_000).cast("long"))
    .withColumn("sales_amount", F.round(F.rand(seed=42) * 1000, 2))
    .withColumn(
        "region",
        F.when(F.col("id") % 3 == 0, "EAST")
         .when(F.col("id") % 3 == 1, "WEST")
         .otherwise("CENTRAL"),
    )
)
total_order_count = orders_df.count()
print(f"orders_df created: {total_order_count:,} rows")

# COMMAND ----------

high_value_orders_df = orders_df.filter(F.col("sales_amount") > threshold)
high_value_count = high_value_orders_df.count()
print(f"Orders > ${threshold}: {high_value_count:,}")

# COMMAND ----------

region_summary_df = (
    high_value_orders_df.groupBy("region")
    .agg(
        F.count("*").alias("order_count"),
        F.round(F.sum("sales_amount"), 2).alias("total_sales"),
        F.round(F.avg("sales_amount"), 2).alias("avg_sales"),
    )
)
region_summary_df.show()

# COMMAND ----------

print(f"Temporary view: Executed Time - {datetime.now()}")
high_value_orders_df.createOrReplaceTempView("session_restore_high_value_orders")
temp_view_count = spark.sql("""
SELECT COUNT(*) AS cnt
FROM session_restore_high_value_orders
""").collect()[0]["cnt"]
print(f"Temporary view created: {temp_view_count:,} rows")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT region,
# MAGIC        COUNT(*) AS high_value_orders,
# MAGIC        ROUND(SUM(sales_amount), 2) AS total_sales,
# MAGIC        ROUND(AVG(sales_amount), 2) AS avg_sales
# MAGIC FROM session_restore_high_value_orders
# MAGIC GROUP BY region
# MAGIC ORDER BY region;

# COMMAND ----------

print(f"UDF: Executed Time - {datetime.now()}")
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

@udf(StringType())
def order_category_udf(amount):
    if amount is None:
        return "UNKNOWN"
    elif amount >= 750:
        return "PREMIUM"
    elif amount >= 500:
        return "HIGH"
    return "STANDARD"

udf_test_df = (
    orders_df
    .select("id", "sales_amount")
    .withColumn("order_category", order_category_udf(col("sales_amount")))
)

udf_test_df.show(5)

# COMMAND ----------

print(f"Spark SQL UDF: Executed Time - {datetime.now()}")
spark.udf.register(
    "session_restore_category",
    lambda x: "HIGH" if x is not None and x >= 500 else "STANDARD",
    StringType()
)

spark.sql("""
SELECT session_restore_category(800) AS category
""").show()

# COMMAND ----------

print(f"Class: Executed Time - {datetime.now()}")
class OrderRule:
    def __init__(self, threshold):
        self.threshold = threshold

    def classify(self, amount):
        return "HIGH" if amount >= self.threshold else "STANDARD"

order_rule = OrderRule(500)

print(order_rule.threshold)
print(order_rule.classify(800))

# COMMAND ----------

print(f"Broad Cast: Executed Time - {datetime.now()}")
from pyspark.sql import functions as F

# Small lookup DataFrame
region_lookup_df = spark.createDataFrame(
    [
        ("EAST", "Eastern Region"),
        ("WEST", "Western Region"),
        ("CENTRAL", "Central Region")
    ],
    ["region", "region_name"]
)

broadcast_join_df = (
    orders_df
    .join(
        F.broadcast(region_lookup_df),
        on="region",
        how="left"
    )
)

print("Small lookup:")
region_lookup_df.show()

print("Join result:")
broadcast_join_df.show(5)

print("Physical plan:")
broadcast_join_df.explain("formatted")

# COMMAND ----------

# DBTITLE 1,materialize
broadcast_join_count = broadcast_join_df.count()

print("Broadcast join count:", f"{broadcast_join_count:,}")

# COMMAND ----------

experiment_results = {
    "experiment": experiment_name,
    "total_orders": total_order_count,
    "high_value_orders": high_value_count,
    "threshold": threshold,
    "temp_view_count": temp_view_count,
    "broadcast_join_count": broadcast_join_count,
    "status": "READY_FOR_INTENTIONAL_FAILURE",
}
print("=" * 70)
print("STATE CREATED BEFORE FAILURE")
print("=" * 70)
for key, value in experiment_results.items():
    print(f"{key:25} : {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## STOP - Intentional failure below
# MAGIC After this job fails, open the failed run and choose **Debug in new notebook**.
# MAGIC Wait for the state to restore, then skip directly to the validation cells.

# COMMAND ----------

print("TRIGGERING INTENTIONAL FAILURE")
raise RuntimeError("INTENTIONAL_FAILURE_FOR_SESSION_RESTORE_EXPERIMENT")

# COMMAND ----------

# MAGIC %md
# MAGIC # Session Restore Validation
# MAGIC Run these cells only in the notebook created by **Debug in new notebook**.
# MAGIC Do not rerun any setup cells above.

# COMMAND ----------

print(f"TEST 1 - PYTHON VARIABLES - {datetime.now()}")
print("experiment_name:", experiment_name)
print("threshold:", threshold)
print("region:", region)
print("experiment_config:", experiment_config)
print("test_regions:", test_regions)
print("experiment_results:", experiment_results)

# COMMAND ----------

print(f"TEST 2 - PYTHON FUNCTION - {datetime.now()}")
print("classify_order(800) =", classify_order(800))
print("classify_order(600) =", classify_order(600))
print("classify_order(100) =", classify_order(100))

# COMMAND ----------

print(f"TEST 3 - ORIGINAL SPARK DATAFRAME - {datetime.now()}")
restored_total_count = orders_df.count()
print(f"Restored orders_df count: {restored_total_count:,}")
orders_df.show(5)

# COMMAND ----------

print(f"TEST 4 - TRANSFORMED DATAFRAME - {datetime.now()}")
restored_high_value_count = high_value_orders_df.count()
print(f"Restored high_value_orders_df: {restored_high_value_count:,}")
high_value_orders_df.show(5)

# COMMAND ----------

print(f"TEST 5 - TEMPORARY VIEW - {datetime.now()}")
spark.sql("""
SELECT region,
       COUNT(*) AS high_value_orders,
       ROUND(SUM(sales_amount), 2) AS total_sales
FROM session_restore_high_value_orders
GROUP BY region
ORDER BY region
""").show()

# COMMAND ----------

# print("=" * 70)
# print("SESSION RESTORE CONSISTENCY CHECK")
# print("=" * 70)
# checks = {
#     "Python scalar": threshold == 500,
#     "Python dictionary": experiment_config["threshold"] == 500,
#     "Python function": classify_order(800) == "HIGH",
#     "Original DataFrame": restored_total_count == total_order_count,
#     "Transformed DataFrame": restored_high_value_count == high_value_count,
#     "Temporary view": spark.sql("SELECT COUNT(*) FROM session_restore_high_value_orders").collect()[0][0] == temp_view_count 
# }
# for test, result in checks.items():
#     print(f"{test:25} : {'PASS' if result else 'FAIL'}")

# COMMAND ----------

print("UDF object exists:", "order_category_udf" in globals())

udf_test_df.show(5)

print(
    "Premium orders:",
    udf_test_df.filter("order_category = 'PREMIUM'").count()
)

# COMMAND ----------

spark.sql("""
SELECT session_restore_category(800) AS category
""").show()

# COMMAND ----------

print("Object exists:", "order_rule" in globals())
print("Threshold:", order_rule.threshold)
print("Classification:", order_rule.classify(800))
print("Class:", type(order_rule))

# COMMAND ----------

print("=" * 70)
print("EDGE TEST - BROADCAST JOIN DATAFRAME")
print("=" * 70)

print(
    "region_lookup_df exists:",
    "region_lookup_df" in globals()
)

print(
    "broadcast_join_df exists:",
    "broadcast_join_df" in globals()
)

restored_broadcast_join_count = broadcast_join_df.count()

print(
    "Restored broadcast join count:",
    f"{restored_broadcast_join_count:,}"
)

broadcast_join_df.show(5)

print("Restored physical plan:")
broadcast_join_df.explain("formatted")

# COMMAND ----------

print("=" * 78)
print("SESSION RESTORE - TEST RESULTS")
print("=" * 78)

def result(name, passed, details=""):
    status = "PASS" if passed else "FAIL"
    print(f"{name:<32} {status:<6} {details}")

# Core tests
result("Python variables",
       threshold == 500 and region == "US")

result("Python dictionary",
       experiment_config["threshold"] == 500)

result("Python function",
       classify_order(800) == "HIGH")

result("Original DataFrame",
       orders_df.count() == 1_000_000,
       f"{orders_df.count():,} rows")

result("Transformed DataFrame",
       high_value_orders_df.count() == 499_326,
       f"{high_value_orders_df.count():,} rows")

# Temporary view
temp_count = spark.sql("""
    SELECT COUNT(*) AS cnt
    FROM session_restore_high_value_orders
""").collect()[0]["cnt"]

result("Temporary view",
       temp_count == 499_326,
       f"{temp_count:,} rows")


# Python UDF
premium_count = (
    udf_test_df
    .filter("order_category = 'PREMIUM'")
    .count()
)

result("Python UDF",
       premium_count == 249_458,
       f"{premium_count:,} PREMIUM")


# SQL temporary UDF
sql_udf_result = spark.sql("""
    SELECT session_restore_category(800) AS category
""").collect()[0]["category"]

result("SQL temporary UDF",
       sql_udf_result == "HIGH",
       sql_udf_result)


# Custom Python object
custom_object_ok = (
    order_rule.threshold == 500
    and order_rule.classify(800) == "HIGH"
)

result("Custom Python object",
       custom_object_ok)


# Broadcast join
broadcast_count = broadcast_join_df.count()

result("Broadcast-join DataFrame",
       broadcast_count == 1_000_000,
       f"{broadcast_count:,} rows")

print("=" * 78)
print("RESTORE VALIDATION COMPLETE")
print("=" * 78)

# COMMAND ----------

restore_test_completed_at = datetime.now()
print("=" * 70)
print("EXPERIMENT COMPLETE")
print("=" * 70)
print("Original experiment started:", experiment_started_at)
print("Restore validation completed:", restore_test_completed_at)
print("No setup cells were rerun before the restore validation.")

# COMMAND ----------

