# ============================================================
# AWS GLUE PYSPARK ETL JOB (REVISED)
# Sales CSV cleaning and transformation
#
# Job parameters:
#   --JOB_NAME
#   --SOURCE_PATH   e.g. s3://bucket/raw/sales/
#   --TARGET_PATH   e.g. s3://bucket/processed/sales/
#
# Rejected rows go to TARGET_PATH + "_rejects/" with a reason.
# ============================================================

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


# =====================================
#            CONFIG
# =====================================


REQUIRED_COLUMNS = {"orderid", "orderdate", "quantity", "unitprice"}

DATE_FORMAT = "yyyy-MM-dd"

# True  -> discount column holds percentages (15 = 15%)
# False -> discount column holds fractions   (0.15 = 15%)
DISCOUNT_IS_PERCENT = True

# Dedup on all columns by default. For business-key dedup, set e.g.
# ["orderid", "product"]. Do NOT use only "orderid" if an order can
# have multiple line items.
DEDUP_KEYS = None

MONEY = DecimalType(18, 2)


# ==========================================
#         JOB PARAMETERS
# ========================================== 

args = getResolvedOptions(sys.argv, ["JOB_NAME", "SOURCE_PATH", "TARGET_PATH"])

JOB_NAME = args["JOB_NAME"]
SOURCE_PATH = args["SOURCE_PATH"]
TARGET_PATH = args["TARGET_PATH"]
REJECT_PATH = TARGET_PATH.rstrip("/") + "_rejects/"


# =====================================================
#          INITIALIZE SPARK + GLUE
# =====================================================

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

print("AWS GLUE SALES ETL JOB STARTED")
print("Source Path :", SOURCE_PATH)
print("Target Path :", TARGET_PATH)
print("Reject Path :", REJECT_PATH)


# ==================================
#           HELPERS
# ==================================

def to_decimal(col_name, precision=18, scale=2):
    """Strip currency symbols / thousand separators, then cast to decimal.
    '$1,200.50' -> 1200.50 ; '' -> NULL ; 'abc' -> NULL"""
    cleaned = F.regexp_replace(F.col(col_name), r"[^0-9.\-]", "")
    return (
        F.when(cleaned == "", F.lit(None))
        .otherwise(cleaned)
        .cast(DecimalType(precision, scale))
    )


print("Reading CSV from S3...")

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .option("multiLine", "true")
    .option("quote", '"')
    .option("escape", '"')
    .load(SOURCE_PATH)
)



df = df.toDF(
    *[c.strip().lower().replace(" ", "_").replace("-", "_") for c in df.columns]
)

print("Columns:", df.columns)

missing = REQUIRED_COLUMNS - set(df.columns)
if missing:
    raise ValueError(
        f"Missing required columns: {sorted(missing)}. Found: {df.columns}"
    )


df = df.select(
    *[
        F.when(F.trim(F.col(c)) == "", F.lit(None)).otherwise(F.trim(F.col(c))).alias(c)
        for c in df.columns
    ]
)

df = df.withColumn("raw_record", F.to_json(F.struct(*df.columns)))

# Cache: reused for counts, valid split and reject split.
df = df.cache()
raw_count = df.count()
print("Raw record count:", raw_count)


df = (
    df.withColumn("orderdate", F.to_date(F.col("orderdate"), DATE_FORMAT))
    .withColumn("quantity", to_decimal("quantity", 18, 0).cast("int"))
    .withColumn("unitprice", to_decimal("unitprice", 18, 2))
)

if "discount" in df.columns:
    discount = to_decimal("discount", 10, 4)
    if not DISCOUNT_IS_PERCENT:
        discount = discount * F.lit(100)
    # Clamp to 0-100 (%)
    discount = F.least(F.greatest(discount, F.lit(0)), F.lit(100))
    df = df.withColumn("discount", discount.cast(DecimalType(10, 4)))



if DEDUP_KEYS:
    df = df.dropDuplicates(DEDUP_KEYS)
else:
    df = df.dropDuplicates()


reject_reason = (
    F.when(F.col("orderid").isNull(), "missing_orderid")
    .when(F.col("orderdate").isNull(), "missing_or_invalid_orderdate")
    .when(F.col("quantity").isNull(), "missing_or_invalid_quantity")
    .when(F.col("unitprice").isNull(), "missing_or_invalid_unitprice")
    .when(F.col("quantity") <= 0, "non_positive_quantity")
    .when(F.col("unitprice") < 0, "negative_unitprice")
)

df = df.withColumn("reject_reason", reject_reason)

rejects = df.filter(F.col("reject_reason").isNotNull()).select(
    "reject_reason", "raw_record"
).withColumn("job_name", F.lit(JOB_NAME)).withColumn(
    "rejected_at", F.current_timestamp()
)

valid = df.filter(F.col("reject_reason").isNull()).drop("reject_reason", "raw_record")

fill_values = {
    "customername": "Unknown Customer",
    "product": "Unknown Product",
    "category": "Unknown Category",
    "status": "Unknown",
}
fill_values = {k: v for k, v in fill_values.items() if k in valid.columns}
if fill_values:
    valid = valid.fillna(fill_values)

if "discount" in valid.columns:
    valid = valid.fillna({"discount": 0})


valid = valid.withColumn(
    "total_amount", (F.col("quantity") * F.col("unitprice")).cast(MONEY)
)

if "discount" in valid.columns:
    valid = valid.withColumn(
        "discount_amount",
        F.round(F.col("total_amount") * F.col("discount") / F.lit(100), 2).cast(MONEY),
    ).withColumn(
        "net_amount",
        (F.col("total_amount") - F.col("discount_amount")).cast(MONEY),
    )
else:
    valid = valid.withColumn("net_amount", F.col("total_amount"))

valid = (
    valid.withColumn("order_year", F.year("orderdate"))
    .withColumn("order_month", F.month("orderdate"))
    .withColumn("order_month_name", F.date_format("orderdate", "MMMM"))
)

if "status" in valid.columns:
    valid = valid.withColumn(
        "is_cancelled",
        F.when(
            F.lower(F.trim(F.col("status"))).isin("cancelled", "canceled"), F.lit(1)
        ).otherwise(F.lit(0)),
    )

#  WRITE OUTPUT

valid = valid.cache()
valid_count = valid.count()
reject_count = raw_count - valid_count  # includes duplicates removed; see rejects for invalid rows

print("Final Schema:")
valid.printSchema()
valid.show(10, truncate=False)

print("Writing valid data to S3...")

writer = (
    valid.repartition("order_year", "order_month")
    .write.mode("overwrite")
    .option("partitionOverwriteMode", "dynamic")  # only replaces partitions being written
    .format("parquet")
    .partitionBy("order_year", "order_month")
)
writer.save(TARGET_PATH)

print("Writing rejected data to S3...")
rejects.write.mode("append").format("parquet").save(REJECT_PATH)


# METRICS (visible in CloudWatch logs)

print("======== JOB METRICS ========")

print("Raw records          :", raw_count)
print("Valid records written:", valid_count)
print("Dropped (dupes + rejected):", reject_count)


df.unpersist()
valid.unpersist()

job.commit()
print("AWS GLUE ETL JOB COMPLETED")