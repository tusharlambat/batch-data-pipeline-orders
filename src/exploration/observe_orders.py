from src.utils.spark_session import create_spark_session
from pyspark.sql.functions import col, sum

# ==================================================
# 1. Create Spark Session
# ==================================================
spark = create_spark_session()

print("\n======================================")
print("DATA EXPLORATION STARTED")
print("======================================\n")


# ==================================================
# 2. Load Dataset
# ==================================================
df = spark.read.csv(
    "data/raw/orders.csv",
    header=True,
    inferSchema=True
)


# ==================================================
# 3. Dataset Size (Data Volume)
# ==================================================
print("1️⃣ Dataset Size")

row_count = df.count()
column_count = len(df.columns)

print("Total Rows:", row_count)
print("Total Columns:", column_count)


# ==================================================
# 4. Sample Data Inspection
# ==================================================
print("\n2️⃣ Sample Data (First 10 rows)")

df.show(10, truncate=False)


# ==================================================
# 5. Schema Inspection
# ==================================================
print("\n3️⃣ Schema Information")

df.printSchema()


# ==================================================
# 6. Column Names
# ==================================================
print("\n4️⃣ Column Names")

for column in df.columns:
    print(column)


# ==================================================
# 7. Missing Values Check
# ==================================================
print("\n5️⃣ Missing Values Check")

df.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in df.columns
]).show()


# ==================================================
# 8. Duplicate Records Check
# ==================================================
print("\n6️⃣ Duplicate Records Check")

unique_rows = df.dropDuplicates().count()

print("Total Rows:", row_count)
print("Unique Rows:", unique_rows)

if row_count == unique_rows:
    print("No duplicate records found.")
else:
    print("Duplicate records exist.")


# ==================================================
# 9. ID Uniqueness Check
# ==================================================
print("\n7️⃣ Order ID Uniqueness Check")

unique_order_ids = df.select("Order Id").distinct().count()

print("Total Rows:", row_count)
print("Unique Order IDs:", unique_order_ids)

if row_count == unique_order_ids:
    print("Order Id column is unique.")
else:
    print("Duplicate Order IDs exist.")


# ==================================================
# 10. Data Distribution
# ==================================================
print("\n8️⃣ Data Distribution")

df.describe().show()


# ==================================================
# 11. Outlier Detection
# ==================================================
print("\n9️⃣ Outlier Detection")

print("\nTop 5 Highest List Prices")
df.orderBy(col("List Price").desc()).show(5)

print("\nLowest Cost Prices")
df.orderBy(col("cost price").asc()).show(5)


# ==================================================
# 12. Logical Data Validation
# ==================================================
print("\n🔟 Logical Data Checks")

print("\nChecking negative cost price")
df.filter(col("cost price") < 0).show()

print("\nChecking negative list price")
df.filter(col("List Price") < 0).show()

print("\nChecking negative quantity")
df.filter(col("Quantity") < 0).show()

print("\nChecking invalid discount percent (>100)")
df.filter(col("Discount Percent") > 100).show()

print("\nChecking invalid discount percent (<0)")
df.filter(col("Discount Percent") < 0).show()

print("\nChecking invalid Order Id")
df.filter(col("Order Id") <= 0).show()


print("\n======================================")
print("DATA EXPLORATION COMPLETED")
print("======================================\n")