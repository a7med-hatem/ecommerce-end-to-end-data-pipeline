df.select([F.count(F.when(F.col(c).isNull(), 1)).alias(c) for c in df.columns]).show()

df.printSchema()

total_rows = df.count()
distinct_rows = df.dropDuplicates().count()
print(f"Duplicates count: {total_rows - distinct_rows}")

df.filter((df.Quantity <= 0) | (df.UnitPrice <= 0)).show()

df.filter((F.trim(F.col("description")) == "") | (F.col("description").isNull())).show()

df.filter(F.trim(F.col("description")) == "").show()

df.filter(F.length(F.col("description")) > F.length(F.trim(F.col("description")))).show()
df.filter(F.length(F.col("description")) > F.length(F.trim(F.col("description")))).count()

df.filter(~F.col("description").rlike("^[A-Z0-9]")).show(truncate = False)