from pyspark.sql.functions import col, explode_outer, concat_ws, array_join, first

# Assuming 'df' is your initial DataFrame loaded from AWS Glue's DynamicFrame

# Explode the 'packaging' array to create multiple rows
df_exploded = df.withColumn("packaging", explode_outer("packaging"))

# Selecting fields and flattening the structure
flattened_df = df_exploded.select(
    col("product_id"),
    col("product_ndc"),
    col("product_type"),
    col("application_number"),
    col("brand_name"),
    col("brand_name_base"),
    col("brand_name_suffix"),
    col("generic_name"),
    col("dosage_form"),
    col("labeler_name"),
    col("marketing_category"),
    col("dea_schedule"),
    col("finished"),
    col("marketing_start_date"),
    col("marketing_end_date"),
    col("listing_expiration_date"),
    array_join(col("pharm_class"), ", ").alias("pharm_class"),
    array_join(col("route"), ", ").alias("route"),
    col("spl_id"),
    # Packaging details
    col("packaging.description").alias("packaging_description"),
    col("packaging.marketing_end_date").alias("packaging_marketing_end_date"),
    col("packaging.marketing_start_date").alias("packaging_marketing_start_date"),
    col("packaging.package_ndc").alias("package_ndc"),
    col("packaging.sample").alias("sample"),
    # Active ingredients details
    concat_ws(", ", col("active_ingredients.name")).alias("active_ingredient_name"),
    concat_ws(", ", col("active_ingredients.strength")).alias("active_ingredient_strength"),
    # OpenFDA details
    array_join(col("openfda.manufacturer_name"), ", ").alias("manufacturer_name"),
    array_join(col("openfda.rxcui"), ", ").alias("rxcui"),
    array_join(col("openfda.unii"), ", ").alias("unii"),
    array_join(col("openfda.spl_set_id"), ", ").alias("spl_set_id"),
    array_join(col("openfda.pharm_class_cs"), ", ").alias("pharm_class_cs"),
    array_join(col("openfda.pharm_class_epc"), ", ").alias("pharm_class_epc"),
    array_join(col("openfda.pharm_class_moa"), ", ").alias("pharm_class_moa"),
    array_join(col("openfda.pharm_class_pe"), ", ").alias("pharm_class_pe"),
    array_join(col("openfda.upc"), ", ").alias("upc"),
    array_join(col("openfda.nui"), ", ").alias("nui")
)

# Show the flattened schema to verify all fields are included
flattened_df.printSchema()
flattened_df.show(truncate=False)