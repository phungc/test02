# Read the properties file
while IFS='=' read -r key value; do
    if [[ "$key" && ! "$key" =~ ^# ]]; then
        # Set Spark configuration properties
        spark_submit_args+=( "--conf" "$key=$value" )
    fi
done < spark.properties
spark-submit "${spark_submit_args[@]}" your_script.py
