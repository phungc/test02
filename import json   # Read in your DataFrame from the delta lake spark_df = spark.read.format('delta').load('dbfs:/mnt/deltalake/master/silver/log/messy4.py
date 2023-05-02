import json 

# Read in your DataFrame from the delta lake
spark_df = spark.read.format('delta').load('dbfs:/mnt/deltalake/master/silver/log/')

def get_avro_schema(spark_df, schema_type:str, name:str, namespace:str):
    '''
    Returns the corresponding avro schema for the passed in spark dataframe. 
    The type mapping covers most commonly used types, every field is made to be nullable.
    '''
    
    schema_base = {
    "type": schema_type,
    "namespace": name ,
    "name": namespace
    }
    
    # Keys are Spark Types, Values are Avro Types
    avro_mapping = {
        'StringType' : ["string", "null"],
        'LongType' : ["long", "null"],
        'IntegerType' :  ["int", "null"],
        'BooleanType' : ["boolean", "null"],
        'FloatType' : ["float", "null"],
        'DoubleType': ["double", "null"],
        'TimestampType' : ["long", "null"],
        'ArrayType(StringType,true)' : [{"type": "array", "items": ["string", "null"]}, "null"],
        'ArrayType(IntegerType,true)' : [{"type": "array", "items": ["int", "null"]}, "null"]
        }
    
    fields = []
    
    for field in spark_df.schema.fields:
        if (str(field.dataType) in avro_mapping):
            fields.append({"name" : field.name, "type": avro_mapping[str(field.dataType)]})
        else:
            fields.append({"name" : field.name, "type": str(field.dataType)})
            
    schema_base["fields"] = fields
    
    return json.dumps(schema_base)
    
    
schema = get_avro_schema(spark_df, "record", "com.example.data", "exampleTable")

print(schema)
