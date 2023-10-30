import pymongo

# Connecting to MongoDB database on local machine
url = "mongodb://127.0.0.1:27017/"
client = pymongo.MongoClient(url)
Database = client['cloudassignment'] #Database name: cloudassignment

# Using aggregation pipeline
def aggregation(collection, pipeline):
    return Database[collection].aggregate(pipeline)


request_for_metrics_id=input("Enter the Request for Metrics ID: ")
benchmark_type = input('Enter Benchmark Type \n1. DVD\n2. NDBench\n')
workload_metric = input('Enter the Workload Metric \n1. CPUUtilization_Average\n2. NetworkIn_Average\n3. NetworkOut_Average\n4. MemoryUtilization_Average\n')
batch_unit = int(input('Enter the number of samples to be contained in each batch: '))
batch_id = int(input('Enter the starting batch ID: '))
batch_size = int(input('Enter the number of batches to be returned: '))
data_type = input('Enter the type of data\n1. Training\n2. Testing\n')

starting_document_id = batch_unit*batch_id
ending_document_id = ((batch_id+batch_size-1)*batch_unit)+batch_unit-1
database_collection = benchmark_type+data_type
val = '$'+workload_metric


match_stage = {
        "$match":{
            '_id':{'$gte':starting_document_id, '$lte':ending_document_id}
        }
    }

sort_stage = {
        "$sort": {workload_metric: 1}
    }

add_field_stage = {
        "$addFields":{
            "batch_unit":batch_unit,
            "batch_size":batch_size,
            "batch_id":batch_id
        }
    }

project_stage_one = {
        "$project":{
            "key":{
                "$function":{
                    "body":'''function(id, batch_unit, batch_size, batch_id){
                                    var no_of_samples=batch_unit;
                                    var no_of_batches=batch_size;
                                    var batch_start_id=batch_id;
                                    for(i=0;i<no_of_batches;i++){
                                        var x = batch_start_id + i;
                                        var y = x*no_of_samples;
                                        for(j=0;j<no_of_samples;j++){
                                            if(id == (y+j)){
                                                return x;
                                            }
                                        }
                                    }
                                }
                    ''',
                    "args":["$_id","$batch_unit","$batch_size","$batch_id"],
                    "lang":"js"
                }
            },
            "values":val
        }
    }

group_stage = {
        "$group":{
            "_id":"$key",
            "max":{
                "$max":"$values"
            },
            "min":{
                "$min":"$values"
            },
            "standard_deviation":{
                "$stdDevPop":"$values"
            },
            "values":{
                "$push":"$values"
            }
        }
    }

project_stage_two = {
        "$project":{
            "max":1,
            "min":1,
            "standard_deviation":1,
            "median":{
                "$function":{
                    "body":'''function(values){
                                var l=0;
                                for(i in values){
                                    l=l+1;
                                }
                                var x =l/2;
                                values.sort();
                                if((l%2)==0){
                                    var m = (values[x-1]+values[x])/2;
                                }
                                if((l%2)!=0){
                                    var m = values[x+0.5-1];
                                }
                                return m;
                                }
                            ''',
                    "args":["$values"],
                    "lang":"js"
                }
            }
        }
    }


final_pipeline = [
    match_stage,
    sort_stage,
    add_field_stage,
    project_stage_one,
    group_stage,
    project_stage_two
]


a = aggregation(database_collection, final_pipeline)

res = []
for i in a:
    res.append(i)

output={
    "request_for_metrics_id":request_for_metrics_id,
    "result":res
}

print(output)
