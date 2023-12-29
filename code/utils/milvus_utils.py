import time
from pymilvus import (
    utility,
    FieldSchema, CollectionSchema, DataType,
    Collection,
)
def create_collection(collection_name):
    has = utility.has_collection(collection_name)
    if has:
        print("Collection "+collection_name+" 已经存在于Milvus中!")
    fields = [
        FieldSchema(name="key_id", dtype=DataType.INT64, is_primary=True, auto_id=True),
        FieldSchema(name="frame_id", dtype=DataType.VARCHAR, max_length=200),
        FieldSchema(name="embeddings", dtype=DataType.FLOAT_VECTOR, dim=512),
    ]
    schema = CollectionSchema(fields, " ")
    return Collection(collection_name, schema, consistency_level="Strong")
def build_index(collection, field_name):
    index = {
        "index_type": "IVF_FLAT",
        "metric_type": "L2",
        "params": {"nlist": 128},
    }
    collection.create_index("embeddings", index)
    print("Create index: {}".format(index))
def search_data(collection, search_vectors,vector_field,limit=10,output_fields=[]):
    start_time = time.time()
    search_params = {
        "metric_type": "L2",
        "params": {"nprobe": 20},
    }
    result = collection.search(search_vectors, vector_field, search_params, limit=limit, output_fields=output_fields)
    end_time = time.time()
    print("Milvus数据库中检索耗时"+str(end_time - start_time)+" s")
    return result
def insert_data(collection, data):
    start_time = time.time()
    length=len(data[0])
    if  length<=5000:
        entities = [
            data[0],
            data[1]
        ]
        collection.insert(entities)
    else:
        num=int(length/5000)
        for i in range(num):
            entities=[
                data[0][i * 5000:(i + 1) * 5000],
                data[1][i * 5000: (i + 1) * 5000]
            ]
            collection.insert(entities)
        entities = [
            data[0][(i + 1) * 5000:],
            data[1][(i + 1) * 5000:]
        ]
        collection.insert(entities)
    collection.flush()
    end_time = time.time()
    print("将视频关键帧特征向量存储入Milvus数据库总耗时为:" + str(end_time - start_time) + "s")
    print(f"Milvus数据库中总的实体的数量为: {collection.num_entities}")  # check the num_entites
