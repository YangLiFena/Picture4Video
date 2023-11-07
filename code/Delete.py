from pymilvus import connections
from utils.milvus_utils import create_collection
from utils.method import delete_frame_by_v_id
#初始化
weight_path="./model/resnet50-19c8e357.pth"#提取特征模型权重的路径
#设置Milvus连接参数
connections.connect("default", host="127.0.0.1", port="19530",user="root",password="123456")
#创建Collection
my_milvus=create_collection("p4v_pytorch")

v_id_list=['f2b11fdacbae0fe2260a67fb4acffd165af5a511ffc45ac71e36ceae48cc4452']
delete_result=delete_frame_by_v_id(my_milvus,v_id_list)
print("删除视频关键帧的特征向量的数量为",delete_result)