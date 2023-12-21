from pymilvus import connections
from utils.milvus_utils import create_collection
from utils.method import Insert2Milvus

#初始化
weight_path='./model/ResNet512.onnx'#提取特征模型权重的路径
#设置Milvus连接参数
connections.connect("default", host="127.0.0.1", port="19530",user="root",password="123456")
#创建Collection
my_milvus=create_collection("p4v")
txt_path='./data.txt'
Insert2Milvus(my_milvus,txt_path,weight_path)


