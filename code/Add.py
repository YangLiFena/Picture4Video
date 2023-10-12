import os
from pymilvus import connections
from utils.milvus_utils import create_collection
from utils.method import add_videos2milvus
#初始化
weight_path="./model/resnet50_weights_tf_dim_ordering_tf_kernels_notop.h5"#提取特征模型权重的路径
#设置Milvus连接参数
connections.connect("default", host="localhost", port="19530",user="root",password="123456")
#创建Collection
my_milvus=create_collection("p4v")
#增量更新
Root_Path='./10videos' #增量更新的视频根路径
file_names = os.listdir(Root_Path)
video_path_list=[]
for file_name in file_names:
    video_path_list.append(Root_Path+'/'+file_name)
result=add_videos2milvus(my_milvus,video_path_list,weight_path)
print(result)