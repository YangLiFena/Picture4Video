from pymilvus import connections
from utils.milvus_utils import create_collection
from utils.method import delete_frame_by_v_id
#初始化
weight_path="./model/resnet50_weights_tf_dim_ordering_tf_kernels_notop.h5"#提取特征模型权重的路径
#设置Milvus连接参数
connections.connect("default", host="localhost", port="19530",user="root",password="123456")
#创建Collection
my_milvus=create_collection("p4v")

v_id_list=['d3d1f97d4be285700df96f022d307937607a084aa8febbbb8d229d238d43bce0']
delete_result=delete_frame_by_v_id(my_milvus,v_id_list)
print("删除视频关键帧的特征向量的数量为",delete_result)