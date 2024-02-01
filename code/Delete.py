from pymilvus import connections
from utils.milvus_utils import create_collection
from utils.method import delete_frame_by_v_id
#初始化
weight_path= 'model/model.onnx'  #提取特征模型权重的路径
#设置Milvus连接参数
connections.connect("default", host="127.0.0.1", port="19530",user="root",password="123456")
#创建Collection
my_milvus=create_collection("p4v")

v_id_list=['3ccf29ba7370423412ef8d2274026f32676629d50bc622512a2ff6ae7c884ba2','5bdeb92bef663a87d2d2e70edd7db39f586414bf1d8f73415007ea77ac609475','5bdeb92bef663a87d2d2e70edd7db39f586414bf1d8f73415007ea77ac609474']
delete_result=delete_frame_by_v_id(my_milvus,v_id_list)
print("删除视频关键帧的特征向量的数量为",delete_result)