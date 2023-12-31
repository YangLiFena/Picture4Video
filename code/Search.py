import time
from pymilvus import connections
from utils.milvus_utils import create_collection
from utils.method import SearchVideoByOnePic
from utils.mysql_utils import USE_MYSQL_QUERYFrame
#初始化
weight_path='./model/ResNet2048_v224.onnx'#提取特征模型权重的路径
#设置Milvus连接参数
connections.connect("default", host="127.0.0.1", port="19530",user="root",password="123456")
#创建Collection
my_milvus=create_collection("p4v")
#获取待检索的图片的特征向量
Pic_Path='./pics/1.png'
startime=time.time()
distance_list,frame_id_list=SearchVideoByOnePic(collection=my_milvus,weight_path=weight_path,pic_path=Pic_Path,similarity=0.9,number=10)
endtime=time.time()
print("图片检索视频耗时为"+str((endtime-startime))+' 秒')
print("检索到的视频的数量",len(distance_list))
print("相似度列表",distance_list)
print("帧id列表",frame_id_list)
video_id_list,video_path_list,frame_position_list=USE_MYSQL_QUERYFrame(frame_id_list) #根据帧id列表去获取关系型数据库中对应帧的视频id,视频路径,帧的位置信息
print("视频id列表",video_id_list)
print("视频路径列表",video_path_list)
print("帧位置信息列表",frame_position_list)
