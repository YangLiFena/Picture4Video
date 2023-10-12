import os
import time
from pymilvus import connections

from utils.mysql_utils import USE_MYSQL_Frame,USE_MYSQL_FV,USE_MYSQL_Video
from utils.milvus_utils import create_collection,insert_data,build_index
from utils.method import GetVideoFrames,GetFramesFeature

#初始化
fmt = "\n=== {:30} ===\n"
weight_path="./model/resnet50_weights_tf_dim_ordering_tf_kernels_notop.h5"#提取特征模型权重的路径
#设置Milvus连接参数
connections.connect("default", host="localhost", port="19530",user="root",password="123456")
#创建Collection
my_milvus=create_collection("p4v")

#调用提取视频关键帧函数进行视频关键帧的提取
startime=time.time()
Root_Path='./Crawer_Video'
file_names = os.listdir(Root_Path)
video_path_list=[]
for file_name in file_names:
    video_path_list.append(Root_Path+'/'+file_name)
videos, frames = GetVideoFrames(video_path_list)
video_ids=videos['video_id_list']
video_paths=videos['video_path_list']
frame_ids=frames['frame_id_list']
videos_frames_ids=[]
for frame_id in frame_ids:
    videos_frames_ids.append(frame_id[0:64])
frame_positions=frames['frame_postion_list']
frame_paths=frames['frame_path_list']
endtime=time.time()
print("提取视频关键帧总耗时为"+str((endtime-startime)/60)+' 分钟')
USE_MYSQL_Video(video_ids,video_paths)
USE_MYSQL_Frame(frame_ids,frame_positions,frame_paths)
USE_MYSQL_FV(videos_frames_ids,frame_ids)
startime=time.time()
features=GetFramesFeature(frame_paths,weight_path)
endtime=time.time()
print("将视频关键帧转换为特征向量总耗时为"+str((endtime-startime)/60)+' 分钟')
data=[frame_ids,features]

insert_data(my_milvus,data)
#创建Collection索引
build_index(my_milvus,"")
#加载Collection
my_milvus.load()







