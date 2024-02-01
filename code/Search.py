import os
import time
from pymilvus import connections
from utils.milvus_utils import create_collection
from utils.method import SearchVideoByOnePic
from utils.mysql_utils import USE_MYSQL_QUERYFrame
import cv2
#初始化
weight_path= './model/model.onnx'  #提取特征模型权重的路径
#设置Milvus连接参数
connections.connect("default", host="127.0.0.1", port="19530",user="root",password="123456")
#创建Collection
my_milvus=create_collection("p4v")
#获取待检索的图片的特征向量
def check_and_create_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"The directory '{directory}' is created.")
    else:
        print(f"The directory '{directory}' already exists.")
def search_function(Pic_Path):
    startime = time.time()
    distance_list, frame_id_list = SearchVideoByOnePic(collection=my_milvus, weight_path=weight_path, pic_path=Pic_Path,
                                                       similarity=0.9, number=10)
    endtime = time.time()
    print("图片检索视频耗时为" + str((endtime - startime)) + ' 秒')
    print("检索到的视频的数量", len(distance_list))
    print("相似度列表", distance_list)
    print("帧id列表", frame_id_list)

    video_id_list, video_path_list, frame_position_list = USE_MYSQL_QUERYFrame(
        frame_id_list)  # 根据帧id列表去获取关系型数据库中对应帧的视频id,视频路径,帧的位置信息
    print("视频id列表", video_id_list)
    print("视频路径列表", video_path_list)
    print("帧位置信息列表", frame_position_list)
    SavePath='./Result/'+Pic_Path.split('/')[-1].split('.')[0]
    check_and_create_dir(SavePath)
    for i,frame_id in enumerate(frame_id_list):
        frame_path='./Frames/'+frame_id[:64]+'/'+'frame'+frame_id[64:]+'.jpg'
        temp=cv2.imread(frame_path)
        cv2.imwrite(SavePath+'/'+'frame'+str(i)+'.jpg',temp)

Pic_Path='./pics/1-1.png'
search_function(Pic_Path)
print("="*100)
Pic_Path='./pics/1-2.png'
search_function(Pic_Path)
print("="*100)
Pic_Path='./pics/2-1.png'
search_function(Pic_Path)
print("="*100)
Pic_Path='./pics/2-2.png'
search_function(Pic_Path)
print("="*100)
Pic_Path='./pics/2-3.png'
search_function(Pic_Path)
print("="*100)
Pic_Path='./pics/3-1.png'
search_function(Pic_Path)
print("="*100)
Pic_Path='./pics/3-2.png'
search_function(Pic_Path)

