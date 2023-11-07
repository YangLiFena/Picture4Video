import os
from utils.method import CreateAndInsert2Database
Root_Path='./Videos' #视频库根路径
file_names = os.listdir(Root_Path)
video_path_list=[]
for file_name in file_names:
    video_path_list.append(Root_Path+'/'+file_name)
result=CreateAndInsert2Database(video_path_list)
print(result)


