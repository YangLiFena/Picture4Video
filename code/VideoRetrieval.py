import os
from utils.method import VideoRetrieval

if __name__ == '__main__':
    Root_Path = './RetrievalVideos'  # 待进行视频在库检测的视频的根路径
    file_names = os.listdir(Root_Path)
    video_path_list = []
    for file_name in file_names:
        video_path_list.append(Root_Path + '/' + file_name)
    result = VideoRetrieval(video_path_list)
    print(result)