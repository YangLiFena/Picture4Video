from utils.method import Similarity

#初始化
weight_path= 'model/ResNet2048_v224.onnx' #提取特征模型权重的路径
frame_path_list=['./SFrames/frame0000.jpg','./SFrames/frame256000.jpg']
Similarity(frame_path_list,weight_path)
