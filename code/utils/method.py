import av
from PIL import Image
import torch
import torchvision.transforms as transforms
import time
import hashlib
import os
import onnxruntime as ort
from utils.milvus_utils import search_data, insert_data, build_index
from utils.mysql_utils import USE_MYSQL_QUERY,USE_MYSQL_DELETE,USE_MYSQL_Video,USE_MYSQL_FV,USE_MYSQL_Frame

# L2距离归一化为相似度
def Normalized_Euclidean_Distance(L2,dim=512):
    return 1/(1+L2/dim)

def compute_sha256(file_path):
    sha256_hash = hashlib.sha256()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()
#功能：批量获取视频关键帧，返回视频主键数组，视频路径数组，关键帧主键数组，关键帧路径数组，关键帧位置信息数组
#输入参数定义
#vides_path_list：视频路径数组
#不需要视频帧率参数，可以在脚本中获取
def GetVideoFrames(videos_path_list):
    frame_root_path= "./Frames"  #视频关键帧根目录
    v_id_list=[]   #视频主键列表
    frame_id_list=[] #关键帧主键列表
    frame_path_list=[]  #关键帧路径数组
    frame_position_list=[] #关键帧的位置信息
    if not os.path.exists(frame_root_path):
        os.makedirs(frame_root_path)

    for cur_video_path in videos_path_list:
        if cur_video_path.split('/')[-1].split('.')[-1]=='mp4' :
            #video_name=cur_video_path.split('/')[-1].split('.')[0]
            container = av.open(cur_video_path)
            sha256_hash = compute_sha256(cur_video_path) #采用MD5对视频进行编码
            v_id = str(sha256_hash)
            v_id_list.append(v_id)
            cur_video_keyframe_dir = frame_root_path + '/' + v_id  # 存储某视频关键帧的路径根目录
            if not os.path.exists(cur_video_keyframe_dir):
                os.makedirs(cur_video_keyframe_dir)
            stream = container.streams.video[0]
            #frame_rate = stream.average_rate  #视频帧率
            stream.codec_context.skip_frame = 'NONKEY'

            for frame in container.decode(stream):
                # print(frame.pts)
                frame_path=cur_video_keyframe_dir + '/' + 'frame{:04d}.jpg'.format(frame.pts)
                frame_path_list.append(frame_path)
                frame.to_image().save(frame_path,quality=90)
                second = int(frame.pts * stream.time_base)
                minute = second // 60
                second = second - 60 * minute
                frame_position='{:02d}m:{:02d}s'.format(minute, second)
                frame_position_list.append(frame_position)
                frame_id=v_id+'{:04d}'.format(frame.pts)
                frame_id_list.append(frame_id)
    videos={
        'video_id_list':v_id_list,
        'video_path_list':videos_path_list
    }
    frames={
        'frame_id_list':frame_id_list,
        'frame_path_list':frame_path_list,
        'frame_postion_list':frame_position_list
    }
    return videos,frames

#功能：批量获取视频关键帧特征向量，返回特征向量数组
#输入参数定义：
#frame_path_list：视频关键帧路径数组
def GetFramesFeature(frame_path_list, weight_path):
    providers = [
        ('CUDAExecutionProvider', {
            'device_id': 0,
            'arena_extend_strategy': 'kNextPowerOfTwo',
            # 'gpu_mem_limit': 2 * 1024 * 1024 * 1024,
            'cudnn_conv_algo_search': 'DEFAULT',
            'do_copy_in_default_stream': True,
        }),
        'CPUExecutionProvider',
    ]
    so = ort.SessionOptions()
    so.log_severity_level = 3
    # 加载onnx模型到onnxruntime的推理
    session = ort.InferenceSession(weight_path, so, providers=providers)
    input_name = session.get_inputs()[0].name

    preprocess = transforms.Compose([
        transforms.Resize([224, 224]),
        transforms.ToTensor(),
        transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])
    ])

    # 提取每个帧的特征向量
    feature_list = []
    starttime = time.time()
    for frame_path in frame_path_list:
        # 打开图像并进行预处理
        image = Image.open(frame_path).convert("RGB")
        image = preprocess(image).unsqueeze(0).numpy()
        # 使用模型提取特征
        # with torch.no_grad():
            # 输入模型进行推理
        feature = session.run(None, {input_name: image})
        feature =torch.from_numpy(feature[0])
        feature = torch.nn.functional.adaptive_avg_pool2d(feature, [1,1])
        # feature = feature.cpu().flatten().numpy()
        feature = feature.flatten().numpy()
        feature = feature.tolist()
        feature_list.append(feature)
    endtime = time.time()
    print("提取特征向量耗时", endtime - starttime, 's')
    return feature_list

#参数定义：
#vides_path_list：视频路径数组
def CreateAndInsert2Database(video_path_list):
    # 调用提取视频关键帧函数进行视频关键帧的提取
    result=[]
    video_path_dict={} #v_id和v_path的对应关系字典
    repeat_path_list=[] #插入视频中存在重复的视频的视频的路径
    real_video_path_list=[] #真正需要插入的视频路径数组
    for video_path in video_path_list:
        if video_path.split('/')[-1].split('.')[-1] == 'mp4':
            sha256_hash = compute_sha256(video_path)  # 采用MD5对视频进行编码
            search_video_id = str(sha256_hash)
            flag=USE_MYSQL_QUERY([search_video_id])
            #将search_video_id送入关系型数据库中检索，如果返回True,则说明已经存在该视频，不插入，返回False则进行后续的判断
            if flag==False:
                # 通过video_path_dict字典来保存v_id和v_path的对应关系来确保重复视频不会被插入，并且用repeat_path_list来保存这些重复的视频的路径
                if search_video_id in video_path_dict:
                    repeat_path_list.append(video_path_dict[search_video_id])
                video_path_dict[search_video_id]=video_path
            else :
                result.append({
                    "video_path":video_path,
                    "isSuccess":False
                               })
    for key, value in video_path_dict.items():
        real_video_path_list.append(value)
    for repeat in  repeat_path_list:
        result.append({
            "video_path": repeat,
            "isSuccess": False
        })
    if len(real_video_path_list)==0 :
        return result
    else :

        startime0 = time.time()
        startime = time.time()
        videos, frames = GetVideoFrames(real_video_path_list)
        endtime = time.time()
        print("提取视频关键帧总耗时为" + str((endtime - startime) / 60) + ' 分钟')
        startime = time.time()
        video_ids = videos['video_id_list']
        video_paths = videos['video_path_list']
        frame_ids = frames['frame_id_list']
        videos_frames_ids = []
        for frame_id in frame_ids:
            videos_frames_ids.append(frame_id[0:64])
        frame_positions = frames['frame_postion_list']
        frame_paths = frames['frame_path_list']
        USE_MYSQL_Video(video_ids,video_paths)
        USE_MYSQL_Frame(frame_ids,frame_positions,frame_paths)
        USE_MYSQL_FV(videos_frames_ids,frame_ids)
        endtime = time.time()
        print("将视频关键帧插入关系型数据库总耗时为" + str((endtime - startime) / 60) + ' 分钟')
        print("提取视频关键帧并插入关系型数据库总耗时为" + str((endtime - startime0) / 60) + ' 分钟')
        for video_path in real_video_path_list:
            result.append({
                "video_path":video_path,
                "isSuccess":True
                           })
        # 打开文件，使用 'w'（写入）模式
        file = open("./data.txt", "w")
        for item1, item2 in zip(frame_ids, frame_paths):
            file.write(str(item1) + "," + str(item2) + "\n")
        # 关闭文件
        file.close()
        return result

#参数定义：
#collection:Milvus中的Collection
#txt_path:存储需要送入Milvus数据库中的视频关键帧id和视频关键帧路径对应关系的文本的路径
#weight_path:提取特征向量的模型的路径
def Insert2Milvus(collection,txt_path,weight_path):
    # 打开文件，使用 'r'（读取）模式
    file = open(txt_path, "r")
    # 逐行读取文件内容
    lines = file.readlines()
    # 关闭文件
    file.close()
    frame_ids = []#视频关键帧id列表
    frame_paths = []#视频关键帧路径列表
    for line in lines:
        # 删除换行符
        line = line.strip()
        elements = line.split(",")
        frame_ids.append(elements[0])
        frame_paths.append(elements[1])
    startime = time.time()
    features = GetFramesFeature(frame_paths, weight_path)
    endtime = time.time()
    print("将视频关键帧转换为特征向量总耗时为" + str((endtime - startime) / 60) + ' 分钟')
    data = [frame_ids, features]
    insert_data(collection, data)
    # 创建Collection索引
    build_index(collection, "")
    # 加载Collection
    collection.load()

#参数定义：
#collection:Milvus中的Collection
#field:检索的列名，如'frame_id'
#value:该列名对应的一个值
def delete_by_filed(collection, field, value):
    expr = field+'=="' + value+'"'
    entitys = collection.query(expr)
    if entitys is None or len(entitys) == 0:
        return 0
    del_expr = 'key_id in ['
    _len = len(entitys)
    for i, entity in enumerate(entitys):
        _id = entity['key_id']
        _expr = str(_id) + ']' if i == (_len-1) else str(_id) + ','
        del_expr = del_expr + _expr

    delete_res = collection.delete(del_expr)
    # collection.flush()
    return delete_res.delete_count

#参数定义：
#collection:Milvus中的Collection
#v_ids:要删除的视频的主键列表
def delete_frame_by_v_id(collection, v_ids):
    result =0
    result_frame_id_list=USE_MYSQL_DELETE(v_ids)
    for frame_id in result_frame_id_list:
        result = result+delete_by_filed(collection,'frame_id',frame_id)
    return result

#参数定义
#collection:Milvus中的Collection
#weight_path:提取特征向量的模型的路径
#pic_path:图片路径
#similarity:相似度
#number:前N个视频
def SearchVideoByOnePic(collection,weight_path,pic_path,similarity,number):
    features = GetFramesFeature([pic_path], weight_path)
    if similarity is not None and number is not None:
        results = search_data(collection=collection, search_vectors=features, vector_field="embeddings", limit=number,
                              output_fields=["frame_id"])
        distance_list = []
        frame_id_list = []
        for result_item in results:
            for i, result in enumerate(result_item):
                if Normalized_Euclidean_Distance(result.distance) >= similarity:
                    distance_list.append(Normalized_Euclidean_Distance(result.distance))
                    frame_id_list.append(result.entity.get('frame_id'))
        return distance_list, frame_id_list
    elif similarity is not None and number is None :
        number = 16384
        results = search_data(collection=collection, search_vectors=features, vector_field="embeddings", limit=number,
                              output_fields=["frame_id"])
        distance_list = []
        frame_id_list = []
        for result_item in results:
            for i, result in enumerate(result_item):
                if Normalized_Euclidean_Distance(result.distance) >= similarity:
                    distance_list.append(Normalized_Euclidean_Distance(result.distance))
                    frame_id_list.append(result.entity.get('frame_id'))
        return distance_list, frame_id_list
    elif similarity is None and number is not None :
        results = search_data(collection=collection, search_vectors=features, vector_field="embeddings", limit=number,
                              output_fields=["frame_id"])
        distance_list = []
        frame_id_list = []
        for result_item in results:
            for i, result in enumerate(result_item):
                distance_list.append(Normalized_Euclidean_Distance(result.distance))
                frame_id_list.append(result.entity.get('frame_id'))
        return distance_list, frame_id_list
    else :
        print("error")
        return [],[]
#参数定义
#video_path_list:待进行视频在库检测的视频的路径列表
def VideoRetrieval(video_path_list):
    result=[]
    for video_path in video_path_list:
        if video_path.split('/')[-1].split('.')[-1] == 'mp4':
            sha256_hash = compute_sha256(video_path)  # 采用MD5对视频进行编码
            search_video_id = str(sha256_hash)
            flag=USE_MYSQL_QUERY([search_video_id])
            #将search_video_id送入关系型数据库中检索，如果返回True,则说明已经存在该视频，返回False则不存在该视频
            if flag==False:
                result.append({
                    "video_path": video_path,
                    "isIn": False
                })
            else :
                result.append({
                    "video_path":video_path,
                    "isIn":True
                               })
    return result

def Similarity(frame_path_list,weight_path):
    feature_result=GetFramesFeature(frame_path_list,weight_path)
    L2_distance= np.linalg.norm(np.array(feature_result[0]) - np.array(feature_result[1]))
    # print(L2_distance)
    # print( Normalized_Euclidean_Distance(L2_distance))
    return Normalized_Euclidean_Distance(L2_distance)