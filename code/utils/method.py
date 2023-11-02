import av
import numpy as np
from PIL import Image
import tensorflow as tf
import time
import hashlib
import os
from utils.milvus_utils import search_data, insert_data, build_index
from utils.mysql_utils import USE_MYSQL_QUERY,USE_MYSQL_DELETE,USE_MYSQL_Video,USE_MYSQL_FV,USE_MYSQL_Frame
os.environ['TF_CPP_MIN_LOG_LEVEL']='3' # 配置tensorflow日志等级
# 0：显示所有日志（默认等级）
# 1：显示info、warning和error日志
# 2：显示warning和error信息
# 3：显示error日志信息

# L2距离归一化为相似度
def Normalized_Euclidean_Distance(L2,dim=2048):
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
            video_name=cur_video_path.split('/')[-1].split('.')[0]

            container = av.open(cur_video_path)
            sha256_hash = compute_sha256(cur_video_path) #采用MD5对视频进行编码
            v_id = str(sha256_hash)
            v_id_list.append(v_id)
            cur_video_keyframe_dir = frame_root_path + '/' + v_id  # 存储某视频关键帧的路径根目录
            if not os.path.exists(cur_video_keyframe_dir):
                os.makedirs(cur_video_keyframe_dir)
            stream = container.streams.video[0]
            frame_rate = stream.average_rate  #视频帧率
            # print(frame_rate)
            stream.codec_context.skip_frame = 'NONKEY'

            for frame in container.decode(stream):
                # print(frame.pts)
                # print(512*frame_rate)
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
def GetFramesFeature(frame_path_list,weight_path) :
    path=weight_path
    starttime=time.time()
    # 获取服务器所有可用的设备
    devices = tf.config.list_physical_devices('GPU')
    # 当服务器包含多个GPU设备时，使用以下指令指定想要使用的设备编号，例如：devices[0], devices[1]表示使用编号为0和1的两张显卡
    # visible_devices = [devices[0], devices[1]]
    visible_devices = [devices[0]]
    tf.config.experimental.set_visible_devices(visible_devices, 'GPU')
    model = tf.keras.applications.ResNet50(include_top=False,weights=None)
    model.load_weights(path)
    endtime=time.time()
    print("加载模型耗时",endtime-starttime,' s')
    starttime=time.time()
    feature_list=[] #特征向量数组
    for frame_path in frame_path_list :
        # if distant:
        #     content = requests.get(url, stream=True).content
        #     byteStream = io.BytesIO(content)
        #     image = Image.open(byteStream)
        # else:
        #     image = Image.open(url)
        image=Image.open(frame_path)
        image = image.resize([224, 224]).convert('RGB')
        y = tf.keras.preprocessing.image.img_to_array(image)
        y = np.expand_dims(y, axis=0)
        # print(y)
        y = tf.keras.applications.resnet50.preprocess_input(y)
        # print(type(y), y)
        y = model.predict(y)
        # y = model(y)
        # print(y.shape)
        result = tf.keras.layers.GlobalAveragePooling2D()(y)
        feature = [x for x in result.numpy()[0].tolist()]
        feature_list.append(feature)
    endtime=time.time()
    print("提取特征向量耗时",endtime-starttime,' s')
    return feature_list


#功能：对新增视频进行插入操作，返回插入结果
#输入参数定义：
#collection：Milvus中的Collection
#video_path_list:新增视频的路径数组
#weight_path:提取特征向量的模型的路径
def add_videos2milvus(collection,video_path_list,weight_path):
    # 调用提取视频关键帧函数进行视频关键帧的提取
    result=[]
    real_video_path_list=[] #真正需要插入的视频路径数组
    for video_path in video_path_list:
        if video_path.split('/')[-1].split('.')[-1] == 'mp4':
            sha256_hash = compute_sha256(video_path)  # 采用MD5对视频进行编码
            search_video_id = str(sha256_hash)
            flag=USE_MYSQL_QUERY([search_video_id])
            #将search_video_id送入关系型数据库中检索，如果返回True,则说明已经存在该视频，不插入，返回False则进行插入
            if flag==False:
                real_video_path_list.append(video_path)
            else :
                result.append({
                    "video_path":video_path,
                    "isSuccess":False
                               })
    if len(real_video_path_list)==0 :
        return result
    else :
        startime = time.time()
        videos, frames = GetVideoFrames(real_video_path_list)
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
        print("提取视频关键帧总耗时为" + str((endtime - startime) / 60) + ' 分钟')
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
        for video_path in real_video_path_list:
            result.append({
                "video_path":video_path,
                "isSuccess":True
                           })
        return result

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
