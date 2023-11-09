import pymysql
import pandas as pd

mysql_host = '127.0.0.1'
mysql_db = 'videos'
mysql_user = 'root'
mysql_pwd = '123456'

video_table = 'video'
frame_video_table = 'frame_video'
frame_table = 'frame'


class MyCustomException(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return f"MyCustomException: {self.message}"


class MYSQL:
    def __init__(self):
        # MySQL
        self.MYSQL_HOST = mysql_host
        self.MYSQL_DB = mysql_db
        self.MYSQ_USER = mysql_user
        self.MYSQL_PWD = mysql_pwd
        self.connect = pymysql.connect(
            host=self.MYSQL_HOST,
            db=self.MYSQL_DB,
            port=3306,
            user=self.MYSQ_USER,
            passwd=self.MYSQL_PWD,
            charset='utf8',
            use_unicode=False
        )
        print(self.connect)
        self.cursor = self.connect.cursor()

    def close(self):
        # 关闭游标对象
        self.cursor.close()
        # 关闭连接对象
        self.connect.close()

    def insert_table_frame(self, data_json):
        sql = "insert into {}(`FrameID`, `FramePosition`,`FramePath`) VALUES (%s, %s,%s)".format(frame_table)
        try:
            self.cursor.execute(sql, (data_json['FrameID'], data_json['FramePosition'], data_json['FramePath']))
            self.connect.commit()  # print('frame库数据插入成功')
        except Exception as e:
            print('e= ', e)
            print('frame库数据插入错误')
            self.connect.rollback()  # 数据回滚

    def insert_table_fv(self, data_json):
        sql = "insert into {}(`VideoMD5`, `FrameID`) VALUES (%s, %s)".format(frame_video_table)
        try:
            self.cursor.execute(sql, (data_json['VideoMD5'], data_json['FrameID']))
            self.connect.commit()  # print('frame_video库数据插入成功')
        except Exception as e:
            print('e= ', e)
            print('frame_video库数据插入错误')
            self.connect.rollback()

    def insert_table_video(self, data_json):
        sql = "insert into {}(`VideoMD5`, `VideoPath`) VALUES (%s, %s)".format(video_table)
        try:
            self.cursor.execute(sql, (data_json['VideoMD5'], data_json['VideoPath']))
            self.connect.commit()
            print('video库插入{}成功'.format(data_json['VideoPath']))
        except Exception as e:
            print('e= ', e)
            print('video库插入{}失败！'.format(data_json['VideoPath']))
            self.connect.rollback()

    def delete_data(self, data_json):
        if self.query_data(data_json):
            sql_select = "SELECT VideoPath FROM {} WHERE VideoMD5 = %s".format(video_table)
            self.cursor.execute(sql_select, (data_json['VideoMD5'],))
            result = self.cursor.fetchone()
            video_path = result[0] if result else None
            print("将从video库中删除该视频：", video_path)
        else:
            print("video库中不存在该数据{}".format(data_json['VideoMD5']))
            return []

        # 删除video库中数据
        sql1 = "DELETE FROM {} WHERE VideoMD5 = %s".format(video_table)
        try:
            self.cursor.execute(sql1, (data_json['VideoMD5']))
            self.connect.commit()
            print('video库中成功删除该视频！')
        except Exception as e:
            print('e= ', e)
            print('video库中删除失败')
            self.connect.rollback()

        # 在frame_video库中查询frameid
        sql2 = "SELECT FrameID FROM {} WHERE VideoMD5 = %s".format(frame_video_table)
        try:
            self.cursor.execute(sql2, (data_json['VideoMD5']))
            FrameID = [row[0] for row in self.cursor.fetchall()]  # print('frame_video库中数据查询并返回成功')
        except Exception as e:
            print('e= ', e)
            print('frame_video库中数据查询错误')

        # 在frame_video库中删除对应的frame-video关系
        sql3 = "DELETE FROM {} WHERE VideoMD5 = %s".format(frame_video_table)
        try:
            self.cursor.execute(sql3, (data_json['VideoMD5']))
            self.connect.commit()  # print('frame_video库中数据删除成功')
        except Exception as e:
            print('e= ', e)
            print('frame_video库中数据删除错误')
            self.connect.rollback()

        # 根据查询到的FrameID删除frame库中数据
        frame_df = pd.DataFrame({'FrameID': FrameID})
        frame_json = frame_df.to_dict(orient='records')
        for frame_dt in frame_json:
            sql4 = "DELETE FROM {} WHERE FrameID = %s".format(frame_table)
            try:
                self.cursor.execute(sql4, (frame_dt['FrameID']))
                self.connect.commit()  # print('frame库中数据删除成功')
            except Exception as e:
                print('e= ', e)
                print('frame库中数据删除错误')
                self.connect.rollback()
        return FrameID

    def query_data(self, data_json):
        """
        数据查询mysql
        :param data_json:
        :return:
        """
        sql = "SELECT COUNT(*) FROM {} WHERE VideoMD5 = %s".format(video_table)
        self.cursor.execute(sql, (data_json['VideoMD5']))
        count = self.cursor.fetchone()[0]
        if count > 0:
            return True  # video数据库中存在该数据
        else:
            return False  # video数据库中不存在该数据

    def query_data_frame(self, data_json):
        # frame库中依据FrameID查询FramePosition
        sql1 = "SELECT FramePosition FROM {} WHERE FrameID = %s".format(frame_table)
        try:
            self.cursor.execute(sql1, (data_json['FrameID']))
            FramePosition = [row[0] for row in self.cursor.fetchall()]  # print('frame库中数据查询并返回成功')
        except Exception as e:
            print('e= ', e)
            print('frame库中查询{}对应FramePosition错误'.format(data_json['FrameID']))
            return None, None, None

        # frame库中依据FrameID查询VideoMD5
        sql2 = "SELECT VideoMD5 FROM {} WHERE FrameID = %s".format(frame_video_table)
        try:
            self.cursor.execute(sql2, (data_json['FrameID']))
            VideoMD5 = [row[0] for row in self.cursor.fetchall()]  # print('frame_video库中数据查询并返回成功')
        except Exception as e:
            print('e= ', e)
            print('frame_video库中数据查询{}对应VideoMD5错误'.format(data_json['FrameID']))
            return None, None, FramePosition

        # video库中依据VideoMD5查询VideoPath
        VideoMD5_df = pd.DataFrame({'VideoMD5': VideoMD5})
        VideoMD5_json = VideoMD5_df.to_dict(orient='records')
        sql3 = "SELECT VideoPath FROM {} WHERE VideoMD5 = %s".format(video_table)
        try:
            self.cursor.execute(sql3, (VideoMD5_json[0]['VideoMD5']))
            VideoPath = [row[0] for row in self.cursor.fetchall()]  # print('video库中数据查询并返回成功')
        except Exception as e:
            print('e= ', e)
            print('video库中数据查询{}对应VideoPath错误'.format(data_json['FrameID']))
            return VideoMD5, None, FramePosition

        return VideoMD5, VideoPath, FramePosition

    # ------
    def select_frame_temp(self):
        sql = "SELECT FrameID, FramePath FROM {}".format(frame_table)
        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            FrameID = [row[0] for row in result]
            FramePath = [row[1] for row in result]  # print('Frame_Temp表中数据查询并返回成功')
            return FrameID, FramePath
        except Exception as e:
            print('e= ', e)
            print('Frame_Temp表中查询FrameID,FramePath失败')
            return [], []


# ---------------------------- insert Frame table
def USE_MYSQL_Frame(FrameID, FramePosition, FramePath):
    mysql = MYSQL()
    df = pd.DataFrame({
        'FrameID': FrameID,
        'FramePosition': FramePosition,
        'FramePath': FramePath
    })
    data_json = df.to_dict(orient='records')
    for dt in data_json:
        mysql.insert_table_frame(dt)
    mysql.close()


# ----------------------------insert Frame_Video table
def USE_MYSQL_FV(VideoMD5, FrameID):
    mysql = MYSQL()
    df = pd.DataFrame({
        'VideoMD5': VideoMD5,
        'FrameID': FrameID
    })
    data_json = df.to_dict(orient='records')
    for dt in data_json:
        mysql.insert_table_fv(dt)
    mysql.close()


# ----------------------------insert Video table
def USE_MYSQL_Video(VideoMD5, VideoPath):
    mysql = MYSQL()
    df = pd.DataFrame({
        'VideoMD5': VideoMD5,
        'VideoPath': VideoPath
    })
    data_json = df.to_dict(orient='records')
    for dt in data_json:
        mysql.insert_table_video(dt)
    mysql.close()


# ----------------------------delete Video/Frame_Video/Frame table
def USE_MYSQL_DELETE(VideoMD5):
    mysql = MYSQL()
    df = pd.DataFrame({'VideoMD5': VideoMD5})
    data_json = df.to_dict(orient='records')
    frameid_lists = []
    for dt in data_json:
        frameid_list = mysql.delete_data(dt)
        frameid_list = [byte_string.decode() for byte_string in frameid_list]
        frameid_lists.append(frameid_list)

    total_list = []
    for i in frameid_lists:
        for item in i:
            total_list.append(item)
    mysql.close()
    return total_list


# ----------------------------query Video table
def USE_MYSQL_QUERY(VideoMD5):
    mysql = MYSQL()
    df = pd.DataFrame({'VideoMD5': VideoMD5})
    data_json = df.to_dict(orient='records')
    for dt in data_json:
        judge = mysql.query_data(dt)
    mysql.close()
    return judge


# ----------------------------query Frame/Video table
def USE_MYSQL_QUERYFrame(FrameID):
    mysql = MYSQL()
    df = pd.DataFrame({'FrameID': FrameID})
    data_json = df.to_dict(orient='records')
    vids = []
    vpaths = []
    framepositions = []
    for dt in data_json:
        vid, vpath, fposition = mysql.query_data_frame(dt)
        vid = vid[0].decode()
        vpath = vpath[0].decode()
        fposition = fposition[0].decode()
        vids.append(vid)
        vpaths.append(vpath)
        framepositions.append(fposition)
    mysql.close()
    return vids, vpaths, framepositions


# -------------------------select FrameID,FramePath from frame_table
def USE_MYSQL_SELECTFrame():
    mysql = MYSQL()
    FID=[]
    FPATH = []
    FrameID, FramePath = mysql.select_frame_temp()  # 返回FrameID,FramePath
    for id,path in zip(FrameID,FramePath):
        i = id.decode()
        j = path.decode()
        FID.append(i)
        FPATH.append(j)
    mysql.close()
    return FID, FPATH
