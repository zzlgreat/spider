import requests
import json
import segment
import gc
from darknet import load_net, load_meta, detect
import base64
import os
import shutil
import cv2
import time
from flask import Flask, request
from flask_restful import reqparse, abort, Api, Resource
import numpy as np
from io import BytesIO
from sys import version_info
from PIL import Image
since = time.time()
ALLOWED_PICEXTENSIONS = set(['png', 'jpg', 'jpeg', 'bmp'])
app = Flask(__name__)
api = Api(app)
def timestamp():
    return int(time.time() * 1000000)
def base64_api(uname, pwd,  img):
    img = img.convert('RGB')
    buffered = BytesIO()
    img.save(buffered, format="JPEG")
    if version_info.major >= 3:
        b64 = str(base64.b64encode(buffered.getvalue()), encoding='utf-8')
    else:
        b64 = str(base64.b64encode(buffered.getvalue()))
    data = {"username": uname, "password": pwd, "image": b64,"typeid":'16'}
    result = json.loads(requests.post("http://api.ttshitu.com/base64", json=data).text)
    if result['success']:
        return result["data"]["result"]
    else:
        return result["message"]
    return ""
@app.route('/ocr', methods=['GET', 'POST'])
def hello_world():
    if request.method == 'POST':
        # 判断是点击的图片还是上传的图片，如果是获取的json为空，则执行上传图片的语句
        if not (request.get_json()):
            starttimeuploadfile = time.time()
            str_decode = base64.b64decode(request.form.get('aim'))
            nparr = np.fromstring(str_decode, np.uint8)
            img_restore = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            img_1_nm = str(timestamp())
            cv2.imwrite('./'+img_1_nm+'.jpg', img_restore)

            str_decode = base64.b64decode(request.form.get('pic'))
            nparr = np.fromstring(str_decode, np.uint8)
            img_restore = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            img_2_nm = str(timestamp()+1)
            cv2.imwrite('./'+img_2_nm+'.jpg', img_restore)
            net = load_net("cfg/yolo3-tiny.cfg".encode('utf-8'),
                           "session/yolo3-tiny.weights".encode('utf-8'), 0)
            meta = load_meta("data/train.data".encode('utf-8'))
            rets = detect(net, meta, str(img_1_nm+'.jpg').encode('utf-8'))
            rets_1 = detect(net, meta, str(img_2_nm+'.jpg').encode('utf-8'))
            dict1 = segment.seg_fir_img(img_1_nm+'.jpg', rets,img_1_nm)
            if len(dict1) > 2:
                endtimeuploadfile = time.time()
                uploadPicTime = endtimeuploadfile - starttimeuploadfile
                info = json.dumps({'move': [], 'status': 'reflesh','time':uploadPicTime})
                shutil.rmtree(img_1_nm.encode('utf-8'))
                os.remove(img_2_nm + '.jpg')
                os.remove(img_1_nm + '.jpg')
                return info
            dict2 = segment.seg_sec_img(img_2_nm+'.jpg', rets_1,img_2_nm)

            shunxu = {}
            results = {}
            hanzi = []
            for img_2 in os.listdir(img_2_nm):
                img = Image.open(img_2_nm+'/'+img_2)
                result_hz = base64_api(uname='zzlgreat', pwd='zzl33818', img=img)
                results.update({result_hz:img_2_nm+'/'+img_2})
                hanzi.append(result_hz)
                print(result_hz)
            for img_1 in os.listdir(img_1_nm):
                #print(img_1)
                img = Image.open(img_1_nm+'/'+img_1)
                result = base64_api(uname='zzlgreat', pwd='zzl33818', img=img)
                if result in hanzi:
                    score = results.get(result)
                else:
                    endtimeuploadfile = time.time()
                    uploadPicTime = endtimeuploadfile - starttimeuploadfile
                    shutil.rmtree(img_1_nm.encode('utf-8'))
                    shutil.rmtree(img_2_nm.encode('utf-8'))
                    os.remove(img_2_nm + '.jpg')
                    os.remove(img_1_nm + '.jpg')
                    return json.dumps({'move': [], 'status': 'reflesh','time':uploadPicTime})
                #score = canny.compare('verifyCode/' + img_1, 'verifyCode_1')
                print(score)
                shunxu[dict1.get(img_1_nm+'/' + img_1)] = dict2.get(score)
                del img
                gc.collect()
                if os.path.exists(score):
                    os.remove(score)
            lt = list(shunxu.keys())
            n = len(lt)
            for x in range(n - 1):
                for y in range(n - 1 - x):
                    if lt[y] > lt[y + 1]:
                        lt[y], lt[y + 1] = lt[y + 1], lt[y]
            print(lt)
            moves = []

            try:
                for i in lt:
                    cor = shunxu.get(i)
                    moves.append([cor[0], cor[1]])
                    del cor
                    gc.collect()
                status = 'move'
            except Exception as e:
                print(e)
                status = 'reflesh'
            endtimeuploadfile = time.time()
            uploadPicTime = endtimeuploadfile - starttimeuploadfile
            info = json.dumps({'move': moves, 'status': status,'time':uploadPicTime})
            shutil.rmtree(img_1_nm.encode('utf-8'))
            shutil.rmtree(img_2_nm.encode('utf-8'))
            os.remove(img_2_nm+'.jpg')
            os.remove(img_1_nm + '.jpg')
            return info

if __name__ == '__main__':
    app.run(processes=1,threaded=False,host='0.0.0.0', port=6006,debug=False)
