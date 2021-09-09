import time
from xml.dom.minidom import Document
import requests
import json
from bs4 import BeautifulSoup
import os
import zipfile

# url1 = "https://api.copymanga.com/api/v3/comic/" + pathWord + "/group/default/chapters?limit=500&offset=0&platform=3"
# url2 = "https://api.copymanga.com/api/v3/comic/" + pathWord + "/chapter2/" + uuid + "?platform=3"
# url3 = "https://api.copymanga.com/api/v3/comic2/" + pathWord + "?platform=3"
def get_comic():
    for i in range(6, 551):
        url0 = "https://www.copymanga.com/comics?ordering=-popular&offset=" + str(i * 50) + "&limit=50"
        a = requests.get(url0).content
        soup = BeautifulSoup(a, 'lxml')
        div = soup.find_all('div', class_="row exemptComic-box")[0]
        a = div.find_all('a')
        for n, h in enumerate(a):
            if n % 2 == 0:
                if h['href'].split('/')[1] == 'comic':
                    pathWord = h['href'].split('/')[2]
                    print(pathWord)
                    url1 = "https://api.copymanga.com/api/v3/comic/" + pathWord + "/group/default/chapters?limit=500&offset=0&platform=3"
                    chapt = requests.get(url1).content.decode()
                    chapt = json.loads(chapt)
                    l = chapt.get('results').get('list')

                    url3 = "https://api.copymanga.com/api/v3/comic2/" + pathWord + "?platform=3"
                    a = requests.get(url3).content.decode()
                    a_info = json.loads(a).get('results')
                    Series = a_info.get('comic').get('name')
                    try:
                        Writer = a_info.get('comic').get('author')[0].get('name')
                    except:
                        Writer = ''
                    Genre = ''
                    try:
                        for g in a_info.get('comic').get('theme'):
                            Genre = g.get('name') + ',' + Genre
                    except:
                        pass
                    Summary = a_info.get('comic').get('brief')
                    Year = a_info.get('comic').get('datetime_updated').split('-')[0]
                    Month = a_info.get('comic').get('datetime_updated').split('-')[1]
                    Day = a_info.get('comic').get('datetime_updated').split('-')[2]
                    path = r'F:\manga\copymanga'
                    paths = os.path.join(path, Series)
                    if os.path.exists(paths):
                        pass
                    else:
                        os.mkdir(paths)

                    for comic in l:
                        # time.sleep()
                        print(comic)
                        uuid = comic.get('uuid')
                        index = comic.get('index')
                        name = comic.get('name')
                        paths1 = os.path.join(paths, Series + " " + str(index) + ' '+ name)
                        if os.path.exists(paths1):
                            print('存在')
                            continue
                        else:
                            os.mkdir(paths1)
                        try:
                            doc = Document()
                            root = doc.createElement('ComicInfo')
                            doc.appendChild(root)
                            objectcontent = doc.createElement("Series")
                            objectcontenttext = doc.createTextNode(Series)
                            objectcontent.appendChild(objectcontenttext)
                            root.appendChild(objectcontent)
                            objectcontent = doc.createElement("Title")
                            objectcontenttext = doc.createTextNode(name)
                            objectcontent.appendChild(objectcontenttext)
                            root.appendChild(objectcontent)
                            objectcontent = doc.createElement("Writer")
                            objectcontenttext = doc.createTextNode(Writer)
                            objectcontent.appendChild(objectcontenttext)
                            root.appendChild(objectcontent)
                            objectcontent = doc.createElement("Genre")
                            objectcontenttext = doc.createTextNode(Genre)
                            objectcontent.appendChild(objectcontenttext)
                            root.appendChild(objectcontent)
                            objectcontent = doc.createElement("Summary")
                            objectcontenttext = doc.createTextNode(Summary)
                            objectcontent.appendChild(objectcontenttext)
                            root.appendChild(objectcontent)
                            objectcontent = doc.createElement("Year")
                            objectcontenttext = doc.createTextNode(Year)
                            objectcontent.appendChild(objectcontenttext)
                            root.appendChild(objectcontent)
                            objectcontent = doc.createElement("Month")
                            objectcontenttext = doc.createTextNode(Month)
                            objectcontent.appendChild(objectcontenttext)
                            root.appendChild(objectcontent)
                            objectcontent = doc.createElement("Day")
                            objectcontenttext = doc.createTextNode(Day)
                            objectcontent.appendChild(objectcontenttext)
                            root.appendChild(objectcontent)
                            pathsj = os.path.join(paths1, 'ComicInfo.xml')
                            f = open(pathsj, 'w')
                            doc.writexml(f, indent='\t', newl='\n', addindent='\t', encoding='gbk')
                            f.close()
                            url2 = "https://api.copymanga.com/api/v3/comic/" + pathWord + "/chapter2/" + uuid + "?platform=3"
                            print(url2)
                            try:
                                hua = requests.get(url2).content.decode()
                                hua = json.loads(hua)
                                cons = hua.get('results').get('chapter').get('contents')
                                words = hua.get('results').get('chapter').get('words')
                                # print(cons)
                                # print(words)
                                for num, c in zip(words, cons):
                                    print(c)
                                    img = requests.get(c.get('url')).content
                                    if int(num) < 100:
                                        pathsj = os.path.join(paths1, str(num).zfill(3) + '.jpg')
                                    else:
                                        pathsj = os.path.join(paths1, str(num) + '.jpg')
                                    with open(pathsj, 'wb') as f:
                                        f.write(img)
                                    f.close()
                            except Exception as e:
                                print(e)
                                continue
                            # pathz = os.path.join(paths,Series + " " + str(index) + ' '+ name)
                            create_zip_file = zipfile.ZipFile(paths1 + '.zip', mode='a',
                                                              compression=zipfile.ZIP_DEFLATED)
                            new_file_path = os.path.join(paths1, r'ComicInfo.xml')
                            file_name = 'ComicInfo.xml'
                            create_zip_file.write(new_file_path, file_name)
                            for current_path, subfolders, filesname in os.walk(paths1):
                                print(current_path, subfolders, filesname)
                                #  filesname是一个列表，我们需要里面的每个文件名和当前路径组合
                                for file in filesname:
                                    # 将当前路径与当前路径下的文件名组合，就是当前文件的绝对路径
                                    create_zip_file.write(os.path.join(current_path, file),
                                                          os.path.join(Series + " " + str(index) + ' ' + name, file))
                            # 关闭资源
                            create_zip_file.close()

                        except:
                            pass


if __name__ == '__main__':
    get_comic()








