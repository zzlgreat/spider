import requests
from bs4 import BeautifulSoup
from selenium import webdriver
import time
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
import os
try:
    driver = webdriver.Chrome()
except:
    time.sleep(2)
    driver = webdriver.Chrome()

a = requests.get('https://ac.qq.com/Comic/comicInfo/id/17114').content.decode()
soup = BeautifulSoup(a,'lxml')
div = soup.find_all('div',class_ = 'works-chapter-list-wr ui-left')[0]
a = div.find_all('a')
innernum = 1
for aa in a:
    paths = r"C:\Users\chia1\Desktop\javlist" + '\尸兄 第' + str(innernum) + '话'
    os.makedirs(paths)
    innernum +=1
    print(aa['href'])
    path = 'https://ac.qq.com'+aa['href']
    driver.get(path)

    for i in range(1, 400):  # 也可以设置一个较大的数，一下到底
        time.sleep(0.05)
        driver.execute_script('window.scrollBy(0,2000)')
        #driver.execute_script('window.scrollTo(0, document.body.scrollHeight)')
        ActionChains(driver).key_down(Keys.DOWN).perform()
        #js = "var q=document.documentElement.scrollTop={}".format(i * 1000)  # javascript语句
        #driver.execute_script(js)
    num = 0
    for image in driver.find_elements_by_tag_name("img"):

        print(image.size)  # 拿到图片的 size
        print(image.size.get('height'))
        print(image.get_attribute('src'))  # 拿到图片的 text
        if image.size.get('height')>1000 or image.size.get('height')>100:
            num+=1
            img = requests.get(image.get_attribute('src')).content
            paths = os.path.join(paths, str(num) + '.jpg')
            with open(paths, 'wb') as f:
                f.write(img)
            f.close()

    #driver.quit()

