import json
import base64
import time
from lxml import etree
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
import urllib.request
import pandas as pd
from bs4 import BeautifulSoup
from selenium.common.exceptions import TimeoutException
from selenium import webdriver
from collections import OrderedDict
from pymysql import connect
import requests
import random
import cv2


def crack_yzm(driver):
    time.sleep(2)
    if '天眼查校验' not in driver.title:
        return driver
    else:
        login_text = driver.page_source
        login_html = etree.HTML(login_text)
        img1_link = login_html.xpath('//*[@id="targetImgie"]/@src')
        img1_path = './verifyCode1.jpg'
        img2_link = login_html.xpath('//*[@id="bgImgie"]/@src')
        img2_path = './verifyCode1_1.jpg'
        urllib.request.urlretrieve(img1_link[0], img1_path)
        urllib.request.urlretrieve(img2_link[0], img2_path)
        wait = WebDriverWait(driver, 2)
        img_1 = cv2.imread('./verifyCode1.jpg')
        img_str = cv2.imencode('.jpg', img_1)[1].tostring()  # 将图片编码成流数据，放到内存缓存中，然后转化成string格式
        aim = base64.b64encode(img_str)  # 编码成base64
        img_2 = cv2.imread('./verifyCode1_1.jpg')
        img_str = cv2.imencode('.png', img_2)[1].tostring()  # 将图片编码成流数据，放到内存缓存中，然后转化成string格式
        pic = base64.b64encode(img_str)  # 编码成base64
        urls = 'http://47.105.205.177:6006/ocr'
        response = json.loads(requests.post(urls, data={'aim': aim, "pic": pic}).content.decode())
        div = wait.until(ec.presence_of_element_located((By.XPATH, '//*[@id="bgImgie"]')))
        print(response)
        if response.get('status') == 'reflesh':
            driver.find_element_by_xpath('//*[@id="refeshie"]').click()
            crack_yzm(driver)
        cors = response.get('move')
        try:
            for cor in cors:
                ActionChains(driver).move_to_element_with_offset(div, cor[0], cor[1]).click().perform()
            submission = driver.find_element_by_xpath('//*[@id="submitie"]')
            submission.click()
        except:
            driver.find_element_by_xpath('//*[@id="refeshie"]').click()
            crack_yzm(driver)
        # if '天眼查校验' in driver.title:
        #     self.crack_yzm(driver)
        return driver
if __name__ == '__main__':
    Url = 'https://antirobot.tianyancha.com/captcha/verify'
    # = r'C:\Users\tanlianai\Desktop\project\Listed_Co\chromedriver.exe'
    driver = webdriver.Chrome(executable_path=r'C:\Users\tanlianai\Desktop\project\Listed_Co\chromedriver.exe')
    driver.get(Url)
    # self.driver.set_window_position(0, 0)
    # self.driver.set_window_size(1920, 1080)
    driver.maximize_window()
    crack_yzm(driver)