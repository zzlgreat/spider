import requests
from bs4 import BeautifulSoup

def get_con_from_brand(band):
    a = requests.get('https://www.jeebei.com/pingce/brand/b2p1.html').content.decode()
    soup = BeautifulSoup(a, 'html.parser')
    body = soup.find_all(class_='news_list_img')
    for b in body:
        nm = b.find_all('a')[0]['title']
        href = b.find_all('a')[0]['href']
        url = 'http://www.jeebei.com'+href
        b = requests.get(url).content.decode()
        soup1 = BeautifulSoup(b, 'html.parser')
        body = soup1.find_all(class_='tab-pane met-editor clearfix active')[0]

        print(body.find_all(class_='safe_detail')[0].text)
if __name__ == '__main__':
    get_con_from_brand('b2p1')