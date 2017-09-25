# -*- coding: utf-8 -*-
import requests
import os
import re
import scheduler
import time

def get_url_from_content(content):
    url_list = []
    pat = re.compile(r'data-original="([^"]+)"')
    res = re.findall(r'<img [^>]+class="img-responsive lazy image_dta"[^>]+>',
        content);
    for it in res:
        tmp = pat.search(it)
        if tmp is not None:
            url = tmp.group(1)
            url_list.append(url)
    return url_list
def get_url_from_page(url):
    r = requests.get(url, timeout = 5)
    if 200 == r.status_code:
        return get_url_from_content(r.text)
    return []
def producer_job():
    for i in range(0, 1):
        url = "https://www.doutula.com/photo/list/?page=" + str(i + 1)
        url_list = get_url_from_page(url)
        for it in url_list:
            scheduler.add_to_queue(it)
       
def consumer_job(item):
    dir = "image"
    try:
        os.mkdir(dir)
    except:
        pass
    url = item
    filename = os.path.basename(url)
    img_path = os.path.join(dir, filename)
    if not os.path.exists(img_path):
        r = requests.get(url, stream = True, timeout = 20)
        if 200 == r.status_code:
            with open(img_path, "wb") as f:
                for chunk in r:
                    f.write(chunk)
            f.close()

def main():

    spider = scheduler.cscheduler_t(producer_job = producer_job, consumer_job = consumer_job, consumer_count = 5, is_monitor_ena = True)
    spider.start()
    spider.wait_to_stop()
    spider.summary()



if __name__ == '__main__':
    main()