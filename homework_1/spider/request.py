#! -*- encoding:utf-8 -*-

import pandas as pd
import numpy as np
import requests
import time
from bs4 import BeautifulSoup
from multiprocessing import Pool
from threading import Thread

#http代理接入服务器地址端口
proxyHost = ""
proxyPort = ""
proxyUser = ""
proxyPass = ""

proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
        "host": proxyHost,
        "port": proxyPort,
        "user": proxyUser,
        "pass": proxyPass,
}

proxies = {
        "http": proxyMeta,
        "https": proxyMeta,
}

# 配置信息
file_name = '../dataProcess/data/id_list/id_list.csv'
aby_dir = '../dataProcess/data/webPages/'
batch_size = 8
threads_num = 8
sleep_time = 0.5
base_url = 'https://www.amazon.com/dp/'
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36",
    "Connection": "closer",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "max-age=0"
}


def thread_loop(t_id, t_max):
    # 读取p_id
    df = pd.read_csv(file_name)['p_id']
    p_id_array = df.values
    for index, p_id in enumerate(p_id_array):
        if (index + 1) % t_max == t_id:
            attempts, success = 0, False
            while attempts < 5 and not success:
                try:
                    while True:
                        url = base_url+p_id
                        result = requests.get(url, proxies=proxies, headers=headers)
                        result = result.text.encode('gbk', 'ignore').decode('gbk')
                        soup = BeautifulSoup(result, 'lxml')
                        movie_title = str(soup.select('title')[0].getText())
                        if (movie_title != 'Robot Check') and (movie_title != 'Sorry! Something went wrong!') and (movie_title != 'Amazon.com'):
                            print('[t_id]: ', t_id, " [p_id]:", p_id)
                            fo = open(aby_dir + p_id + ".html", "w")
                            fo.write(result)
                            fo.close()
                            success = True
                            break
                        else:
                            print('[title error]', movie_title, '[t_id]: ', t_id, " [p_id]:", p_id)
                except Exception as e:
                    print('[error]', e)
                    attempts += 1
                    time.sleep(sleep_time)
                    if attempts == 3:
                        break


if __name__ == '__main__':
    for t_id in range(threads_num):
        print('[Thread]:', t_id, ' begins')
        t = Thread(target=thread_loop, args=(t_id,threads_num,))
        t.start()

