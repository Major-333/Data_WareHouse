import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from UnionFind import UnionFind
import pickle
import os
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

def get_imdb_title_list(imdb_path):
    return np.array(pd.read_csv(imdb_path)['title'])

def get_filtered_list(filtered_path):
    return np.array(pd.read_csv(filtered_path)['p_id'])

def get_all_titles(file_list, file_path, filtered_list):
    # 新建 dataFrame
    df = pd.DataFrame(columns=["p_id", "title"])
    error_df = pd.DataFrame(columns=["p_id"])
    # 遍历 file获取title
    for index, file_name in enumerate(file_list):
        # 测试代码
        if index % 10000 == 0:
            df.to_csv('data/titles_list.csv')
            error_df.to_csv('data/error_titles_list.csv')
        ct_id = file_name.split('.')[0][-10:]
        if index % 100 == 0:
            print('[index]:', index)
        # label需要符合要求
        if ct_id in filtered_list:
            # 解析html文件
            content = open(file_path+'/'+file_name, 'r').read()
            soup = BeautifulSoup(content, 'lxml')
            # 获取当前title
            try:
                spans = soup.find_all(id="productTitle")
                ct_title = spans[0].string.replace('\n', '')
                ct_title = ct_title.split('(')[0]
                ct_title = ct_title.split('[')[0]
                df = df.append(pd.DataFrame({'p_id':[ct_id], 'title':[ct_title]}),ignore_index=True)
            except :
                error_df = error_df.append(pd.DataFrame({'p_id':[ct_id]}),ignore_index=True)



if __name__ == '__main__':
    imdb_path = 'data/imdb_movies.csv'
    filtered_path = 'data/filtered.csv'
    # web file
    path = '/media/googlecamp/Teclast_S301/zips/_unzip'  
    f_list = os.listdir(path)
    imdb_title_list = get_imdb_title_list(imdb_path)
    filtered_list = get_filtered_list(filtered_path)
    get_all_titles(file_list=f_list, file_path=path, filtered_list=filtered_list)

            
