import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from UnionFind import UnionFind
import pickle
import os
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

class Filter(object):
    
    def __init__(
        self,
        save_path,
        prime_video_path,
        label_path,
        deduplicate_path,
        file_list,
        file_path,
        pv_filter_path,
        label_filter_path
    ):
        # init path
        self._uf_dict = pickle.load(open(deduplicate_path, 'rb'))
        self._save_path = save_path
        self._pv_path = prime_video_path
        self._label_path = label_path
        self._deduplicate_path = deduplicate_path
        self._file_list = file_list
        self._file_path = file_path
        self._pv_filter_path = pv_filter_path
        self._label_filter_path = label_filter_path
        # filter
        self._filtered_list = []
        self.get_deduplicate_list()
        final = np.union1d(self.filter_by_label(),self.filter_prime_video())
        print('[length of final]: ', len(final))
        
    def get_deduplicate_list(self):
        for index, key in enumerate(self._uf_dict):
            if key == sorted(list(self._uf_dict[key]))[0]:
                self._filtered_list.append(key)
        self._filtered_list = np.array(self._filtered_list)
        print('[length of deduplicate_list]: ', len(self._filtered_list))
            
    def filter_by_label(self, reCalculate=False):
        # 注意，这个过滤器基于deduplicate后的filtered_list
        # 注意，这个过滤器会滤掉所有prime video
        if reCalculate:
            tmp_list = []
            df = pd.read_csv(self._label_path)
            label_list = np.array(df['title'])
            id_list = np.array(df['id'])
            for label, p_id in zip(label_list, id_list):
                if label != '[]' and 'Movies' in label and p_id in self._filtered_list:
                    tmp_list.append(p_id)
            ans = np.intersect1d(np.array(self._filtered_list), np.array(tmp_list)) 
            print('[length of filtered_list by label]: ', ans.shape)
            pd.DataFrame(ans,index=None, columns=['p_id']).to_csv(self._label_filter_path)
            print('finish write to label_filter_list.csv')
            return ans
        else:
            return np.array(pd.read_csv(self._label_filter_path)['p_id'])

        
    def filter_prime_video(self, reCalculate=False):
        # 注意，这个过滤器基于deduplicate后的filtered_list
        # 注意，这个过滤器只能保留prime video
        if reCalculate:

            tmp_list = []
            prime_video_list = np.array(pd.read_csv(self._pv_path)['p_id'])
            print(prime_video_list.shape)
            for index, file_name in enumerate(self._file_list):
                if index % 5000 == 0:
                    print('[index]:', index)          
                ct_id = file_name.split('.')[0][-10:]
                if ct_id in prime_video_list and ct_id in self._filtered_list:      
                    # 解析html文件
                    content = open(self._file_path+'/'+file_name, 'r').read()
                    soup = BeautifulSoup(content, 'lxml')
                    try:
                        div = str(soup.find(id="dv-action-box-wrapper"))
                        if 'titleType=movie' in div:
                            tmp_list.append(ct_id)
                    except :
                        pass
            print('[length of filtered_list by prime video]: ', len(tmp_list))
            pd.DataFrame(np.array(tmp_list),index=None, columns=['p_id']).to_csv(self._pv_filter_path)
            print('finish write to pv_filter_list.csv')
            return np.array(tmp_list)
        else:
            return np.array(pd.read_csv(self._pv_filter_path)['p_id'])

    if __name__ == '__main__':
        path = '/media/googlecamp/Teclast_S301/zips/_unzip'  # 待读取文件的文件夹绝对地址
        f_list = os.listdir(path)  # 获得文件夹中所有文件的名称列表
        label_path = 'data/labels.csv'
        save_path = 'data/filtered.csv'
        deduplicate_path = 'data/UFMap/component_mapping.pickle'
        prime_video_path = 'data/error_id_list.csv'
        pv_filter_path = 'data/pv_filter_list.csv'
        label_filter_path = 'data/label_filter_list.csv'
        filter = Filter(save_path, prime_video_path, label_path, deduplicate_path, f_list, path, pv_filter_path,label_filter_path)