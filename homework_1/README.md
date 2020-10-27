# ETL Practise
> 数据来源：http://snap.stanford.edu/data/web-Movies.html (Links to an external site.)
> ETL要求：
> 1）获取用户评价数据中的253,059个ProductID
> 
> 2）从Amazon网站中利用网页中所说的方法利用爬虫获取253,059个Product页面
> 
> 3）挑选其中的电影页面
> 
> 4）分析其中不同的电影一共有多少部
> 
> 可以参考的工具：
> 1）Pentaho Data Integration: https://sourceforge.net/projects/pentaho/ (Links to an external site.)
> 2） Web爬虫：https://scrapy.org

[TOC]

## 一、Overview

依据上述问题，我们使用以下方法完成ETL需求

1. 对snap的原始数据进行预处理：得到ProductID共：253059个ID保存为：id_list.csv文件
2. 爬取id_list.csv文件中所有ID对应的网页，使用多线程并发提高爬虫效率，使用IP代理池解决Amazon反爬策略
3. 维护并查集，将指向同一个作品的多个ProductID看成一个连通分量。将并查集持久化为：component_mapping.pickle。此时连通分量数为：167893个。
4. 在并查集中的代表ID中进行标签过滤，过滤有Movie Label的电影。数量为：156208个。
5. 在过滤后的ID中使用第三方可信数据集：IMDB，将过滤后的156208个title在IMDB的全部title数据中进行模糊匹配，为每一个title返回一个最佳匹配分数。最佳匹配分数成双峰分布（有匹配到和未匹配到），通过设置合适的threshold对title做过滤。



## 二、Extract

### 1. 爬虫获取网页数据

#### 1.1. 数据预处理

&emsp;&emsp;在进行数据爬取之前，需要对数据源进行预处理：即按照作业要求，提取出用户评价数据中的253,059个 ProductID.
&emsp;&emsp;具体做法为使用 Python 对文件进行预处理：逐行读取文件中的数据，以 productId 字段为关键字进行匹配，如果匹配成功则进行字符串切割，将 productId 后的 Id 字段切割出来，并进行存储以期有效查询字段的持久化.

#### 1.2. URL拼接和请求头构造

&emsp;&emsp;爬虫的基本原理既是自动抓取网页信息的代码，其中使用 httpGet 请求来获取网页的内容，在获取到网页的具体内容之后，可以开始对爬取到的网页内容进行处理。

&emsp;&emsp;要获取一个网页的内容并进行处理，首先需要得到要爬取网页的 URL ，经过归纳总结可得，Amazon 网站中的 Product 页面全部可以通过以下方式构造而来：
```
baseUrl = "https://www.amazon.com/dp/"
url = baseUrl + productId
```
&emsp;&emsp;除此之外，对于没有请求头的 httpGet 请求，会直接被 Amazon 的服务器拒绝访问，所以还需要对请求头进行构造，观察真实浏览器的请求头结构，可以构造结构类似于以下请求头的爬取用请求头：

```
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/86.0.4240.75 Safari/537.36",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "max-age=0"
}
```

&emsp;&emsp;当URL和请求头全部构造完毕之后，可以调用 Python 的 requests 库内方法，结合上一步中所得到的 productId 进行相关网页的爬取进程.虽然目前为止已经可以正常爬取网页，但是 Amazon 所具有的反爬虫措施会将持续发出 httpGet 请求的爬虫 IP 所封禁，故仍需要使用 IP 池进行进一步优化以期获取全部正确结果，具体优化措施将在 一.3 陈述.


### 2 多线程/多进程优化

#### 2.1 优化原理：

&emsp;&emsp;十万级网页数据的爬取的性能瓶颈主要在于网络IO上和硬盘IO上。

&emsp;&emsp;通过htop工具可以检测到服务器的CPU处于较为空闲的状态。因此使用多线程方法可以使得在单核CPU上减少因为IO阻塞而浪费的CPU性能。

&emsp;&emsp;使用多进程并行处理，我们采用的8进程同时占用全部CPU资源，通过对ID取余的方法并行爬取优化。


### 3 ip代理池反爬优化


#### 3.1. 反爬原理：

&emsp;&emsp;代理主要为了解决我们被目标网站封 IP 的问题。在对只使用前两个技术爬取的网页结果进行分析，发现有大量网页为错误格式，如：验证码检验，somethingWrong等IP被封问题。

&emsp;&emsp;可以利用网上大量公开的免费代理，和购买付费的代理 IP。

&emsp;&emsp;但是不论是免费的还是付费的代理IP，都不能保证它们每一个都是可用的，毕竟可能其他人也可能在用此 IP 爬取同样的目标站点而被封禁，或者代理服务器突然出故障或网络繁忙。一旦我们选用了一个不可用的代理，势必会影响我们爬虫的工作效率。 所以说，在用代理时，我们需要提前做一下筛选，将不可用的代理剔除掉，保留下可用代理，接下来在获取代理时从可用代理里面取出直接使用就好了。

- 代理池，提供如下功能：
    - 定时抓取免费代理网站，简易可扩展。
    - 使用 Redis 对代理进行存储并对代理可用性进行排序。
    - 定时测试和筛选，剔除不可用代理，留下可用代理。
    - 提供代理 API，随机取用测试通过的可用代理。

#### 3.2. 代理池实现

&emsp;&emsp;存储在这里我们使用 Redis 的有序集合，集合的每一个元素都是不重复的，对于代理池来说，集合的元素就变成了一个个代理，也就是 IP 加端口的形式，如:60.207.237.111:8888。
&emsp;&emsp;这样的一个代理就是集合的一个元素。另外有序集合的每一个元素还都有一个分数字段，分数是可以重复的，是一个浮点数类型，也可以是整数类型。该集合会根据每一个元素的分数对集合进行排序，数值小的排在前面，数值大的排在后面，这样就可以实现集合元素的排序了。对于代理池来说，这个分数可以作为我们判断一个代理可用不可用的标志。

基于：https://github.com/Python3WebSpider/ProxyPool
可以快速搭建一个可用代理池。
另外：http://www.dobel.cn/ 
可以提供高可用的付费IP。


## 三、Transform

### 1. 并查集去重

#### 1.1.去重原理

&emsp;&emsp;通过对抓取后的网页数据进行分析，可以发现在：Amazon.com/dp/ID 对应的网页中，位于\<div id="MediaMatrix">\</div>中的链接地址为同一个电影的不同产品。
&emsp;&emsp;如：Get Carter (1971)(ID为B005SYZZ7Q)网页的上述div块中有若干同电影的DVD，VHS，PrimeVideo等不同产品。这些产品ID本质都指向是同一部ID。
&emsp;&emsp;因此可以利用BeautifulSoup库对爬取的HTML网页文件进行解析，筛选出位于上述div块中所有的相关ID。

#### 1.2. 并查集的使用

&emsp;&emsp;由于指向同一个电影的不同产品网页之间不一定互相全连通。（即：ID为01，02，03指向同一个电影，其中01页面可以获得02和03的ID，但03的页面不一定能获取到02的ID）因此不能通过简单去重实现，所以我们选用了并查集这一数据结构对去重进行维护。

&emsp;&emsp;同时我们在实现并查集的基础上利用pickle库实现了并查集的持久化。



### 2. 标签过滤

#### 2.1. 页面分类

数据集中ID涉及到的Amazon网页可以分为两类，一类为prime video类别网页，一类是普通网页。两种网页的html格式区别较大，因此分别处理。

#### 2.2 普通页面标签过滤

普通页面可以通过解析HTML文件中的id="wayfinding-breadcrumbs_feature_div"的div块中解析电影类别Label。首先从并查集去重后的ID集合中过滤掉是普通页面类别且Label不是Movies & TV 的ID。

#### 2.3 prime video页面标签过滤

prime video可以通过解析HTML文件中的id="TitleType"的div块中解析电影类别Label。从并查集去重后的ID集合中过滤掉是普通页面类别且Label不是Movies的ID。


### 3. 模糊匹配

#### 3.1 Levenshtein Distance
初步采用Levenshtein Distance来计算字符串之间的相似度。

Levenshtein Distance 算法，又叫 Edit Distance 算法，是指两个字符串之间，由一个转成另一个所需的最少编辑操作次数。许可的编辑操作包括将一个字符替换成另一个字符，插入一个字符，删除一个字符。一般来说，编辑距离越小，两个串的相似度越大。

#### 3.2 分数计算

1. 计算长度T = len(str1) + len(str2)
2. 计算分数S = (len(str1) + len(str2) - Levenshtein) / (len(str1) + len(str2))

#### 3.3 注

1. 这种Levenshtein Distance方法存在的问题较大，因此尚未采信模糊匹配的过滤结果。在后续阶段将继续探索更好的模糊匹配的算法，用来利用IMDB的title进行过滤
2. 基于字典树的精准匹配无法利用IMDB的title进行过滤，因为Amazon的title和IMDB的title区别过大，有大量符号替代/插入，表述差异等问题


## 四、Assignment Solutions

#### 1. 获取用户评价数据中的253,059个ProductID

&emsp;&emsp;如 二.1.1 中所述，采用 Python 逐行读取原始文件中的内容，以 productId 作为关键字进行匹配，匹配成功则进行字符串切割，将 productId 所对应的值切割出来，并存储到另一个文件中以实现持久化.

#### 2. 从Amazon网站中利用网页中所说的方法利用爬虫获取253,059个Product页面

&emsp;&emsp;如 二.1.2 和 二.3 中所述，利用 Python 的 requests 库利用构造好的 URL 和 http 请求头发送 httpGet 请求（添加 IP 池以解决 IP 被封禁的问题）爬取网页，获取页面内容并加以保存.
&emsp;&emsp;其中，约有超过1300个页面由于年份过久、商品下架等原因，已经无法获取其页面内容.

#### 3. 挑选其中的电影页面

- 普通页面：如 三.2.2 所述，普通页面可以通过解析 HTML 文件中的 id = "wayfinding-breadcrumbs_feature_div" 的 div 块中解析电影的类别 label，从中可以挑选出 Label 是 Moveies & TV 的 ID
- Prime Video 页面：如 三.2.3 所述， Prime Video 可以通过解析 HTML 文件中的 id = "TitleType" 的 div 块中解析电影类别 Label，从中可以挑选出 Label 是 Movies 的 ID

#### 4. 分析其中不同的电影一共有多少部

&emsp;&emsp;如 三.1 所述可以采用并查集来实现电影页面的去重：从页面中的 \<div id=“MediaMatrix”>\</div> 提取出同一个电影的不同产品，并且由于不同产品页面内不一定连通，采用并查集来实现不同产品的合并和去重.

&emsp;&emsp;在实际的操作过程中， 3 和 4 是同时进行的，即同时进行电影页面的挑选和去重.



