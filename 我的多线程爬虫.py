# coding:utf-8

from threading import Thread, Lock
from queue import Queue
import json
from lxml import etree
import requests


# 采集线程退出信号
PAGECRAWL_EXIT = False

# 解析线程退出信号
PARSECRAWL_EXIT = False


# 第二步,写页面数据采集类
# 采用子类创建线程的方式,把处理逻辑写在run()方法中

class PageCrawl(Thread):
    def __init__(self, crawl_name, page_queue, data_queue):
        self.crawl_name = crawl_name
        self.page_queue = page_queue
        self.data_queue = data_queue
        super(PageCrawl, self).__init__()   # 重写了父类的初始化方法,一定要记得调用一下父类的方法

    def run(self):

        print("启动" + self.crawl_name)

        while not PAGECRAWL_EXIT:
            try:
                # 构造请求对象
                page = self.page_queue.get(False)   # 设置为不阻塞的话,如果对列为空会报错
                url = 'http://www.qiushibaike.com/8hr/page/' + str(page) + '/'
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36',
                    'Accept-Language': 'zh-CN,zh;q=0.8'}

                resp_data = requests.get(url, headers=headers).text

                # 把响应数据放到待进行数据处理的队列
                self.data_queue.put(resp_data)
            except Exception as e:
                pass

        print("结束" + self.crawl_name)

# 第五步,创建页面解析类
class ParsePage(Thread):
    def __init__(self, parse_name, data_queue, file_name, lock):
        self.parse_name = parse_name
        self.data_queue = data_queue
        self.file_name = file_name
        self.lock = lock
        super(ParsePage, self).__init__()

    def run(self):
        print("开启" + self.parse_name)

        while not PARSECRAWL_EXIT:
            try:
                # 从队列中取出待解析的数据
                data = self.data_queue.get(False)
                print(data)
                html = etree.HTML(data)
                # 返回所有段子的结点位置，contains()模糊查询方法，第一个参数是要匹配的标签，第二个参数是标签名部分内容
                node_list = html.xpath('//div[contains(@id, "qiushi_tag")]')
                for node in node_list:
                    # xpath返回的列表，这个列表就这一个参数，用索引方式取出来，用户名
                    username = node.xpath('./div/a/@title')[0]
                    # 图片连接
                    image = node.xpath('.//div[@class="thumb"]//@src')  # [0]
                    # 取出标签下的内容,段子内容
                    content = node.xpath('.//div[@class="content"]/span')[0].text
                    # 取出标签里包含的内容，点赞
                    zan = node.xpath('.//i')[0].text
                    # 评论
                    comments = node.xpath('.//i')[1].text

                    items = {
                        "username": username,
                        "image": image,
                        "content": content,
                        "zan": zan,
                        "comments": comments
                    }

                    # self.file_name.write("fushaokai666")

                    with self.lock:
                        self.file_name.write(json.dumps(items, ensure_ascii=False) + "\n")

            except Exception as e:
                print(e)

        print("结束" + self.parse_name)


# 第一步,写主函数

def main():

    # 创建页码队列
    page_queue = Queue()
    # 接收用户请求
    start_page = int(input("请输入开始的页码: "))
    end_page = int(input("请输入结束的页码："))

    # 生成页码，添加到页码队列中
    for page in range(start_page, end_page+1):
        page_queue.put(page)


    # 创建待进行数据处理的队列
    data_queue = Queue()

    # 创建页面数据采集线程
    crawl_list = ["采集线程1", "采集线程2", "采集线程3"]

    # 开启页面数据采集线程
    threads = []
    for crawl_name in crawl_list:
        # 页面数据采集线程采用的子类创建线程的方法,所以这里通过实例化.start()开启线程
        thread = PageCrawl(crawl_name, page_queue, data_queue)
        thread.start()
        threads.append(thread)


    # 第四步,创建解析线程
    Parse_list = ["解析线程一号", "解析线程2号", "解析线程3号"]

    # 用于保存数据的文件
    file_name = open("duanzi.json", "a")


    # 开启页面解析线程,因为解析数据之后要写入数据,防止数据发生错误,采用锁
    lock = Lock()
    parse_threads = []
    for parse_name in Parse_list:
        parse_thread = ParsePage(parse_name, data_queue, file_name, lock)
        parse_threads.append(parse_thread)
        parse_thread.start()


    # 第三步,随时关注到page_queue的情况,如果为空,修改采集线程退出信号为True,即采集线程可以退出
    # 不为空,主线程会在这里等待子线程退出循环
    while not page_queue.empty():
        pass

    global PAGECRAWL_EXIT
    PAGECRAWL_EXIT = True
    print("page_queue为空")


    # 加一个阻塞状态,使所有采集线程结束后再退出
    for thread in threads:
        thread.join()
        print("fushaokai666")


    # 第六步,随时关注到data_queue的情况,如果为空,修改解析线程退出信号为True,即解析线程可以退出
    # 不为空,主线程会在这里等待子线程退出循环
    if not data_queue.empty():
        pass
    global PARSECRAWL_EXIT
    PARSECRAWL_EXIT = True
    print("解析队列为空")

    # 加一个阻塞状态,使所有解析线程结束后再退出
    for parse_thread in parse_threads:
        parse_thread.join()



if __name__ == "__main__":
    main()


