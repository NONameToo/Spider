# coding:utf-8
# 使用多线程爬虫

from threading import Thread,Lock
from queue import Queue

from lxml import etree


# 爬取数据类
class Spiders(Thread):
    def __init__(self, crawl, page_queue):
        super().__init__()
        self.crawl = crawl
        self.page_queue = page_queue

    def run(self):
        print('启动%s线程' % self.crawl)
        self.page_spider()
        print('结束%s线程' % self.crawl)

    def page_spider(self):
        while True:
            if self.page_queue.empty():
                break
            else:
                # 取出页数
                page = self.page_queue
                # 组织请求

                url = 'http://www.qiushibaike.com/8hr/page/' + str(page) + '/'
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36',
                    'Accept-Language': 'zh-CN,zh;q=0.8'}
        # 多次尝试失败结束、防止死循环
        timeout = 4
        while timeout > 0:
            timeout -= 1
            try:
                # 发送请求
                content = requests.get(url, headers=headers)
                # 把页面返回的数据放到队列中
                data_queue.put(content.text)
                break
            except Exception as e:
                print('qiushi_spider: %s' % e)
        if timeout < 0:
            print('网页超时timeout %s' % url)


# 解析数据类
class Parsers(Thread):
    def __init__(self, parse, data_queue, lock, file_name):
        super().__init__()
        self.parse = parse
        self.data_queue = data_queue
        self.lock = lock
        self.file_name = file_name

    def run(self):
        print('启动%s线程' % self.parse)
        global total, exitFlag_Parser

    while not exitFlag_Parser:
        try:
            '''
                调用队列对象的get()方法从队头删除并返回一个项目。可选参数为block，默认为True。
                如果队列为空且block为True，get()就使调用线程暂停，直至有项目可用。
                如果队列为空且block为False，队列将引发Empty异常。
                '''
            item = self.data_queue.get(False)
            if not item:
                pass
            # 调用方法对页面数据进行解析
            self.parse_page(item)
            # 每task_done一次 就从队列里删掉一个元素，这样在最后join的时候根据队列长度是否为零来判断队列是否结束，从而执行主线程。
            self.data_queue.task_done()
            print('线程%s, %d' % (self.parse, total))
        except:
            pass

        print('结束%s线程' % self.parse)

    def parse_page(self, item):
        """网页数据解析函数"""
        global total

        try:
            html = etree.HTML(item)
            results = html.xpath('xpath规则xxxxxxxx')
            for result in results:
                # 对数据进行解析的代码xxxxxxxxx

                # 本地文件写入数据的时候一定要加上互斥锁
                with self.lock:
                    self.file_name.write(xxxxxxx)

        except Exception as e:
            print('数据解析线程错误,%s' % e)

        with self.lock:
            total += 1

# 这四个变量作为全局变量，等会在解析数据线程里面会用到
data_queue = Queue()
# 是否可以退出线程,默认是False,当所有的线程都处理完之后,把他改为True
exitFlag_Parser = False
# 线程中的互斥锁
lock = Lock()
total = 0

def main():
    file_name = open('xxx.json', 'a')

    # 创建页码队列
    page_queue = Queue(50)
    start_page = int(input('请输入开始的页码：\n'))
    end_page = int(input('请输入结束的页码：\n'))
    # 把要爬取得页码放入页码队列
    for page in range(start_page, end_page+1):
        page_queue.put(page)


    # 初始化采集线程
    crawl_threads = []

    # 创建线程
    crawl_list = ['crawl-1', 'crawl-2', 'crawl-3','crawl-4']


    # 启动线程
    for crawl in crawl_list:
        thread = Spiders(crawl, page_queue)
        thread.start()
        crawl_threads.append(crawl)




    # 初始化数据处理线程
    parse_threads = []
    # 创建线程
    parse_list = ['parser-1', 'parser-2', 'parser-3','parser-4']


    # 启动线程
    for parse in parse_list:
        thread = Parsers(parse, data_queue, lock, file_name)
        thread.start()
        crawl_threads.append(parse)



    # 等待页码队列清空在继续做下面的事情：
    while not page_queue.empty():
        pass

    # 等待所有的子线程完成
    for each in crawl_threads:
        each.join()

    # 等待数据存放队列清空再继续做下面的事情：
    while not page_queue.empty():
        pass

    # 如果以上条件都满足了,那么,就证明队列中已经没有需要处理的了,可以通知线程退出了
    global exitFlag_Parser # 函数中修改全局变量要声明
    exitFlag_Parser = True

    # 等待所有的数据处理线程完成
    for each in parse_threads:
        each.join()

    print('主线程结束')

    # 关闭文件
    with lock:
        file_name.close()


    # 在对数据进行写入的时候要记得加锁



if __name__ == "__main__":
    main()
