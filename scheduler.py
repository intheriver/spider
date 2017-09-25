# -*- coding: utf-8 -*-

import Queue
import threading
import time
import datetime

g_products_pool = Queue.Queue()
g_is_producer_stop = 0

def add_to_queue(item):
    g_products_pool.put(item)
def get_from_queue(timeout = 5):
    return g_products_pool.get(timeout = timeout)
    
class cproducer_t(threading.Thread):
    q = None
    job = None
    def __init__(self, job):
        threading.Thread.__init__(self)
        self.q = g_products_pool
        g_is_producer_stop = 0
        self.job = job
        
    def run(self):
        self.job()
        g_is_producer_stop = 1
        print("+++++++++++++++++++producer end+++++++++++++++++++")
class cconsumer_t(threading.Thread):
    q = None
    job = None
    jobs_done = 0
    def __init__(self, job):
        threading.Thread.__init__(self)
        self.q = g_products_pool
        self.job = job
        self.jobs_done = 0
        
    def run(self):
        
        self.jobs_done = 0
        while not (self.q.empty() and 1 == g_is_producer_stop):
            try:
                item = get_from_queue()
            except Queue.Empty:
                break
            try:
                #print(self.name + " --> " + item)
                self.job(item) 
                self.q.task_done()
                self.jobs_done = self.jobs_done + 1
            except Exception as ex:
                print("error occured when handle " + str(item) + ", drawback this task")
                print ex
                # break
                add_to_queue(item)
    def get_jobs_done(self):
        return self.jobs_done
class cthread_monitor_t(threading.Thread):
    threads_list = []
    def __init__(self, threads_list):
        threading.Thread.__init__(self)
        self.threads_list = threads_list
    def run(self):
        while 1:
            time.sleep(2)
            is_all_stop = 1
            jobs_result = {}
            for t in self.threads_list:
                if t.isAlive():
                    is_all_stop = 0
                jobs_done = t.get_jobs_done()
                jobs_result[t.name] = jobs_done
            if is_all_stop:
                print("all stopped , monitor stop")
                break
            print jobs_result

class cscheduler_t():
    producer = None
    consumer_list = []
    time_start = None
    time_stop = None
    monitor = None
    is_monitor_ena = True

    def __init__(self, producer_job, consumer_job, consumer_count = 5, is_monitor_ena = False):
        self.consumer_list = []
        self.producer = cproducer_t(producer_job)

        for i in range(0, consumer_count):
            consumer = cconsumer_t(consumer_job)
            consumer.name = "c" + str(i + 1)
            self.consumer_list.append(consumer)
        
        self.monitor = cthread_monitor_t(self.consumer_list)
        self.is_monitor_ena = is_monitor_ena


    def start(self):
        self.time_start = datetime.datetime.now()
        self.producer.start()
        for t in self.consumer_list:
            t.start()
        if self.is_monitor_ena:
            self.monitor.start()
    
    def wait_to_stop(self):
        self.producer.join()
        for t in self.consumer_list:
            t.join()
        if self.is_monitor_ena:
            self.monitor.join()
        self.time_stop = datetime.datetime.now()

    def summary(self):
        total = 0
        jobs_result = {}
        for t in self.consumer_list:
            jobs_done = t.get_jobs_done()
            total = total + jobs_done
            jobs_result[t.name] = jobs_done
        print("++++++++++++++++++++++++++++summary++++++++++++++++++++++")
        print("total:%-10d%s" % (total, str(self.time_stop - self.time_start)))
        for k in jobs_result.keys():
            v = jobs_result[k]
            if total > 0:
                percentage = v * 100.0 / total
            else:
                percentage = 0
            print("%-4s:%-5d\t%.2f%%" % (str(k), v, percentage))
