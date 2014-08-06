import Queue
import threading
import sys
import time

class TaskList(object):
    """
    Helper object to construct tassk data for the MethodThreader
    """
    def __init__(self):
        self._task_list = []

    def addTaskToList(self, method, kwargs, name=None):
        """
        method - reference to method to perform work on
        kwargs - dictionary containing the key-value pairs arguments
        name - the name of the task for tracking
        """
        self._task_list.append((method, kwargs, name))

    def getTaskList(self):
        return self._task_list


class TaskResult(object):
    """
    Helper object to organize the response of a threaded task
    """
    def __init__(self):
        self.result = None
        self.error = False
        self.name = None
        self.exception = None
        self.exec_method_name = None
        self.start_time = None
        self.stop_time = None


class MethodThreader():
    """
    Orchestrates tasks and manages errors and output
    """

    def __init__(self, task_list, threads=None, raise_on_error=True):
        """
        Input
        -----
        task_list - an instance of a TaskList object
        threads - the int of the maximum number of threads to run.  By default one thread per task is created
        raise_on_error (boolean) - whether to raise an exception on error or just pass back the results potentially with the exception embedded in the taskresult obj
        """
        self._task_list = task_list.getTaskList()
        self._input_queue = Queue.Queue()
        self._output_queue = Queue.Queue()
        self.queueBuilder()
        self.threads = self._getThreadNum(threads)
        self.raise_on_error = raise_on_error

    def queueBuilder(self):
        for task in self._task_list:
            self._input_queue.put(task)

    def _getThreadNum(self, threads):
        """
        calculates the appropriate thread number.  If threads is NOT provided it will default
        to one thread per task.  If threads providing is greate than the total tasks then threads
        will be one thread per task.  If threads is less than the tasks then threads will be used
        """
        if not threads:
            _thread_num = self._input_queue.qsize()
        elif threads <= 0:
            raise ValueError("Number of threads must be a positive integer.  Invalid number " + str(threads))
        else:
            if threads > self._input_queue.qsize():
                _thread_num = self._input_queue.qsize()
            else:
                _thread_num = threads
        return _thread_num


    def run(self):
        for i in range(self.threads):
            t = MyThread(self._input_queue, self._output_queue)
            #http://www.ibm.com/developerworks/aix/library/au-threadingpython/
            t.setDaemon(True)
            t.start()
        self._input_queue.join()

        #Iterate through our result and decide whether or not to reraise an error
        return_list = []
        while not self._output_queue.empty():
            result = self._output_queue.get()

            if result.error:
                #The task failed and raised an exception
                if self.raise_on_error:
                    raise result.exception[1], None, result.exception[2]

            return_list.append(result)
            self._output_queue.task_done()

        return return_list


class MyThread(threading.Thread):
    """
    Worker threads
    """

    def __init__(self, input_queue, output_queue):
        super(MyThread, self).__init__()
        self._input_q = input_queue
        self._output_q = output_queue

    def run(self):
        while not self._input_q.empty():
            items = self._input_q.get()
            func = items[0]
            kwargs = items[1]
            name = items[2]

            #Construct our return object
            task_result = TaskResult()
            task_result.name = name
            task_result.exec_method_name = func.__name__

            try:
                task_result.start_time = time.time()
                task_result.result = func(**kwargs)

            except Exception:
                #Place the exception on the task_result
                task_result.error = True
                task_result.exception = sys.exc_info()

            finally:
                #Always return the task_result obj
                self._output_q.put(task_result)

                #Always mark the task done to handle exception cases
                self._input_q.task_done()

                task_result.stop_time = time.time()
