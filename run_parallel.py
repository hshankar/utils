#!/bin/env python

import sys
import Queue
import logging
import subprocess
import time
import threading

logging.basicConfig(level=logging.INFO,
                    format='(%(threadName)-9s) %(message)s',)

BUF_SIZE = 10
q = Queue.Queue(BUF_SIZE)
NUM_THREADS = 10

class ConsumerThread(threading.Thread):
  def __init__(self, thread_id, out_file_prefix='out', group=None, target=None, name=None,
               args=(), kwargs=None, verbose=None):
    super(ConsumerThread,self).__init__()
    self.target = target
    self.name = name
    self.out_file = open(out_file_prefix + '_' + str(thread_id), 'w')
    return

  def run(self):
    while True:
      if not q.empty():
        item = q.get()
        if item is None:
          break
        line_no = item[0]
        command = item[1]
        logging.debug('Getting ' + str(command) 
                    + ' : ' + str(q.qsize()) + ' items in queue')
        subp = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        curlstdout, curlstderr = subp.communicate()
        curlstdout.replace('\n', ' ').replace('\r', '')
        self.out_file.write(str(line_no) + '\t' + curlstdout + '\n')
    self.out_file.close()
    return



def print_usage():
  print 'Usage: python run_parallel.py <input_commands_file> <out_file_prefix>'
  sys.exit(1)


def run_all(in_filepath):
  with open(in_filepath) as in_file:
    line_no = 0
    for line in in_file:
      line_no = line_no + 1
      line = line.strip()
      q.put((line_no, line, ))


if __name__ == '__main__':
  if len(sys.argv) != 3:
    print_usage()
  in_filepath = sys.argv[1]
  out_file_prefix = sys.argv[2]
  threads = []
  for i in range(0,NUM_THREADS):
    c = ConsumerThread(name='Thread# ' + str(i), out_file_prefix=out_file_prefix, thread_id=i)
    threads.append(c)
    c.start()
    
  run_all(in_filepath)

  for t in threads:
    # Marker to indicate that all lines are consumed. One for each thread.
    q.put(None)

  logging.info('All inputs added to queue. Waiting for tasks to finish...')
  while not q.empty():
    time.sleep(1)
  logging.info('Joining.')

  for t in threads:
    t.join()
  logging.info('All consumers terminated.')
