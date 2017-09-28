#!/bin/env python

import sys
import Queue
import logging
import subprocess
import threading
import os
import time
import heapq

logging.basicConfig(level=logging.INFO,
                    format='(%(threadName)-9s) %(message)s',)

BUF_SIZE = 1000
q = Queue.Queue(BUF_SIZE)
NUM_THREADS = 7


def get_out_filename(prefix, thread_id):
  return prefix + '_' + str(thread_id)


class ConsumerThread(threading.Thread):
  def __init__(self, thread_id, out_file_prefix='out', group=None, target=None, name=None,
               args=(), kwargs=None, verbose=None):
    super(ConsumerThread,self).__init__()
    self.target = target
    self.name = name
    self.out_file = open(get_out_filename(out_file_prefix, thread_id), 'w')
    return

  def run(self):
    num_lines_processed = 0
    while True:
      if not q.empty():
        item = q.get()
        if item is None:
          break
        num_lines_processed = num_lines_processed + 1
        line_no = item[0]
        command = item[1]
        logging.debug('Getting ' + str(command) 
                    + ' : ' + str(q.qsize()) + ' items in queue')
        subp = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        curlstdout, curlstderr = subp.communicate()
        curlstdout.replace('\n', ' ').replace('\r', '')
        self.out_file.write(str(line_no) + '\tOK\t200\t1.00\tabc123\t' + curlstdout)
        if num_lines_processed % 10 == 0:
          logging.info('Processed ' + str(num_lines_processed) + ' lines.')
    self.out_file.close()
    logging.info('Processed ' + str(num_lines_processed) + ' lines.')
    return



def print_usage():
  print 'Usage: python run_parallel.py <input_commands_file> <out_file_prefix>'
  sys.exit(1)


def add_line(heap, line, tid):
  line = line.strip()
  line_no = int(line.split('\t')[0])
  heapq.heappush(heap, (line_no, (tid, line)))
  
def rem_file(out_files, prefix, tid):
  out_files[tid].close()
  del out_files[tid]
  logging.info('Deleting %s' % get_out_filename(prefix, tid))
  #os.remove(get_out_filename(prefix, tid))


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
  prefix = sys.argv[2]
  
  threads = []
  for i in range(NUM_THREADS):
    c = ConsumerThread(name='Thread# ' + str(i), out_file_prefix=prefix, thread_id=i)
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
  
  logging.info('All consumers terminated. Merging and sorting outputs...')
  out_files = {}
  for i in range(NUM_THREADS):
    out_files[i] = open(get_out_filename(prefix, i), 'r')

  merge_file = open(prefix, 'w')
  heap = []
  empty_file_thread_ids = []
  for thread_id, file in out_files.iteritems():
    line = file.readline()
    if not line:
      empty_file_thread_ids.append(thread_id)
    add_line(heap, line, thread_id)
  
  for tid in empty_file_thread_ids:
    rem_file(out_files, prefix, tid)

  while len(heap) != 0:
    nextelem = heapq.heappop(heap)
    nextline = nextelem[1][1]
    merge_file.write(nextline + '\n')
    removed_tid = nextelem[1][0]
    if removed_tid not in out_files:
      continue
    file = out_files[removed_tid]
    line = file.readline()
    if not line:
      rem_file(out_files, prefix, removed_tid)
      continue
    add_line(heap, line, removed_tid)
    
  merge_file.close()
  logging.info('Outputs merged to %s. Deleting out files...' % prefix)
  logging.info('Done')

  

