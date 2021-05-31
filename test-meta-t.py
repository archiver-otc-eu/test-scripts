#!/usr/bin/env python3

import json
import threading
from time import time, sleep
import random
import requests
from vars import oz_token, onezone, time_to_run, nr_names, nr_words, nr_threads, harvester_id, index_id, report_interval

def load_words_list():
    word_source_url = 'https://svnweb.freebsd.org/csrg/share/dict/words?view=co&content-type=text/plain'
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/77.0.3865.90 Chrome/77.0.3865.90 Safari/537.36'
    }
    response = requests.get(word_source_url, headers=headers)
    words = [word.decode('utf-8') for word in response.content.splitlines()]
    return {
        'name_words': [w for w in words if w[0].isupper() and not w.isupper()],
        'words': [w for w in words if not w[0].isupper() and not w.isupper()],
    }

def lookup_rnd_meta(s):
    statuses = ('open', 'pending', 'closing', 'closed', 'rejected')
    cr = random.choice(names)
    st = { 'status': {
            'now': random.choice(statuses)
        }
    }
    creator_body='{\\"from\\":0,\\"size\\":10,\\"sort\\":[{\\"_score\\":\\"desc\\"}],\\"query\\":{\\"bool\\":{\\"must\\":[{\\"simple_query_string\\":{\\"query\\":\\"'+cr+'\\",\\"fields\\":[\\"creator\\"],\\"default_operator\\":\\"and\\"}}]}}}'

    t1 = time()
    try:
        r = s.post("https://"+onezone+"/api/v3/onezone/harvesters/"+harvester_id+"/indices/"+index_id+"/query", headers=myheaders, data='{"method":"post","path":"_search","body":"'+creator_body+'"}')
        t2 = time()
        if r.status_code != 200:
            print("Lookup request REST call failed with code:", r.status_code)
        return(t2-t1)
    except (requests.exceptions.ConnectionError) as e:
        print("Time:", round(time()-t1,4), ", My exception:", e)
        return(time()-t1)


def send_graphite(graphite_host, graphite_port, metrics):
    sock = socket.socket()
    sock.connect((graphite_host, int(graphite_port)))
    for k,v in metrics.items():
        msg="reg-test"+"."+socket.gethostname()+"."+str(k)+" "+str(v)+" "+str(int(time()))+"\n"
        sock.send(msg.encode())

class monitor (threading.Thread):
   def __init__(self, threadID, name):
      threading.Thread.__init__(self)
      self.threadID = threadID
      self.name = name
   def run(self):
      global shared
      start_time = time()
      ttmin = 9999
      ttmax = 0 
      # grs = graphite_url.split(':')
      # graphite_host = grs[0]
      # graphite_port = grs[1]
      f = True
      while True:
          t1 = time()
          if f:
              prev_shared = 1
              f = False
          else:
              prev_shared = cur_shared
          sleep(report_interval)
          cur_shared = shared
          if time() - start_time >= time_to_run:
              # print ("Exiting " + self.name)
              print("\n=================== Total =======================")
              print(round(shared/(time() - start_time),1), "lookups/sec, Response time[s] (avg: ", round(((time() - start_time)*nr_threads)/shared,4), ", min: ", round(ttmin,4), ", max: ", round(ttmax, 4),")")              
              return
          t2 = time()
          tmax = tmin = rt[prev_shared]
          sum = 0
          not_finished = 0
          n=0
          for i in range(prev_shared,cur_shared):
              if rt[i] != 0:
                  sum += rt[i]
                  n+=1
                  if rt[i] < tmin:
                      tmin = rt[i]
                      if tmin < ttmin:
                          ttmin = tmin
                  if rt[i] > tmax:
                      tmax = rt[i]
                      if tmax > ttmax:
                          ttmax = tmax
              else:
                  not_finished += 1
          if n != 0:        
              print(round((cur_shared-prev_shared)/(t2-t1),1), " lookups/sec, Response time[s] (avg: ", round(sum/n,4), ", tmin: ", round(tmin,4), ", tmax: ", round(tmax, 4),")", sep="")
              # metrics = {
              #     'number-of-files':cur_shared,
              #     'file-reg-time-avg':sum/n,
              #     'file-reg-time-min':tmin,
              #     'file-reg-time-max':tmax
              # }
              # send_graphite(graphite_host, graphite_port, metrics)
              
    
class myThread (threading.Thread):
   def __init__(self, threadID, name):
      threading.Thread.__init__(self)
      self.threadID = threadID
      self.name = name
   def run(self):
      global shared
      global rt
      s = requests.Session()
      start_time = time()
      # print ("Starting " + self.name)
      while True:
          lock.acquire()
          shared += 1
          cur_shared = shared
          lock.release()
          if time() - start_time >= time_to_run:
              return
          rt[cur_shared] = lookup_rnd_meta(s)

if __name__ == "__main__":
    exitFlag = 0
    shared = 0

    myheaders = {"X-Auth-Token": oz_token }
    myheaders["Content-Type"] = "application/json"

    rt = [0.0] * time_to_run * 400        # Assuming 400 req/s
    # Lock
    lock = threading.Lock()

    sample_data = load_words_list()
    random.seed(1)
    names = [random.choice(sample_data['name_words']) for _ in range(nr_names)]
    words = [random.choice(sample_data['words']) for _ in range(nr_words)]
    
    # Create monitor/reporting thread
    tm = monitor(0, "TM0")
    tm.start()
    # Create worker threads
    ths = []
    print("Starting", nr_threads, "threads...")
    for t in range(nr_threads):
        tt = myThread(t, "T"+str(t))
        ths.append(tt)
        tt.start()

    print ("Waiting for joins...")
    for t in range(nr_threads):
        ths[t].join()
    t_end = time()
    print ("Number of lookups: ", shared)
    
    tm.join()
    exit(0)
    
