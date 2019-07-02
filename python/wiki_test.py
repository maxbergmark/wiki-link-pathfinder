from urllib.request import urlopen, quote
import json
import time
import queue
import threading
import pickle
import copy

# global q
# global m
# global saved
# global processed
# global lock
# global stopped
# global save_events




def get_jsonparsed_data(url):
    response = None
    tries = 0
    while not response:
    	try:
    		response = urlopen(url)
    	except:
    		# print('Exception')
    		tries += 1
    		time.sleep(0.2)
    	if (tries > 0 and tries % 10 == 0):
    		print('Failed attempt number %d.' % (tries))
    data = bytes(response.read()).decode('utf-8')
    return json.loads(data)

def get_url(id, n):
	return "http://en.wikipedia.org/w/api.php?action=query&format=json&pageids=" + str(id) + "&generator=links&gpllimit=" + str(n)

def get_continued(id, cont, n):
	c_f = quote(cont.encode('utf-8'))
	return "http://en.wikipedia.org/w/api.php?action=query&pageids=" + str(id) + "&generator=links&format=json&gplcontinue=" + str(c_f) + "&gpllimit=" + str(n)


def save_data():
	global q
	global m
	global saved
	global processed
	global lock
	global sync_interval
	global save_events
	global last_print

	print('Starting save operation.')
	tot = sum([len(m[i]) for i in m.keys()])
	print('Added connections:', tot)

	d = {}

	n2 = q.qsize()
	l2 = list(q.queue)
	d['q'] = l2
	# d['m'] = m

	d['saved'] = saved+sync_interval
	d['save_events'] = save_events+1
	d['processed'] = processed

	print('Everything processed, dumping to file.')

	pickle.dump(m, open("data/backup" + str(save_events) + "_" + str(sync_interval) + ".p", "wb"))

	m = {}
	save_events += 1
	last_print = 0

	pickle.dump(d, open("data.p", "wb"))
	lock = False
	print("Data saved!")

def worker(process_id):

	global q
	global m
	global saved
	global processed
	global lock
	global stopped
	global np
	global n_start
	global sync_interval
	global last_print

	num = 500
	sync_interval = 100000
	last_print = 0
	print_interval = 400


	# print('Thread %d started.' % (process_id))

	while not q.empty():
		article_id = int(q.get())
		while article_id in m:
			article_id = int(q.get())

		stopped[process_id] = True
		# print(article_id)
		url = get_url(article_id, num)

		stopped[process_id] = False
		
		count = 0
		currList = []
		t1 = time.time()
		if (process_id == 0 and len(m) >= last_print):
			print("%8d %8d %8d %8d %7.2f %8.2f" % (int(article_id), len(processed), q.qsize(), saved+len(m), (saved + len(m)-n_start)/(t1-t0), 100*(saved+len(m))/(1+len(processed))))
			last_print += print_interval

		while True:

			# print('looping', url)
			data = get_jsonparsed_data(url)
			if 'query' in data:
				temp = [int(i) for i in data['query']['pages'].keys() if int(i) > 0]
				currList.extend(temp)
			if not 'continue' in data:
				break
			cont = data['continue']['gplcontinue']
			url = get_continued(article_id, cont, num)
			count += 1
			# print(count*num)
		

		if (process_id == 0 and len(m) > sync_interval):
			print('\nPreparing for sync.')
			while (sum(stopped[1:]) < np-1):
				# print("sum:", stopped)
				time.sleep(0.01)
			save_data()
			saved += sync_interval
			stopped = [False for _ in range(np)]
			print("\n%8s %8s %8s %8s %7s %8s" % ('article#', 'total', 'in_queue', 'complete', 'art/sec', 'progress'))


		if (len(m) > sync_interval):
			lock = True
			stopped[process_id] = True
			while lock:
				time.sleep(0.01)

		m[int(article_id)] = currList

		for i in m[int(article_id)]:
			if not int(i) in processed:
				q.put(int(i))
				processed.add(int(i))

	# print("Process %d finished!" % (process_id))

	finished[process_id] = True



global q
global m
global saved
global save_events
global processed
global lock
global stopped
global np
global n_start
global finished

m = {}
q = queue.Queue()
saved = 0
processed = set()
np = 200
t0 = time.time()
save_events = 0
n_start = 0
finished = [False for _ in range(np)]

try:
	print('\nLoading data.')
	d = pickle.load(open("data.p", "rb"))
	print('Data loaded from file.')
	q.queue = queue.deque([int(i) for i in d['q']])
	print('Queue loaded.')
	processed = d['processed']
	processed = set([int(i) for i in processed])
	print('Process set loaded.')
	saved = d['saved']
	save_events = d['save_events']
	n_start = saved
except:
	print('Data not loaded, fresh start.')
	q.put(12610483)
	# processed.add(12610483)

print('Starting %d threads.' % (np))

lock = False
stopped = [False for _ in range(np)]
# article_id = '12610483'
# article_id = '4408452'
# q.put(article_id)

print("\n%8s %8s %8s %8s %7s %8s" % ('article#', 'total', 'in queue', 'complete', 'art/sec', 'progress'))


threads = []
for i in range(np):
	t = threading.Thread(target=worker, kwargs={"process_id": i})
	threads.append(t)
	# print('Starting thread %d.' % (i))
	t.start()

while False in finished:
	time.sleep(.01)
print('Attempting join procedure.')
for t in threads:
	t.join()


print("Finished, saving last data.")

save_data()

print("Data saved, execution complete!")