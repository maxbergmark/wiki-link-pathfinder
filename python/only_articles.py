from urllib.request import urlopen, quote
import json
import time
import queue
import threading
import pickle
import copy

def format_url(cont, apcont):
	c_f = quote(cont.encode('utf-8'))
	ap_f = quote(apcont.encode('utf-8'))
	return "https://en.wikipedia.org/w/api.php?action=query&format=json&prop=&list=allpages&continue=" + c_f + "&apcontinue=" + ap_f + "&apfilterredir=nonredirects&aplimit=max"

def get_url(id, n):
	return "http://en.wikipedia.org/w/api.php?action=query&format=json&pageids=" + str(id) + "&generator=links&gpllimit=" + str(n)

def get_multiple_urls(ids, n):
	s = "%7C".join(ids)
	return "http://en.wikipedia.org/w/api.php?action=query&format=json&pageids=" + s + "&generator=links&gpllimit=" + str(n)

def get_continued(id, cont, n):
	c_f = quote(cont.encode('utf-8'))
	return "http://en.wikipedia.org/w/api.php?action=query&pageids=" + str(id) + "&generator=links&format=json&gplcontinue=" + str(c_f) + "&gpllimit=" + str(n)



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

def worker(process_id, pages):

	global d_count

	d = {}

	# print("Thread", process_id, "started.")

	for page in pages:

		page_url = get_url(page, 500)
		currList = []

		while True:
			data = get_jsonparsed_data(page_url)
			if 'query' in data:
				temp = [int(i) for i in data['query']['pages'].keys() if int(i) > 0]
				currList.extend(temp)
			if not 'continue' in data:
				break
			cont = data['continue']['gplcontinue']
			page_url = get_continued(page, cont, 500)

		d[page] = currList
		d_count += 1
	save_data(process_id, d)


def save_data(process_id, d):
	with open("new_data/data_dump_%d.txt" % (process_id), "w") as f:
		for key in sorted(d.keys()):
			f.write("%X\n" % (key))
			f.write("%X\n" % (len(d[key])))
			for value in sorted(d[key]):
				f.write("%X " % (value))
			f.write("\n")

	print("Process", process_id, "saved.")





global d_count

url = format_url("", "")
count = 0
threads = []
d_count = 0
t0 = time.time()

while True:
	count += 1
	data = get_jsonparsed_data(url)
#	print(data)
	pages = [int(i['pageid']) for i in data['query']['allpages']]
	t = threading.Thread(target=worker, kwargs={"process_id": len(threads), "pages": pages})
	threads.append(t)
	t.start()

	print(count, d_count, "%.2f" % (d_count/(time.time()-t0)), end = ' ')
	if not 'continue' in data:
		break
	cont = data['continue']['continue']
	apcont = data['continue']['apcontinue']
	print("%7.3f %s" % (50000*count/5331333, apcont))
	url = format_url(cont, apcont)

print("Joining threads.")

for thr in threads:
	thr.join()

print("Done!")