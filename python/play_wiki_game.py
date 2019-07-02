from urllib.request import urlopen, quote
import json
import time
import threading
import pickle
import re


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

def get_url(id):
	return "https://en.wikipedia.org/w/api.php?format=json&action=query&pageids=" + str(id)

def get_id(title):
	return "https://en.wikipedia.org/w/api.php?format=json&action=query&titles=" + 	quote(title.encode('utf-8'))

def get_all_ids(process_id, ids, result):
	temp_url = get_url(ids[process_id])
	temp_data = get_jsonparsed_data(temp_url)
	result[process_id] = temp_data['query']['pages'][str(ids[process_id])]['title']

def get_all_n(n):
	global d
	count = 0
	thr = []
	for k in d.keys():
		count += 1
		if (d[k][0] == n):
			print("\r%8d %.3f%%" % (k, 100*count/len(d)), end = '')
			t = threading.Thread(target=play_game, kwargs={"input_id": k, "restrict": True })
			thr.append(t)
			t.start()
			# play_game(k, True)

	for t in thr:
		t.join()

def get_average_distance():
	global d

	s = 0
	d_1 = [0 for _ in range(10)]
	m = 0
	m_id = -1
	unreachable = 0
	for k in d.keys():
		if (d[k][0] < 1000000000):
			if (d[k][0] > m):
				m = d[k][0]
				m_id = k
			if (d[k][0] < 10):
				d_1[d[k][0]] += 1
			# if (d[k][0] > 110):
			# 	if (d[k][0] < 1000000):
			# 		play_game(k)
			s += d[k][0]
		else:
			unreachable += 1

	print("Average distance: %.3f" % (s/len(d)))
	print("Articles without path:", unreachable)
	print("Maximum distance:", m)
	print("Article ID:", m_id)
	for i in range(10):
		print("Articles with distance %7d: %8d (%.2f)" % (i, d_1[i], 100*sum(d_1[:(i+1)])/(len(d)-unreachable)))


def play_game(input_id = None, restrict = False):

	global d
	title = None

	if (input_id == None):
		title = str(input("\nChoose start point: "))
	if (title == 'get_average_distance'):
		get_average_distance()
		return
	if (title == 'get_all_n'):
		n = int(input("Distance: "))
		get_all_n(n)
		return
	if (input_id == None):
		title_url = get_id(title)
		id_data = get_jsonparsed_data(title_url)
		start_id = int(list(id_data['query']['pages'].keys())[0])
	else:
		start_id = input_id
	if not restrict:
		print("ID for starting point:", start_id)

	if (start_id not in d):
		print("Article not found.")
		return

	if (d[start_id][0] > 1000000000):
		print("\nNo path to goal.")
		return
	if not restrict:
		print("Starting search.")
	ids = [None for _ in range(d[start_id][0]+1)]
	id = start_id
	ids[0] = start_id

	for i in range(d[start_id][0]):
		id = d[id][1]
		ids[i+1] = id;
	if not restrict:
		print("All IDs found.")

	result = [None for i in ids]

	count = 0
	if not restrict:
		print("Fetching titles.")
	np = len(ids)

	finished = [False for _ in range(np)]

	threads = []
	for i in range(np):
		t = threading.Thread(target=get_all_ids, kwargs={"process_id": i, "ids": ids, "result": result})
		threads.append(t)
		# print('Starting thread %d.' % (i))
		t.start()

	if not restrict:
		print('Attempting join procedure.')
	for t in threads:
		t.join()

	if not restrict:
		print("Titles fetched.")

	# print(result)
	pattern = re.compile("([0-9]{2,}|User|Category|Wiki|Template|Talk|File|Portal)")
	if (not restrict) or (not pattern.match(result[0])):
		print("\n" + (" ->\n".join(result)), "Done!")
		print("Game completed in %d clicks!" % (len(result)-1))
		print()


global d
game = str(input("\nEnd Point: "))
file_name = "game_data_0.p"
if (game.lower() == "jesus"):
	file_name = "game_data_jesus.p"
if (game.lower() == "reg todd"):
	file_name = "game_data_reg_todd.p"
print('Loading data...')
d = pickle.load(open(file_name, "rb"))
print('Data loaded!')


while True:
	play_game()