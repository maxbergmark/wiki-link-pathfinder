import ctypes
from flask import Flask, render_template, request
from waitress import serve
import json
import os

lib = ctypes.CDLL('/home/max/Documents/wiki-link-pathfinder/cpp/src/wiki_searcher.so')
lib.search_wiki.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_int]
lib.search_wiki.restype = ctypes.c_void_p
lib.free_pointer.argtypes = [ctypes.c_void_p]
lib.free_pointer.restype = None

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY', 'development_key')
project_version = "1.0.4"
content_type = {'Content-Type': 'application/json; charset=utf-8'}

class WikiSearcher(object):
	def __init__(self):
		self.obj = lib.WikiSearcher_new()

	def load_files(self):
		lib.load_files(self.obj)

	def search(self, i, j):
		return lib.search_wiki(self.obj, i, j)
		
	def free_pointer(self, p):
		lib.free_pointer(p)

searcher = WikiSearcher()
print("created searcher")
searcher.load_files()
print("loaded files\n")

@app.route('/get-paths/', methods = ['GET'])
def get_paths():
	start = int(request.args.get('start'))
	print(start)
	end = int(request.args.get('end'))
	print(end)
	ptr = searcher.search(start, end)
	print("hex:", hex(ptr))
	res = ctypes.cast(ptr, ctypes.c_char_p).value
	print(res)
	searcher.free_pointer(ptr)
	return res, 200, content_type

@app.route('/')
def welcome():
	res = {"message": "welcome"}
	return json.dumps(res), 200, content_type

@app.route('/ping')
def ping():
	res = {"message": "pong", "version": project_version, "time": time.time()}
	return json.dumps(res), 200, content_type

@app.errorhandler(404)
def page_not_found(e):
	res = {"message": "endpoint_not_found", "code": 404}
	return json.dumps(res), 404, content_type

@app.errorhandler(405)
def page_not_found(e):
	res = {"message": "method_not_allowed", "code": 405}
	return json.dumps(res), 405, content_type

if __name__ == '__main__':
	serve(app, host = '0.0.0.0', port = 8080)