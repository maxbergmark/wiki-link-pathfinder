import pywikibot
from pywikibot import pagegenerators

import requests
import asyncio
from timeit import default_timer
from concurrent.futures import ThreadPoolExecutor
import os


def get_links(page):
	# print("Getting links for", page)
	for link in page.interwiki():
		print(link)

async def get_data_asynchronous(executor, pages):
		loop = asyncio.get_event_loop()
		print("tasks created")
		tasks = [
			loop.run_in_executor(
				executor,
				get_links,
				*(page,) # Allows us to pass in multiple arguments to `fetch`
			)
			for page in pages
		]

		# Initializes the tasks to run and awaits their results
		for response in await asyncio.gather(*tasks):
			pass


def main():
	site = pywikibot.Site('en', 'wikipedia')
	tmp = []
	loop = asyncio.get_event_loop()
	futures = []
	with ThreadPoolExecutor(max_workers = 10) as executor:
		for i, page in enumerate(site.allpages()):
			# print(i, i+1%500, page)
			tmp.append(page)
			if (i+1) % 500 == 0:
				# print("testing")
				future = asyncio.ensure_future(get_data_asynchronous(executor, tmp))
				loop.run_until_complete(future)
				futures.append(future)
				tmp = []
	for future in futures:
		loop.run_until_complete(future)



main()

