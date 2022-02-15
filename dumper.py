import sys
import os
from os import system
import pickle
from lstore.page import Page

while True:
	location = input('File location: ')
	location = location.strip()
	if location == '':
		_ = system('clear')
		continue
	
	if os.path.exists(location):
		with open(location, 'rb', pickle.HIGHEST_PROTOCOL) as in_f:
			data = pickle.load(in_f)
			page = Page()
			page.open(data)
			print(page)
	else:
		print('Invalid file')