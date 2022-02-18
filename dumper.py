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
		if location.endswith('.db'):
			with open(location, 'rb') as in_f:
				page = Page()
				page.open(bytearray(in_f.read()))
				print(page)
		else:
			with open(location, 'rb', pickle.HIGHEST_PROTOCOL) as in_f:
				data = pickle.load(in_f)
				print(data)
	else:
		print('Invalid file')