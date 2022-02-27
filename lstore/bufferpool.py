import pickle
import os
from collections import defaultdict
from lstore.config import BUFFERPOOL_SIZE
from lstore.page import Page
import time
from threading import Lock

class BufferpoolPage:

	__slots__ = 'pinned', 'pages', 'last_access', 'access_count', 'path', 'version'

	def __init__(self, path, version):
		self.pinned = 1
		self.pages = [] # contains an array of physical page
		self.last_access = time.time()
		self.path = path
		self.version = version

	def __str__(self):
		string = 'BufferpoolPage\n'
		string += 'path: {}\n'.format(self.path)
		string += 'Pinned: {}, version: {}, last_access: {}\n'.format(self.pinned, self.version, self.last_access)
		return string

	def __repr__(self):
		return self.__str__()

class Bufferpool:

	__slots__ = ('path', 'logical_pages', 'bufferpool_size', 'logical_pages_directory', 'directory_lock')

	def __init__(self):
		self.start()

	"""
	Database().open() will call this function to start the bufferpool
	"""

	def start(self):
		self.path = './database'
		self.logical_pages = [None] * BUFFERPOOL_SIZE # array of BufferpoolPage
		self.bufferpool_size = 0
		self.logical_pages_directory = dict() # map logical base/tail pages to index
		self.directory_lock = Lock()

	"""
	Set the database path
	"""

	def db_path(self, path):
		self.path = path
		self.create_folder('')

	"""
	Return True if database exists in the current path
	"""

	def db_exists(self):
		return os.path.exists(self.path + '/database.db')

	"""
	Create folder given relative path
	"""

	def create_folder(self, relative_path):
		os.makedirs(self.path + '/' + relative_path, exist_ok=True)

	"""
	Write a physical page to disk
	"""

	def write_page(self, relative_path, physical_page_number, data, version=0):
		with open(f'{self.path}/{relative_path}/{physical_page_number}-{version}.db', 'wb') as out_f:
			out_f.write(data)

	"""
	Read a physical page from disk
	"""

	def read_page(self, relative_path, physical_page_number, version=0):
		with open(f'{self.path}/{relative_path}/{physical_page_number}-{version}.db', 'rb') as in_f:
			return bytearray(in_f.read())
		return None

	"""
	Write metadata to disk
	"""

	def write_metadata(self, relative_path, data):
		with open(self.path + '/' + relative_path, 'wb', pickle.HIGHEST_PROTOCOL) as out_f:
			pickle.dump(data, out_f)

	"""
	Read metadata from disk
	"""

	def read_metadata(self, relative_path):
		with open(self.path + '/' + relative_path, 'rb', pickle.HIGHEST_PROTOCOL) as in_f:
			return pickle.load(in_f)
		return None

	"""
	Get logical pages for our merge thread to read, won't affect bufferpool
	"""

	def merge_get_logical_pages(self, path, num_columns, version=0): # -> [Page]
#		print('merge_get_logical_pages', path, version)
		if version < 0:
			version = 0

		in_bufferpool = False
		pages = []

		self.directory_lock.acquire()

		if path in self.logical_pages_directory:
			index = self.logical_pages_directory[path]
			if self.logical_pages[index].version == version:
				pages = self.logical_pages[index].pages
				self.directory_lock.release()
				return pages
			else:
				pages = self.logical_pages[index].pages[:4]
				in_bufferpool = True
#				print(path, 'is in bufferpool but not up to date!')

		self.directory_lock.release()

		try:
			if os.path.isdir(self.path + '/' + path):
				for physical_page_number in range(num_columns):
					if physical_page_number < 4:
						if in_bufferpool:
							continue

						physical_page = Page()
						physical_page.open(self.read_page(path, physical_page_number, 0))
						pages.append(physical_page)
					else:
						physical_page = Page()
						physical_page.open(self.read_page(path, physical_page_number, version))
						pages.append(physical_page)
				return pages
			return None
		except ValueError:
			return None

	"""
	Replace our old base page with merged base page
	"""

	def merge_save_logical_pages(self, path, num_columns, pages, version):
		# notify main thread we finished a merge
		self.create_folder(path)

		for i in range(4, num_columns):
			pages[i].close()
			self.write_page(path, i, pages[i].data, version)

	"""
	Given path, return BufferpoolPage
	"""

	def get_logical_pages(self, path, num_columns, version=0, atomic=False, column=None, rec_num=None, set_to=None, set_and_get=False): # -> BufferpoolPage
		if version < 0:
			version = 0
		try:
			next_bufferpool_index = -1
			if path in self.logical_pages_directory:
				# Cache hit, already in bufferpool
				next_bufferpool_index = self.logical_pages_directory[path]
				bufferpool_page = self.logical_pages[next_bufferpool_index]
				if bufferpool_page.version == version:
					bufferpool_page.last_access = time.time()

					if atomic:
						# Only need once, no need to pin!
						if set_to is not None:
							if set_and_get:
								return bufferpool_page.pages[column].get_and_set(rec_num, set_to)
							bufferpool_page.pages[column].set(rec_num, set_to)
							return
	
						return bufferpool_page.pages[column].get(rec_num)
	
					# Reader/writer wants to hold the bufferpool page
					if bufferpool_page.pinned == 0:
						# Pin the page
						bufferpool_page.pinned += 1
					return bufferpool_page
				else:
#					print('PAGE OUT DATED!!!', path, 'need:', version, 'has', bufferpool_page.version)
					# base page is outdated, save first 4 columns
					for physical_page_number in range(4):
						if bufferpool_page.pages[physical_page_number].dirty:
							bufferpool_page.pages[physical_page_number].close() # close physical page
							self.write_page(
								bufferpool_page.path,
								physical_page_number,
								bufferpool_page.pages[physical_page_number].data,
							)

			# Page not in bufferpool
			if next_bufferpool_index == -1:
				next_bufferpool_index = self.bufferpool_size
				if next_bufferpool_index == BUFFERPOOL_SIZE: # if bufferpool doesnt have free space anymore
#					print('need to kick for', path)
					oldest_page = self.logical_pages[0]
					oldest_page_index = 0
					has_free_space = False

					for index, logical_page in enumerate(self.logical_pages):
						if logical_page.pinned <= 0: # not pinned
							has_free_space = True

							# Using LRU, find a page to evict
							if logical_page.last_access < oldest_page.last_access:
								oldest_page_index = index
								oldest_page = logical_page

					if not has_free_space: # error if we fail to evict any page
						print('ERROR: We are kicking out a pinned page!')

					self.create_folder(oldest_page.path)

					# write to disk if dirty
					for physical_page_number in range(len(oldest_page.pages)):
						if oldest_page.pages[physical_page_number].dirty:
							oldest_page.pages[physical_page_number].close() # close physical page
							read_version = 0 if physical_page_number < 4 else oldest_page.version
							self.write_page(
								oldest_page.path,
								physical_page_number,
								oldest_page.pages[physical_page_number].data,
								read_version
							)

					# kick out this logical page
					self.directory_lock.acquire()

					if oldest_page.path in self.logical_pages_directory:
						del self.logical_pages_directory[oldest_page.path]

					self.directory_lock.release()

					# we have free space now
					next_bufferpool_index = oldest_page_index
				else:
					# bufferpool has spaces
					self.bufferpool_size += 1

			# Create a BufferpoolPage object
			new_logical_page = BufferpoolPage(path, version)

			# pages in database
			if os.path.isdir(self.path + '/' + path):
				for physical_page_number in range(num_columns):
					physical_page = Page()

					if physical_page_number < 4:
						physical_page.open(self.read_page(path, physical_page_number, 0))
					else:
						physical_page.open(self.read_page(path, physical_page_number, version))

					new_logical_page.pages.append(physical_page)
			else:
				# pages not in database (new pages)
				new_logical_page.pages = [Page(dirty=True) for i in range(num_columns)]

			self.logical_pages[next_bufferpool_index] = new_logical_page	# Put page in bufferpool
			self.logical_pages_directory[path] = next_bufferpool_index	# Set index

			if atomic:
				# Only need once, no need to pin!
				self.logical_pages[next_bufferpool_index].pinned = 0

				if set_to is not None:
					if set_and_get:
						return self.logical_pages[next_bufferpool_index].pages[column].get_and_set(rec_num, set_to)

					self.logical_pages[next_bufferpool_index].pages[column].set(rec_num, set_to)
					return

				return self.logical_pages[next_bufferpool_index].pages[column].get(rec_num)

			# Reader/writer wants to hold the bufferpool page
			return self.logical_pages[next_bufferpool_index]
		except ValueError as e:
			return None # this will crash the program

	"""
	Closing database, save all dirty pages
	"""

	def close(self):
		for page in self.logical_pages:
			if page is not None:
				self.create_folder(page.path)
				for physical_page_number in range(len(page.pages)):
					if page.pages[physical_page_number].dirty:
						page.pages[physical_page_number].close() # Save the page
						version = 0 if physical_page_number < 4 else page.version

						self.write_page(
							page.path,
							physical_page_number,
							page.pages[physical_page_number].data,
							version
						)


shared = Bufferpool()
