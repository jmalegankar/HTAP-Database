from lstore.page import Page


class BasePage:
	def __init__(self, columns:int):
        self.num_records = 0
        phys_pages = [Page()] * columns
    def get(self):
    	pass
    def set(self):
    	pass
    def update(self):
    	pass


        