from threading import Thread
import lstore.bufferpool as bufferpool
from queue import Queue
import time
import copy
from lstore.parser import get_physical_page_offset, get_page_range_number
 
class MergeWorkerThread(Thread):

    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    """
    Start the merging process
    """

    def run(self):
        while True:
            try:
                base_page_object, arr_of_tail_pages = self.queue.get()

                base_tps = base_page_object.tps
                base_path = base_page_object.path
                num_columns = base_page_object.num_columns
                num_user_columns = base_page_object.num_user_columns
#               print('merging', base_path, 'with current tps:', base_tps)

                # Array of physical pages (Base [Page])
                base_pages = bufferpool.shared.merge_get_logical_pages(base_path, num_columns, base_tps)
                if base_pages is None:
                    print('Base_page invalid', base_path, base_tps)
                    # bufferpool doesn't have the page, something is wrong
                    self.queue.task_done()
                    continue

                # Make a deep copy of the [Page]
                merged_base_pages = copy.deepcopy(base_pages)
                del base_pages
                last_page_range_number = get_page_range_number(base_tps)
                arr_of_tail_pages = copy.deepcopy(arr_of_tail_pages)
                tail_pages_len = len(arr_of_tail_pages)

                # Array of array of physical pages [Tail [Page]]
                logical_tail_pages = [
                    None if number < last_page_range_number else
                    bufferpool.shared.merge_get_logical_pages(
                        arr_of_tail_pages[number].path, arr_of_tail_pages[number].num_columns
                    )
                    for number in range(tail_pages_len)
                ]

                start_rid, end_rid = merged_base_pages[1].get(0), merged_base_pages[1].get(510)

                latest_tps = 0
                finished_merging = False
                merged_base_rid = set()
                merged_records = 0

                # From len(arr_of_tail_pages) - 1 to 0
                for page_number in range(tail_pages_len - 1, last_page_range_number - 1, -1):
                    tail_page = logical_tail_pages[page_number]
                    # From tail_page.num_records - 1 to 0
                    for i in range(arr_of_tail_pages[page_number].num_records - 1 , -1, -1):
                        tail_rid = tail_page[1].get(i)
                        base_rid = tail_page[4].get(i)
                        schema = tail_page[3].get(i)

                        if latest_tps == 0:
                            # Last record's TPS
                            latest_tps = tail_rid

                        if tail_rid <= base_tps:
                            # finished merging
                            finished_merging = True
                            break

                        if start_rid <= base_rid <= end_rid and base_rid not in merged_base_rid:
                            # merge this page
                            offset = get_physical_page_offset(base_rid)
                            indirection = merged_base_pages[0].get(offset)
                            if indirection != 200000000:
                                for j in range(num_user_columns):
                                    if (schema & (1 << (num_user_columns - j - 1))):
                                        merged_base_pages[j + 4].set(
                                            offset, tail_page[j + 5].get(i)
                                        )

                            merged_records += 1
                            merged_base_rid.add(base_rid)

                    if finished_merging:
                        # finished merging
                        break

                if merged_records > 0:
                    # need to save the pages
                    bufferpool.shared.merge_save_logical_pages(base_path, num_columns, merged_base_pages, latest_tps)
                    base_page_object.tps = latest_tps
#                   print('Finished', base_path, 'latestTPS:',latest_tps)

                self.queue.task_done()
            except ValueError as e:
                print(e)
                # something is wrong
                self.queue.task_done()


class MergeWorker:
    def __init__(self):
        self.queue = Queue()
        self.thread = MergeWorkerThread(self.queue)
        self.thread.daemon = True
        self.thread.start()
        