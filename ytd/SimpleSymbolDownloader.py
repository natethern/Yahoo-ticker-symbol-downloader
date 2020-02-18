import requests
import string
from time import sleep
import math

from ytd.compat import text
from ytd.compat import quote

from Query import Query

from threading import Thread
from collections import deque as Deque
from ytd.compat import is_py3
if is_py3:
    from queue import Queue
else:
    from Queue import Queue

user_agent = 'yahoo-ticker-symbol-downloader'
general_search_characters = 'abcdefghijklmnopqrstuvwxyz0123456789.='
first_search_characters = 'abcdefghijklmnopqrstuvwxyz1234567890'

class SymbolDownloader:
    """Abstract class"""

    def __init__(self, type):
        # All downloaded symbols are stored in a dict before exporting
        # This is to ensure no duplicate data
        self.symbols = {}
        self.rsession = requests.Session()
        self.type = type
        self.current_queries = None
        self.completed_queries = []
        self.done = False
        self.insecure = None
        self.status_print = None

        # Attempt to deal with API results < 10 not containing all results
        # Assume if results = 10 then there are more
        # Assume if results = 0 or results = 1 then there are no more
        self.result_count_action = [
            # for a result count of 0, False means we know it's complete
            False,
            # for a result count of 1, False means we know it's complete
            False,
            # for a result count of 2 thru 9, None means we don't know
            #  so we assume it's incomplete
            None, None, None, None, None, None, None, None,
            # for a result count of 10, True means we know it's incomplete
            True ]

        # In stage 1, queries are processed FIFO
        # After stage 1, queries are processed LIFO
        self.stage1 = True

        # instantiate the queues
        self.queries = Deque()
        # The queue of queries to be fetched by the threaded downloaders
        #  queries are worked in batches so the downloader state can be saved occasionally
        self.fetch_jobs = Queue()
        # The queue of completed fetches that need to be processed
        # A blocking queue is used for this as well
        self.fetch_returns = Queue()
        
        # instantiate the "master" query
        self.master_query = Query('', None)
        # put the first real queries in the queue
        self._add_queries(self.master_query, first_search_characters)
        # "0" isn't valid alone as a search string
        #self._add_queries(self.master_query, '0')
        q= self.queries.pop()
        if q.query_string != "0":
            sys.exit("not 0")
        self._add_queries(q, general_search_characters)
        self.completed_queries.append(q)

        # instantiate workers
        # First the multiple http fetchers
        for x in range(100):
            t = Thread(target=self._fetch_worker)
            t.daemon = True
            t.start()
        # there is a single instance to process fetch returns
        # it is a thread so it can work while all the fetchers are idle
        t = Thread(target=self._fetch_processor)
        t.daemon = True
        t.start()

    def save_state(self):
        return [ self.symbols, self.current_queries, self.completed_queries, self.done,
                 self.queries, self.master_query,  self.result_count_action, self.stage1 ]

    def restore_state(self, downloader_data):
        (self.symbols, current_queries, self.completed_queries, self.done,
         self.queries, self.master_query, self.result_count_action, self.stage1) = downloader_data
        if self.stage1:
            self.queries.extendleft(current_queries)
        else:
            self.queries.extend(current_queries)

    def _add_queries(self, query, search_characters):
        # This method will add child queries to query and put the children in the queue
        # Each child query will have an additional character appended to the parent query string
        #  (taken from search_characters)
        query.addChildren(search_characters)
        if self.stage1:
            self.queries.extend(query.children)
        else:
            self.queries.extend(query.children[::-1])

    def _encodeParams(self, params):
        encoded = ''
        for key, value in params.items():
            encoded += ';' + quote(key) + '=' + quote(text(value))
        return encoded

    def _fetch(self, insecure, query_string):
        params = {
            'searchTerm': query_string,
        }
        query_string = {
            'device': 'console',
            'returnMeta': 'true',
        }
        protocol = 'http' if insecure else 'https'
        req = requests.Request('GET',
            protocol+'://finance.yahoo.com/_finance_doubledown/api/resource/searchassist'+self._encodeParams(params),
            headers={'User-agent': user_agent},
            params=query_string
        )
        req = req.prepare()
        msg = "req " + req.url
        resp = self.rsession.send(req, timeout=(12, 12))
        resp.raise_for_status()

        return [ resp.json(), msg ]

    def decodeSymbolsContainer(self, symbolsContainer):
        raise Exception("Function to extract symbols must be overwritten in subclass. Generic symbol downloader does not know how.")

    def nextRequest(self, status_print, insecure=False, pandantic=False):
        self.status_print = status_print
        self.insecure = insecure
        # not threading, so blocking is irrelevant
        if self.stage1:
            # switch to LIFO when there are 2500 staged queries
            if len(self.queries) >= 2000:
                self.stage1 = False
            q = self.queries.popleft()
            self.current_queries = [ q ]
            self.fetch_jobs.put(q)
        else:
            self.current_queries = []
            for x in range(2000):
                if len(self.queries) > 0:
                    q = self.queries.pop()
                    self.current_queries.append(q)
                    self.fetch_jobs.put(q)

        #json = self._fetch_worker(insecure, self.current_query)
        self.fetch_jobs.join()

        #symbols = self._fetch_processor(self.current_query, json)
        self.fetch_returns.join()
        self.current_queries = []

        self.querySurvey()

        if len(self.queries) == 0:
            self.done = True
        else:
            self.done = False

    def _fetch_worker(self):
        while True:
            current_query = self.fetch_jobs.get()
            success = False
            retryCount = 0
            json = None
            # Eponential back-off algorithm
            # to attempt 5 more times sleeping 5, 25, 125, 625, 3125 seconds
            # respectively.
            maxRetries = 10
            while(success == False):
                try:
                    (json, msg) = self._fetch(self.insecure, current_query.query_string)
                    success = True
                except (requests.HTTPError,
                        requests.exceptions.ChunkedEncodingError,
                        requests.exceptions.ReadTimeout,
                        requests.exceptions.ConnectionError) as ex:
                    if retryCount < maxRetries:
                        attempt = retryCount + 1
                        sleepAmt = int(math.pow(5,attempt))
                        print("Retry attempt: " + str(attempt) + " of " + str(maxRetries) + "."
                              " Sleep period: " + str(sleepAmt) + " seconds.")
                        sleep(sleepAmt)
                        retryCount = attempt
                    else:
                        raise
            self.fetch_returns.put([current_query, json, msg])
            self.fetch_jobs.task_done()

    def _fetch_processor(self):
        while True:
            (current_query, json, msg) = self.fetch_returns.get()
            (symbols, count) = self.decodeSymbolsContainer(json)

            for symbol in symbols:
                self.symbols[symbol.ticker] = symbol
                # record symbols returned for this query
                current_query.results.append(symbol.ticker)

            if(count > 10):
                # This should never happen with this API, it always returns at most 10 items
                raise Exception("Funny things are happening: count "
                                + text(count) + " > 10. Content:\n"
                                + repr(json))

            # There is no pagination with this API.
            # If we receive X results, we assume there are more than X and
            #  add another layer of queries to narrow the search further
            # In the past, X was known to be 10. Now it is some number 1 < X <= 10
            if self.result_count_action[count] is None:
                # the action for this number of results is unknown,
                # so assume search narrowing is required
                self._add_queries(current_query, general_search_characters)
            elif self.result_count_action[count]:
                # this number of results is known to require search narrowing
                self._add_queries(current_query, general_search_characters)
            else:
                # Tell the query it's done
                current_query.done()

            print(msg)
            self.status_print(symbols)
            self.completed_queries.append(current_query)
            self.fetch_returns.task_done()

    def querySurvey(self):
        # return if all actions are known
        if not any([ True if a is None else False for a in self.result_count_action ]):
            return
        lsrca = len(self.result_count_action)
        #print(self.result_count_action)
        #for i in range(lsrca):
        #    self.descent_actions[i] = self.result_count_action[i]
        #    if self.result_count_action[i] is None:
        #        self.descent_actions[i] = 0
        actions = [ 0 if a is None else a for a in self.result_count_action ]
        #print(actions)
        self.descendQueries(self.master_query, actions)
        print(actions)
        # looking for queries where children returned same number of results as the parent
        # if this occurred 200 times then that result number doesn't require narrowing
        for i in range(lsrca):
            if not isinstance(actions[i], bool):
                if actions[i] >= 20:
                    for j in range(i+1):
                        self.result_count_action[j] = False
            elif actions[i]:
                # a new search narrowing count has been found
                for j in range(i, lsrca):
                    self.result_count_action[j] = True
        #print(self.result_count_action)

    def descendQueries(self, query, actions):
        if query.num_children > 0:
            count = len(query.results)
            child_count = len(query.children_results)
            if query.is_done and not isinstance(actions[count], bool):
                if child_count > count:
                    # we have found a return count for which search narrowing is required
                    actions[count] = True
                elif child_count == count:
                    # record a probable non-narrowing result
                    actions[count] += 1;
            for child in query.children:
                self.descendQueries(child, actions)

    def isDone(self):
        return self.done

    def getCollectedSymbols(self):
        return self.symbols.values()

    def getRowHeader(self):
        return ["Ticker", "Name", "Exchange"]

    def printProgress(self):
        if self.isDone():
            print("Progress: Done!")
        else:
            print("Progress:"
                  + " Query " + str(len(self.completed_queries)) + "/" +
                  str(len(self.completed_queries) + len(self.queries)) + "."
                  + "\n"
                  + str(len(self.symbols)) + " unique " + self.type + " entries collected so far."
                 )
        print ("")
