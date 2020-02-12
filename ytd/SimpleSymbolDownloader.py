import requests
import string
from time import sleep
import math

from ytd.compat import text
from ytd.compat import quote

from collections import deque

from Query import Query

user_agent = 'yahoo-ticker-symbol-downloader'
general_search_characters = 'abcdefghijklmnopqrstuvwxyz0123456789.='
first_search_characters = 'abcdefghijklmnopqrstuvwxyz0123456789'

class SymbolDownloader:
    """Abstract class"""

    def __init__(self, type):
        # All downloaded symbols are stored in a dict before exporting
        # This is to ensure no duplicate data
        self.symbols = {}
        self.rsession = requests.Session()
        self.type = type
        self.current_query = None
        self.completed_queries = []
        self.done = False

        # instantiate the queue
        self.queries = deque()
        # instantiate the "master" query
        self.master_query = Query('', None)
        # put the first real queries in the queue
        self._add_queries(self.master_query, first_search_characters)

    def _add_queries(self, query, search_characters):
        # This method will add child queries to query and put the children in the queue
        # Each child query will have an additional character appended to the parent query string
        #  (taken from search_characters)
        query.addChildren(search_characters)
        # reverse children order when extending the queue because it's LIFO queue
        self.queries.extend(query.children[::-1])

    def _encodeParams(self, params):
        encoded = ''
        for key, value in params.items():
            encoded += ';' + quote(key) + '=' + quote(text(value))
        return encoded

    def _fetch(self, insecure):
        params = {
            'searchTerm': self.current_query.query_string,
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
        print("req " + req.url)
        resp = self.rsession.send(req, timeout=(12, 12))
        resp.raise_for_status()

        return resp.json()

    def decodeSymbolsContainer(self, symbolsContainer):
        raise Exception("Function to extract symbols must be overwritten in subclass. Generic symbol downloader does not know how.")

    def nextRequest(self, insecure=False, pandantic=False):
        # not threading, so blocking is irrelevant
        self.current_query = self.queries.pop()
        success = False
        retryCount = 0
        json = None
        # Eponential back-off algorithm
        # to attempt 5 more times sleeping 5, 25, 125, 625, 3125 seconds
        # respectively.
        maxRetries = 5
        while(success == False):
            try:
                json = self._fetch(insecure)
                success = True
            except (requests.HTTPError,
                    requests.exceptions.ChunkedEncodingError,
                    requests.exceptions.ReadTimeout,
                    requests.exceptions.ConnectionError) as ex:
                if retryCount < maxRetries:
                    attempt = retryCount + 1
                    sleepAmt = int(math.pow(5,attempt))
                    print("Retry attempt: " + str(attempt) + " of " + str(maxRetries) + "."
                        " Sleep period: " + str(sleepAmt) + " seconds."
                        )
                    sleep(sleepAmt)
                    retryCount = attempt
                else:
                    raise

        self.completed_queries += [ self.current_query ]
        (symbols, count) = self.decodeSymbolsContainer(json)

        for symbol in symbols:
            self.symbols[symbol.ticker] = symbol
            # record symbols returned for this query
            self.current_query.results.append(symbol.ticker)

        # There is no pagination with this API.
        # If we receive 10 results, we assume there are more than 10 and
        #  add another layer of queries to narrow the search further
        if(count == 10):
            self._add_queries(self.current_query, general_search_characters)
        elif(count < 10 and count > 5):
            # The API has started returning less than 10 results even though there are more results
            # For now, assume that 6+ queries means incomplete results
            self._add_queries(self.current_query, general_search_characters)
        elif(count > 10):
            # This should never happen with this API, it always returns at most 10 items
            raise Exception("Funny things are happening: count "
                            + text(count)
                            + " > 10. "
                            + "Content:"
                            + "\n"
                            + repr(json))
        else:
            # Tell the query it's done
            self.current_query.done()

        if len(self.queries) == 0:
            self.done = True
        else:
            self.done = False

        return symbols

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
