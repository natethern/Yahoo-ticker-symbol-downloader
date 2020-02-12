class Query:
    """Query String, results and summary of children's results"""
    def __init__(self, query_string, parent):
        self.query_string = query_string
        self.parent = parent # <--- may be "None"
        self.children = []
        self.num_children = 0
        self.num_complete = 0
        self.results = []
        self.children_results = []
        self._done = False

    def addChildren(self, search_characters):
        # ensure search_characters are all unique
        #search_list = set(search_characters) # will not preserve order
        search_list = OrderedDict.fromkeys(search_characters).keys()
        self.num_children += len(search_list)
        for e in search_list:
            element = self.query_string + e
            self.children.append(Query(element, self))
                
    def addChildResults(self, results):
        for r in results:
            if r not in self.children_results:
                self.children_results.append(r)

    def done(self):
        self._done = True
        self.parent.child_done(self)

    def child_done(self, child):
        self.num_complete++
        self.children_results = set(self.children_results + child.results + child.children_results)
        if self.num_complete == self.num_children:
            self.done()
