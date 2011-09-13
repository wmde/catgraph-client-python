from FuzzerBase import FuzzerBase
from Client import Connection
import random
import sys

class ArcFuzzer(FuzzerBase):
    """Tests the TCP client connection"""
 
    def __init__(self):
        self.offset = 1
        FuzzerBase.__init__(self)
    
    def prepare(self):
        global gpTestGraphName
        
        # in case we use a persistent graph, fund an unused offset
        Range = range(self.offset,10)
        for i in Range:
            if not self.gp.capture_list_successors(i):
                self.gp.add_arcs(((i, i+1)))
                print "fuzz offset: i (" + TestGraphName + ")"
                return
            
            self.offset = i + 1
            #? self.offset verstehen!
        
        exit("no free offset left (or "
          + TestGraphName + "needs purging)")
         
    
    def random_node(self):
        return random.randint(10, 1000) * 10 + self.offset
         
    
    def random_arcs(self, n=0):
        if not n:
            n = random.randint(2, 80)
        arcs = []
        for i in range(0, n):
            a = self.random_node()
            b = self.random_node()
            arcs.append((a, b))
        return arcs
    
    def random_set(self, n=0):
        if not n:
            n = random.randint(2, 80)
        arcs = []
        for i in range(0, n):
            x = self.random_node()
            arcs.append(x)
        return arcs
    
    def doFuzz(self):
        self.gp.add_arcs(self.random_arcs())
        self.gp.remove_arcs(self.random_arcs())
        
        self.gp.replace_successors(self.random_node(), self.random_set())
        self.gp.replace_predecessors(self.random_node(), self.random_set())
        
        return False
         
    
     

fuzzer = ArcFuzzer()

fuzzer.run(sys.argv)
