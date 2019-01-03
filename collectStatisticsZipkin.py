import subprocess
import sys
import re
import os
import time
import argparse
import collections
import pprint
import csv
import json
import sys
import datetime
import pprint




#### Comparison based on Zipkin documentaion after Clean step-  MergeV2ById:
#https://github.com/openzipkin/zipkin/blob/master/zipkin-ui/js/component_data/spanCleaner.js
def sharedCompare(a,b):
   leftNotShared = not 'shared' in a or (not a['shared'])
   rightNotShared = not 'shared' in b or (not b['shared'])

   if leftNotShared and rightNotShared:
      return -1 if a['kind'] == 'CLIENT' else 1

   if leftNotShared: 
      return -1
   if rightNotShared: 
      return 1

   return 0

def compareSpan(a,b):
   if 'parentId' not in a and 'parentId' in b:
      return -1
   if 'parentId' in a and 'parentId' not in b:
      return 1
   
   if a['id'] == b['id']:
      return sharedCompare(a,b)
   
   return compare(a['timestamp'],b['timestamp'])

def compare(a, b):
    return 1 if a > b else 0 if a == b else -1

#### End of Comparison:

def compareEndpoint(left, right):
   if left is None:
      return -1
   if right is None: 
      return 1

   byService = compare(left['serviceName'], right['serviceName'])

   if byService != 0: 
      return byService
   
   byIpV4 = compare(left['ipv4'], right['ipv4'])
   if byIpV4 != 0: 
      return byIpV4

   return compare(left['ipv6'], right['ipv6'])
"""
def compareStr(a, b): 
   if not a and not b: 
      return 0
   if a: 
      return -1
   if b: 
      return 1  
   return (a > b) - (a < b)
"""
def cleanupComparator(left, right):
   bySpanId = compare(left['id'], right['id'])
   if bySpanId != 0: 
      return bySpanId
   
   byShared = sharedCompare(left, right)
   if byShared != 0: 
      return byShared
   return compareEndpoint(left['localEndpoint'], right['localEndpoint'])

def keyString(id, endpoint,shared = False):
   if not shared: 
      return id
   endpointString = json.dumps(endpoint) if endpoint  else 'x'
   return id + endpointString


"""
 constructor(span) {
    this._parent = undefined; // no default
    this._span = span; // undefined is possible when this is a synthetic root node
    this._children = [];
"""
class SpanNode:
   parent = dict()
   span = dict()
   children = list()
    
   def __init__(self, span):
      self.parent = None
      self.span = span
      self.children =[]

class SpanNodeBuilder:
   spanToParent = dict()
   debug = False
   rootSpan = None
   keyToNode = dict()

   def index(self, span):
      idKey = ""
      parentKey = ""

      if 'shared' in span and span['shared']:
         #we need to classify a shared span by its endpoint in case multiple servers respond to the
         #same ID sent by the client.
         idKey = keyString( span['id'], span['localEndpoint'],True)
         #the parent of a server span is a client, which is not ambiguous for a given span ID.
         parentKey = span['id']
      else:
         idKey = span['id']
         parentKey = span['parentId'] if 'parentId' in span else "undefined"
      
      self.spanToParent[idKey] = parentKey

   def process(self, span):
      localEndPoint = span['localEndpoint']
      key = keyString(span['id'], span['localEndpoint'], (span['shared'] if 'shared' in span else False))
      noEndPointKey = keyString(span['id'],{}, (span['shared'] if 'shared' in span else False)) if localEndPoint else key
      #print json.dumps(span)
      
      print span['localEndpoint']['serviceName'] , span['localEndpoint']['ipv4'], span['id'], span['kind'], (span['shared'] if 'shared' in span else ""),(span['parentId'] if 'parentId' in span else "undefined")
      
      parentId = ""
      if 'shared' in span and span['shared']:
         # Shared is a server span. It will very likely be on a different endpoint than the client.
         # Clients are not ambiguous by ID, so we don't need to qualify by endpoint.
         parentId = span['id']
         print "shared: " + parentId
      elif 'parentId' in span and span['parentId']:
         #We are not a root span, and not a shared server span. Proceed in most specific to least.
         #We could be the child of a shared server span (ex a local (intermediate) span on the same
         #endpoint). This is the most specific case, so we try this first.
         parentId = keyString((span['parentId'] if 'parentId' in span else "undefined"), localEndPoint, True)
         print parentId
         if parentId in self.spanToParent.keys():
            self.spanToParent[noEndPointKey] = parentId
         else:
            # If there's no shared parent, fall back to normal case which is unqualified beyond ID.
            parentId = span['parentId']
      else:
          if self.rootSpan:
             pass # missing parentID - we are root or don't know parent.

      node = SpanNode(span)
      if not parentId and not self.rootSpan:
         #special-case root, and attribute missing parents to it. In
         #other words, assume that the first root is the "real" root.
         self.rootSpan = node
         #self.spanToParent.
         del self.spanToParent[noEndPointKey]
         print "Found Root: "+ json.dumps(node.span)
      elif 'shared' in span and span['shared']:
         #In the case of shared server span, we need to address it both ways, in case intermediate
         #spans are lacking endpoint information.
         self.keyToNode[key] = node
         self.keyToNode[noEndPointKey] = node
      else:
         self.keyToNode[noEndPointKey] = node
      
      # At this point, we have the most reliable parent-child relationships and can allocate spans
      # corresponding the the best place in the trace tree.
   
   
   def build(self, trace):
      orderedTrace = list()
   
      orderedClean = sorted(trace, cleanupComparator)
      for t in orderedClean:
         print t['localEndpoint']['serviceName'] , t['localEndpoint']['ipv4'], t['id'], t['kind'], (t['shared'] if 'shared' in t else "") 
         
      print "-------------"
      # After Cleanup ------------
      orderedTrace = sorted(orderedClean, compareSpan)
      for t in orderedTrace:
         self.index(t)
         stamp = t['timestamp'] / 1000000.0
         duration = t['duration'] / 1000000.0
         print t['localEndpoint']['serviceName'], t['id'], t['kind']
         #print datetime.datetime.fromtimestamp(stamp).strftime('%Y-%m-%d %H:%M:%S.%f')
         #print float(datetime.datetime.fromtimestamp(duration).strftime('%S.%f')) # in seconds
         #print float(datetime.datetime.fromtimestamp(stamp).strftime('%S.%f'))# in seconds
      
      print "-------------"
      
      pprint.pprint(self.spanToParent)
      print ""
      print ""
      print "++++++++++++"
      count = 0
      for sp in orderedTrace:
         self.process(sp)
         if count == 4:
            sys.exit(0)
         count = count + 1
      

   


def parse_trace(trace):
   #print trace[0]
   #getInitialService(trace)
   builder = SpanNodeBuilder()
   builder.build(trace)
      
     


"""
>>> import datetime
>>> s = 1236472051807 / 1000.0
>>> datetime.datetime.fromtimestamp(s).strftime('%Y-%m-%d %H:%M:%S.%f')
'2009-03-08 09:27:31.807000'
"""

def main():
    inputFile = sys.argv[1]
    if os.path.exists(inputFile):
        with open(inputFile) as jsonTrace:
            trace = json.load(jsonTrace)
            parse_trace(trace)



if __name__ == '__main__':
    main()
    
