'''
Created on 16-Oct-2013

@author: raju
'''
import networkx as nx
from Queue import *
import sys

def processGraph(graph):
    for node in graph.nodes_iter():
        queue = Queue()
        queue._put(node)
#         queue._put("null")
        f = open("/home/raju/Work/communities/betweenness/mapReduceInput" + str(node), "w")
        root = node
        visitedMap = {}
        path = "null"
        while(queue.empty() == False):
            node = queue.get()
            if node not in visitedMap:
                visitedMap[node] = 1
#                 if str(node) == "null":
#                     #distance += 1
#                     if queue.empty == False:
#                         queue._put("null")
                if str(node)!= "null":
                    adjList = ""
                    for adjNode in graph.successors_iter(node):
                        if adjList == "":
                            adjList = str(adjNode)
                        else:
                            adjList += "," + str(adjNode) 
                        queue._put(adjNode)
                    color  = "white"
                    path = "null"
                    distance = "Integer.MAX_VALUE"
                    if node == root:
                        color = "grey"
                        path = "source"
                        distance = 0
                    if adjList == "":
                        adjList = "null"
                    f.write(str(node) + "," + str(root) + " " + str(adjList) + "|" + str(distance) + "|"+str(color) +"|" + path)
                    f.write("\n")
        f.flush()
        f.close()
            

if __name__ == '__main__':
    pass