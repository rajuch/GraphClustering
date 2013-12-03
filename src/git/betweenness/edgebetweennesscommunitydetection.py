'''
Created on 01-Oct-2013

@author: raju
'''
import networkx as nx
from Queue import *
import sys, os

def edgebetweennesscommunity(g):
    #g = nx.DiGraph()
    actualGraph = g.copy() 
    noofEdges = nx.number_of_edges(g)
    noOfNodes = nx.number_of_nodes(g)
    print noOfNodes
    result = []
    betweennessWeight = {}
    for i in range(noOfNodes):
        betweennessWeight[i] = 0
    for i in range(noofEdges):
        try:
            for node in nx.nodes_iter(g):
                q = Queue()
                distance = {}
                weight = {}
                q.put(node)
                distance[node] = 0
                weight[node] = 1
                stack = []
                tempWeight = {}
                for i in range(noOfNodes):
                    tempWeight[i] = 0
                
                while(q.empty() == False):
                    actnode = q.get()
                    for neighbour in g.neighbors_iter(actnode):
                        """ we haven't seen this node yet """ 
                        if neighbour not in distance:
                            distance[neighbour] = distance[actnode] + 1
                            weight[neighbour] = weight[actnode]
                            stack.append(neighbour)
                        else:
                            if distance[neighbour] == distance[actnode] + 1:
                                weight[neighbour] = weight[actnode] + 1
            
            
                """ calculate the betweenness"""
                while(len(stack) != 0):
                    actnode = stack.pop()
                    for neighbour in g.predecessors_iter(actnode):
                        key = str(neighbour) + "-" + str(actnode)
                        if neighbour in distance and actnode in distance and neighbour in weight:
                            if (distance[neighbour] == distance[actnode]-1 and weight[neighbour] != 0):
                                print key
                                tempWeight[neighbour] += (tempWeight[actnode]+1)*weight[neighbour]/weight[actnode];
                                if key not in betweennessWeight:
                                    betweennessWeight[key] = (tempWeight[actnode]+1)*weight[neighbour]/weight[actnode];
                                else:
                                    betweennessWeight[key] += (tempWeight[actnode]+1)*weight[neighbour]/weight[actnode];
            
            """ removing the max. edge """
            maxEdge = findMaxEdge(betweennessWeight) 
            nodes = maxEdge.split("-")
            u = int(nodes[0])
            v = int(nodes[1]) 
            print "remove edge", maxEdge
            g.remove_edge(u, v)
            result.append(maxEdge)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            raise e
    print "removing completed"
    membership = getmerges(actualGraph, result)
    print "membership completed"
    printCommunities(actualGraph, membership)

def getmerges(graph, merges):
    try:
        noOfNodes = nx.number_of_nodes(graph)
        membership = []
        myMembership = []
        for i in range(noOfNodes):
            myMembership.append(i)
        membership = list(myMembership)
        mod = modularity(graph, myMembership)
        maxMod = mod
        midx = 0
        for edge in reversed(merges):
            nodes = edge.split("-")
            fromNode = int(nodes[0])
            toNode = int(nodes[1])
            if(fromNode != toNode):
                for i in range(noOfNodes):
                    if(myMembership[i] == fromNode or myMembership[i] == toNode):
                        myMembership.insert(i, (noOfNodes + midx)) 
                mod = modularity(graph, myMembership)
                print "modularity", mod
                if mod > maxMod:
                    maxMod = mod
                    membership = list(myMembership)
                midx += 1
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        raise e
    return membership
 
def printCommunities(graph, membership):
    for edge in membership:
        nodes = edge.split("-")
        u = nodes[0]
        v = nodes[1]
        graph.remove_edge(u, v)
    
    for node in nx.nodes_iter(graph):
        pass
                                     
        
def modularity(graph, membership):
    try:
        noOfEdges = nx.number_of_edges(graph)
        types = max(membership) + 1
        print types, len(membership)
        a = {}
        e = {}
        for i in range(types):
            a[i] = 0
            e[i] = 0
        for edge in graph.edges():
            fromNode = int(edge[0])
            toNode = int(edge[1])
            c1 = membership[fromNode]
            c2 = membership[toNode]
            if c1 == c2:
                e[c1] += 2
            #print "c1= ", c1, "c2=", c2
            a[c1] += 1
            a[c2] += 1
            modularity = 0.0
            if(noOfEdges > 0):
                for i in range(types):
                    tmp = a[i]/2/noOfEdges
                    modularity += e[i]/2/noOfEdges
                    modularity -= tmp*tmp
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        raise e
    return modularity

        
def findMaxEdge(betweennessWeight):
    maximum = 0
    maxEdge = ""
    for edge in betweennessWeight.keys():
        val = betweennessWeight[edge]
        if val > maximum:
            maximum = val
            maxEdge = edge
    return maxEdge
    

if __name__ == '__main__':
    g = nx.DiGraph()
    g.add_edge(0, 2)
    g.add_edge(2,3)
    g.add_edge(4,2)
    for node in nx.nodes_iter(g):
        print node
