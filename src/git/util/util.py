'''
Created on 27-Aug-2013

@author: raju
'''
from Queue import *
from sets import Set
import MySQLdb as mdb

import networkx as nx
import os
import sys
import src.git.betweenness.graphProcessor as gp

def executeSQL(con,sql):
    cursor = con.cursor()
    cursor.execute(sql)    
    return cursor.fetchall()

def loadUsers():
    sql = 'select distinct actor from FollowEvents'
    global userMap
    res=executeSQL(conn,sql)
    count = 0
    for row in res:
        user = row[0]
        if user not in userMap:
            userMap[user] = count
            print user, count
            count += 1
    sql1 = 'select distinct followedUser_login from FollowEvents'
    res1 = executeSQL(conn,sql1)
    for row in res1:
        user = row[0]
        if user not in userMap:
            userMap[user] = count
            print user, count
            count += 1
    sql2 = 'select distinct author from repository'
    res2 = executeSQL(conn1,sql2)
    for row in res2:
        user = row[0]
        if user not in userMap:
            userMap[user] = count
            print user, count
            count += 1
    f = open('/home/raju/Work/communities/betweenness/UserIndex.txt', 'w')
    for key in userMap.keys():
        f.write(str(key) + ' ' + str(userMap[key]))
        f.write('\n')
    f.flush()
    f.close()
#retrieve the owners of repositories
def getUsers(repos):
    sql = 'select distinct author from repository where id in (' +repos + ')'
    print sql
    res = executeSQL(conn1, sql)
    usersSet =Set()
    global userMap, userCount, userIndexFile
    for row in res:
        if row[0] not in userMap:
            userMap[row[0]] = userCount
            userIndexFile.write(row[0] + " " + str(userCount))
            userIndexFile.write("\n")
            userCount += 1
        usersSet.add(row[0])
    return usersSet

def getFollowers(user):
    """
    retrieves the followers of a given user from FollowEvents table
    Parameters:
    user - user name
    """
    sql = 'select actor from FollowEvents where followedUser_login="'+user+'"';
    #print sql
    res = executeSQL(conn,sql)
    followersSet = Set()
    for row in res:
        followersSet.add(row[0])
    return followersSet
    

def getFollowing(user):
    """
    retrieves the the users followed by given user
    parameters:
    user - user name
    """
    sql = 'select followedUser_login from FollowEvents where actor="'+user+'"';
    #print sql
    res = executeSQL(conn,sql)
    followingSet = Set()
    for row in res:
        followingSet.add(row[0])
    return followingSet

#adds the incoming edges to the graph. for ex: user1 has followers user2, user3 then the edges (user2, user1)
# (user2, user1) will be added to the graph
def addIncomingEdges(user, followersList, userConnectedGraph):
    global usermap, userCount, userIndexFile
    try:
        if user not in userMap:
            index = userCount
            userMap[user] = userCount
            userConnectedGraph.add_node(userCount)
            userIndexFile.write(str(user) + " " + str(userCount))
            userIndexFile.write("\n")
            userCount += 1
        else:
            index = userMap[user]
        for followUser in followersList:
            try:
                if followUser not in userMap:
                    userMap[followUser] = userCount
                    userConnectedGraph.add_node(userCount)
                    userIndexFile.write(str(followUser) + " " + str(userCount))
                    userIndexFile.write("\n")
                    userCount += 1
                followUserIndex = userMap[followUser]
                userConnectedGraph.add_edge(followUserIndex, index)
            except Exception as e:
                pass
    except Exception as e:
        print e
        #userConnectedGraph.add_edge(followUser,user)

#adds the outgoing edges to the graph for ex: user1 follows user2, user3 then the edges (user1, user2)
# (user1, user3) will be added to the graph
def addOutGoingEdges(user, followingUsersList, userConnectedGraph):
    global userMap, userCount, userIndexFile
    try:
        if user not in userMap:
            index = userCount
            userMap[user] = userCount
            userConnectedGraph.add_node(userCount)
            userIndexFile.write(str(user) + " " + str(userCount))
            userIndexFile.write("\n")
            userCount += 1
        else:
            index = userMap[user]
        for followingUser in followingUsersList:
            try:
                if followingUser not in userMap:
                    userMap[followingUser] = userCount
                    userConnectedGraph.add_node(userCount)
                    followingUserIndex = userCount
                    userIndexFile.write(str(user) + " " + str(userCount))
                    userIndexFile.write("\n")
                    userCount += 1
                else:
                    followingUserIndex = userMap[followingUser]
                userConnectedGraph.add_edge(index, followingUserIndex)
            except Exception as e:
                pass
    except Exception as e:
        print e
        #userConnectedGraph.add_edge(user,followingUser)

def addNodes(userList, userConnectedGraph):
    for user in userList:
        userConnectedGraph.add_node(user)

def createUserConnectedWholeGraph(userConnectedGraph,unExploredUserQueue, ExploredUserMap):
    """
    traverse the unExploredUserQueue, for every user get the following users and followed users and add these to the graph
    and to the queue
    parameters:
    userConnectedGraph - graph
    unExploredUserQueue - queue contains the users
    ExploredUserMap - map contains the explored users
    """
    while unExploredUserQueue.empty() == False:
        user = unExploredUserQueue.get()
        if user not in ExploredUserMap:
            ExploredUserMap[user] = 1
        
            followersList = getFollowers(user)
            followingUsersList = getFollowing(user)
        
#             userConnectedGraph.add_nodes_from(followersList)
#             userConnectedGraph.add_nodes_from(followingUsersList)
        
            addOutGoingEdges(user, followersList, userConnectedGraph)
            addIncomingEdges(user, followingUsersList, userConnectedGraph)
            print len(followersList), len(followingUsersList)
            for followUser in followersList:
                unExploredUserQueue.put(followUser)
            for followingUser in followingUsersList:
                unExploredUserQueue.put(followingUser)
            print 'queueSize::', unExploredUserQueue.qsize()

def start(repos, counter):
    """
    creates the whole user connected graph
    parameters:
    repos - comma separated repository ids
    counter - int value
    """
    unExploredUserQueue = Queue() #queue for maintaining the unexplored users
    ExploredUserMap = {} #map for maintaining the explored users
    
    global userMap, userCount, userIndexFile
    usersSet = getUsers(repos)
    print len(usersSet)
    
    userConnectedGraph = nx.DiGraph()
    userConnectedGraph.add_nodes_from(usersSet)
    try:
        for user in usersSet:
            if user not in userMap:
                userMap[user] = userCount
                userIndexFile.write(user + " " + str(userCount))
                userIndexFile.write("\n")
                userCount += 1
            unExploredUserQueue.put(user)
            createUserConnectedWholeGraph(userConnectedGraph,unExploredUserQueue, ExploredUserMap)
        gp.processGraph(userConnectedGraph)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        raise e

try:
    conn =mdb.connect(host="localhost",user="root",passwd="root",db="github")
    conn1 = mdb.connect(host="localhost",user="root",passwd="root",db="github_cluster")
    userMap = {}
    userCount = 0
    #loadUsers()
    f = open('/home/raju/Work/Cluster/clusterOutput100_lang5','r')
    userIndexFile = open("/home/raju/Work/communities/betweenness/UserIndex","w")
    counter=1
    for line in f:
        line=line.replace('\n','')
        if counter < 2:
            start(line, counter)
        counter += 1
    userIndexFile.flush()
    userIndexFile.close()
except Exception as e:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    print(exc_type, fname, exc_tb.tb_lineno)
    raise e