

package com.betweenness;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

// the type parameters are the input keys type, the input values type, the
// output keys type, the output values type
public class ForwardMapper extends Mapper<Object, Text, Text, Text> {

	//the parameters are the types of the input key, input value and the Context object through which the Mapper communicates with the Hadoop framework
	public void map(Object key, Text value, Context context, Node inNode)
			throws IOException, InterruptedException {

		

		// For each GRAY node, emit each of the adjacent nodes as a new node (also GRAY)
		//if the adjacent node is already processed and colored BLACK, the reducer retains the color BLACK 
		if (inNode.getColor() == Node.Color.grey) {
			for (String neighbor : inNode.getEdges()) { // for all the adjacent nodes of
												// the gray node

				Node adjacentNode = new Node(); // create a new node

				adjacentNode.setId(neighbor); // set the id of the node
				adjacentNode.setKey(neighbor+ "," + inNode.getRoot());
				//adjacentNode.setEdges(null);
				adjacentNode.setDistance(inNode.getDistance() + 1); // set the distance of the node, the distance of the adjacentNode is set to be the distance
				//of its predecessor node+ 1, this is done since we consider a graph of unit edge weights
				adjacentNode.setColor(Node.Color.grey); // set the color of the node to be GRAY
				String path = inNode.getPath();
				if(path == null || path.equals("") || path.equalsIgnoreCase("null")){
					adjacentNode.setPath(inNode.getId());
				} else{
					adjacentNode.setPath(path + "," + inNode.getId()); // set the parent of the node, if the adjacentNode is already visited by some other parent node, it is not update in the reducer
					//this is because the nodes are processed in terms of levels from the source node, i.e all the nodes in the level 1 are processed first, then the nodes in the level 2 and so on.
				}
				

				//emit the information about the adjacent node in the form of key-value pair where the key is the node Id and the value is the associated information
				//for the nodes emitted here, the list of adjacent nodes will be empty in the value part, the reducer will merge this with the original list of adjacent nodes associated with a node
				System.out.println("in mapper" + adjacentNode.getNodeInfo().toString());
				context.write(new Text(adjacentNode.getKey()), adjacentNode.getNodeInfo());

			}
			// this node is done, color it black
			inNode.setColor(Node.Color.black);
		}

		// No matter what, emit the input node
		// If the node came into this method GRAY, it will be output as BLACK
		//otherwise, the nodes that are already colored BLACK or WHITE are emitted by setting the key as the node Id and the value as the node's information
		context.write(new Text(inNode.getKey()), inNode.getNodeInfo());

	}
}