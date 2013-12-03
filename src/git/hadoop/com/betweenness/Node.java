package com.betweenness;
import java.util.*;

import org.apache.hadoop.io.Text;

/**
 *  
 *         Description : Node class to process the information about the nodes. This class contains getter and setter methods to access and
 *         set information about the nodes. The information generally includes the list of adjacent nodes, distance from the source, color of the node
 *         and the parent/predecessor node.
 * 
 *          
 *         Reference :
 *         http://www.johnandcailin.com/blog/cailin/breadth-first-graph
 *         -search-using-iterative-map-reduce-algorithm
 *  
 *         changes made from the source code in the reference: parent node field is added.
 * 
 *         Hadoop version used : 0.20.2
 */
public class Node {

	// three possible colors a node can have (to keep track of the visiting status of the nodes during graph search)

	public static enum Color {
		white, grey, black
	};

	private String id; // id of the node
	private int distance; // distance of the node from the source
	private List<String> edges = new ArrayList<String>(); // list of edges
	private Color color = Color.white;
	private String path; // parent/predecessor of the node : The parent of the source is marked "source" to leave it unchanged
	private String root;
	private String key;

	public Node() {
		
		distance = Integer.MAX_VALUE;
		color = Color.white;
		path = null;
	}

	// constructor
	//the parameter nodeInfo  is the line that is passed from the input, this nodeInfo is then split into key, value pair where the key is the node id
	//and the value is the information associated with the node
	public Node(String nodeInfo) {

		String[] inputLine = nodeInfo.split(" "); //splitting the input line record by tab delimiter into key and value
		if(inputLine.length != 2){
			inputLine = nodeInfo.split("\t");
		}
		String key = "", value = ""; //initializing the strings 'key' and 'value'

		try {
			key = inputLine[0];
			this.key = key;
			String tokens[] = key.split(",");
			this.id = tokens[0];
			this.root = tokens[1];
			value = inputLine[1]; // the list of adjacent nodes, distance, color, parent

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);

		}

		String[] tokens = value.split("\\|"); // split the value into tokens where
		//tokens[0] = list of adjacent nodes, tokens[1]= distance, tokens[2]= color, tokens[3]= parent

		//this.id = key; // set the id of the node

		// setting the edges of the node
		if(tokens[0]!=null && !tokens[0].equalsIgnoreCase("null")){
			for (String s : tokens[0].split(",")) {
				if (s.length() > 0) {
					edges.add(s);
				}
			}
		}

		// setting the distance of the node
		if (tokens[1].equals("Integer.MAX_VALUE")) {
			this.distance = Integer.MAX_VALUE;
		} else {
			this.distance = Integer.parseInt(tokens[1]);
		}

		// setting the color of the node
		this.color = Color.valueOf(tokens[2]);
		
		this.path = tokens[3];
		// setting the parent of the node
//		if(tokens[3].equalsIgnoreCase("null") || tokens[3].equalsIgnoreCase("source")){
//			this.path = "";
//		} else {
//			this.path = tokens[3];
//		}
//		

	}

	// this method appends the list of adjacent nodes, the distance , the color and the parent and returns all these information as a single Text
	public Text getNodeInfo() {
		StringBuffer s = new StringBuffer();

		// forms the list of adjacent nodes by separating them using ','
		try {
			if(edges.size() != 0){
				int size = edges.size();
				for (int i=0; i< size; i++) {
					if(i == size-1){
						s.append(edges.get(i));
					} else {
						s.append(edges.get(i)).append(",");
					}
				} 
			} else {
				s.append("null");
			}
		} catch (NullPointerException e) {
			e.printStackTrace();
			System.exit(1);
		}

		// after the list of edges, append '|'
		s.append("|");

		// append the minimum distance between the current distance and
		// Integer.Max_VALUE
		if (this.distance < Integer.MAX_VALUE) {
			s.append(this.distance).append("|");
		} else {
			s.append("Integer.MAX_VALUE").append("|");
		}

		// append the color of the node
		s.append(color.toString()).append("|");

		// append the parent of the node
		s.append(getPath());

		return new Text(s.toString());
	}

	// getter and setter methods

	public String getId() {
		return this.id;
	}

	public int getDistance() {
		return this.distance;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setDistance(int distance) {
		this.distance = distance;
	}

	public Color getColor() {
		return this.color;
	}

	public void setColor(Color color) {
		this.color = color;
	}

	public List<String> getEdges() {
		return this.edges;
	}

	public void setEdges(List<String> edges) {
		this.edges = edges;
	}

	public void setPath(String parent) {
		this.path = parent;
	}

	public String getPath() {
		return path;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getRoot() {
		return root;
	}

	public void setRoot(String root) {
		this.root = root;
	}

}