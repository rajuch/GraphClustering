package com.betweennness.util;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.betweenness.Node;

public class Util {

	public String findMaxEdge(String path) {

		Map<String, Integer> edgeWeightMap = new HashMap<String, Integer>();
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			FileStatus[] status = fs.listStatus(new Path(path));

			for (int i = 0; i < status.length; i++) {
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(status[i].getPath())));
				String line;
				while ((line = br.readLine()) != null) {
					String[] tokens = line.split(" ");
					if(tokens.length != 2){
						tokens = line.split("\t");
					}
					edgeWeightMap.put(tokens[0], Integer.parseInt(tokens[1]));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		String maxEdge = "";
		int maxWeight = 0;
		for (String edge : edgeWeightMap.keySet()) {
			int weight = edgeWeightMap.get(edge);
			if (weight > maxWeight) {
				maxWeight = weight;
				maxEdge = edge;
			}
		}
		return maxEdge;
	}
	
	public void removeEdge(String edge, String path) {
		
		Map<String, Node> nodeInfoMap;
		BufferedReader br;
		BufferedWriter bw;
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			FileStatus[] status = fs.listStatus(new Path(path));

			for (int i = 0; i < status.length; i++) {
				nodeInfoMap = new HashMap<String, Node>();
				br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
				Path tempPath = new Path(path + "temp");
				bw = new BufferedWriter(new OutputStreamWriter(fs.create(tempPath, true)));
				
				String line;

				while ((line = br.readLine()) != null) {
					// trim newline when comparing with lineToRemove
					String trimmedLine = line.trim();
					Node node = new Node(trimmedLine);
					nodeInfoMap.put(node.getId(), node);
				}
				String[] tokens = edge.split("-");
				String node1 = tokens[0];
				String node2 = tokens[1];
				if (nodeInfoMap.get(node1) != null) {
					Node node = nodeInfoMap.get(node1);
					List<String> edges = node.getEdges();
					int index;
					if ((index = edges.indexOf(node2)) != -1) {
						edges.remove(index);
					}
				}
				for (String nodeId : nodeInfoMap.keySet()) {
					Node node = nodeInfoMap.get(nodeId);
					String nodeInfo = node.getNodeInfo().toString();
					bw.write(node.getKey() + " " + nodeInfo);
					bw.write("\n");
				}
				bw.flush();
				bw.close();
				fs.delete(status[i].getPath(), false);
				boolean success = fs.rename(tempPath, status[i].getPath());
				if(!success){
					System.out.println("something went wrong while renaming");
					System.exit(0);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void writeRemovedEdgeInfo(String edge, String path){
		Path pt=new Path(path);
        FileSystem fs;
        BufferedWriter bw;
		try {
			fs = FileSystem.get(new Configuration());
			if(!fs.exists(pt)){
				bw = new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
			} else {
				bw = new BufferedWriter(new OutputStreamWriter(fs.append(pt)));
			}
			bw.append(edge);
			bw.append("\n");
			bw.flush();
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	/*
	public static void main(String[] args){
		Util util = new Util();
		util.writeRemovedEdgeInfo(args[1], args[0]);
	} */
}
