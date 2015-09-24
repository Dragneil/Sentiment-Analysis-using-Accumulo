/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Takes a table and outputs the specified column to a set of part files on hdfs accumulo accumulo.examples.mapreduce.TableToFile <username> <password>
 * <tablename> <column> <hdfs-output-path>
 */
public class East extends Configured {
  /**
   * The Mapper class that given a row number, will generate the appropriate output line.
   */
  public static class EastMapper extends Mapper<Key,Value,Text,Text> {
    public void map(Key row, Value data, Context context) throws IOException, InterruptedException {
      final Key r = row;
      final Value v = data;
      Map.Entry<Key,Value> entry = new Map.Entry<Key,Value>() {
        @Override
        public Key getKey() {
          return r;
        }
        
        @Override
        public Value getValue() {
          return v;
        }
        
        @Override
        public Value setValue(Value value) {
          return null;
        }
      };
      String line = DefaultFormatter.formatEntry(entry, false);
      String[] element = line.split(" ");
      String[] tab = line.split("	");
      String[] winLoss = element[1].split(":");
      Text key = new Text(winLoss[1]);
      Text value = new Text(element[0]+"/"+tab[1]);
      System.out.println(line);
      context.write(key, value);
    }
  }
  public static class EastReduce extends Reducer<Text, Text, Text, Text> {
	  private Text value = new Text();
	  public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
	  List<String> east = new ArrayList<String>(Arrays.asList("Celtics","Knicks","76ers","Nets","Raptors","Bulls","Pacers","Bucks","Pistons","Cavs","MiamiHeat","OrlandoMagic","Hawks","Bobcats","Wizards"));
	  Map<String, String> EastTeam = new HashMap(){
		  {
			put("Celtics","Boston Celtics");
			put("Knicks","New York Knicks");
			put("76ers","Philadelphia 76ers");
			put("Nets","New Jersey Nets");
			put("Raptors","Toronto Raptors");
		   
			put("Bulls","Chicago Bulls");
		    put("Pacers","Indiana Pacers");
		    
		    put("Bucks","Milwaukee Bucks");
		    put("Pistons","Detroit Pistons");
		    
		    put("Cavs","Cleveland Cavaliers");
		    put("MiamiHeat","Miami Heat");
		    
		    put("OrlandoMagic","Orlando Magic");
		    put("Hawks","Atlanta Hawks");
		    
		    put("Bobcats","Charlotte Bobcats");
		    put("Wizards","Washington Wizards");
		    }};
	
	  Map<String, Integer> map = new HashMap<String, Integer>();
	  for(String x: east){
		  map.put(x, 0);
	  }
	  System.out.println(key);
	  for (Text value:values){
	  String[] h = value.toString().split("/");
	  	//System.out.println(h[0]+ "    "+h[1]);
	  if(map.containsKey(h[0])){
		  map.put(h[0].toString(), Integer.parseInt(h[1]));
	  }
	  }
	  String finallist = key.toString()+ "\n";
	  Map<String, Integer> sortedmap = SortByValue(map);
	  for(String x : sortedmap.keySet()){
		  System.out.println(x+" "+ sortedmap.get(x));
		  finallist = finallist + EastTeam.get(x)+ "," +x + ","+ sortedmap.get(x).toString()+ "\n";
	  }
	  key.set(" ");
	  value.set(finallist);
	  context.write(key,value);
	  }
	  //Reference: http://stackoverflow.com/questions/12738216/sort-hashmap-with-duplicate-values
	  static Map<String, Integer> SortByValue(Map<String, Integer> unsortedmap) {
	  // TODO Auto-generated method stub
	  List list = new LinkedList(unsortedmap.entrySet());
	  //System.out.println(list.size()+ "Size");
	  Collections.sort(list,new Comparator(){
	  @Override
	  public int compare(Object o1, Object o2) {
	                 return ((Comparable) ((Map.Entry) (o2)).getValue()).compareTo(((Map.Entry) (o1)).getValue());
	             }
	  });
	  //int M = 0;
	  Map result = new LinkedHashMap();
	  Iterator it = list.iterator();
	  while (it.hasNext()) {
	             Map.Entry entry = (Map.Entry) it.next();
	             result.put(entry.getKey(), entry.getValue());
	         //    M +=1;
	         }
	  //System.out.println(M);
	  return result;
	  }
	  }
}