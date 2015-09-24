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
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.SecurityOperationsImpl;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner.Type;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Parser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {
  private static Options opts;
  private static Option passwordOpt;
  private static Option usernameOpt;
  private static String USAGE = "wordCount <instance name> <zoo keepers> <input dir> <output table>";
  static List<String> east = new ArrayList<String>(Arrays.asList("Celtics","Knicks","76ers","Nets","Raptors","Bulls","Pacers","Bucks","Pistons","Cavs","MiamiHeat","OrlandoMagic","Hawks","Bobcats","Wizards"));
  static List<String> west = new ArrayList<String>(Arrays.asList("okcthunder","Nuggets","TrailBlazers","UtahJazz","TWolves","Lakers","Suns","GSWarriors","Clippers","NBAKings","GoSpursGo","Mavs","Hornets","Grizzlies","Rockets"));
  
  static {
    usernameOpt = new Option("u", "username", true, "username");
    passwordOpt = new Option("p", "password", true, "password");
    
    opts = new Options();
    
    opts.addOption(usernameOpt);
    opts.addOption(passwordOpt);
  }
  
  public static class MapClass extends Mapper<LongWritable,Text,Text,Mutation> {
    
    public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {
    	FileSplit filesplit = (FileSplit) output.getInputSplit();
    	String filename = filesplit.getPath().getName();
    	String[] elements = value.toString().split(",");
    	String[] team = filename.split("\\.");
    	String visible = null;
    	//System.out.println(team[0]);
    	if(east.contains(team[0])){
    		visible = "E";
    	}else if(west.contains(team[0])){
    		visible = "W";
    	}
    	
      String[] words = elements[1].split(" ");
      
      for (String word : words) {
    	  
    	  //System.out.println(word);
        if(word.equalsIgnoreCase("win")){
        	System.out.println("Win");
        	Mutation mutation = new Mutation(new Text(team[0]));
            mutation.put(new Text("win"), new Text("win"),new ColumnVisibility(visible), new Value("1".getBytes()));
            output.write(null, mutation);
        }else if(word.equalsIgnoreCase("lose")){
        	System.out.println("Lose");
        	Mutation mutation = new Mutation(new Text(team[0]));
            mutation.put(new Text("lose"), new Text("lose"),new ColumnVisibility(visible), new Value("1".getBytes()));
            output.write(null, mutation);
        }
        
        
        
      }
    }
  }
  //Reference: http://stackoverflow.com/questions/1252468/java-converting-string-to-and-from-bytebuffer-and-associated-problems
  public static ByteBuffer str_to_bb(String msg){
	  try{
		  Charset charset = Charset.forName("UTF-8");
		   CharsetEncoder encoder = charset.newEncoder();
		   CharsetDecoder decoder = charset.newDecoder();
	    return encoder.encode(CharBuffer.wrap(msg));
	  }catch(Exception e){e.printStackTrace();}
	  return null;
	}
  public int run(String[] unprocessed_args) throws Exception {
    Parser p = new BasicParser();
    
    CommandLine cl = p.parse(opts, unprocessed_args);
    String[] args = cl.getArgs();
    
    String username = cl.getOptionValue(usernameOpt.getOpt(), "root");
    String password = cl.getOptionValue(passwordOpt.getOpt(), "secret");
    
    if (args.length != 4) {
      System.out.println("ERROR: Wrong number of parameters: " + args.length + " instead of 4.");
      return printUsage();
    }
    
    ZooKeeperInstance instance = new ZooKeeperInstance(args[0], args[1]);
    Connector conn = instance.getConnector(cl.getOptionValue(usernameOpt.getOpt(), "root"),cl.getOptionValue(passwordOpt.getOpt(), "secret")) ;
    TableOperations tableOps = conn.tableOperations();
    if (tableOps.exists(args[3])) {             //
    	tableOps.delete(args[3]);
        
    }
    tableOps.create(args[3]);
    IteratorSetting is = new IteratorSetting(10,"my-iter",SummingCombiner.class);
    SummingCombiner.setEncodingType(is,Type.STRING);
    SummingCombiner.setLossyness(is, true); 
    SummingCombiner.setCombineAllColumns(is, true);
    tableOps.attachIterator(args[3],is);
    SecurityOperationsImpl manager = new SecurityOperationsImpl(instance,new AuthInfo(username, str_to_bb(password), instance.getInstanceID()));

    manager.createUser("east", "east".getBytes(), new Authorizations("E"));
    manager.createUser("west", "west".getBytes(), new Authorizations("W"));
    manager.grantSystemPermission("east", SystemPermission.CREATE_TABLE);
    manager.grantSystemPermission("west", SystemPermission.CREATE_TABLE);
    manager.grantTablePermission("east", args[3], TablePermission.READ);
    manager.grantTablePermission("west", args[3], TablePermission.READ);
    manager.changeUserAuthorizations(username, new Authorizations("E", "W"));
    
    Job job = new Job(getConf(), Main.class.getName());
    job.setJarByClass(this.getClass());
    
    job.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.setInputPaths(job, new Path(args[2]));
    
    job.setMapperClass(MapClass.class);
    
    job.setNumReduceTasks(0);
    
    job.setOutputFormatClass(AccumuloOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Mutation.class);
    AccumuloOutputFormat.setOutputInfo(job.getConfiguration(), username, password.getBytes(), true, args[3]);
    AccumuloOutputFormat.setZooKeeperInstance(job.getConfiguration(), args[0], args[1]);
    job.waitForCompletion(true);
    Job job1 = new Job(getConf(), this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
  job1.setJarByClass(this.getClass());
  
  job1.setInputFormatClass(AccumuloInputFormat.class);
  AccumuloInputFormat.setZooKeeperInstance(job1.getConfiguration(), args[0], args[1]);
  AccumuloInputFormat.setInputInfo(job1.getConfiguration(), "east", "east".getBytes(), args[3], new Authorizations("E"));
  String columnstring = "win:win,lose:lose";
  HashSet<Pair<Text,Text>> columnsToFetch = new HashSet<Pair<Text,Text>>();
  for (String col : columnstring.split(",")) {
    int idx = col.indexOf(":");
    Text cf = new Text(idx < 0 ? col : col.substring(0, idx));
    Text cq = idx < 0 ? null : new Text(col.substring(idx + 1));
    if (cf.getLength() > 0)
      columnsToFetch.add(new Pair<Text,Text>(cf, cq));
  }
  if (!columnsToFetch.isEmpty())
    AccumuloInputFormat.fetchColumns(job1.getConfiguration(), columnsToFetch);
  
  job1.setMapperClass(East.EastMapper.class);
  job1.setReducerClass(East.EastReduce.class);
  job1.setMapOutputKeyClass(Text.class);
  job1.setMapOutputValueClass(Text.class);
  
  
  job1.setNumReduceTasks(1);
  
  job1.setOutputFormatClass(TextOutputFormat.class);
  TextOutputFormat.setOutputPath(job1, new Path("/Eastoutput"));
  
  job1.waitForCompletion(true);
  
  Job job2 = new Job(getConf(), this.getClass().getSimpleName() + "_" + System.currentTimeMillis());
job2.setJarByClass(this.getClass());

job2.setInputFormatClass(AccumuloInputFormat.class);
AccumuloInputFormat.setZooKeeperInstance(job2.getConfiguration(), args[0], args[1]);
AccumuloInputFormat.setInputInfo(job2.getConfiguration(), "west", "west".getBytes(), args[3], new Authorizations("W"));
//String columnstring = "win:win,lose:lose";
HashSet<Pair<Text,Text>> columnsToFetch1 = new HashSet<Pair<Text,Text>>();
for (String col : columnstring.split(",")) {
  int idx = col.indexOf(":");
  Text cf = new Text(idx < 0 ? col : col.substring(0, idx));
  Text cq = idx < 0 ? null : new Text(col.substring(idx + 1));
  if (cf.getLength() > 0)
    columnsToFetch1.add(new Pair<Text,Text>(cf, cq));
}
if (!columnsToFetch1.isEmpty())
  AccumuloInputFormat.fetchColumns(job2.getConfiguration(), columnsToFetch1);

job2.setMapperClass(West.WestMapper.class);
job2.setReducerClass(West.WestReduce.class);
job2.setMapOutputKeyClass(Text.class);
job2.setMapOutputValueClass(Text.class);


job2.setNumReduceTasks(1);

job2.setOutputFormatClass(TextOutputFormat.class);
TextOutputFormat.setOutputPath(job2, new Path("/Westoutput"));

job2.waitForCompletion(true);


    return 0;
  }

  
  private int printUsage() {
    HelpFormatter hf = new HelpFormatter();
    hf.printHelp(USAGE, opts);
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(CachedConfiguration.getInstance(), new Main(), args);
    System.exit(res);
  }
}