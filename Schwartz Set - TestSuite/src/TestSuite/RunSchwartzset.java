package TestSuite;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.ActionOnFailure;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsResult;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.ListStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;

import edu.princeton.cs.algs4.*;


/**
 * This Java Class provides functions used to run MapReduce Schwartzset program using Amazon Web Services with generated data.
 * <br>
 * The method {@link #experiments(AmazonElasticMapReduce, String, String, AmazonS3, String, String, String) experiments()} contains the code
 * for performing the experiments presented in the AAAI-17 submission "Winner Determination in Huge-Elections with MapReduce".
 *
 *  
 */
public class RunSchwartzset {
	
	/**
	 * Compute the Schwartzset of the given graph locally. 
	 * The number of SCCs in the graph and the size of the Schwartzset are written to the logFile.
	 * Returns the list of vertices contained in the Schwartzset.
	 * 
	 * @param graph Contains a directed graph (DiGraph).
	 * @param logFile PrintWriter object of log File.
	 * @return schwartzset List of vertices contained in the schwartzset of graph.
	 */
	public static List<Integer> SchwartzSet(Digraph graph, PrintWriter logFile){
		
		//Reverse the Graph: 
		 Digraph graphrev = graph.reverse();
		 
		 //Compute all SCCs: 
		 TarjanSCC sccs= new TarjanSCC(graph);
		 
		 //Compute the Transitive Closure of the Graph:
		 TransitiveClosure closure = new TransitiveClosure(graph);
		 
		 //schwartzlist contains all SCCs contained in the schwartzset
		 List<Integer> schwartzlist = new ArrayList<Integer>(); 
		 
		 //checked is the list of all SCCs that have already been checked if they are undominated
		 List<Integer> checked = new ArrayList<Integer>();
		 
		 //schwartzset contains the list of vertices contained in the schwartzset
		 List<Integer> schwartzset = new ArrayList<Integer>();
		 
		 //Number of Vertices in the Graph:
		 int n=graph.V();		 
		 
		 //other variables: 
		 int scc; 
		 boolean schwartz;
		 
		 logFile.write("Total SCCs:"+sccs.count()+"\n");

		 /*
		  * First get all IDs of the SCCs that are undominated.
		  * The list checked contains all IDs of SCCs that have already been checked. 
		  * 
		  */
		 for(int i=0; i<n; i++){
			 scc=sccs.id(i);
			 if(checked.contains(scc)){
				 /* 
				  * this SCC has already been checked - go to the next
				  */
			 }else{
				 /*
				  * check if SCC is undominated
				  */
				 schwartz=true;
				 for(int j : graphrev.adj(i)){
					 if(sccs.id(j)!=scc){
							 if(!closure.reachable(i,j)){
								 /*
								  * the vertex j is dominated by at least one vertex outside of its SCC, 
								  * therefore it is not in the Schwartzset.
								  */
								 schwartz=false;
								 break;
							 }
						 }
						 //StdOut.printf("%d is not in the schwarz set", i);
				 }
				 checked.add(scc);
				 if(schwartz){
					 schwartzlist.add(scc);
				 }
			 }
		 }
		 
		 /*
		  * schwartzlist now contains all SCCs that are in the Schwartzset
		  * Next we have to get all vertices contained in those SCCs
		  */
		 for(int i=0; i<n; i++){
			 if(schwartzlist.contains(sccs.id(i))){
				 schwartzset.add(i);
			 }
		 }
			
		 /*
		  * Write information about the Schwartzset to the logFile.
		  */
		 logFile.write("Vertices in Schwartzset: "+schwartzset.size()+"\n");
		 logFile.write("SCCs in Schwartzset: "+schwartzlist.size()+"\n");
	
		return schwartzset;
	 }
	 
	
	/**
	 * Writes a graph to one single txt-file in the format used as input for the MapReduce algorithms.
	 * 
	 * @param file Destination file of the graph. 
	 * @param graph The graph that is written to the file. 
	 * 
	 */
	public static void writeGraphToFile(File file, Digraph graph){
		
		//used to store the adjacent vertices of one vertex.
		List<Integer> adj = new ArrayList<Integer>();
		
		//number of vertices in graph
		int n=graph.V();
		 
		 try {
			file.createNewFile();
			FileOutputStream stream = new FileOutputStream(file);
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(stream));
			
			
			for (int i = 0; i < n; i++) {
				writer.write(i + "\t");
				adj.clear();
				for(int v : graph.adj(i)){
					adj.add(v);
				}
				for(int j = 0; j<n; j++){
					if (adj.contains(j)){
						writer.write("1");
					}else{
						writer.write("0");
					}
					if(j!=(n-1)) writer.write(",");
				}
				writer.newLine();
			}
		 
			writer.close();
		 
		} catch (IOException e) {
			System.out.println("some error while writing to file");
		}
	}
	
	
	/**
	 * Writes a graph to several files. maxEntries is the maximum number of entries per file and 
	 * should be a multiple of the number of vertices in the graph. 
	 * 
	 * The naming of the files is only configured for up to 999 files. 
	 * 
	 * @param path Path where the files are written to. 
	 * @param graph Input graph. 
	 * @param maxEntries maximum number of entries per file.  
	 * 
	 */
	public static void writeGraphToFiles(String path, Digraph graph, Integer maxEntries){
		List<Integer> adj = new ArrayList<Integer>();
		int n=graph.V();
		int linesperfile=(int) Math.floor((double) maxEntries / (double) n);
		int nFiles=(int) Math.ceil((double) n / (double) linesperfile);
		File file = null;
		int start, end;
		BufferedWriter writer = null;
		FileOutputStream stream;
		Boolean check;
		
		System.out.println("linesperfile="+linesperfile+", nFiles="+nFiles+", total vertices="+n+"\n");
		if(maxEntries<n) {
			System.out.println("maxEntries is smaller than number of vertices. Choose a larger number of maximum entries per File.\n");
		}else if(nFiles>999) {
			System.out.println("Too many files. Try larger number of Entries per File");
		}else{
		 
		 try {
			 File folder = new File(path);
			 if(folder.exists()){
				 File[] listOfFiles = folder.listFiles();
				 if(listOfFiles!=null){
					 for (int i = 0; i < listOfFiles.length; i++) {
					      if (listOfFiles[i].isFile()) {
					    	  listOfFiles[i].delete();
					      } 
					 }
				 }
			}else{
				folder.createNewFile();
				
			}
			 
			for(int f=1;f<=nFiles;f++){
				if(f<10){
					file = new File(path+"part00"+f+".txt");
				}else if(f<100){
					file = new File(path+"part0"+f+".txt");
				}else if(f<1000){
					file = new File(path+"part"+f+".txt");
				}else{
					System.out.println("Too many files.");
				}
				
				check=file.createNewFile();
				if(check==false){
					file.delete();
					file.createNewFile();
				}
				stream = new FileOutputStream(file);
				 
				writer = new BufferedWriter(new OutputStreamWriter(stream));
				
				start=(f-1)*linesperfile;
				end=f*linesperfile;
				if(end>n){
					end=n;
				}
				
				for (int i = start; i < end; i++) {
					writer.write(i + "\t");
					adj.clear();
					for(int v : graph.adj(i)){
						adj.add(v);
					}
					for(int j = 0; j<n; j++){
						if (adj.contains(j)){
							writer.write("1");
						}else{
							writer.write("0");
						}
						if(j!=(n-1)) writer.write(",");
					}
					writer.newLine();
				}
				writer.close();
				
			}
		
		 
			
		 
		} catch (IOException e) {
			System.out.println("some error while writing to files");
		}
		}
	}
	
	/**
	 * Uploads all files in localpath to a s3 bucket, with bucketName and destination path given by inputpaths3.
	 * 
	 * @param localpath Local path containing the files.
	 * @param s3 AmazonS3 object.
	 * @param bucketName Name of the destination bucket.
	 * @param inputpaths3 Destination path on the bucket. 
	 */
	public static void uploadFiles(String localpath,AmazonS3 s3, String bucketName, String inputpaths3){
		File folder = new File(localpath);
		File[] listOfFiles = folder.listFiles();
		
		//delete files on s3 first
		deleteFiles(bucketName,inputpaths3,s3);
		
		for (int i = 0; i < listOfFiles.length; i++) {
		      if (listOfFiles[i].isFile()) {
		    	  s3.putObject(new PutObjectRequest(bucketName, inputpaths3+listOfFiles[i].getName(),listOfFiles[i]));
		      } 
		}
		
	}
	
	/**
	 * Start a Mapreduce Computation using Amazon Elastic Mapreduce with the jar file located on s3 (jarlocation) and
	 * with inputpath and outputpath on the same s3-bucket. The files in the inputpath are ideally created using the 
	 *  provided method {@link #writeGraphToFiles(String, Digraph, Integer) writeGraphToFiles()}.
	 * 
	 * @param emr AmazonElasticMapReduce object
	 * @param jarlocation location of the jar file on the s3 bucket with the MapReduce program
	 * @param bucketName name of the s3 bucket
	 * @param inputpaths3 path on the s3 bucket containing the input files
	 * @param outputpaths3 outputpath on the s3 bucket 
	 * @param clusterID ID of the used cluster
	 * @param N number of vertices in the input graph
	 * @return AddJobFlowStepsResult
	 */
	public static AddJobFlowStepsResult startComputation(AmazonElasticMapReduce emr,String jarlocation,String bucketName,String inputpaths3,String outputpaths3,String clusterID, String N){	 
			System.out.println("HadoopJarStepConfig\n");
			HadoopJarStepConfig SchwartzSet = new HadoopJarStepConfig()
		        .withJar(jarlocation)
		        .withArgs("s3://"+bucketName+"/"+inputpaths3, "s3://"+bucketName+"/"+outputpaths3+"step", N); 
			
			System.out.println("StepConfig\n");
		    StepConfig Step1 = new StepConfig("SchwartzSet", SchwartzSet)
		    	.withActionOnFailure(ActionOnFailure.CONTINUE);
		   
		    System.out.println("AddJobFlowStep\n");
		    AddJobFlowStepsResult result = emr.addJobFlowSteps(new AddJobFlowStepsRequest()
			  .withJobFlowId(clusterID)
			  .withSteps(Step1));
		    return result;
	
	}
	
	/**
	 * Wait until all running and pending jobs are finished on the cluster.
	 * 
	 * @param emr AmazonElasticMapReduce object
	 * @param clusterID ID of the cluster.
	 * 
	 */
    public static void WaitUntilFinished(AmazonElasticMapReduce emr, String clusterID){
				 try {
					Thread.sleep(60000); //wait one minute until Steps start
				    while(
				    		  emr.listSteps(new ListStepsRequest()
				      		  	.withClusterId(clusterID)
				      		  	.withStepStates("RUNNING")).getSteps().isEmpty()==false 
				      		  	|| emr.listSteps(new ListStepsRequest()
				      			.withClusterId(clusterID)
				      			.withStepStates("PENDING")).getSteps().isEmpty()==false){
				      	  					Thread.sleep(30000); //wait another half minute before checking again
				        }
					} catch (InterruptedException e1) {
						System.out.println("some error while waiting");
					} 
				 
				
    }
	
    
    /**
     * Delete all files in the given path on the s3 bucket.
     * 
     * @param bucketName name of the used s3 bucket.
     * @param path all files with this prefix are going to be deleted.
     * @param s3 AmazonS3 object
     */
    public static void deleteFiles(String bucketName,String path, AmazonS3 s3){
	    ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
	    	.withBucketName(bucketName)
	    	.withPrefix(path));
	    
	    for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
	    	s3.deleteObject(new DeleteObjectRequest(bucketName, objectSummary.getKey()));	
	    }
}
    
	/**
	 * Reads the output of the SchwartzSet Mapreduce Program from the outputpath on s3
	 * and prints the result to stdout.
	 * 
	 * @param bucketName name of the used s3 bucket.
	 * @param outputpaths3 outputpath of the SchwartzSet job.
	 * @param s3 AmazonS3 object
	 */
    public static void printOutput(String bucketName,String outputpaths3, AmazonS3 s3){
			    ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
			    	.withBucketName(bucketName)
			    	.withPrefix(outputpaths3+"stepbyrow/part"));
			    
			    for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
			    	System.out.println("file: "+ objectSummary.getKey()+ "\n");
			        S3Object object = s3.getObject(new GetObjectRequest(bucketName,objectSummary.getKey()));
			        InputStream objectData = object.getObjectContent();
			            try{
			            	// Read one text line at a time and display.
			                BufferedReader reader = new BufferedReader(new InputStreamReader(objectData));
			                while (true) {
			                    String line = reader.readLine();
			                    if (line == null) break;
			
			                    System.out.println("    " + line);
			                }
			                System.out.println();
			            }catch(IOException e) {
							System.out.println("Error while reading data from Buffer");

			    		}
			    }
    }
    
    
    /**
     * Checks whether the two sets are identical.
     * 
     * @param set1
     * @param set2
     * @return true if the sets are identical, false otherwise.
     */
    public static Boolean check(List<Integer> set1, List<Integer> set2){
    	if(set1.size()!=set2.size()){
    		return false;
    	}else{
    		for(int i : set1){
    			if(!set2.contains(i)){
    				return false;
    			}
    		}
    		for(int j : set2){
    			if(!set1.contains(j)){
    				return false;
    			}
    		}
    	}
    	return true;
    	
    }
    
    
	/**
	 * Reads the result of the SchwartzSet Mapreduce Program from outputpath on s3. 
	 * 
	 * @param bucketName name of the used s3 bucket.
	 * @param outputpaths3 outputpath of the SchwartzSet job.
	 * @param s3 AmazonS3 object
	 * @return Schwartzset
	 */
    public static List<Integer> getOutput(String bucketName,String outputpaths3, AmazonS3 s3){
	    ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
	    	.withBucketName(bucketName)
	    	.withPrefix(outputpaths3+"stepbyrow/part"));
	    
	    List<Integer> schwartzset = new ArrayList<Integer>();
	    
	    for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
	    	//System.out.println("file: "+ objectSummary.getKey()+ "\n");
	        S3Object object = s3.getObject(new GetObjectRequest(bucketName,objectSummary.getKey()));
	        InputStream objectData = object.getObjectContent();
	            try{
	            	// Read one text line at a time and display.
	                BufferedReader reader = new BufferedReader(new InputStreamReader(objectData));
	                while (true) {
	                    String line = reader.readLine();
	                    if (line == null) break;
	                    schwartzset.add(Integer.parseInt(line));
	                    //System.out.println("    " + line);
	                }
	                System.out.println();
	            }catch(IOException e) {
					System.out.println("Error while reading data from Buffer");

	    		}
	    }
	    return schwartzset;
}
    
    
    /**
     * Unzip the .gz-file at filepath and write the result to "output".
     * 
     * @param filepath path of the .gz file
     * @param output the path of the unzipped file
     */
    public static void unzip(String filepath, String output){

        byte[] buffer = new byte[1024];

        try{

       	 GZIPInputStream gzis =
       		new GZIPInputStream(new FileInputStream(filepath));

       	 FileOutputStream out =
               new FileOutputStream(output);

           int len;
           while ((len = gzis.read(buffer)) > 0) {
           	out.write(buffer, 0, len);
           }

           gzis.close();
       	out.close();

       }catch(IOException ex){
          ex.printStackTrace();
       }
      }
    

    /**
     * Appends the second file to the first file.
     * 
     * @param filepath1 path of the first file.
     * @param filepath2 path of the second file. 
     * @throws IOException
     */
    public static void mergefiles(String filepath1, String filepath2) throws IOException{
		FileWriter fw = new FileWriter(filepath1, true);
		BufferedWriter bw = new BufferedWriter(fw);
		PrintWriter writer = new PrintWriter(bw);
		BufferedReader reader = new BufferedReader(new FileReader(filepath2));
		String line = reader.readLine();
		while(line != null){
			writer.write(line+"\n");
			line = reader.readLine();
		}
		writer.close();
		reader.close();
    }
    
    /**
     * Reads statistics from the syslog files created by the MapReduce jobs. The clusterlog at clusterlogpath was created 
     * by {@link #runComputationsOnCluster(AmazonElasticMapReduce, String, String, String, int[], double[], int, PrintWriter) runComputationsOnCluster()}.
     * 
     * @param clusterlogpath path of the clusterlog file (created by {@link  #runComputationsOnCluster(AmazonElasticMapReduce, String, String, String, int[], double[], int, PrintWriter) runComputationsOnCluster()})
     * @param sourcepath local path containing the syslogs (downloaded with {@link #getSyslogCluster(String, String, String[], AmazonS3, String) getSyslogCluster()})
     * @param output destination file (a csv file)
     * @param merge if true, than: if there is more than one syslog file the unzipped files are merged using {@link #mergefiles(String,String) mergefiles()}
     * @throws IOException
     */
    public static void getStatisticsWithIDs(String clusterlogpath, String sourcepath, String output, Boolean merge) throws IOException{
    	String thispath=null;
    	File folder = null;
		File clusterlogs = new File(clusterlogpath);
		FileInputStream fis = new FileInputStream(clusterlogs);
		BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
		String line = null;
		String[] parts;
		line = null;
		
		while((line=reader.readLine())!= null){
			parts = line.split(",");
			thispath= sourcepath+parts[0]+"/"+parts[1].trim().replaceAll("[\\[\\]]","");
			folder = new File(thispath);
			if(folder.isDirectory()){
				if(merge){
					if(folder.listFiles().length==4){
						mergefiles(thispath+"/syslog1.txt",thispath+"/syslog2.txt");
					}else if(folder.listFiles().length==6){
						mergefiles(thispath+"/syslog1.txt",thispath+"/syslog2.txt");
						mergefiles(thispath+"/syslog1.txt",thispath+"/syslog3.txt");
					}
				}
				
				getStatistics(thispath+"/syslog1.txt",output,parts[1].trim().replaceAll("[\\[\\]]",""));
			}
		}
		reader.close();
    }
    
    /**
     * Reads statistics from an unzipped syslog file and writes to output.
     *  
     * @param input unzipped syslog file
     * @param output .csv file containing the gathered statistics
     * @param stepID stepID of the computation step (not necessarily needed to extract the data from the syslog file)
     * @throws IOException
     */
	public static void getStatistics(String input, String output, String stepID) throws IOException {
		 
		FileWriter fw = new FileWriter(output, true);
		BufferedWriter bw = new BufferedWriter(fw);
		PrintWriter writer = new PrintWriter(bw);
    	 String[] parts;
		// unzip(input, tmpfile);
 	     BufferedReader reader = new BufferedReader(new FileReader(input));
 	     String tmp="-";
 	     
		 StringBuilder newline=new StringBuilder();
		 newline.append(stepID+";");
 	            try{
 	            	// Read one text line at a time and display.
 	                
 	                while (true) {
 	                    String line = reader.readLine();
 	                   // System.out.print(line);
 	                    if (line == null) break;
 	                    if(line.contains("INFO org.apache.hadoop.mapreduce.Job (main): Running job:")){
 	                    	parts=line.split(":");
 	                    	newline.append(parts[4]+";");
 	                    	parts=line.split(",");
 	                    	newline.append(parts[0]+";");	
	 	                    while(!line.contains("INFO org.apache.hadoop.mapreduce.Job (main):  map")){
	 	                    	line = reader.readLine();
	 	                    	if(line==null) {
	 	                    		reader.close();
	 	                    		writer.close();
	 	                    		return;
	 	                    	}
	 	                    }
 	                    	parts=line.split(",");
 	                    	newline.append(parts[0]+";"); //map starting time
 	                    	
 	                    	while(!line.contains("INFO org.apache.hadoop.mapreduce.Job (main):  map 100% reduce")){
 	                    		line = reader.readLine();
 	                    		if(line==null) {
	 	                    		reader.close();
	 	                    		writer.close();
	 	                    		return;
	 	                    	}
 	                    		if(line.contains("reduce 0%")){
 	                    			parts=line.split(",");
 	 	                    		tmp=parts[0];
 	                    		}
 	                    	}
 	                    	parts=line.split(",");
 	                    	newline.append(parts[0]+";"); //map finishing time
 	                    	
 	                    	//reduce starting time:
 	                    	if(line.contains("reduce 0%")){
 	                    		newline.append(parts[0]+";"); 
 	                    	}else{
 	                    		newline.append(tmp+";"); 
 	                    	}
 	                    	
 	                    	while(!line.contains("INFO org.apache.hadoop.mapreduce.Job (main):  map 100% reduce 100%")){
 	                    		line = reader.readLine();
 	                    		if(line==null) {
	 	                    		reader.close();
	 	                    		writer.close();
	 	                    		return;
	 	                    	}
 	                    	}
 	                    	parts=line.split(",");
	                    	newline.append(parts[0]+";"); //reduce finishing time
	                    	while(!line.contains("INFO org.apache.hadoop.mapreduce.Job (main): Job")){
	                    		line = reader.readLine();
	                    		if(line==null) {
	                    			reader.close();
	 	                    		writer.close();
	 	                    		return;
	                    		}
	                    	}
	                    	parts=line.split(",");
	                    	newline.append(parts[0]+";"); //job finishing time
 	                    	
	                    }else{
	                    	line.replaceAll("\\t", "");
	                    	parts = line.split("=");
	                    	if(parts.length==2){
	 	                    	if(parts[0].contains("Launched map tasks")){//Job Counters
	 	                    		newline.append(parts[1]+";");
	 	                    	}else if(parts[0].contains("Launched reduce tasks")){//Job Counters
	 	                    		newline.append(parts[1]+";");
	 	                    	}else if(parts[0].contains("Total time spent by all map tasks (ms)")){	 //Job Counters
	 	                    		newline.append(parts[1]+";");
	 	                    	}else if(parts[0].contains("Total time spent by all reduce tasks (ms)")){//Job Counters
	 	                    		newline.append(parts[1]+";");
	 	                    	}else if(parts[0].contains("Map input records")){//Map-Reduce Framework
	 	                    		newline.append(parts[1]+";");
	 	                    	}else if(parts[0].contains("Reduce input records")){//Map-Reduce Framework
	 	                    		newline.append(parts[1]+";");
	 	                    	}else if(parts[0].contains("Reduce output records")){//Map-Reduce Framework
	 	                    		newline.append(parts[1]+";");
	 	                    	}else if(parts[0].contains("CPU time spent (ms)")){//Map-Reduce Framework
	 	                    		newline.append(parts[1]+";");
	 	                    	}else if(parts[0].contains("Physical memory (bytes)")){//Map-Reduce Framework
	 	                    		newline.append(parts[1]+";");
	 	                    	}else if(parts[0].contains("Virtual memory (bytes)")){//Map-Reduce Framework
	 	                    		newline.append(parts[1]+";");
	 	                    	}else if(parts[0].contains("Total committed heap usage")){//Map-Reduce Framework
	 	                    		newline.append(parts[1]+";");
	 	                    	}else if(parts[0].contains("Bytes Read")){
	 	                    		newline.append(parts[1]+";");
	 	                    	}else if(parts[0].contains("Bytes Written")){
	 	                    		newline.append(parts[1]+"\n");
	 	                    		writer.write(newline.toString());
	 	                    		newline = new StringBuilder();
	 	                    		newline.append(stepID+";");
	 	                    	}
	 	                    }
	                    }

 	                }
 	                writer.close();
 	               
 	            }catch(IOException e) {
 					System.out.println("Error while getting stats");
	    		}
	}
    
    
    
    /**
     *  Downloads all syslogs written to s3 bei EMR jobs for a given clusterID and stepID. 
     *  The bucketname is the name of the bucket where Amazon is saving your logs.
     *  
     * @param clusterID
     * @param stepID
     * @param s3
     * @param destination local output path
     * @return s3 download object
     * @throws IOException
     */
	
	public static Download getSyslog(String bucketName, String clusterID, String stepID,AmazonS3 s3, String destination) throws IOException{
    	//String bucketName = "aws-logs-789275885207-us-east-1";
    	 ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
	    	.withBucketName(bucketName)
	    	.withPrefix("elasticmapreduce/"+clusterID+"/steps/"+stepID+"/syslog"));

 		TransferManager tx=new TransferManager(s3);
 		Download download=null;
 		File file = new File(destination);
    	file.createNewFile();
    	
 	    for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {

 	       download = tx.download(new GetObjectRequest(bucketName,objectSummary.getKey()), file);
 	       
 	   
 	    }
 	    
 	    return(download);
    }
	

	/**
	 * Gets the syslogs of a cluster for a list of steps, saves them locally and unzips them.
	 * 
	 * @param clusterID the ID of the AWS Cluster
	 * @param stepIDs an array containing the stepIDs
	 * @param s3 AmazonS3 object
	 * @param path the syslogs are downloaded to this local path
	 * @throws IOException
	 * @throws AmazonServiceException
	 * @throws AmazonClientException
	 * @throws InterruptedException
	 */
	
	public static void getSyslogCluster(String bucketName, String clusterID, String[] stepIDs,AmazonS3 s3, String path) throws IOException, AmazonServiceException, AmazonClientException, InterruptedException{
	
    	 ObjectListing objectListing;
    	 TransferManager tx=new TransferManager(s3);
    	 File file;
    	 String filepath; 
    	 int f;
			System.out.println("get syslogs of cluster: "+clusterID+"\n");

    	 for (String stepID: stepIDs){
    		 objectListing= s3.listObjects(new ListObjectsRequest()
	 	    	.withBucketName(bucketName)
	 	    	.withPrefix("elasticmapreduce/"+clusterID+"/steps/"+stepID+"/syslog"));

    		System.out.println("stepID:"+stepID+"\n");
    		System.out.println("Prefix:elasticmapreduce/"+clusterID+"/steps/"+stepID+"/syslog");

	     	f=1;
	  	    for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
	  	    		(new File(path+clusterID+"/"+stepID)).mkdirs();
	  	    		filepath = path+clusterID+"/"+stepID+"/syslog"+f;
			  		file = new File(filepath+".gz");
			  		System.out.println(filepath+".gz should have been created now \n");
			     	file.createNewFile();
		  	       Download download = tx.download(new GetObjectRequest(bucketName,objectSummary.getKey()), file);
		  	       download.waitForCompletion();
		  	       unzip(filepath+".gz", filepath+".txt");
		  	       f+=1;
	  	    }
  	    
    	}
    	
    }
	
	
	/**
	 * Downloads all syslogs of the emr steps saved in the clusterlog to the local destinationpath.
	 * 
	 * @param s3 AmazonS3 object
	 * @param bucketName name of the bucket where your syslogs are stored
	 * @param clusterlogpath path of the clusterlog
	 * @param destinationpath the local path where the syslogs are downloaded to
	 * @throws IOException
	 * @throws AmazonServiceException
	 * @throws AmazonClientException
	 * @throws InterruptedException
	 */
	public static void getAllSyslogs(AmazonS3 s3, String bucketName, String clusterlogpath, String destinationpath) throws IOException, AmazonServiceException, AmazonClientException, InterruptedException{
		File clusterlogs = new File(clusterlogpath);
		FileInputStream fis = new FileInputStream(clusterlogs);
		BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
		String clusterID=null,line = null;
		String[] parts;
		List<String> stepIDs = new ArrayList<String>();
		line = reader.readLine(); //first line contains the header
		if(line==null) {
     		reader.close();
     		return;
     	}
		parts = line.split(",");
		clusterID=parts[0].toString();
		stepIDs.add(parts[1].trim().replaceAll("[\\[\\]]",""));
		System.out.println("clusterID: "+clusterID+" parts[0]:"+parts[0]);
		
		while((line=reader.readLine())!= null){
			parts = line.split(",");
			if (clusterID.equals(parts[0])){
				stepIDs.add(parts[1].trim().replaceAll("[\\[\\]]",""));
				System.out.println("added stepID "+parts[1]);
			}else{
				System.out.println("last clusterID:"+clusterID+"\n");
				getSyslogCluster(bucketName,clusterID,stepIDs.toArray(new String[stepIDs.size()]),s3,destinationpath);
				clusterID=parts[0];
				stepIDs.clear();
				stepIDs.add(parts[1].trim().replaceAll("[\\[\\]]",""));
			}
		}
		getSyslogCluster(bucketName, clusterID,stepIDs.toArray(new String[stepIDs.size()]),s3,destinationpath);
		reader.close();
	}
	
	
	/**
	 * Generates a random graph and saves it to files. This files are used as input for MapReduce algorithms.
	 * 
	 * @param path the graph is saved there
	 * @param n number of vertices
	 * @param edges number of edges
	 * @param maxEntries maximum number of entries per file (a multiple of n)
	 * @param logFile logFile path
	 * @param check true: compute the schwartzset of the graph and write the result to the logFile
	 */
	public static void generateData(String path,int n, int edges,int maxEntries, PrintWriter logFile, Boolean check){
			List<Integer>  schwartzset;	
			(new File(path)).mkdirs(); //create directories if they do not exist
			System.out.println("a new graph is being created \n");
			 Digraph graph = new Digraph(n);
			 //graph = DigraphGenerator.rootedOutDAG(n,n*2); //every SCC has size 1, the graph has a condorcet winner
			 graph = DigraphGenerator.dag(n,edges); //every SCC has size 1, there are several winners
			 //graph = DigraphGenerator.strong(n, edges, sccs); // has at least 10 SCCs, sometimes has a schwartzset
	
			 logFile.write("Vertices: "+graph.V()+"\n");
			 logFile.write("Edges: "+graph.E()+"\n");
			 
			if(check){
				schwartzset = SchwartzSet(graph, logFile);
				logFile.write("Schwartzset: "+schwartzset.toString()+"\n");
			}
			 
 
			 // save Inputgraph to file to use as Input for parallel computation of schwartz set
			// File file = new File(inputpath+"/graph.txt");
			 //writeGraphToFile(file, graph);
			 writeGraphToFiles(path, graph, maxEntries);
			 System.out.println("saved the graph to files\n");			
		 }

	/**
	 * Uploads the files in the local source folder to the destination on the s3 bucket.
	 * 
	 * @param s3 AmazonS3 object
	 * @param source path of the folder containing the files
	 * @param destination destination prefix on the s3 bucket
	 * @param bucketName name of the s3 bucket
	 * @param logFile path of the local logFile
	 */
	public static void uploadData(AmazonS3 s3, String source, String destination,String bucketName, PrintWriter logFile){		
		System.out.println("Uploading files\n");
		uploadFiles(source,s3, bucketName, destination);
		 logFile.write("Graph Location S3:"+destination+"\n");
		 logFile.write("s3 Bucket:"+bucketName+"\n");
	
	}
	
	/**
	 * Creates random Datasets (graphs) and uploads them to s3. The graphs are created with each given number of vertices (candidates) and edges
	 * multiplicator (edges = candidates * fs). For each possible size configuration several datasets are created. 
	 *  
	 * @param s3 AmazonS3 object
	 * @param path local path to save the datasets
	 * @param candidates array of all possible numbers of vertices
	 * @param fs array of all possible edges-multiplicators (edges=candidates*fs)
	 * @param datasets number of graphs created for each possible size configuration
	 * @param bucketName name of the s3 bucket
	 * @throws FileNotFoundException
	 * @throws UnsupportedEncodingException
	 */
	public static void createDatasets(AmazonS3 s3, String path,int[] candidates, double[] fs, int datasets, String bucketName) throws FileNotFoundException, UnsupportedEncodingException{
		 String basepath, inputpath,inputpaths3,logFilePath, extension;
		 PrintWriter logFile;

		
		 for(int n : candidates){
			 for(double f : fs){
				 for(int i=1; i<=datasets; i++){
					 extension = String.valueOf(n)+"-"+String.valueOf(f)+"-"+String.valueOf(i)+"/";
					 basepath =path+extension;
					 inputpath = basepath+"data/";
					 (new File(inputpath)).mkdirs();
					 logFilePath = basepath+"log.txt";
					 logFile = new PrintWriter(logFilePath, "UTF-8");
					 inputpaths3 = "input/"+extension;
					 logFile.write("Graph Location:"+inputpath+"\n");	 	
					 generateData(inputpath,n, (int) (n*f),n*500, logFile, true);
					 uploadData(s3,inputpath, inputpaths3,bucketName, logFile);					 
					 logFile.close();
				 }
			 }
		 }
		 
	}
	
	/**
	 * 
	 * Creates random Datasets (graphs) and uploads them to s3. The graphs are created with each given number of vertices (candidates) and edges
	 * multiplicator (edges = candidates * fs). For each possible size configuration several datasets are created. 
	 *  
	 * @param s3 AmazonS3 object
	 * @param path local path to save the datasets
	 * @param candidates array of all possible numbers of vertices
	 * @param fs array of all possible edges-multiplicators (edges=candidates*fs)
	 * @param datasets array of indices of graphs that should be created 
	 * @param bucketName name of the s3 bucket
	 * @throws FileNotFoundException
	 * @throws UnsupportedEncodingException
	 */
	public static void createDatasets(AmazonS3 s3, String path,int[] candidates, double[] fs, int[] datasets, String bucketName) throws FileNotFoundException, UnsupportedEncodingException{
		 String basepath, inputpath,inputpaths3,logFilePath, extension;
		 PrintWriter logFile;
		 
		
		 for(int n : candidates){
			 for(double f : fs){
				 for(int i : datasets){
					 extension = String.valueOf(n)+"-"+String.valueOf(f)+"-"+String.valueOf(i)+"/";
					 basepath =path+extension;
					 inputpath = basepath+"data/";
					 (new File(inputpath)).mkdirs();
					 logFilePath = basepath+"log.txt";
					 logFile = new PrintWriter(logFilePath, "UTF-8");
					 inputpaths3 = "input/"+extension;
					 logFile.write("Graph Location:"+inputpath+"\n");	 	
					 generateData(inputpath,n, (int) (n*f),n*500, logFile, true);
					 uploadData(s3,inputpath, inputpaths3,bucketName, logFile);					 
					 logFile.close();
				 }
			 }
		 }
		 
	}
	
	/**
	 * Creates random Datasets (graphs) and uploads them to s3. The graphs are created with each given number of vertices (candidates) and 
	 * density (edges = candidates^2 * p). For each possible size configuration several datasets are created. 
	 *  
	 * @param s3 AmazonS3 object
	 * @param path local path to save the datasets
	 * @param candidates array of all possible numbers of vertices
	 * @param p edges = candidates^2 * p
	 * @param datasets number of graphs created for each possible size configuration
	 * @param bucketName name of the s3 bucket
	 * @throws FileNotFoundException
	 * @throws UnsupportedEncodingException
	 */
	public static void createDatasets(AmazonS3 s3, String path,int[] candidates, double p, int datasets, String bucketName) throws FileNotFoundException, UnsupportedEncodingException{
		 String basepath, inputpath,inputpaths3,logFilePath, extension;
		 PrintWriter logFile;
		 int edges;
		
		 for(int n : candidates){
			 edges=(int) (n*n*p);
			 for(int i=1; i<=datasets; i++){
				 extension = String.valueOf(n)+"-"+String.valueOf(p)+"-"+String.valueOf(i)+"/";
				 basepath =path+extension;
				 inputpath = basepath+"data/";
				 (new File(inputpath)).mkdirs();
				 logFilePath = basepath+"log.txt";
				 logFile = new PrintWriter(logFilePath, "UTF-8");
				 inputpaths3 = "input/"+extension;
				 logFile.write("Graph Location:"+inputpath+"\n");	 	
				 generateData(inputpath,n, edges, n*500, logFile, false);
				 uploadData(s3,inputpath, inputpaths3,bucketName, logFile);					 
				 logFile.close();
			 }
			 
		 }
		 
	}
	
	/**
	 * Performing the first Experiment for the AAAI17 submission (paperID 2255)
	 * 
	 * 
	 *  You have to specify the following parameters:
	 *  A local path to store the datasets and logfiles:
	 *  <br> 
	 *  String path= "/home/yourname/somefolder/";
	 *  <br>
	 *  <br>
	 *  The name of your s3 bucket, where you are going to store the input and output of the computations:
	 *  <br>
	 *  String bucketName = "bucketName"; 
	 *  <br>
	 *  <br>
	 *  Amazon EMR writes the logfiles to a specific s3 bucket. The name of the s3 bucket comes here:
	 *  <br>
	 *  String logBucketName = "logbucketname";
	 *  <br>
	 *  <br>
	 *  insert a valid EMR clusterID here:
	 *  <br>
	 *  String clusterID = "clusterid1234";
	 *  <br>
	 *  <br>
	 *  Manually upload the jar file for the mapreduce computation to a s3 bucket. The location of the jarfile is stored in jarlocation:
	 *  <br>String jarlocation = "s3://yourbucket/schwartzset.jar";
	 *  <br><br>
	 *  Provide your AWS credentials and start the AmazonS3 client and EMR service:
	 *  <br><br>
	 *  AWSCredentials credentials = null;
	 *  <br>credentials = new ProfileCredentialsProvider("yourname").getCredentials();
	 *  <br>AmazonS3 s3 = new AmazonS3Client(credentials);
	 *  <br>AmazonElasticMapReduce emr = new AmazonElasticMapReduceClient(credentials);
	 *  <br><br>
	 *  Than the experiments are ready to start:
	 *  <br>experiments( emr, path, bucketName, s3,jarlocation,clusterID,logBucketName); 
	 * 
	 * 
	 * @param path local path for storing the datasets
	 * @param bucketName name of the s3 bucket
	 * @param s3 AmazonS3 object
	 * @param jarlocation location of the jar file for the MapReduce Computation
	 * @param clusterID 
	 * @param logBucketName name of the bucket where EMR saves the syslogs
	 * @throws InterruptedException 
	 * @throws IOException 
	 * @throws AmazonClientException 
	 * @throws AmazonServiceException 
	 */
	
	public static void experiments(AmazonElasticMapReduce emr,String path, String bucketName, AmazonS3 s3,
			String jarlocation, String clusterID, String logBucketName) throws InterruptedException, AmazonServiceException, AmazonClientException, IOException{

		//Use this initializations for the 10*m edges experiments
		 int[] candidates = new int[]{1000,3000,5000,7000};
		 double[] fs = new double[]{10};
		 int datasets = 5;	
		
		 /* 
		 * Initializations for m^2/10 experiments: 
		 * 
		 * 	int[] candidates = new int[]{1000,3000,5000,7000};
		 *	double fs = 0.1;
		 *  int datasets = 1;	
		 * 
		 */
				 
		 /**
		  * Generate the Datasets and save them to a local path:
		  */
	     createDatasets(s3,path,candidates, fs, datasets,bucketName);
	     	     
	
	     String logFilePath = path+"log.csv";
	     PrintWriter logFile = new PrintWriter(logFilePath);
			
	     /**
	      * runComputationsOnCluster starts all steps on the cluster with the given clusterID
	      */
	     runComputationsOnCluster(emr,clusterID, bucketName, jarlocation,candidates, fs, datasets, logFile);
	     logFile.close();
	     
	     //Wait for the Jobs to finish
	     WaitUntilFinished(emr,clusterID);
	     
	     //getSyslogs
	     getAllSyslogs(s3, logBucketName, logFilePath, path+"syslogs/");
	     
	     //get statistics from syslogs
	     
	     (new File(path+"syslogs/")).mkdirs();
	     (new File(path+"stats/")).mkdirs();
	     
	     getStatisticsWithIDs(logBucketName, path+"syslogs/", path+"stats/stats.csv", true);	     
	}
	
	

	

	/**
	 * Run Computations of existing datasets (on s3). The datasets should be created using one of the {@link #createDatasets(AmazonS3, String, int[], double, int, String) createDatasets()} functions.
	 * Use the same input values of candidates, fs and datasets, as for generating the datasets.
	 * The logFile stores the clusterID, stepID and information on the input graph.
	 * 
	 * @param emr AmazonElasticMapReduce object
	 * @param clusterID the ID of the cluster
	 * @param bucketName the name of the s3 bucket wher the input data is stored
	 * @param jarlocation location of the jar-file on the s3 bucket
	 * @param candidates 
	 * @param fs
	 * @param datasets
	 * @param logFile PrintWriter
	 * @throws InterruptedException
	 */
	public static void runComputationsOnCluster(AmazonElasticMapReduce emr,String clusterID, String bucketName, String jarlocation, int[] candidates, double[] fs, int datasets, PrintWriter logFile) throws InterruptedException{
		String inputpaths3,outputpaths3,extension;
		for(int n : candidates){
			 for(double f : fs){
				 for(int i=1; i<=datasets; i++){
					 extension = String.valueOf(n)+"-"+String.valueOf(f)+"-"+String.valueOf(i)+"/";
					 inputpaths3 = "input/"+extension;
					 outputpaths3 = "output/experiments/"+clusterID+"/"+extension;
					 AddJobFlowStepsResult result=startComputation(emr,jarlocation,bucketName,inputpaths3,outputpaths3,clusterID,String.valueOf(n));
					 logFile.write(clusterID+", "+result.getStepIds()+", "+String.valueOf(n)+", "+String.valueOf(n*f)+", "+inputpaths3+", "+outputpaths3+"\n");
						Thread.sleep(10000);

				 }
			 }
		}
	}

	
	public static void main(String args[]) throws IOException, InterruptedException
	  {

	     /**
	      * Before running you have to set some parameters!
	      */
		
		
		/**
		 * A local path to store the datasets and logfiles: 
		 */
		
		String path= "/home/yourname/somefolder/";
		
		/**
		 * The name of your s3 bucket, where you are going to store the input and output of the computations: 
		 */
		String bucketName = "bucketName"; 
		/**
	      * Amazon EMR write the logfiles to a specific s3 bucket. The name of the s3 bucket comes here:
	      */
	     String logBucketName = "logbucketname";

		/**
		 * 
	      * insert a valid clusterID here:
	     */
	     String clusterID = "clusterid1234";
	     
	     
	     /**
	      * Manually upload the jar file for the mapreduce computation to a s3 bucket. The location of the jarfile is stored in jarlocation:
	      */
	     String jarlocation = "s3://yourbucket/schwartzset.jar";
	     
	     /**
	      * Provide your AWS credentials and start the AmazonS3 client and EMR service:
	      */
	     AWSCredentials credentials = null;
		credentials = new ProfileCredentialsProvider("yourname").getCredentials();
		AmazonS3 s3 = new AmazonS3Client(credentials);
		
		final AmazonElasticMapReduce emr = new AmazonElasticMapReduceClient(credentials);
	    
	     
		/**
		 * Now you are ready to start the experiments:
		 */
		experiments( emr, path, bucketName, s3,jarlocation,clusterID,logBucketName);
	     

		
	  }
}

