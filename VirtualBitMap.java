package ProbablisticCounting;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

import org.jfree.ui.RefineryUtilities;

public class VirtualBitMap {

	final static String inputFile = "C:\\sem 1\\ITM\\Project\\FlowTraffic.txt";
	Double fractionOfZeroes= 0.0;
	String line = "";

	String sourceIP = "";
	String destIP = "";
	int actualCardinality = 0;
	int estimatedCardinality = 0;

	final static int MAX_BITMAP_SIZE = 100000000;
	final static int MAX_VIRTUAL_BITMAP_SIZE = 1500;

	int[] physicalBitMap = new int[MAX_BITMAP_SIZE];
	int Vm = 0;
	int[] R = new int[MAX_VIRTUAL_BITMAP_SIZE];			
	NavigableMap<String, List<String>> flowList = new TreeMap<String, List<String>>();
	Map<String,int[]> virtual = new HashMap<String,int[]>();
	private static HashMap<Integer,Integer> resultGraph = new HashMap<Integer,Integer>();    



	VirtualBitMap(){

		try{

			/* Initialize bitmap to zero*/			
			initialize_R();

			/*Build hashmap for the flow*/
			buildFlowHashMap();

			/*** ONLINE OPERATION***/
			/*Build bitmap from flow hashmap */
			buildPhysicalBitMap();

			Vm = zeroCountPhysicalBitmap();
			
			/*** OFFLINE OPERATION***/
			virtualCounting();


		}
		catch(Exception e){
			System.out.println("Exception occured : "+e);
			e.printStackTrace();
		}



	}


	public void buildPhysicalBitMap() {

		for (Map.Entry<String, List<String>> flow : flowList.entrySet()) {
			List<String> destIP = flow.getValue();
			String sourceIP = flow.getKey();
			int size = destIP.size();

			while(size>0){

				System.out.println("In buildPhysicalBitMap key = "+sourceIP+" eachflow : "+destIP.get(size-1));
				
				/* Calculate physical bitmap index for each destIP */
				set_physical_bitmap(sourceIP,destIP.get(size-1));
				size--;
			}

		}

	}	

	/* Flip Physical Bitmap for each destination*/
	void set_physical_bitmap(String sourceIP,String destIP)
	{
		int phyIndex = 0;
		int index = 0;

		try{
			
			index= (int)(((customHash(destIP)) & 0x7fffffff) % MAX_VIRTUAL_BITMAP_SIZE);
			
			String sourceIPNew = sourceIP.replaceAll("\\.", "");
			
			Long exor = ((Long.parseLong(sourceIPNew))^R[index]);
			
			phyIndex = (int)(((customHash(String.valueOf(exor)))&0x7fffffff)%MAX_BITMAP_SIZE);
			
			physicalBitMap[phyIndex] = 1;
		}
		catch(Exception e){
			System.out.println("Exception occured = "+e);
		}
	}

	/*Custom SecureRandom Hash function using SHA-256*/
	static int customHash(String name) throws NoSuchAlgorithmException {

		MessageDigest md = MessageDigest.getInstance("SHA-256");
		md.reset();

		md.update(name.getBytes());

		byte[] hash = md.digest();
		int result = getInt(hash);
		//System.out.println(result);
		return result;

	} 

	private static int getInt(byte[] bytes) {
		return bytes[0] << 24 | (bytes[1] & 0xFF) << 16 | (bytes[2] & 0xFF) << 8 | (bytes[3] & 0xFF);
	}



	/*** ONLINE OPERATION ***/
	public void buildFlowHashMap() throws IOException{

		int start = 1;
		int i=0;
		BufferedReader br = new BufferedReader(new FileReader(inputFile));

		while((line = br.readLine()) != null) {
			//i++;
			/* Split each line of the file on " "	*/
			String[] columns = line.split(" ");

			/* Assign first column of the file to source IP */
			sourceIP = columns[0];

			/* Assign Destination IP */
			for(int k=1; k<columns.length;k++){				 
				if(!(columns[k].equalsIgnoreCase(""))){
					destIP = columns[k];
					break;
				}
			}

			/* For first line of the text file*/
			if(start ==1){
				List<String> newFlow = new ArrayList<String>();
				newFlow.add(destIP);
				flowList.put(sourceIP, newFlow);
				start = 0;
			}

			/* Checks if sourceIP exists in last stored key value*/
			else if(flowList.lastKey().equals(sourceIP)){
				flowList.get(sourceIP).add(destIP);

			}

			/* Adds new flow to hashmap*/
			else{
				List<String> newFlow = new ArrayList<String>();
				newFlow.add(destIP);
				flowList.put(sourceIP, newFlow);
			}

		}
		br.close();
	}

	/* Initialize Random number array*/
	public void initialize_R(){


		for(int i=0; i<MAX_VIRTUAL_BITMAP_SIZE; i++){
			Random rand = new Random();
			R[i] = (int) rand.nextInt(1000000000);  // Initialize the bitmap with 0	
		}
	}


	/*** OFFLINE OPERATION ***/
	public int zeroCountPhysicalBitmap(){

		int noOfZeroes = 0;

		/* Calculate number of zeroes in the physical bitmap*/
		for(int i=0 ; i<MAX_BITMAP_SIZE ; i++){
			if (physicalBitMap[i] == 0){
				noOfZeroes++;
			}
		}

		return noOfZeroes;

	}

	/*** OFFLINE OPERATION***/
	public void virtualCounting(){

		try{
			Long index = 0L;
			int virtualIndex= 0;
			int count = 0;
			for (String flow: flowList.keySet()){

				for(int i = 0; i<MAX_VIRTUAL_BITMAP_SIZE;i++){
					
					String flowNew = flow.replaceAll("\\.", "");
					index = (Long.parseLong(flowNew))^R[i];

					virtualIndex = (int)(((customHash(String.valueOf(index)))&0x7fffffff)%MAX_BITMAP_SIZE);

					//System.out.println("VI  = "+virtualIndex);
					if(physicalBitMap[virtualIndex]==0){
						count++;

					}

				}
				/* Calculating estimated cardinality of each flow*/
				calculateCardinality(flow,count);
				count = 0;
			}

		}
		catch(Exception e){
			e.printStackTrace();
		}
	}

	/* Calculate cardinality of each source IP*/
	public void calculateCardinality(String sourceIP, int count){

		double Vs_fraction = 0.0;
		double Vm_fraction = 0.0;
		
		Vm_fraction = (double) Vm / MAX_BITMAP_SIZE;
		Vs_fraction = (double) count / MAX_VIRTUAL_BITMAP_SIZE;

		/* Calculating actual cardinality of each flow*/
		actualCardinality = flowList.get(sourceIP).size();
		
		/* Calculating estimated cardinality of each flow*/
		estimatedCardinality =(int) (( MAX_VIRTUAL_BITMAP_SIZE * java.lang.Math.log(Vm_fraction)) + (-1 * MAX_VIRTUAL_BITMAP_SIZE * java.lang.Math.log(Vs_fraction) ));
		System.out.println("n = " + actualCardinality + " n^ = " + estimatedCardinality);
		
		/* For plotting purposes*/
		//if(actualCardinality <=9000 && estimatedCardinality <=9000){
		resultGraph.put(actualCardinality, estimatedCardinality);
		//	}

	}	



	public static void main (String[] x){

		VirtualBitMap vb = new VirtualBitMap();

		/* Chart Plotting*/		
		ScatterPlot chart = new ScatterPlot("ITM Project 2", "Virtual Bitmap", resultGraph);
		chart.pack( );          
		RefineryUtilities.centerFrameOnScreen( chart );          
		chart.setVisible( true ); 
	}




}


