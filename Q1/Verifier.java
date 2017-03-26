import java.util.Map;
import java.util.HashMap;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Linear java prog to verify output by Hadoop
 */
public class Verifier
{
	/**
	 * Usage: java Verifier <input file> <output file by hadoop>
	 */
	public static void main(String[] args)
			throws FileNotFoundException, IOException
	{
		Map<String, Integer> weightMap = new HashMap<>();
		doCompute(args[0], weightMap);
		doCompare(args[1], weightMap);
	}
	
	private static void doCompare(String file, Map<String, Integer> weightMap)
			throws FileNotFoundException, IOException
	{
		boolean same = true;
		
		try (BufferedReader br = new BufferedReader(new FileReader(file))) 
		{
		    String line;
		    String[] toks;
		    int wt;
		    
		    //Line fmt: <tgt> <wt>
		    while ((line = br.readLine()) != null)
		    {
		    	toks = line.split("\\s+");
		    	wt = Integer.parseInt(toks[1]);
		    	
		    	if (!weightMap.containsKey(toks[0])
		    			|| wt != weightMap.get(toks[0]))
		    	{
		    		same = false;
		    		break;
		    	}
		    }
		}
		
		System.out.println("Same? " + same);
	}
	
	private static void doCompute(String file, Map<String, Integer> weightMap)
					throws FileNotFoundException, IOException
	{	
		try (BufferedReader br = new BufferedReader(new FileReader(file))) 
		{
		    String line;
		    String[] toks;
		    int wt;
		    
		    //Line fmt: <src> <tgt> <wt>
		    while ((line = br.readLine()) != null)
		    {
		    	toks = line.split("\\s+");
		    	wt = Integer.parseInt(toks[2]);
		    	
		    	if (!weightMap.containsKey(toks[1])
		    			|| weightMap.get(toks[1]) < wt)
		    		weightMap.put(toks[1], wt);
		    }
		}
	}
}