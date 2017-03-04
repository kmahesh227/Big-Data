package JavaHDFS.JavaHDFS;
import java.io.IOException;


public class AssignmentQ2 
{
    public static void main( String[] args )
    {
    	
    	String[] listOfUrls = {"http://corpus.byu.edu/wikitext-samples/text.zip"};
        
        String[] corpusName = {"text.zip"};
        
    	String saveDir = args[0];
        int i=0;
        
        try {
        	
          for(String url:listOfUrls ){
        	  Boolean result=HttpURLDownload.downloadFile(url, saveDir+"/"+corpusName[i]);
              if(result){ 
            	  System.out.println("Downloaded the corpus Succesfully!!");
            	  FileDecompressor.extractZip(saveDir+"/"+corpusName[i],saveDir+"/result");
            	  System.out.println("Decompressed the corpus Successfully!!!");
              }
        	  i++;     	  
        }
            
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        
    }
    
}
