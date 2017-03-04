package JavaHDFS.JavaHDFS;
import java.io.IOException;


public class AssignmentPart1
{
    public static void main( String[] args )
    {
    	
    	String[] UrlList = {"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/20417.txt.bz2",
    						"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/5000-8.txt.bz2",
    						"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/132.txt.bz2",
    						"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/1661-8.txt.bz2",
    						"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/972.txt.bz2",
    						"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/19699.txt.bz2"};
        
        String[] bookNames = {"OutlineOfScience.txt.bz2","LeonardoDaVinci.txt.bz2","ArtOfWar.txt.bz2",
        					"AdvOfSherlockHolmes.txt.bz2","DevilsDictionary.txt.bz2","EncyclopediaBrit.txt.bz2"};
        
    	String saveDir = args[0];
        int j=0;
        
        try {
        	
          for(String url:UrlList ){
        	  //True is returned if the book is downloaded; false if book exists already.
        	  Boolean status=HttpURLDownload.downloadFile(url, saveDir+"/"+bookNames[j]);
              if(status){ //Decompresses the book if true
            	  System.out.println("Downloaded");
            	  FileDecompressor.decompress(saveDir+"/"+bookNames[j]);
            	  System.out.println("Decompressed");
              }
        	  j++;     	  
        }
            
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        
    }
    
}
