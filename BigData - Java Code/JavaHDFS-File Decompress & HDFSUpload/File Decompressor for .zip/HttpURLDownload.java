package JavaHDFS.JavaHDFS;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HttpURLDownload {
	private static final int BUFFER_SIZE = 4096;
	
    public static boolean downloadFile(String fileURL, String saveDir)
            throws IOException {
        	URL url = new URL(fileURL);
        
            // opens input stream from the HTTP connection
            InputStream inputStream = new BufferedInputStream(url.openStream());
            
             
            Configuration conf = new Configuration();
            conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
            conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));

            FileSystem fs = FileSystem.get(URI.create(saveDir), conf);
            if(!fs.exists(new Path(saveDir))){
            	//the book is downloaded if it does not already exist
            	OutputStream outStream = fs.create(new Path(saveDir));
            
            	IOUtils.copyBytes(inputStream, outStream, 4096, true);
            	
            	return true;
            }
            else{
            	System.out.println("File exists!");
            	return false;
            }
    }	
}
