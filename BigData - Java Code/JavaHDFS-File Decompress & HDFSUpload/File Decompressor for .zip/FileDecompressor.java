package JavaHDFS.JavaHDFS;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

public class FileDecompressor {
	private static final int BUFFER_SIZE = 4096;
	
	
	
	public static void extractZip(String zipFilePath, String destDirectory) throws IOException {
		
		Configuration config = new Configuration();
		config.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
		config.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));
	    FileSystem fs = FileSystem.get(URI.create(zipFilePath), config);
	    
	    Path inputPath = new Path(zipFilePath);
        Path destDir = new Path(destDirectory);
        if (!fs.exists(destDir)) {
            fs.mkdirs(destDir);
        }
    
        FSDataInputStream fsInputobj = fs.open(inputPath);
        ZipInputStream inStreamZip = new ZipInputStream(fsInputobj);
        ZipEntry zipentry = inStreamZip.getNextEntry();
        FSDataOutputStream fsopObj = fs.create(new Path(destDirectory+"/"+zipentry.getName()));
        //take each file from the zipped folder
        while (zipentry != null) {
            String filePath = destDirectory ;
            //check for subdirectory
            if (!zipentry.isDirectory()) 
            {
                extractFile(inStreamZip, fsopObj);
            } else {
                File dir = new File(filePath);
                dir.mkdir();
            }
            inStreamZip.closeEntry();
            zipentry = inStreamZip.getNextEntry();
        }
        inStreamZip.close();
    }
    
	
	//Extracting each  file entry inside
    private static void extractFile(ZipInputStream inStreamZip, FSDataOutputStream destDirectory) throws IOException {
        BufferedOutputStream bos = new BufferedOutputStream(destDirectory);
        byte[] bytesIn = new byte[BUFFER_SIZE];
        int read = 0;
        while ((read = inStreamZip.read(bytesIn)) != -1) {
            bos.write(bytesIn, 0, read);
        }
        bos.close();
    }
    
    
    
}

