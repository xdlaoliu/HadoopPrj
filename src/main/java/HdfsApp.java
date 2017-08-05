import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.SyncFailedException;

/**
 * Created by Administrator on 2017/7/17.
 */
public class HdfsApp {

    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
//        System.out.println(fileSystem);

//        String filename = "/user/jason/test.txt";
//        Path path = new Path(filename);
//        FSDataInputStream inputStream = fileSystem.open(path);
//        IOUtils.copyBytes(inputStream, System.out,4096,false);
//        IOUtils.closeStream(inputStream);


        String putFileName = "/user/jason/test_put";
        Path writePath = new Path(putFileName);
        FSDataOutputStream outputStream = fileSystem.create(writePath);
        FileInputStream inputStream = new FileInputStream(new File("C:\\OnKeyDetector.log"));
        IOUtils.copyBytes(inputStream, outputStream,4096,false);
        IOUtils.closeStream(inputStream);
        IOUtils.closeStream(outputStream);
    }

}
