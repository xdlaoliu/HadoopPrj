import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import javax.security.auth.login.AppConfigurationEntry;
import java.io.*;

/**
 * Created by Administrator on 2017/7/22.
 */
public class TestHDFS {

    private FileSystem fs;

    @Before
    public void setUp() throws IOException {
        Configuration conf = new Configuration();
        fs = FileSystem.get(conf);
    }

    @Test
    public void testMakedir() throws IOException {
        Path path = new Path("/user/jason/TestMkdir");
        fs.mkdirs(path);
        FileStatus fileStatus = fs.getFileStatus(path);
        System.out.println(fileStatus.isDirectory());
        System.out.println(fileStatus.getOwner());
        System.out.println(fileStatus.getGroup());
        System.out.println(fileStatus.getLen());
    }

    @Test
    public void testListStatus() throws IOException {
        Path path = new Path("/user/jason");
        FileStatus[] statuses = fs.listStatus(path);
        for(FileStatus status:statuses){
            System.out.println(status.getPath());
        }
    }

    @Test
    public void testGlobalStatus() throws IOException {
        Path path = new Path("/user/jason/test*");
        FileStatus[] statuses = fs.globStatus(path);
        for(FileStatus status:statuses){
            System.out.println(status.getPath());
        }
    }

    @Test
    public void testFileUpload() throws IOException {
        String putFileName = "/user/jason/RHDSetup_hdfs.log";
        Path writePath = new Path(putFileName);
        FSDataOutputStream outputStream = fs.create(writePath);
        FileInputStream inputStream = new FileInputStream(new File("C:\\RHDSetup.log"));
        IOUtils.copyBytes(inputStream, outputStream,4096,false);
        IOUtils.closeStream(inputStream);
        IOUtils.closeStream(outputStream);
    }

    @Test
    public void testFileDownload() throws IOException {
        String downloadFileName = "/user/jason/wcinput";
        Path readPath = new Path(downloadFileName);
        FSDataInputStream inputStream = fs.open(readPath);
        FileOutputStream fileOutputStream = new FileOutputStream(new File("F:\\TestSpace\\wcinput.txt"));
        IOUtils.copyBytes(inputStream,fileOutputStream,4096,false);
        IOUtils.closeStream(inputStream);
        IOUtils.closeStream(fileOutputStream);
    }

    @Test
    public void testCreateFile() throws IOException {
        String newFileName = "/user/jason/newfile.txt";
        Path path = new Path(newFileName);
        FSDataOutputStream fsDataOutputStream = fs.create(path);
        fsDataOutputStream.write("This is a new file".getBytes());
        fsDataOutputStream.close();
    }

    @Test
    public void testModifyFile() throws IOException {
        String modifyFileName = "/user/jason/newfile.txt";
        Path path = new Path(modifyFileName);
        FSDataOutputStream fsDataOutputStream = fs.append(path);
        fsDataOutputStream.writeUTF("add one more row into it\r\n");
        fsDataOutputStream.close();
    }

    @Test
    public void testDeleteFile() throws IOException {
        String modifyFileName = "/user/jason/newfile.txt";
        Path path = new Path(modifyFileName);
        fs.delete(path,true);
    }

}
