import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;


/**
 * java api操作hdfs的io代码
 */
public class HdfsDemo {
    private FileSystem fs = null;
    private Configuration conf = null;

    @Before
    public void begin() throws IOException {
        // conf默认读取src目录下的配置文件,此处src下放入core-site.xml和hdfs-site.xml
        conf = new Configuration();
        fs = FileSystem.get(conf);
    }

    @After
    public void close() throws IOException {
        fs.close();
    }

    /**
     * hdfs创建目录
     *
     * @param createPath 创建目录的url路径
     * @throws IOException io异常
     */
    @Test
    public void mkDir(String createPath) throws IOException {
        Path path = new Path(createPath);
        if (fs.exists(path)) {
            fs.delete(path, true);
            System.out.println("存在的目录已删除");
        }
        fs.mkdirs(path);
    }

    /**
     * hdfs上传文件
     *
     * @param copyedFile 本地文件的url路径
     * @throws IOException io异常
     */
    @Test
    public void upLoad(String copyedFile) throws IOException {
        Path path = new Path("/tmpTest/test");
        if (fs.exists(path)) {
            fs.delete(path, true);
            System.out.println("存在的目录已删除");
        }
        FSDataOutputStream out = fs.create(path);
        FileUtils.copyFile(new File(copyedFile), out);
    }

    /**
     * hdfs小文件合并大文件上传
     *
     * @param localPath 本地小文件的文件夹
     * @param hdfsPath  上传到hdfs中的路径
     * @throws IOException io异常
     */
    @Test
    public void smallFileUpload(String localPath, String hdfsPath) throws IOException {
        Path path = new Path(hdfsPath);
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, Text.class, Text.class);
        File file = new File(localPath);
        for (File f : file.listFiles()) {
            writer.append(new Text(f.getName()), new Text(FileUtils.readFileToString(f)));
        }
    }

    /**
     * 查看文件夹
     *
     * @param dirPath 被查找的文件夹的地址
     * @throws IOException io异常
     */
    @Test
    public void queryDir(String dirPath) throws IOException {
        Path path = new Path(dirPath);
        FileStatus[] fss = fs.listStatus(path);
        for (FileStatus f : fss) System.out.println("文件路径：" + f.getPath() + "-大小：" + f.getLen());
    }

    /**
     * 从hdfs下载文件
     *
     * @param hdfsFile hdfs中文件的路径
     */
    @Test
    public void downLoad(String hdfsFile) {
        Path path = new Path(hdfsFile);
        // FIXME: fs.copyToLocalFile(path,);
    }

    /**
     * 从hdfs下载合并的小文件
     *
     * @param hdfsPath hdfs中被合并的小文件的路径
     * @throws IOException io异常
     */
    @Test
    public void smallFileDownLoad(String hdfsPath) throws IOException {
        Path path = new Path(hdfsPath);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
        Text key = new Text();
        Text value = new Text();
        while (reader.next(key, value)) {
            // 文件名
            System.out.println(key);
            // 文件内容
            System.out.println(value);
            System.out.println("--------------");
        }
    }

    /**
     * 从hdfs中删除文件
     *
     * @param deleteFilePath 被删除文件的路径
     * @throws IOException io异常
     */
    @Test
    public void deleteFile(String deleteFilePath) throws IOException {
        Path path = new Path(deleteFilePath);
        fs.delete(path, true);
    }


}
