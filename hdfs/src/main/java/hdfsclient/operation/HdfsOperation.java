package hdfsclient.operation;

import hdfsclient.init.HdfsInit;
import hdfsclient.pojo.HdfsFile;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HdfsOperation {
    FileSystem fileSystem = null;
    public static List<HdfsFile> list = null;

    public HdfsOperation() {
        list =  new ArrayList<HdfsFile>();
        this.fileSystem = HdfsInit.fileSystem;
    }


    /**
     * 获取某一路径下的文件详情,默认hdfs根目录
     *
     * @return
     * @throws IOException
     */
    public List<HdfsFile> getHdfsList(String pathString) throws IOException {
        list.clear();
        // 默认路径为hdfs根目录
        if (pathString == null) {
            pathString = "/";
        }
        Path path = new Path(pathString);

        // 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(path, true);
        while (listFiles.hasNext()) {
            LocatedFileStatus status = listFiles.next();

            HdfsFile hdfsFile = new HdfsFile(
                    status.getPath().getName(),
                    status.getLen(),
                    status.getPermission(),
                    status.getGroup(),
                    status.getBlockLocations(),
                    status.getPath()
            );
            list.add(hdfsFile);

        }
        return list;
    }

    public void downHdfsFile(String localPath, String valueOf) {
        try {
            fileSystem.copyToLocalFile(false, new Path(valueOf), new Path(localPath), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void uploadFromLocal(String path) {
        String fileName = path.substring(path.lastIndexOf("/"));
        try {
            fileSystem.copyFromLocalFile(new Path(path),new Path("hdfs://192.168.80.136:9000/tmpLoad/"+fileName));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除hdfs文件
     */
    public void deleteHdfsFile(String path) {
        try {
            fileSystem.delete(new Path(path),true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
