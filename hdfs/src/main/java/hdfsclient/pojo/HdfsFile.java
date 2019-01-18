package hdfsclient.pojo;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.util.Arrays;

public class HdfsFile {
    private String name;
    private long len;
    private FsPermission permission;
    private String group;
    private BlockLocation[] blockLocations;
    private Path path;

    public HdfsFile() {
    }

    public HdfsFile(String name, long len, FsPermission permission, String group, BlockLocation[] blockLocations, Path path) {
        this.name = name;
        this.len = len;
        this.permission = permission;
        this.group = group;
        this.blockLocations = blockLocations;
        this.path = path;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getLen() {
        return len;
    }

    public void setLen(long len) {
        this.len = len;
    }

    public FsPermission getPermission() {
        return permission;
    }

    public void setPermission(FsPermission permission) {
        this.permission = permission;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public BlockLocation[] getBlockLocations() {
        return blockLocations;
    }

    public void setBlockLocations(BlockLocation[] blockLocations) {
        this.blockLocations = blockLocations;
    }

    public Path getPath() {
        return path;
    }

    public void setPath(Path path) {
        this.path = path;
    }

    @Override
    public String toString() {
        return "HdfsFile{" +
                "name='" + name + '\'' +
                ", len=" + len +
                ", permission=" + permission +
                ", group='" + group + '\'' +
                ", blockLocations=" + Arrays.toString(blockLocations) +
                ", path=" + path +
                '}';
    }
}
