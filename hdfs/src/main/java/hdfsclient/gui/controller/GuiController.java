package hdfsclient.gui.controller;

import hdfsclient.operation.HdfsOperation;
import hdfsclient.pojo.HdfsFile;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.io.IOException;
import java.util.List;

public class GuiController {
    private HdfsOperation operation = null;
    private Object[][] tableMessage = null;
    private DefaultTableModel model = null;

    public GuiController() {
        operation = new HdfsOperation();
    }

    public void updateTable(JTable table, String pathString) throws IOException {
        String userTitle[] = {"文件名", "文件大小", "文件权限", "文件所属组",
                "托管块的缓存副本的主机", "托管此块的名称列表",
                "此块关联的文件的起始偏移量", "每个主机的网络拓扑路径列表", "路径"};
        int index = 0;

        List<HdfsFile> files = null;
        try {
            files = operation.getHdfsList(pathString);
        } catch (IOException e) {
            e.printStackTrace();
        }

        tableMessage = new Object[files.size()][9];

        for (HdfsFile file : files) {
            tableMessage[index][0] = file.getName();
            tableMessage[index][1] = file.getLen();
            tableMessage[index][2] = file.getPermission();
            tableMessage[index][3] = file.getGroup();

            for (int i = 0; i < file.getBlockLocations().length; i++) {
                tableMessage[index][4] = file.getBlockLocations()[i].getCachedHosts();
                tableMessage[index][5] = file.getBlockLocations()[i].getNames();
                tableMessage[index][6] = file.getBlockLocations()[i].getOffset();
                tableMessage[index][7] = file.getBlockLocations()[i].getTopologyPaths();
            }

            tableMessage[index][8] = file.getPath();

            index++;
        }

        model = new DefaultTableModel(tableMessage, userTitle);
        table.setEnabled(false);
        table.setModel(model);
    }

    public void downHdfsFile(JFileChooser chooser, Object valueAt, Object fileName) {
        String path = chooser.getSelectedFile().getPath();

        operation.downHdfsFile(path + "/" + fileName, String.valueOf(valueAt));
    }

    public void uploadFromLocal(JFileChooser chooser) {
        String path = chooser.getSelectedFile().getPath();
        operation.uploadFromLocal(path);
    }

    public void deleteHdfsFile(String valueOf) {
        operation.deleteHdfsFile(valueOf);
    }
}
