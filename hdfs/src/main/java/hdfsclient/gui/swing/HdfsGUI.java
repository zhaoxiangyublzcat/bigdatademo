/*
 * Created by JFormDesigner on Sun Nov 04 00:06:56 CST 2018
 */

package hdfsclient.gui.swing;

import hdfsclient.gui.controller.GuiController;
import hdfsclient.init.HdfsInit;
import hdfsclient.init.NetWorkLink;

import javax.swing.*;
import java.awt.*;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.IOException;

/**
 * @author blz
 */
public class HdfsGUI {
    public HdfsGUI() {
        initComponents();
    }

    private void initMainTable() {
        try {
            controller.updateTable(MainTable, null);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private JFileChooser initJFileChooser(String type) {
        // 动态获取保存文件路径
        JFileChooser chooser = new JFileChooser();
        chooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
        int i = 0;
        if (type == "down") {
            i = chooser.showSaveDialog(null);
        } else if (type == "upload") {
            chooser.setFileSelectionMode(JFileChooser.FILES_AND_DIRECTORIES);
            i = chooser.showOpenDialog(null);
        }

        //提示框
//        JOptionPane.showMessageDialog(null, "保存成功!");

        if (i == JFileChooser.APPROVE_OPTION) { //判断窗口是否点的是打开或保存
            return chooser;
        } else {
            return null;
        }
    }

    private JPopupMenu makePopup(final MouseEvent paresentE) {
        JPopupMenu popupMenu = new JPopupMenu();
        // 菜单项名称
        String[] str = {"下载文件", "查看文件", "删除文件"};
        // 创建3个菜单项
        JMenuItem items[] = new JMenuItem[3];
        // 菜单点击事件

        for (int i = 0; i < items.length; i++) {
            // 实例化菜单项
            items[i] = new JMenuItem(str[i]);
            // 将菜单项添加到菜单中
            popupMenu.add(items[i]);
        }

        items[0].addMouseListener(new MouseAdapter() {
            @Override
            public void mousePressed(MouseEvent e) {
                downHdfsFile(paresentE);
            }
        });
        items[2].addMouseListener(new MouseAdapter() {
            @Override
            public void mousePressed(MouseEvent e) {
                deleteHdfsFile(paresentE);
                tabbedPaneMouseClicked();
            }
        });
        return popupMenu;
    }

    private void downHdfsFile(MouseEvent e) {
        Object valueAt = ((JTable) e.getComponent()).getValueAt(((JTable) e.getComponent()).rowAtPoint(e.getPoint()), 8);
        Object fileName = ((JTable) e.getComponent()).getValueAt(((JTable) e.getComponent()).rowAtPoint(e.getPoint()), 0);
        // 输出位置GUI的初始化
        JFileChooser chooser = initJFileChooser("down");
        if (chooser != null) {
            controller.downHdfsFile(chooser, valueAt, fileName);
        }
    }

    private void deleteHdfsFile(MouseEvent e) {
        Object valueAt = ((JTable) e.getComponent()).getValueAt(((JTable) e.getComponent()).rowAtPoint(e.getPoint()), 8);
        controller.deleteHdfsFile(String.valueOf(valueAt));

    }

    private void MainTableMouseClicked(MouseEvent e) {
        JPopupMenu popMenu = null;
        JTable table = (JTable) e.getComponent();
        //获取鼠标右键选中的行
        int row = table.rowAtPoint(e.getPoint());
        if (row == -1) {
            return;
        }
        //获取已选中的行
        int[] rows = table.getSelectedRows();
        boolean inSelected = false;
        //判断当前右键所在行是否已选中
        for (int r : rows) {
            if (row == r) {
                inSelected = true;
                break;
            }
        }
        //当前鼠标右键点击所在行不被选中则高亮显示选中行
        if (!inSelected) {
            table.setRowSelectionInterval(row, row);
        }
        //生成右键菜单
        popMenu = makePopup(e);
        popMenu.show(e.getComponent(), e.getX(), e.getY());
    }

    private void upLoadButtonMouseClicked(MouseEvent e) {
        JFileChooser chooser = initJFileChooser("upload");
        if (chooser != null) {
            controller.uploadFromLocal(chooser);
        }
    }

    private void tabbedPaneMouseClicked(MouseEvent e) {
        initMainTable();
    }

    private void tabbedPaneMouseClicked() {
        initMainTable();
    }

    private void HDFSClientWindowClosed(WindowEvent e) {
        System.exit(0);
    }

    /**
     * 连接服务器按钮
     *
     * @param e
     */
    private void ipButtonMouseClicked(MouseEvent e) {
        String pathString = ipTextField.getText();
        boolean b = new NetWorkLink().tryNetWorkLink(pathString);
        System.out.println(b);
        if (!b){
            JOptionPane.showMessageDialog(null, "连接失败!!!!");
        } else {

            new HdfsInit().initHdfs(pathString);
            controller = new GuiController();
            initMainTable();
            // 跳转到hdfs页面
            tabbedPane.setSelectedIndex(1);

        }
    }

    private void initComponents() {
        // JFormDesigner - Component initialization - DO NOT MODIFY  //GEN-BEGIN:initComponents
        HDFSClient = new JFrame();
        tabbedPane = new JTabbedPane();
        initHdfsJPanel = new JPanel();
        label1 = new JLabel();
        ipTextField = new JTextField();
        ipButton = new JButton();
        MainJPanel = new JPanel();
        MainLabel1 = new JLabel();
        MainSearchTextFile = new JTextField();
        MainSearchButton = new JButton();
        MainscrollPane = new JScrollPane();
        MainTable = new JTable();
        MainJPanel2 = new JPanel();
        upLoadButton = new JButton();

        //======== HDFSClient ========
        {
            HDFSClient.setTitle("HDFS\u5ba2\u6237\u7aef");
            HDFSClient.setVisible(true);
            HDFSClient.addWindowListener(new WindowAdapter() {
                @Override
                public void windowClosed(WindowEvent e) {
                    HDFSClientWindowClosed(e);
                }
            });
            Container HDFSClientContentPane = HDFSClient.getContentPane();
            HDFSClientContentPane.setLayout(null);

            //======== tabbedPane ========
            {
                tabbedPane.addMouseListener(new MouseAdapter() {
                    @Override
                    public void mouseClicked(MouseEvent e) {
                        tabbedPaneMouseClicked(e);
                    }
                });

                //======== initHdfsJPanel ========
                {
                    initHdfsJPanel.setLayout(null);

                    //---- label1 ----
                    label1.setText("\u8bf7\u8f93\u5165\u76ee\u6807\u7ec8\u7aefIP");
                    label1.setFont(new Font("Apple Color Emoji", label1.getFont().getStyle() | Font.BOLD, label1.getFont().getSize() + 15));
                    initHdfsJPanel.add(label1);
                    label1.setBounds(245, 40, 240, 65);
                    initHdfsJPanel.add(ipTextField);
                    ipTextField.setBounds(220, 105, 275, 65);

                    //---- ipButton ----
                    ipButton.setText("\u8fde\u63a5");
                    ipButton.setFont(ipButton.getFont().deriveFont(ipButton.getFont().getSize() + 12f));
                    ipButton.addMouseListener(new MouseAdapter() {
                        @Override
                        public void mouseClicked(MouseEvent e) {
                            ipButtonMouseClicked(e);
                        }
                    });
                    initHdfsJPanel.add(ipButton);
                    ipButton.setBounds(325, 195, 62, 42);

                    { // compute preferred size
                        Dimension preferredSize = new Dimension();
                        for(int i = 0; i < initHdfsJPanel.getComponentCount(); i++) {
                            Rectangle bounds = initHdfsJPanel.getComponent(i).getBounds();
                            preferredSize.width = Math.max(bounds.x + bounds.width, preferredSize.width);
                            preferredSize.height = Math.max(bounds.y + bounds.height, preferredSize.height);
                        }
                        Insets insets = initHdfsJPanel.getInsets();
                        preferredSize.width += insets.right;
                        preferredSize.height += insets.bottom;
                        initHdfsJPanel.setMinimumSize(preferredSize);
                        initHdfsJPanel.setPreferredSize(preferredSize);
                    }
                }
                tabbedPane.addTab("hdfs\u521d\u59cb\u5316", initHdfsJPanel);

                //======== MainJPanel ========
                {
                    MainJPanel.setLayout(null);

                    //---- MainLabel1 ----
                    MainLabel1.setText("\u8bf7\u8f93\u5165\u67e5\u8be2\u8def\u5f84");
                    MainJPanel.add(MainLabel1);
                    MainLabel1.setBounds(10, 5, 100, 30);
                    MainJPanel.add(MainSearchTextFile);
                    MainSearchTextFile.setBounds(110, 5, 250, 35);

                    //---- MainSearchButton ----
                    MainSearchButton.setText("\u67e5\u8be2");
                    MainJPanel.add(MainSearchButton);
                    MainSearchButton.setBounds(365, 10, 70, 25);

                    //======== MainscrollPane ========
                    {

                        //---- MainTable ----
                        MainTable.addMouseListener(new MouseAdapter() {
                            @Override
                            public void mouseClicked(MouseEvent e) {
                                MainTableMouseClicked(e);
                            }
                        });
                        MainscrollPane.setViewportView(MainTable);
                    }
                    MainJPanel.add(MainscrollPane);
                    MainscrollPane.setBounds(10, 40, 680, 355);

                    { // compute preferred size
                        Dimension preferredSize = new Dimension();
                        for(int i = 0; i < MainJPanel.getComponentCount(); i++) {
                            Rectangle bounds = MainJPanel.getComponent(i).getBounds();
                            preferredSize.width = Math.max(bounds.x + bounds.width, preferredSize.width);
                            preferredSize.height = Math.max(bounds.y + bounds.height, preferredSize.height);
                        }
                        Insets insets = MainJPanel.getInsets();
                        preferredSize.width += insets.right;
                        preferredSize.height += insets.bottom;
                        MainJPanel.setMinimumSize(preferredSize);
                        MainJPanel.setPreferredSize(preferredSize);
                    }
                }
                tabbedPane.addTab("Hdfs\u6587\u4ef6", MainJPanel);

                //======== MainJPanel2 ========
                {
                    MainJPanel2.setLayout(null);

                    //---- upLoadButton ----
                    upLoadButton.setText("\u70b9\u51fb\u9009\u62e9\u4e0a\u4f20\u6587\u4ef6");
                    upLoadButton.addMouseListener(new MouseAdapter() {
                        @Override
                        public void mouseClicked(MouseEvent e) {
                            upLoadButtonMouseClicked(e);
                        }
                    });
                    MainJPanel2.add(upLoadButton);
                    upLoadButton.setBounds(165, 85, 375, 120);

                    { // compute preferred size
                        Dimension preferredSize = new Dimension();
                        for(int i = 0; i < MainJPanel2.getComponentCount(); i++) {
                            Rectangle bounds = MainJPanel2.getComponent(i).getBounds();
                            preferredSize.width = Math.max(bounds.x + bounds.width, preferredSize.width);
                            preferredSize.height = Math.max(bounds.y + bounds.height, preferredSize.height);
                        }
                        Insets insets = MainJPanel2.getInsets();
                        preferredSize.width += insets.right;
                        preferredSize.height += insets.bottom;
                        MainJPanel2.setMinimumSize(preferredSize);
                        MainJPanel2.setPreferredSize(preferredSize);
                    }
                }
                tabbedPane.addTab("\u6587\u4ef6\u4e0a\u4f20", MainJPanel2);
            }
            HDFSClientContentPane.add(tabbedPane);
            tabbedPane.setBounds(0, 5, 725, 450);

            { // compute preferred size
                Dimension preferredSize = new Dimension();
                for(int i = 0; i < HDFSClientContentPane.getComponentCount(); i++) {
                    Rectangle bounds = HDFSClientContentPane.getComponent(i).getBounds();
                    preferredSize.width = Math.max(bounds.x + bounds.width, preferredSize.width);
                    preferredSize.height = Math.max(bounds.y + bounds.height, preferredSize.height);
                }
                Insets insets = HDFSClientContentPane.getInsets();
                preferredSize.width += insets.right;
                preferredSize.height += insets.bottom;
                HDFSClientContentPane.setMinimumSize(preferredSize);
                HDFSClientContentPane.setPreferredSize(preferredSize);
            }
            HDFSClient.pack();
            HDFSClient.setLocationRelativeTo(HDFSClient.getOwner());
        }
        // JFormDesigner - End of component initialization  //GEN-END:initComponents
    }

    // JFormDesigner - Variables declaration - DO NOT MODIFY  //GEN-BEGIN:variables
    private JFrame HDFSClient;
    private JTabbedPane tabbedPane;
    private JPanel initHdfsJPanel;
    private JLabel label1;
    private JTextField ipTextField;
    private JButton ipButton;
    private JPanel MainJPanel;
    private JLabel MainLabel1;
    private JTextField MainSearchTextFile;
    private JButton MainSearchButton;
    private JScrollPane MainscrollPane;
    private JTable MainTable;
    private JPanel MainJPanel2;
    private JButton upLoadButton;
    // JFormDesigner - End of variables declaration  //GEN-END:variables
    private GuiController controller;
}
