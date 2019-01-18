package hdfsclient.init;

import java.io.IOException;
import java.net.*;

public class NetWorkLink {
    private Socket socket = null;

    public boolean tryNetWorkLink(String ip) {
        int ipTemp = Integer.parseInt(ip.substring(ip.lastIndexOf(".") + 1));

        if (ipTemp <= 0 || ipTemp > 255) {
            return false;
        }

        String[] ips = ip.split("\\.");
        byte[] bytes = new byte[4];
        for (int i = 0; i < ips.length; i++) {
            bytes[i] = (byte) Integer.parseInt(ips[i]);
        }


        InetAddress ia = null;
        try {
            ia = InetAddress.getByAddress(bytes);
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return false;
        }

        try {
            socket = new Socket();
            SocketAddress sa = new InetSocketAddress(ia, 9000);
            socket.connect(sa, 3000);
            socket.close();
            return true;
        } catch (SocketTimeoutException e2) {
            e2.printStackTrace();
            return false;
        } catch (ConnectException e1) {
            e1.printStackTrace();
            return false;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

    }
}
