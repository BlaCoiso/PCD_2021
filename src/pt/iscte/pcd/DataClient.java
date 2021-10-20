package pt.iscte.pcd;

public class DataClient {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Expected 2 arguments: <addr> <port>");
            return;
        }
        String addrStr = args[0];
        String portStr = args[1];

        int port;
        try {
            port = Integer.parseInt(portStr);
        } catch (NumberFormatException e) {
            System.err.println("port must be an integer in range 1-" + 0xFFFF);
            return;
        }
        if (port <= 0 || port > 0xFFFF) {
            System.err.println("port must be in range 1-" + 0xFFFF);
            return;
        }

        connect(addrStr, port);
        //TODO: Launch client UI
    }

    private static void connect(String addr, int port) {
        //TODO
    }
}
