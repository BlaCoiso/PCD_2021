package pt.iscte.pcd;

import javax.swing.*;
import java.awt.*;

public class DataClient {
    private final JFrame frame;
    private final String addr;
    private final int port;

    private boolean frameInitialized = false;

    private DataClient(String addr, int port) {
        frame = new JFrame("Data Client");
        this.addr = addr;
        this.port = port;
    }

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
            System.err.println("Argument port must be an integer in range 1-" + 0xFFFF);
            return;
        }
        if (port <= 0 || port > 0xFFFF) {
            System.err.println("Argument port must be in range 1-" + 0xFFFF + ", got " + port);
            return;
        }

        DataClient client = new DataClient(addrStr, port);
        client.start();
    }

    private void initFrame() {
        if (frameInitialized) return;

        GridBagLayout layout = new GridBagLayout();
        GridBagConstraints c = new GridBagConstraints();

        frame.setLayout(layout);

        //TODO: UI initialization
        JLabel positionLabel = new JLabel("Posição a consultar");
        JLabel lengthLabel = new JLabel("Comprimento");
        JTextField positionField = new JTextField(10);
        JTextField lengthField = new JTextField(10);
        JTextArea resultArea = new JTextArea(3, 80);
        JButton SearchButton = new JButton("Consultar");
        SearchButton.addActionListener(e -> {

        });

        c.fill = GridBagConstraints.HORIZONTAL;
        c.anchor = GridBagConstraints.NORTH;
        c.weighty = 0.9;
        c.weightx = 0.5;
        c.gridx = 0;
        c.gridy = 0;
        frame.add(positionLabel, c);
        c.gridx = 1;
        frame.add(positionField, c);
        c.gridx = 2;
        frame.add(lengthLabel, c);
        c.gridx = 3;
        frame.add(lengthField, c);
        c.gridx = 4;
        frame.add(SearchButton, c);
        c.gridx = 0;
        c.gridy = 1;
        c.gridwidth = 5;
        frame.add(resultArea, c);
        frameInitialized = true;
    }

    private void start() {
        initFrame();

        //Try to set native UI look
        try {
            UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | UnsupportedLookAndFeelException e) {
            System.err.println("Failed to set native look and feel");
            e.printStackTrace();
        }

        frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        frame.pack();
        frame.setVisible(true);
    }
}
