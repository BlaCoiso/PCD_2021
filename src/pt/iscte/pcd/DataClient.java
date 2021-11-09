package pt.iscte.pcd;

import javax.swing.*;
import java.awt.*;

public class DataClient {
    private final JFrame frame;
    private final String addr;
    private final int port;
    private final JTextField positionField;
    private final JTextField lengthField;
    private final JTextArea resultArea;

    private boolean frameInitialized = false;

    private DataClient(String addr, int port) {
        frame = new JFrame("Data Client");
        this.addr = addr;
        this.port = port;
        positionField = new JTextField(10);
        lengthField = new JTextField(10);
        resultArea = new JTextArea("Respostas aparecerão aqui...");
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

        c.fill = GridBagConstraints.HORIZONTAL;

        JLabel positionLabel = new JLabel("Posição a consultar: ");
        c.gridx = 0;
        c.gridy = 0;
        c.weightx = 0;
        c.ipadx = 0;
        frame.add(positionLabel, c);

        c.gridx = 1;
        c.weightx = 0.5;
        c.ipadx = 16;
        frame.add(positionField, c);

        JLabel lengthLabel = new JLabel("Comprimento: ");
        c.gridx = 2;
        c.weightx = 0;
        c.ipadx = 0;
        frame.add(lengthLabel, c);

        c.gridx = 3;
        c.weightx = 0.5;
        c.ipadx = 16;
        frame.add(lengthField, c);

        JButton SearchButton = new JButton("Consultar");
        c.gridx = 4;
        c.weightx = 0.25;
        c.ipadx = 8;
        frame.add(SearchButton, c);
        SearchButton.addActionListener(e -> {

        });

        resultArea.setLineWrap(true);
        resultArea.setWrapStyleWord(true);
        resultArea.setEditable(false);
        c.fill = GridBagConstraints.BOTH;
        c.gridx = 0;
        c.gridy = 1;
        c.gridwidth = 5;
        c.weighty = 0.9;
        frame.add(new JScrollPane(resultArea), c);

        frame.setMinimumSize(new Dimension(500, 140));

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
