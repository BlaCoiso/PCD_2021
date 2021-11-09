package pt.iscte.pcd;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class StorageNode {
    private ServerSocket serverNodeSocket;
    private final int portDirectory;
    private final int portNode;
    private Socket socket;
    PrintWriter out;
    BufferedReader in;
    private InetAddress inetAddress;
    private final CloudByte[] dados = new CloudByte[1000000];
    private final int REQUESTSSIZE = 10000;
    private final LinkedList<ByteBlockRequest> requests = new LinkedList<>();

    public StorageNode(String addr, int portDirectory, int port, String fileName) {
        this.portNode = port;
        this.portDirectory = portDirectory;
        for (int i = 0; i < REQUESTSSIZE; i += 100) {
            requests.add(new ByteBlockRequest(i, 100));
        }
        try {
            this.serverNodeSocket = new ServerSocket(port);
            this.inetAddress = InetAddress.getByName(addr);
            socket = new Socket(inetAddress, this.portDirectory);
            out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (!fileName.isEmpty()) {
            loadDataFromFile(fileName);
            signUp();
        } else {
            signUp();
            loadDataFromNodes();
        }
    }

    private class NodeThread extends Thread {
        private final Socket socketNT;
        ObjectOutputStream outObj;
        ObjectInputStream inObj;

        public NodeThread(Socket socketNT) {
            this.socketNT = socketNT;
            try {
                outObj = new ObjectOutputStream(socketNT.getOutputStream());
                inObj = new ObjectInputStream(socketNT.getInputStream());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void run() {
            sendDataToNode();
        }

        public void sendDataToNode() {
            try {
                ByteBlockRequest bloco = (ByteBlockRequest) inObj.readObject();
                CloudByte[] bytesToSend = new CloudByte[bloco.getLength()];
                for (int i = bloco.getStartIndex(), j = 0; i < bloco.getLength(); i++, j++) {
                    bytesToSend[j] = dados[i];
                }
                outObj.writeObject(bytesToSend);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void signUp() {
        String msg = "INSC " + inetAddress.toString() + " " + portNode;
        //enviar mensagem de inscricao para o diretorio
        out.println(msg);
    }

    public void loadDataFromFile(String fileName) {
        Path path = Paths.get(fileName);
        try {
            byte[] data = Files.readAllBytes(path);
            for (int i = 0; i < data.length; i++)
                dados[i] = new CloudByte(data[i]);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /* Caso os dados não existam localmente (por o ficheiro não ser dado como argumento, não
    existir ou não ter conteúdo válido), devem ser descarregados dos outros nós existentes na
    rede. Para tal, deve ser logo consultado o diretório, para conhecer os endereços e portos de
    todos os nós existentes no momento*/
    public void loadDataFromNodes() {
        String msg = "nodes";
        //escrever mensagem no proprio canal para obter os nós
        out.println(msg);
        List<String> arrayNodes = new ArrayList<>();
        try {
            Thread.sleep(1000);
            while (in.ready()) {
                String msgInicial = in.readLine();
                if (!msgInicial.equalsIgnoreCase("END")) {
                    String[] componentesMensagem = msgInicial.split(" ");
                    arrayNodes.add(componentesMensagem[2]);
                }
            }
            //consultar um dos nós para descarregar dados
            if (arrayNodes.size() > 0) {
                while (requests.size() != 0) {
                    for (String nodePort : arrayNodes) {
                        Socket socketNodeData = new Socket(inetAddress, Integer.parseInt(nodePort));
                        ObjectInputStream inObj = new ObjectInputStream(socketNodeData.getInputStream());
                        ObjectOutputStream outObj = new ObjectOutputStream(socketNodeData.getOutputStream());
                        outObj.writeObject(getByteBlockRequest());
                    }
                }
                //Apenas quando o descarregamento estiver completo deve a aplicação prosseguir para o seu funcionamento normal.
                //falta a estrutura de coordenacao, abaixo apenas teste para ver se funciona
                int k = 0;
                for (String nodePort : arrayNodes) {
                    Socket socketNodeData = new Socket(inetAddress, Integer.parseInt(nodePort));
                    ObjectInputStream inObj = new ObjectInputStream(socketNodeData.getInputStream());
                    ObjectOutputStream outObj = new ObjectOutputStream(socketNodeData.getOutputStream());
                    while (true) {
                        CloudByte[] bytes = (CloudByte[]) inObj.readObject();
                        if (bytes == null || bytes.length == 0)
                            break;
                        for (int i = 0; i < bytes.length; i++) {
                            dados[k] = bytes[i];
                            k++;
                        }
                    }
                }

            } else
                System.err.println("Não Existe nenhum nó no diretorio");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public synchronized ByteBlockRequest getByteBlockRequest() {
        return requests.pop();
    }

    public void serve() {
        //noinspection InfiniteLoopStatement
        while (true) {
            try {
                Socket s = this.serverNodeSocket.accept();
                (new NodeThread(s)).start();
            } catch (IOException var3) {
                System.err.println("Erro ao aceitar ligação do outro nó.");
            }
        }
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Expected 3 arguments: <addrDiretory> <portDiretory> <portStorageNode>");
            return;
        }
        String addrStr = args[0];
        String portDiretoryStr = args[1];
        String portStorageNodeStr = args[2];
        String dataFileName = "";
        if (args.length == 4)
            dataFileName = args[3];
        int port, portDirectory;
        try {
            port = Integer.parseInt(portStorageNodeStr);
            portDirectory = Integer.parseInt(portDiretoryStr);
        } catch (NumberFormatException e) {
            System.err.println("Argument port must be an integer in range 1-" + 0xFFFF);
            return;
        }
        if (port <= 0 || port > 0xFFFF) {
            System.err.println("Argument port must be in range 1-" + 0xFFFF + ", got " + port);
            return;
        }

        StorageNode node = new StorageNode(addrStr, portDirectory, port, dataFileName);
        node.serve();
    }
}
