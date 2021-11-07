package pt.iscte.pcd;

import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class StorageNode {
    private Socket socket;
    private final int portDiretory;
    private final int port;
    private final String addr;
    private final CloudByte[] dados = new CloudByte[1000000];

    public StorageNode(String addr, int portDiretory, int port, String fileName) {
        this.port = port;
        this.portDiretory = portDiretory;
        this.addr = addr;
        signUp();
        if (!fileName.isEmpty())
            loadDataFromFile(fileName);
        else
            loadDataFromNodes();
    }

    public void signUp() {
        try {
            socket = new Socket();
            socket.connect(new InetSocketAddress(portDiretory));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            out.println("INSC " + addr + " " + port);
            out.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*  Se os dados já estiverem disponíveis localmente num ficheiro dado como argumento de
    execução, deve lê-los para uma estrutura adequada, que se sugere que seja um array de
    CloudByte, uma classe que é disponibilizada com o enunciado, que apenas permite guardar
    valores inteiros no intervalo 0..127. Por convenção, admite-se que os dados têm um
    comprimento fixo de 1 000 000 de bytes.*/
    public boolean loadDataFromFile(String fileName) {
        Path path = Paths.get(fileName);
        try {
            byte[] data = Files.readAllBytes(path);
            for (int i = 0; i < data.length; i++)
                dados[i] = new CloudByte(data[i]);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    /* Caso os dados não existam localmente (por o ficheiro não ser dado como argumento, não
    existir ou não ter conteúdo válido), devem ser descarregados dos outros nós existentes na
    rede. Para tal, deve ser logo consultado o diretório, para conhecer os endereços e portos de
    todos os nós existentes no momento*/
    public boolean loadDataFromNodes() {

        return true;
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
    }
}
