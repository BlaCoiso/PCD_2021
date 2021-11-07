package pt.iscte.pcd;

import java.util.LinkedList;

public class ByteBlockRequest {

    private final int PEDIDOS = 10000;
    private final LinkedList<Integer> blocos = new LinkedList<>();

    public ByteBlockRequest() {
        for (int i = 0; i < PEDIDOS; i++) {
            blocos.add(i);
        }
    }

    public synchronized int retirarPedido() {
        return blocos.pop();
    }


/*    A comunicação com os outros nós deve ser feita sobre canais de objetos, transmitindo pedidos
    através da classe ByteBlockRequest, com os atributos inteiros startIndex e length,
    este sempre com o valor 100. Os dados destes blocos devem ser devolvidos num array de
    CloudByte (ver descrição abaixo).
    Deve ser implementada uma estrutura de coordenação para compor os blocos recebidos.
    Apenas quando o descarregamento estiver completo deve a aplicação prosseguir para o seu
    funcionamento normal. */
}
