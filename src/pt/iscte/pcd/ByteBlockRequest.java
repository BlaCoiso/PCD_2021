package pt.iscte.pcd;

import java.io.Serializable;

public class ByteBlockRequest implements Serializable {

    private int startIndex;
    private int length;

    public ByteBlockRequest(int startIndex, int length) {
        this.startIndex = startIndex;
        this.length = length;
    }

    public int getStartIndex() {
        return startIndex;
    }

    public void setStartIndex(int startIndex) {
        this.startIndex = startIndex;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    /*    A comunicação com os outros nós deve ser feita sobre canais de objetos, transmitindo pedidos
    através da classe ByteBlockRequest, com os atributos inteiros startIndex e length,
    este sempre com o valor 100. Os dados destes blocos devem ser devolvidos num array de
    CloudByte (ver descrição abaixo).
    Deve ser implementada uma estrutura de coordenação para compor os blocos recebidos.
    Apenas quando o descarregamento estiver completo deve a aplicação prosseguir para o seu
    funcionamento normal. */
}
