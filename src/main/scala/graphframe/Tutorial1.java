package graphframe;

/**
 * Created by hadoop on 2016/11/29.
 */
import org.graphstream.graph.*;
import org.graphstream.graph.implementations.SingleGraph;

import java.util.Iterator;

public class Tutorial1 {
    public static void main(String args[]) {
        Graph graph = new SingleGraph("Tutorial1");
        graph.setStrict(false);
        graph.setAutoCreate( true );
        graph.addEdge( "AB", "A", "B" );
        graph.addEdge( "BC", "B", "C" );
        graph.addEdge( "CA", "C", "A" );

        graph.display();

        for(Node n:graph) {
            System.out.println(n.getId());
        }

        for(Edge e:graph.getEachEdge()) {
            System.out.println(e.getId());
        }
    }
}
