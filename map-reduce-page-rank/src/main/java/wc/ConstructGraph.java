package wc;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class ConstructGraph {
    public static void constructGraph(String path, long k) {
        String fileName = path + "/pagerank_input.txt";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            double value = 1.0 / (k * k);
            for(long i = 1; i <= k*k; i++) {
                if (i % k == 0)
                    writer.write(i + ":[" + 0 + "]:"+value+"\n");
                else
                    writer.write(i + ":[" + (i + 1) + "]:"+value+"\n");
            }
            System.out.println("Input File '" + fileName + "' created successfully.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        try {
            String inputpath = "input";
            int k = 100;
            ConstructGraph.constructGraph(inputpath, k);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
