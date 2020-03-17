package org.corfudb.runtime.checkpoint;

import java.io.File;
import java.util.Scanner;

public class ParseFile {
    public static void main(String[] args) throws Exception {
        // pass the path to the file as a parameter
        File myObj = new File(args[0]);
        Scanner myReader = new Scanner(myObj);
        int cnt = 0;
        while (myReader.hasNextLine()) {
            //System.out.print((char) i);
            String data = myReader.nextLine();
            String[] parts = data.split(" ");
            String lastWord = parts[parts.length - 1];
            cnt	+= Integer.parseInt(lastWord);
        }
    }
}
