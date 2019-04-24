package org.corfudb.infrastructure.performance.log;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DataGenerator {
    private static final Random rnd = new Random();

    public static String generateDataString(int size) {
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder sb = new StringBuilder();
        for (int j = 0; j < size; j++) {
            sb.append(chars.charAt(rnd.nextInt(chars.length())));
        }

        return sb.toString();
    }

    public static byte[] generateData(int size) {
        return generateDataString(size).getBytes();
    }

    public static String[] getProgression(int from, int to) {
        int power = (int) Math.log10(to) + (int) Math.log(to) + 1;
        List<String> dataSizeParams = new ArrayList<>(power + 1);

        for (int i = 0; i <= power; i++) {
            int val = 1 << i + from;
            if (val < from) {
                continue;
            }
            dataSizeParams.add(String.valueOf(val));
        }

        return dataSizeParams.toArray(new String[0]);
    }
}
