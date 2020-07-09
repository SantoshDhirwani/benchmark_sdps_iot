package de.adrianbartnik.job.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ParallelSocketArgumentParser {

    private static final int[] PORTS = {31000, 31001, 31002, 31003, 31004, 31005, 31006, 31007, 31008, 31009};

    static boolean IsPowerMachineNumber(String string) {
        switch (string) {
            case "1":
            case "2":
            case "3":
            case "4":
            case "5":
            case "6":
            case "7":
            case "8":
            case "9":
                return true;
            default:
                return false;
        }
    }

    static boolean IsSingleDigit(String string) {
        return (Integer.valueOf(string) / 10) == 0;
    }

    public static List<Integer> ParsePorts(String ports_string) {
        List<String> separated_ports = Arrays.asList(ports_string.split(","));

        List<Integer> ports = new ArrayList<>();
        for (String port : separated_ports) {
            if (IsSingleDigit(port)) {
                ports.add(PORTS[Integer.valueOf(port)]);
            } else {
                ports.add(Integer.valueOf(port));
            }
        }

        return ports;
    }

    public static List<String> ParseHostnames(String hostnames_string) {
        List<String> hostnames = Arrays.asList(hostnames_string.split(","));

        for (int i = 0; i < hostnames.size(); i++) {
            if (IsPowerMachineNumber(hostnames.get(i))) {
                hostnames.set(i, "power" + hostnames.get(i));
            } else if (hostnames.get(i).equals("l")) {
                hostnames.set(i, "localhost");
            }
        }

        return hostnames;
    }
}
