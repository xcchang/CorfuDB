package org.corfudb.perf;

import com.beust.jcommander.JCommander;
import org.corfudb.perf.SimulatorArguments;

final public class Utils {

    /**
     * Parse an array of string arguments into specific SimulatorArgument types
     * @param <T> an argument object to parse the string arguments into
     * @param stringArgs an array of string arguments
     */
    public static <T extends SimulatorArguments> void parse(final T arguments, final String[] stringArgs) {
        final JCommander jc = new JCommander(stringArgs);
        jc.parse(stringArgs);
        if (arguments.help) {
            jc.usage();
            System.exit(0);
        }
    }
}
