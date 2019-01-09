package org.corfudb.universe;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.corfudb.universe.dynamic.Dynamic;
import org.corfudb.universe.dynamic.PutGetDataDynamic;
import org.corfudb.universe.dynamic.RandomDynamic;
import org.corfudb.universe.dynamic.ServerDynamic;
import py4j.GatewayServer;

import java.time.Duration;


@Slf4j
public class LongevityApp {
    private static final String TIME_UNIT = "time_unit";
    private static final String TIME_AMOUNT = "time_amount";
    private static final String DYNAMIC = "dynamic";
    private static final String PYTHON_GATEWAY = "python_gateway";

    private Dynamic universeDynamic = null;

    public void registerListener(LongevityListener listener) {
        this.universeDynamic.registerListener(listener);
    }

    public void unregisterListener(LongevityListener listener) {
        this.universeDynamic.unregisterListener(listener);
    }

    public static void main(String[] args) {
        Options options = new Options();

        StringBuilder dynamicOptioStr = new StringBuilder("dynamic to use: ");
        dynamicOptioStr.append("\n r for random stop-star nodes and put-get data");
        dynamicOptioStr.append("\n s for stop-star nodes and put-get data");
        dynamicOptioStr.append("\n d for put-get data");
        Option dynamic = new Option("d", DYNAMIC, true, dynamicOptioStr.toString());
        dynamic.setRequired(true);
        Option amountTime = new Option("t", TIME_AMOUNT, true, "time amount");
        amountTime.setRequired(true);
        Option timeUnit = new Option("u", TIME_UNIT, true, "time unit (s, m, h)");
        timeUnit.setRequired(true);
        Option pythonGateway = new Option("g", PYTHON_GATEWAY, false, "enable python gateway");

        options.addOption(dynamic);
        options.addOption(amountTime);
        options.addOption(timeUnit);
        options.addOption(pythonGateway);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
            String dynamicValue = cmd.getOptionValue(DYNAMIC);
            if (!dynamicValue.equals("r") &&
                    !dynamicValue.equals("s") &&
                    !dynamicValue.equals("d")){
                throw new ParseException("Dynamic should be {r,s,d}");
            }
            String timeUnitValue = cmd.getOptionValue(TIME_UNIT);
            if (!timeUnitValue.equals("m") &&
                    !timeUnitValue.equals("s") &&
                    !timeUnitValue.equals("h")){
                throw new ParseException("Time unit should be {s,m,h}");
            }
        } catch (ParseException e) {
            log.info(e.getMessage());
            formatter.printHelp("longevity", options);

            System.exit(1);
            return;
        }


        long longevity;
        long amountTimeValue = Long.parseLong(cmd.getOptionValue(TIME_AMOUNT));
        String timeUnitValue = cmd.getOptionValue(TIME_UNIT);
        switch (timeUnitValue) {
            case "s":
                longevity = Duration.ofSeconds(amountTimeValue).toMillis();
                break;
            case "m":
                longevity = Duration.ofMinutes(amountTimeValue).toMillis();
                break;
            case "h":
                longevity = Duration.ofHours(amountTimeValue).toMillis();
                break;
            default:
                longevity = Duration.ofHours(1).toMillis();
        }

        LongevityApp app = new LongevityApp();

        boolean startsPythonGateway = cmd.hasOption(PYTHON_GATEWAY);
        GatewayServer gateway = null;
        String dynamicValue = cmd.getOptionValue(DYNAMIC);
        switch (dynamicValue) {
            case "r":
                app.universeDynamic = new RandomDynamic(longevity, startsPythonGateway);
                break;
            case "s":
                app.universeDynamic = new ServerDynamic(longevity, startsPythonGateway);
                break;
            case "d":
                app.universeDynamic = new PutGetDataDynamic(longevity, startsPythonGateway);
                break;
            default:
                app.universeDynamic = new RandomDynamic(longevity, startsPythonGateway);
        }
        try{
            if(startsPythonGateway){
                gateway = new GatewayServer(app);
                gateway.start(true);
            }
            app.universeDynamic.initialize();
            app.universeDynamic.run();
            log.info("Longevity Test finished!!!");
            //TODO: send report to a spreadsheet in the column Dynamic.REPORT_FIELD_EVENT
        }
        catch (Exception ex){
            log.error("Unexpected error running a universe dynamic.", ex);
            //TODO: send report to a spreadsheet in the column Dynamic.REPORT_FIELD_EVENT
        }
        finally {
            if(gateway != null)
                gateway.shutdown();
            app.universeDynamic.shutdown();
        }
    }
}
