package org.corfudb.generator;

import java.time.Duration;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import lombok.extern.slf4j.Slf4j;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.LoggerFactory;

/**
 * Created by rmichoud on 7/27/17.
 */

/**
 * This longevity test launcher will set the duration of the test
 * based on inputs.
 */
@Slf4j
public class LongevityRun {
    private static final String TIME_UNIT = "time_unit";
    private static final String TIME_AMOUNT = "time_amount";
    private static final String CORFU_ENDPOINT = "corfu_endpoint";
    private static final String CHECKPOINT = "checkpoint";
    private static final String THREAD_NUM = "thread_number";

    private static final int DEFAULT_THREAD_NUM = 10;
    private static final String DEFAULT_CORFU_ENDPOINT = "localhost:9000";



    public static void main(String[] args) {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.INFO);

        long longevity;

        Options options = new Options();

        Option amountTime = new Option("t", TIME_AMOUNT, true, "time amount");
        amountTime.setRequired(true);
        Option timeUnit = new Option("u", TIME_UNIT, true, "time unit (s, m, h)");
        timeUnit.setRequired(true);
        Option corfuEndpoint = new Option("c", CORFU_ENDPOINT, true,
                "corfu server to connect to");
        Option checkPointFlag = new Option("cp", CHECKPOINT, false,
                "enable checkpoint");
        Option thread = new Option("T", THREAD_NUM, true, "thread number");

        options.addOption(amountTime);
        options.addOption(timeUnit);
        options.addOption(corfuEndpoint);
        options.addOption(checkPointFlag);
        options.addOption(thread);



        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
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

        long amountTimeValue = Long.parseLong(cmd.getOptionValue(TIME_AMOUNT));
        String timeUnitValue = cmd.getOptionValue(TIME_UNIT);

        String configurationString = cmd.hasOption(CORFU_ENDPOINT) ?
                cmd.getOptionValue(CORFU_ENDPOINT) : DEFAULT_CORFU_ENDPOINT;

        boolean checkPoint = cmd.hasOption(CHECKPOINT) ?
                true : false;

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

        int threadNum = cmd.hasOption(THREAD_NUM) ?
                Integer.parseInt(cmd.getOptionValue(THREAD_NUM)) : DEFAULT_THREAD_NUM;

        LongevityApp la = new LongevityApp(longevity, threadNum, configurationString, checkPoint);
        la.runLongevityTest();
    }
}
