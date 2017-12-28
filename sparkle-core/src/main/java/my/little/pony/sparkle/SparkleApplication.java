/*
 * Copyright 2017 stephanetrou
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package my.little.pony.sparkle;

import java.io.IOException;
import java.net.URL;
import java.util.ServiceLoader;

import my.little.pony.sparkle.cli.CliOptions;
import my.little.pony.sparkle.cli.DefaultOptions;
import my.little.pony.sparkle.io.SparkleResources;
import my.little.pony.sparkle.job.Job;
import my.little.pony.sparkle.job.JobContext;
import my.little.pony.sparkle.spi.Register;
import org.apache.commons.cli.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class SparkleApplication {                

    private final static Logger LOGGER = LogManager.getLogger(SparkleApplication.class);

    public static void run(Class<? extends Job> job, String[] args) {
        Job runner;
        JobContext jobContext = new JobContext(job);

        try {

            banner();

            System.out.printf("Start class : %s\n\n", job.getName());

            runner = job.newInstance();
            CommandLine cmd = parseCommandLine(job.getSimpleName(), runner.getCliOptions(), args);


            registerServices(jobContext);

            injectConfiguration(runner, cmd);

            jobContext.fireJobStart();
            runner.run();

        } catch (Exception e) {
            LOGGER.error("Error ", e);
        } finally {
            jobContext.fireJobEnd();
        }
    }
    
    private static void injectConfiguration(Job runner, CommandLine cmd) throws IllegalAccessException {
        for(Option option : cmd.getOptions()) {
            String longOpt = option.getLongOpt();
            Object value;

            if (option.hasArgs()) {
                value = option.getValues();
            } else if (option.hasArg()) {
                value = option.getValue();
            } else {
                value = true;
            }

            FieldUtils.writeField(runner, longOpt, value, true);
        }
    }

    private static CommandLine parseCommandLine(String name, Class<CliOptions> clasz, String[] args) throws IllegalAccessException, InstantiationException {
        CliOptions cliOptions = clasz.newInstance();
        Options options = CliOptions.merge(new DefaultOptions(), cliOptions);

        CommandLine commandLine = null;

        HelpFormatter formatter = new HelpFormatter();

        try {
            commandLine = new BasicParser().parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp(name, options);
        }

        return commandLine;
    }


    private static void registerServices(JobContext jobContext) {
        ServiceLoader<Register>  serviceLoader = ServiceLoader.load(Register.class);

        for (Register register : serviceLoader) {
            LOGGER.info("Adding service : " + register.name());
            register.register(jobContext);
        }
    }

    private static void banner() {
        URL url = SparkleResources.getResource("banner.txt");
        try {
            IOUtils.copy(url.openStream(), System.out);
        } catch (IOException e) {
            // ignore
        }

    }


}
