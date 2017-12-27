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

package my.little.pony.sparkle.cli;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;


public abstract class CliOptions {

    private Options options = new Options();

    public abstract void register();

    public final Options options() {
        register();
        return options;
    }

    public static Options merge(CliOptions... cliOptions) {
        Options options = new Options();

        for(CliOptions cli : cliOptions) {
            for(Object option : cli.options().getOptions())
                options.addOption((Option) option);
        }

        return options;
    }


    protected final OptionBuilder option(String opt, String description) {
        return new CliOptions.OptionBuilder(opt, description);
    }

    public class OptionBuilder {

        private Option option;

        private OptionBuilder(String opt, String description) {
            option = new Option(opt, description);
        }

        public OptionBuilder longOpt(String longOpt) {
            option.setLongOpt(longOpt);
            return this;
        }

        public OptionBuilder description(String description) {
            option.setDescription(description);
            return this;
        }

        public OptionBuilder required() {
            option.setRequired(true);
            return this;
        }

        public OptionBuilder required(boolean required) {
            option.setRequired(required);
            return this;
        }

        public OptionBuilder arg(String name) {
            option.setArgs(1);
            option.setArgName(name);
            return this;
        }

        public OptionBuilder args(String name) {
            option.setArgs(Option.UNLIMITED_VALUES);
            option.setArgName(name);
            if (option.getValueSeparator() == Character.MIN_VALUE) {
                option.setValueSeparator(',');
            }
            return this;
        }


        public OptionBuilder optionalArg(boolean optionalArg) {
            option.setOptionalArg(optionalArg);
            return this;
        }

        public OptionBuilder type(Class<?> type) {
            option.setType(type);
            return this;
        }

        public OptionBuilder separator(char sep) {
            if (option.getArgs() == Option.UNINITIALIZED || option.getArgs() == 1) {
                option.setArgs(Option.UNLIMITED_VALUES);
            }
            option.setValueSeparator(sep);
            return this;
        }

        public void register() {
            CliOptions.this.options.addOption(option);
        }

    }



}
