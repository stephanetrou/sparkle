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

public class DefaultOptions extends CliOptions {

    @Override
    public void register() {

        // Input Options
        option("s", "Source Dataset").longOpt("source").args("source")
                .required().register();

        // Output Options
        option("d", "Destination Dataset")
                .longOpt("destination").arg("destination").required()
                .register();

        option(null, "Partitionning the output by given columns")
                .longOpt("columns-partition").args("COLUMNS")
                .register();

        option(null, "number of partitions")
                .longOpt("num-partitions").arg("NUM_PARTITIONS")
                .register();


        // Dev Options
        option(null, "Enable debug mode").longOpt("debug").register();

        // Help Options
        option("h", "Show this message").longOpt("help").register();
    }
}
