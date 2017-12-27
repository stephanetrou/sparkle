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

package my.little.pony.sparkle.io;

import java.net.URL;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.spark.sql.SparkSession;

public class SparkleResources {

    private SparkleResources() {}

    public static URL getResource(String name) {
        ClassLoader[] classLoaders = new ClassLoader[] {SparkSession.class.getClassLoader(), Thread.currentThread().getContextClassLoader(), SparkleResources.class.getClassLoader()};

        Optional<URL> urlOptional = Stream.of(classLoaders)
                .map(loader -> loader.getResource(name))
                .filter(Objects::nonNull)
                .findFirst();

        return urlOptional.orElseThrow(() -> new IllegalArgumentException(String.format("resource %s not found", name)));
    }

}
