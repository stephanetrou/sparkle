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

package my.little.pony.sparkle.impl.spi.util.annotation;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.util.*;
import java.util.stream.Collectors;

import com.google.common.reflect.ClassPath;
import my.little.pony.sparkle.job.JobContext;

public class AnnotationScanner {

    private final Class type;

    public AnnotationScanner(Class type) {
        this.type = type;
    }

    public Set<Class<?>> scan(Class<? extends Annotation> annotation, JobContext jobContext) throws IOException {

        ClassPath classPath = ClassPath.from(type.getClassLoader());

        Set<ClassPath.ClassInfo> set = new HashSet<>();

        for(String _package : jobContext.getPackages()) {
           set.addAll(classPath.getTopLevelClassesRecursive(_package));
        }

        return set.stream()
                .map(ClassPath.ClassInfo::load)
                .filter(clasz -> clasz.isAnnotationPresent(annotation))
                .collect(Collectors.toSet());
    }
}
