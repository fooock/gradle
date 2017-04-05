/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradle.internal.buildevents;

import com.google.common.base.Preconditions;
import org.gradle.api.logging.LogLevel;
import org.gradle.internal.logging.text.StyledTextOutput;
import org.gradle.internal.logging.text.StyledTextOutputFactory;

public class TaskOutcomeStatisticsReporter {
    private final StyledTextOutputFactory textOutputFactory;

    public TaskOutcomeStatisticsReporter(StyledTextOutputFactory textOutputFactory) {
        this.textOutputFactory = textOutputFactory;
    }

    public void buildFinished(final int tasksAvoided, final int tasksExecuted) {
        Preconditions.checkArgument(tasksAvoided >= 0, "tasksAvoided must be non-negative");
        Preconditions.checkArgument(tasksExecuted >= 0, "tasksExecuted must be non-negative");

        final int total = tasksAvoided + tasksExecuted;
        if (total > 0) {
            final long avoidedPercentage = Math.round(tasksAvoided * 100.0 / total);
            StyledTextOutput textOutput = textOutputFactory.create(BuildResultLogger.class, LogLevel.LIFECYCLE);
            textOutput.formatln("%d actionable tasks: %d executed, %d avoided (%d%%)", total, tasksExecuted, tasksAvoided, avoidedPercentage);
        }
    }
}
