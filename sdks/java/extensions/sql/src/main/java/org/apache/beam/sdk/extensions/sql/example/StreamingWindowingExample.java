/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.sql.example;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class StreamingWindowingExample {
  private static final Duration WINDOW_LENGTH = Duration.standardMinutes(2);
  private static final Duration LATENESS_HORIZON = Duration.standardDays(1);

  public static void main(String[] args) {
    Instant baseTime = new Instant(0L);
    Duration one_min = Duration.standardMinutes(1);

    PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
    pipelineOptions.setRunner(DirectRunner.class);
    Pipeline p = Pipeline.create(pipelineOptions);

    TestStream<KV<String, Long>> events =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))
            .advanceWatermarkTo(baseTime)

            // First element arrives
            .addElements(TimestampedValue.of(KV.of("laurens", 0L), baseTime.plus(one_min)))
            .advanceProcessingTime(Duration.standardMinutes(5))

            // Second element arrives
            .addElements(TimestampedValue.of(KV.of("laurens", 0L), baseTime.plus(one_min)))
            .advanceProcessingTime(Duration.standardMinutes(5))

            // Third element arrives
            .addElements(TimestampedValue.of(KV.of("laurens", 0L), baseTime.plus(one_min)))
            .advanceProcessingTime(Duration.standardMinutes(5))

            // Window ends
            .advanceWatermarkTo(baseTime.plus(WINDOW_LENGTH).plus(one_min))

            // Late element arrives
            .addElements(TimestampedValue.of(KV.of("laurens", 0L), baseTime.plus(one_min)))
            .advanceProcessingTime(Duration.standardMinutes(5))

            // Fire all
            .advanceWatermarkToInfinity();

    PCollection<KV<String, Long>> userCount = p.apply(events).apply(new CountPipeline());

    IntervalWindow window = new IntervalWindow(baseTime, WINDOW_LENGTH);

    PAssert.that(userCount) // This test works
        .inEarlyPane(window)
        .containsInAnyOrder(
            KV.of("laurens", 1L), // First firing
            KV.of("laurens", 2L), // Second firing
            KV.of("laurens", 3L) // Third firing
            );

    PAssert.that(userCount) // This test works as well
        .inOnTimePane(window)
        .containsInAnyOrder(
            KV.of("laurens", 3L) // On time firing
            );

    PAssert.that(userCount) // Test fails
        .inLatePane(window)
        .containsInAnyOrder(
            KV.of("laurens", 4L) // Late firing
            );

    p.run().waitUntilFinish();
  }

  private static class CountPipeline
      extends PTransform<PCollection<KV<String, Long>>, PCollection<KV<String, Long>>> {

    @Override
    public PCollection<KV<String, Long>> expand(PCollection<KV<String, Long>> events) {
      return events
          .apply(
              "window",
              Window.<KV<String, Long>>into(FixedWindows.of(WINDOW_LENGTH))
                  .triggering(
                      AfterWatermark.pastEndOfWindow()
                          .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane())
                          .withLateFirings(AfterProcessingTime.pastFirstElementInPane()))
                  .withAllowedLateness(LATENESS_HORIZON)
                  .accumulatingFiredPanes()
                  .withOnTimeBehavior(Window.OnTimeBehavior.FIRE_ALWAYS))
          .apply("Count", Count.perKey());
    }
  }
}
