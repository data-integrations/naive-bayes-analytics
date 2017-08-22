/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.hydrator.plugin.spark.test;

import co.cask.cdap.api.artifact.ArtifactRange;
import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.artifact.ArtifactVersion;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.datapipeline.DataPipelineApp;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkflowManager;
import co.cask.hydrator.plugin.spark.NaiveBayesClassifier;
import co.cask.hydrator.plugin.spark.NaiveBayesTrainer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Tests for Spark plugins.
 */
public class SparkPluginTest extends HydratorTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  protected static final ArtifactId DATAPIPELINE_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-pipeline", "4.0.0");
  protected static final ArtifactSummary DATAPIPELINE_ARTIFACT = new ArtifactSummary("data-pipeline", "4.0.0");
  protected static final ArtifactId DATASTREAMS_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-streams", "0.0.0");
  private static final String CLASSIFIED_TEXTS = "classifiedTexts";

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();


  @BeforeClass
  public static void setupTest() throws Exception {
    // add the artifact for data pipeline app
    setupBatchArtifacts(DATAPIPELINE_ARTIFACT_ID, DataPipelineApp.class);

    // add artifact for spark plugins
    Set<ArtifactRange> parents = ImmutableSet.of(
      new ArtifactRange(NamespaceId.DEFAULT.getNamespace(), DATAPIPELINE_ARTIFACT_ID.getArtifact(),
                        new ArtifactVersion(DATAPIPELINE_ARTIFACT_ID.getVersion()), true,
                        new ArtifactVersion(DATAPIPELINE_ARTIFACT_ID.getVersion()), true),
      new ArtifactRange(NamespaceId.DEFAULT.getNamespace(), DATASTREAMS_ARTIFACT_ID.getArtifact(),
                        new ArtifactVersion(DATASTREAMS_ARTIFACT_ID.getVersion()), true,
                        new ArtifactVersion(DATASTREAMS_ARTIFACT_ID.getVersion()), true)
    );
    addPluginArtifact(NamespaceId.DEFAULT.artifact("spark-plugins", "1.0.0"), parents,
                      NaiveBayesTrainer.class, NaiveBayesClassifier.class);
  }

  @Test
  public void testSparkSinkAndCompute() throws Exception {
    // use the SparkSink to train a model
    testSinglePhaseWithSparkSink();
    // use a SparkCompute to classify all records going through the pipeline, using the model build with the SparkSink
    testSinglePhaseWithSparkCompute();
  }

  private void testSinglePhaseWithSparkSink() throws Exception {
    /*
     * source --> sparksink
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin("messages")))
      .addStage(new ETLStage("customsink",
                             new ETLPlugin(NaiveBayesTrainer.PLUGIN_NAME, SparkSink.PLUGIN_TYPE,
                                           ImmutableMap.of("fileSetName", "modelFileSet",
                                                           "path", "output",
                                                           "fieldToClassify", SpamMessage.TEXT_FIELD,
                                                           "predictionField", SpamMessage.SPAM_PREDICTION_FIELD,
                                                           "numFeatures", SpamMessage.SPAM_FEATURES),
                                           null)))
      .addConnection("source", "customsink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("SinglePhaseApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);


    // set up five spam messages and five non-spam messages to be used for classification
    List<StructuredRecord> messagesToWrite = new ArrayList<>();
    messagesToWrite.add(new SpamMessage("buy our clothes", 1.0).toStructuredRecord());
    messagesToWrite.add(new SpamMessage("sell your used books to us", 1.0).toStructuredRecord());
    messagesToWrite.add(new SpamMessage("earn money for free", 1.0).toStructuredRecord());
    messagesToWrite.add(new SpamMessage("this is definitely not spam", 1.0).toStructuredRecord());
    messagesToWrite.add(new SpamMessage("you won the lottery", 1.0).toStructuredRecord());
    messagesToWrite.add(new SpamMessage("how was your day", 0.0).toStructuredRecord());
    messagesToWrite.add(new SpamMessage("what are you up to", 0.0).toStructuredRecord());
    messagesToWrite.add(new SpamMessage("this is a genuine message", 0.0).toStructuredRecord());
    messagesToWrite.add(new SpamMessage("this is an even more genuine message", 0.0).toStructuredRecord());
    messagesToWrite.add(new SpamMessage("could you send me the report", 0.0).toStructuredRecord());

    // write records to source
    DataSetManager<Table> inputManager = getDataset(NamespaceId.DEFAULT.dataset("messages"));
    MockSource.writeInput(inputManager, messagesToWrite);

    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);
  }

  private void testSinglePhaseWithSparkCompute() throws Exception {
    String textsToClassify = "textsToClassify";
    /*
     * source --> sparkcompute --> sink
     */
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin(textsToClassify)))
      .addStage(new ETLStage("sparkcompute",
                             new ETLPlugin(NaiveBayesClassifier.PLUGIN_NAME, SparkCompute.PLUGIN_TYPE,
                                           ImmutableMap.of("fileSetName", "modelFileSet",
                                                           "path", "output",
                                                           "fieldToClassify", SpamMessage.TEXT_FIELD,
                                                           "predictionField", SpamMessage.SPAM_PREDICTION_FIELD,
                                                           "numFeatures", SpamMessage.SPAM_FEATURES),
                                           null)))
      .addStage(new ETLStage("sink", MockSink.getPlugin(CLASSIFIED_TEXTS)))
      .addConnection("source", "sparkcompute")
      .addConnection("sparkcompute", "sink")
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("SinglePhaseApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);


    // write some some messages to be classified
    List<StructuredRecord> messagesToWrite = new ArrayList<>();
    messagesToWrite.add(new SpamMessage("how are you doing today").toStructuredRecord());
    messagesToWrite.add(new SpamMessage("free money money").toStructuredRecord());
    messagesToWrite.add(new SpamMessage("what are you doing today").toStructuredRecord());
    messagesToWrite.add(new SpamMessage("genuine report").toStructuredRecord());

    DataSetManager<Table> inputManager = getDataset(NamespaceId.DEFAULT.dataset(textsToClassify));
    MockSource.writeInput(inputManager, messagesToWrite);

    // manually trigger the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(5, TimeUnit.MINUTES);


    DataSetManager<Table> classifiedTexts = getDataset(CLASSIFIED_TEXTS);
    List<StructuredRecord> structuredRecords = MockSink.readOutput(classifiedTexts);


    Set<SpamMessage> results = new HashSet<>();
    for (StructuredRecord structuredRecord : structuredRecords) {
      results.add(SpamMessage.fromStructuredRecord(structuredRecord));
    }

    Set<SpamMessage> expected = new HashSet<>();
    expected.add(new SpamMessage("how are you doing today", 0.0));
    // only 'free money money' should be predicated as spam
    expected.add(new SpamMessage("free money money", 1.0));
    expected.add(new SpamMessage("what are you doing today", 0.0));
    expected.add(new SpamMessage("genuine report", 0.0));

    Assert.assertEquals(expected, results);
  }
}
