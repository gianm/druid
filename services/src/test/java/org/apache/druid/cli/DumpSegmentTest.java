/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.cli;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.RoaringBitmapFactory;
import org.apache.druid.data.input.ResourceInputSource;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.guice.BuiltInTypesModule;
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.initialization.ServerInjectorBuilder;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.NestedDataTestUtils;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.metadata.SegmentMetadataQueryConfig;
import org.apache.druid.query.metadata.SegmentMetadataQueryQueryToolChest;
import org.apache.druid.query.metadata.SegmentMetadataQueryRunnerFactory;
import org.apache.druid.query.metadata.metadata.ColumnAnalysis;
import org.apache.druid.query.metadata.metadata.ListColumnIncluderator;
import org.apache.druid.query.metadata.metadata.SegmentAnalysis;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.query.spec.SpecificSegmentSpec;
import org.apache.druid.segment.DefaultColumnFormatConfig;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentViewTestUtil;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.column.BaseColumnHolder;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.file.SegmentFileMetadata;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.index.semantic.DictionaryEncodedStringValueIndex;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.testing.TemporaryFolderExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

public class DumpSegmentTest extends InitializedNullHandlingTest
{
  private final Closer closer;

  @RegisterExtension
  public final TemporaryFolderExtension tempFolder = TemporaryFolderExtension.testCaseScoped();

  public DumpSegmentTest()
  {
    BuiltInTypesModule.registerHandlersAndSerde();
    this.closer = Closer.create();
  }

  @AfterEach
  public void teardown() throws IOException
  {
    closer.close();
  }

  @Test
  public void testExecuteSegmentMetadataQuery()
  {
    final SegmentAnalysis analysis = runSegmentMetadataQuery(TestIndex.getMMappedTestIndex(), List.of("market"));
    Assertions.assertEquals(TestIndex.getMMappedTestIndex().getNumRows(), analysis.getNumRows());
    assertStringColumnsAnalyzed(analysis, List.of("market"));
  }

  @Test
  public void testExecuteSegmentMetadataQueryOnClusteredSegment() throws IOException
  {
    // Analyzing size reads each string column through a cursor, which cannot span cluster groups.
    final QueryableIndex index = closer.register(SegmentViewTestUtil.buildThreeGroupIndex(tempFolder.newFolder()));
    final SegmentAnalysis analysis = runSegmentMetadataQuery(index, List.of("tenant", "region"));
    Assertions.assertEquals(4, analysis.getNumRows());
    assertStringColumnsAnalyzed(analysis, List.of("tenant", "region"));
  }

  @Test
  public void testDumpRows() throws Exception
  {
    Injector injector = Mockito.mock(Injector.class);
    ObjectMapper mapper = TestHelper.makeJsonMapper();
    mapper.registerModules(BuiltInTypesModule.getJacksonModulesList());
    mapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(ExprMacroTable.class.getName(), TestExprMacroTable.INSTANCE)
            .addValue(ObjectMapper.class.getName(), mapper)
            .addValue(DefaultColumnFormatConfig.class, new DefaultColumnFormatConfig(null, null, null, null))
    );
    Mockito.when(injector.getInstance(Key.get(ObjectMapper.class, Json.class))).thenReturn(mapper);
    Mockito.when(injector.getInstance(DefaultColumnFormatConfig.class)).thenReturn(new DefaultColumnFormatConfig(null, null, null, null));

    List<Segment> segments = createSegments(tempFolder, closer);
    QueryableIndex queryableIndex = segments.get(0).as(QueryableIndex.class);

    File outputFile = tempFolder.newFile();

    DumpSegment.runDump(
        injector,
        outputFile.getPath(),
        queryableIndex,
        DumpSegment.getColumnsToInclude(queryableIndex, Collections.emptyList()),
        null,
        false
    );
    final byte[] fileBytes = Files.readAllBytes(outputFile.toPath());
    final String output = StringUtils.fromUtf8(fileBytes);
    final String expected = "{\"__time\":1609459200000,\"nest\":{\"x\":200,\"y\":2.2},\"count\":1}\n"
                            + "{\"__time\":1609459200000,\"nest\":{\"x\":400,\"y\":1.1,\"z\":\"a\"},\"count\":1}\n"
                            + "{\"__time\":1609459200000,\"nest\":{\"x\":200,\"z\":\"b\"},\"count\":1}\n"
                            + "{\"__time\":1609459200000,\"nest\":{\"x\":100,\"y\":1.1,\"z\":\"a\"},\"count\":1}\n"
                            + "{\"__time\":1609459200000,\"nest\":{\"y\":3.3,\"z\":\"b\"},\"count\":1}\n";
    Assertions.assertEquals(expected, output);
  }

  @Test
  public void testDumpBitmap() throws IOException
  {
    Injector injector = Mockito.mock(Injector.class);
    QueryableIndex queryableIndex = Mockito.mock(QueryableIndex.class);
    ObjectMapper mapper = new DefaultObjectMapper();
    BitmapFactory bitmapFactory = new RoaringBitmapFactory();
    BaseColumnHolder xHolder = Mockito.mock(BaseColumnHolder.class);
    BaseColumnHolder yHolder = Mockito.mock(BaseColumnHolder.class);
    ColumnIndexSupplier indexSupplier = Mockito.mock(ColumnIndexSupplier.class);
    DictionaryEncodedStringValueIndex valueIndex = Mockito.mock(DictionaryEncodedStringValueIndex.class);

    ImmutableBitmap bitmap = bitmapFactory.complement(bitmapFactory.makeEmptyImmutableBitmap(), 10);
    Mockito.when(injector.getInstance(Key.get(ObjectMapper.class, Json.class))).thenReturn(mapper);

    Mockito.when(queryableIndex.getBitmapFactoryForDimensions()).thenReturn(bitmapFactory);

    Mockito.when(queryableIndex.getColumnHolder("x")).thenReturn(xHolder);
    Mockito.when(queryableIndex.getColumnHolder("y")).thenReturn(yHolder);

    Mockito.when(xHolder.getIndexSupplier()).thenReturn(indexSupplier);
    Mockito.when(indexSupplier.as(DictionaryEncodedStringValueIndex.class)).thenReturn(valueIndex);
    Mockito.when(valueIndex.getCardinality()).thenReturn(1);
    Mockito.when(valueIndex.getBitmap(0)).thenReturn(bitmap);
    Mockito.when(valueIndex.getValue(0)).thenReturn("val");

    DumpSegment.runBitmaps(
        injector,
        tempFolder.newFile().getPath(),
        queryableIndex,
        ImmutableList.of("x", "y"),
        false
    );
    Assertions.assertTrue(true);
  }

  @Test
  public void testDumpNestedColumn() throws Exception
  {
    Injector injector = Mockito.mock(Injector.class);
    ObjectMapper mapper = TestHelper.makeJsonMapper();
    mapper.registerModules(BuiltInTypesModule.getJacksonModulesList());
    mapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(ExprMacroTable.class.getName(), TestExprMacroTable.INSTANCE)
            .addValue(ObjectMapper.class.getName(), mapper)
            .addValue(DefaultColumnFormatConfig.class, new DefaultColumnFormatConfig(null, null, null, null))
    );
    Mockito.when(injector.getInstance(Key.get(ObjectMapper.class, Json.class))).thenReturn(mapper);
    Mockito.when(injector.getInstance(DefaultColumnFormatConfig.class)).thenReturn(new DefaultColumnFormatConfig(null, null, null, null));

    List<Segment> segments = createSegments(tempFolder, closer);
    QueryableIndex queryableIndex = segments.get(0).as(QueryableIndex.class);

    File outputFile = tempFolder.newFile();
    DumpSegment.runDumpNestedColumn(
        injector,
        outputFile.getPath(),
        queryableIndex,
        "nest"
    );
    final byte[] fileBytes = Files.readAllBytes(outputFile.toPath());
    final String output = StringUtils.fromUtf8(fileBytes);
    Assertions.assertEquals(
        "{\"nest\":{\"fields\":[{\"path\":\"$.x\",\"types\":[\"LONG\"]},{\"path\":\"$.y\",\"types\":[\"DOUBLE\"]},{\"path\":\"$.z\",\"types\":[\"STRING\"]}],\"dictionaries\":{\"strings\":[{\"globalId\":0,\"value\":null},{\"globalId\":1,\"value\":\"a\"},{\"globalId\":2,\"value\":\"b\"}],\"longs\":[{\"globalId\":3,\"value\":100},{\"globalId\":4,\"value\":200},{\"globalId\":5,\"value\":400}],\"doubles\":[{\"globalId\":6,\"value\":1.1},{\"globalId\":7,\"value\":2.2},{\"globalId\":8,\"value\":3.3}],\"nullRows\":[]}}}",
        output
    );
  }

  @Test
  public void testDumpNestedColumnPath() throws Exception
  {
    Injector injector = Mockito.mock(Injector.class);
    ObjectMapper mapper = TestHelper.makeJsonMapper();
    mapper.registerModules(BuiltInTypesModule.getJacksonModulesList());
    mapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(ExprMacroTable.class.getName(), TestExprMacroTable.INSTANCE)
            .addValue(ObjectMapper.class.getName(), mapper)
            .addValue(DefaultColumnFormatConfig.class, new DefaultColumnFormatConfig(null, null, null, null))
    );
    Mockito.when(injector.getInstance(Key.get(ObjectMapper.class, Json.class))).thenReturn(mapper);
    Mockito.when(injector.getInstance(DefaultColumnFormatConfig.class)).thenReturn(new DefaultColumnFormatConfig(null, null, null, null));

    List<Segment> segments = createSegments(tempFolder, closer);
    QueryableIndex queryableIndex = segments.get(0).as(QueryableIndex.class);

    File outputFile = tempFolder.newFile();
    DumpSegment.runDumpNestedColumnPath(
        injector,
        outputFile.getPath(),
        queryableIndex,
        "nest",
        "$.x"
    );
    final byte[] fileBytes = Files.readAllBytes(outputFile.toPath());
    final String output = StringUtils.fromUtf8(fileBytes);
    Assertions.assertEquals(
        "{\"bitmapSerdeFactory\":{\"type\":\"roaring\"},\"nest\":{\"$.x\":{\"types\":[\"LONG\"],\"dictionary\":[{\"localId\":0,\"globalId\":0,\"value\":null,\"rows\":[4]},{\"localId\":1,\"globalId\":3,\"value\":\"100\",\"rows\":[3]},{\"localId\":2,\"globalId\":4,\"value\":\"200\",\"rows\":[0,2]},{\"localId\":3,\"globalId\":5,\"value\":\"400\",\"rows\":[1]}],\"column\":[{\"row\":0,\"raw\":{\"x\":200,\"y\":2.2},\"fieldId\":2,\"fieldValue\":\"200\"},{\"row\":1,\"raw\":{\"x\":400,\"y\":1.1,\"z\":\"a\"},\"fieldId\":3,\"fieldValue\":\"400\"},{\"row\":2,\"raw\":{\"x\":200,\"z\":\"b\"},\"fieldId\":2,\"fieldValue\":\"200\"},{\"row\":3,\"raw\":{\"x\":100,\"y\":1.1,\"z\":\"a\"},\"fieldId\":1,\"fieldValue\":\"100\"},{\"row\":4,\"raw\":{\"y\":3.3,\"z\":\"b\"},\"fieldId\":0,\"fieldValue\":null}]}}}",
        output
    );
  }

  @Test
  public void testGetModules()
  {
    DumpSegment dumpSegment = new DumpSegment();
    Injector injector = ServerInjectorBuilder.makeServerInjector(
        new StartupInjectorBuilder().forServer().build(),
        Collections.emptySet(),
        dumpSegment.getModules()
    );
    Assertions.assertNotNull(injector.getInstance(ColumnConfig.class));
    Assertions.assertEquals("druid/tool", injector.getInstance(Key.get(String.class, Names.named("serviceName"))));
    Assertions.assertEquals(9999, (int) injector.getInstance(Key.get(Integer.class, Names.named("servicePort"))));
    Assertions.assertEquals(-1, (int) injector.getInstance(Key.get(Integer.class, Names.named("tlsServicePort"))));
  }

  @Test
  public void testDumpV10Metadata() throws IOException
  {
    Injector injector = Mockito.mock(Injector.class);
    ObjectMapper mapper = TestHelper.makeJsonMapper();
    mapper.registerModules(BuiltInTypesModule.getJacksonModulesList());
    mapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(ExprMacroTable.class.getName(), TestExprMacroTable.INSTANCE)
            .addValue(ObjectMapper.class.getName(), mapper)
            .addValue(DefaultColumnFormatConfig.class, new DefaultColumnFormatConfig(null, null, null, null))
    );
    Mockito.when(injector.getInstance(Key.get(ObjectMapper.class, Json.class))).thenReturn(mapper);
    Mockito.when(injector.getInstance(DefaultColumnFormatConfig.class)).thenReturn(new DefaultColumnFormatConfig(null, null, null, null));

    File f = buildV10Segment();

    File outputFile = tempFolder.newFile();
    DumpSegment.dumpV10Metadata(
        injector,
        f.getPath() + "/" + IndexIO.V10_FILE_NAME,
        outputFile.getPath()
    );
    final byte[] fileBytes = Files.readAllBytes(outputFile.toPath());
    SegmentFileMetadata dumped = mapper.readValue(fileBytes, SegmentFileMetadata.class);
    Assertions.assertNotNull(dumped);
    Assertions.assertEquals(1, dumped.getContainers().size());
    Assertions.assertEquals(2, dumped.getColumnDescriptors().size());
    Assertions.assertEquals(12, dumped.getFiles().size());
  }


  private File buildV10Segment() throws IOException
  {
    final File segmentDir = tempFolder.newFolder();
    IndexBuilder bob = IndexBuilder.create()
                                   .useV10()
                                   .tmpDir(segmentDir)
                                   .schema(
                                       IncrementalIndexSchema.builder()
                                                             .withTimestampSpec(
                                                                 new TimestampSpec(
                                                                     "timestamp",
                                                                     null,
                                                                     null
                                                                 )
                                                             )
                                                             .withDimensionsSpec(
                                                                 DimensionsSpec.builder()
                                                                               .useSchemaDiscovery(true)
                                                                               .build()
                                                             )
                                                             .withQueryGranularity(Granularities.NONE)
                                                             .withRollup(false)
                                                             .withMinTimestamp(0)
                                                             .build()
                                   )
                                   .inputSource(ResourceInputSource.of(
                                       getClass().getClassLoader(),
                                       "nested-test-data.json"
                                   ))
                                   .inputFormat(TestIndex.DEFAULT_JSON_INPUT_FORMAT)
                                   .inputTmpDir(tempFolder.newFolder());
    return bob.buildMMappedIndexFile();
  }


  private static SegmentAnalysis runSegmentMetadataQuery(final QueryableIndex index, final List<String> columns)
  {
    final Injector injector = Mockito.mock(Injector.class);
    final QueryRunnerFactoryConglomerate conglomerate = Mockito.mock(QueryRunnerFactoryConglomerate.class);
    Mockito.when(injector.getInstance(QueryRunnerFactoryConglomerate.class)).thenReturn(conglomerate);
    Mockito.doReturn(
        new SegmentMetadataQueryRunnerFactory(
            new SegmentMetadataQueryQueryToolChest(new SegmentMetadataQueryConfig()),
            QueryRunnerTestHelper.NOOP_QUERYWATCHER
        )
    ).when(conglomerate).findFactory(ArgumentMatchers.any());

    // Like the query that DumpSegment runs for "--dump metadata".
    final SegmentMetadataQuery query = new SegmentMetadataQuery(
        new TableDataSource("dataSource"),
        new SpecificSegmentSpec(new SegmentDescriptor(index.getDataInterval(), "0", 0)),
        new ListColumnIncluderator(columns),
        false,
        null,
        EnumSet.allOf(SegmentMetadataQuery.AnalysisType.class),
        false,
        null,
        null
    );
    return Iterables.getOnlyElement(DumpSegment.executeQuery(injector, index, query).toList());
  }

  private static void assertStringColumnsAnalyzed(final SegmentAnalysis analysis, final List<String> columns)
  {
    for (String column : columns) {
      final ColumnAnalysis columnAnalysis = analysis.getColumns().get(column);
      Assertions.assertFalse(columnAnalysis.isError(), columnAnalysis.getErrorMessage());
      Assertions.assertEquals(ColumnType.STRING, columnAnalysis.getTypeSignature(), column);
    }
  }

  public static List<Segment> createSegments(
      TemporaryFolderExtension tempFolder,
      Closer closer
  ) throws Exception
  {
    return NestedDataTestUtils.createSegments(
        tempFolder,
        closer,
        "nested-test-data.json",
        TestIndex.DEFAULT_JSON_INPUT_FORMAT,
        TimestampSpec.DEFAULT,
        DimensionsSpec.builder().useSchemaDiscovery(true).build(),
        null,
        new AggregatorFactory[] {
            new CountAggregatorFactory("count")
        },
        Granularities.HOUR,
        true,
        IndexSpec.getDefault()
    );
  }
}
