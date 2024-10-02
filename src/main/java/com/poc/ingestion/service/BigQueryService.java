package com.poc.ingestion.service;

import com.google.cloud.bigquery.*;
import com.google.cloud.storage.Blob;
import com.poc.ingestion.configuration.BigQueryConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.google.common.collect.Lists;


@Service
public class BigQueryService {
    @Autowired
    private BigQueryConfig bigQueryConfig;

    public BigQueryService(BigQueryConfig bigQueryConfig) {
        this.bigQueryConfig = bigQueryConfig;
    }

    public void insertRowsIntoBigQuery(String datasetId, String tableId, Blob blob) throws Exception {
        TableId table = TableId.of(datasetId, tableId);

        // Convert the byte[] from blob.getContent() into InputStream using ByteArrayInputStream
        BufferedReader reader = new BufferedReader(new InputStreamReader(
                new ByteArrayInputStream(blob.getContent()), StandardCharsets.UTF_8));

        String line;
        boolean isFirstLine = true;
        String[] headers = null;

        InsertAllRequest.Builder insertRequest = InsertAllRequest.newBuilder(table);

        while ((line = reader.readLine()) != null) {
            if (isFirstLine) {
                headers = line.split(","); // Assuming the first line contains column headers
                isFirstLine = false;
            } else {
                String[] values = line.split(",");
                Map<String, Object> rowContent = new HashMap<>();
                for (int i = 0; i < headers.length; i++) {
                    rowContent.put(headers[i], values[i]);
                }

                // Add row to BigQuery insert request
                insertRequest.addRow(InsertAllRequest.RowToInsert.of(rowContent));
            }
        }

        // Insert into BigQuery
        InsertAllResponse response = bigQueryConfig.bigQuery().insertAll(insertRequest.build());

        if (response.hasErrors()) {
            throw new Exception("Errors occurred while inserting data into BigQuery: " + response.getInsertErrors());
        }
    }

    public List<FieldValueList> getBigQueryData(String projectId, String datasetId, String tableId) throws InterruptedException {
        // Build a query to select all data from a table
        String query = String.format("SELECT * FROM `%s.%s.%s`", projectId, datasetId, tableId);

        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

        // Run the query
        TableResult result = bigQueryConfig.bigQuery().query(queryConfig);

        return Lists.newArrayList(result.iterateAll());



    }
}