package com.poc.ingestion.service;

import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.poc.ingestion.configuration.DatastoreConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DatastoreService {
    @Autowired
    private DatastoreConfig datastoreConfig;
    @Autowired
    private BigQueryService bigQueryService;

    public DatastoreService(DatastoreConfig datastoreConfig, BigQueryService bigQueryService) {
        this.datastoreConfig = datastoreConfig;
        this.bigQueryService = bigQueryService;
    }

    public void writeToDatastore(String projectId, List<FieldValueList> bigQueryData) {

        Datastore datastore = datastoreConfig.datastore;
        // Loop through each row and write it to Datastore
        for (FieldValueList row : bigQueryData) {
            // Create a new entity with a unique key
            KeyFactory keyFactory = datastore.newKeyFactory().setKind("YourEntityKind");
            Key key = datastore.allocateId(keyFactory.newKey());

            // Map the BigQuery data to Datastore properties
            Entity entity = Entity.newBuilder(key)
                    .set("zip", row.get("zip").getStringValue()) // Map your fields accordingly
                    .set("numberOfYears", row.get("numberOfYears").getStringValue())
                    .build();

            // Insert the entity into Datastore
            datastore.put(entity);
        }
    }

    public void exportBigQueryToDatastore(String projectId, String datasetId, String tableId) throws InterruptedException {
        // Step 1: Get the data from BigQuery
        List<FieldValueList> bigQueryData = bigQueryService.getBigQueryData(projectId, datasetId, tableId);

        // Step 2: Write the data to Datastore
        writeToDatastore(projectId, bigQueryData);
    }


}
