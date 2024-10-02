package com.poc.ingestion.controller;


import com.google.cloud.bigquery.*;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.poc.ingestion.service.BigQueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/ingest")
public class IngestionController {
    @Autowired
    private final BigQueryService bigQueryService;
    private final Storage storage;

    public IngestionController(BigQueryService bigQueryService, BigQuery bigQuery) {
        this.bigQueryService = bigQueryService;
        this.storage = StorageOptions.getDefaultInstance().getService();
    }

    @PostMapping("/fileFromGCS")
    public ResponseEntity<String> uploadFileFromGCS(@RequestParam("bucketName") String bucketName,
                                                    @RequestParam("fileName") String fileName,
                                                    @RequestParam("datasetId") String datasetId,
                                                    @RequestParam("tableId") String tableId) {

        try {
            Blob blob = storage.get(bucketName, fileName);
            if (blob == null) {
                return new ResponseEntity<>("File not found in Cloud Storage", HttpStatus.NOT_FOUND);
            }
            bigQueryService.insertRowsIntoBigQuery(datasetId, tableId, blob);
            return new ResponseEntity<>("File ingested successfully!", HttpStatus.OK);

        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseEntity<>("File ingestion failed: " + e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

}