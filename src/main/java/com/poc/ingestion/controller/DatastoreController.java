package com.poc.ingestion.controller;


import com.poc.ingestion.service.DatastoreService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/export/datastore")
public class DatastoreController {

    @Autowired
    private DatastoreService datastoreService;

    public DatastoreController(DatastoreService datastoreService) {
        this.datastoreService = datastoreService;
    }

    @PostMapping("/export")
    public ResponseEntity<String> exportData(@RequestParam String projectId, @RequestParam String datasetId, @RequestParam String tableId) {
        try {
            datastoreService.exportBigQueryToDatastore(projectId, datasetId, tableId);
            return ResponseEntity.ok("Export successful");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error during export: " + e.getMessage());
        }
    }

}
