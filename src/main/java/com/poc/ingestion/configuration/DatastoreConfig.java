package com.poc.ingestion.configuration;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import org.springframework.context.annotation.Configuration;


@Configuration
public class DatastoreConfig {

    public Datastore datastore = DatastoreOptions.getDefaultInstance().getService();
}
