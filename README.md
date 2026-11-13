# OCR-Underwriter
Ingest PDFs for insurance underwriting 
PDF OCR to Trino & Cloudera Semantic Search Pipeline

This repository contains a ready-to-import Apache NiFi (Cloudera DataFlow) pipeline definition. The flow automatically ingests multipage PDFs, extracts structured and unstructured data via a RESTful OCR service, and branches the output into relational storage (Cloudera Trino/Iceberg) and vector search (Cloudera Semantic Search).

🏗 Architecture Overview

The pipeline executes a single external OCR call per document and fans out the resulting JSON payload into three parallel tracks:

graph TD
    A[Ingest PDF & Assign Metadata] --> B[InvokeHTTP: POST to OCR Endpoint]
    B --> C[Validate & Enrich JSON]
    C --> D{Branching}
    
    D -->|Branch A: Form Fields| E[Jolt Flatten]
    E --> F[PutDatabaseRecord: Trino Staging]
    
    D -->|Branch B: Free Text| G[Jolt Flatten]
    
    G -->|Sub-Branch B1| H[PutDatabaseRecord: Trino Staging]
    
    G -->|Sub-Branch B2| I[ExecuteGroovyScript: Text Chunking]
    I --> J[InvokeHTTP: Bulk Index to Semantic Search]


Key Features

Predictable OCR Contract: Expects a JSON response containing form_fields and free_text_blocks (grouped by page/section).

Idempotent Correlation: Stigmatizes every row and chunk with a document_id and ingest_timestamp generated at the source.

Intelligent Chunking: Uses a sliding window Groovy script to chunk free text (default 1500 chars with 200 char overlap) into OpenSearch NDJSON bulk format.

Server-side Embeddings: Leverages Cloudera Semantic Search neural ingest pipelines to generate vectors automatically upon ingestion.

📋 Prerequisites

Cloudera DataFlow / Apache NiFi: Version 2.x (or 1.x with minor Groovy processor adjustments).

Cloudera Trino: Accessible via JDBC.

Cloudera Semantic Search: An active index with a neural embedding pipeline configured.

OCR API: A REST endpoint capable of accepting PDF binaries and returning the expected JSON contract.

🚀 Installation & Setup

Import the Flow:

Download the pdf-ocr-trino-semantic-search.json file from this repository.

In your NiFi UI, navigate to the Process Group level or the Catalog, and select Import Flow Definition.

Upload the JSON file and drop the instantiated process group onto your canvas.

Connect the Source:

Connect your desired ingress processor (e.g., ListS3 -> FetchS3Object, or GetFile) to the PDF_Documents_In input port.

Configure Parameters:

Right-click the Process Group, select Parameters, and fill in the values for the OCR_Trino_SemanticSearch_Params context:

Parameter

Description

Example

OCR_ENDPOINT_URL

OCR model HTTPS endpoint

https://ocr.internal/v1/extract

TRINO_JDBC_URL

JDBC string for staging schema

jdbc:trino://trino-coord:8443/iceberg/staging?SSL=true

TRINO_USER

Trino service account username

nifi_ingest

TRINO_PASSWORD

Trino service account password

[Sensitive]

TRINO_FORM_TABLE

Target table for form key/values

ocr_form_fields

TRINO_TEXT_TABLE

Target table for raw free text

ocr_free_text

CSS_BULK_ENDPOINT_URL

OpenSearch bulk ingest URL

https://css.internal:9200/_bulk?pipeline=embedding-pipeline

CSS_INDEX_NAME

Target OpenSearch index

pdf_document_chunks

Enable Controller Services:

Open the Process Group configurations and navigate to Controller Services.

Configure CDF_Shared_SSL_Context with your cluster's truststore/keystore.

Enable the SSL Context, the Trino Connection Pool, and the JSON Tree Reader.

🗄️ Target Schemas

Before starting the flow, ensure your target destinations are created to match the parsed NiFi records.

1. Trino / Iceberg Staging Tables

CREATE TABLE iceberg.staging.ocr_form_fields (
    document_id      VARCHAR,
    source_filename  VARCHAR,
    ingest_timestamp VARCHAR,
    page_number      INTEGER,
    field_name       VARCHAR,
    field_value      VARCHAR,
    confidence       DOUBLE
) WITH (format = 'PARQUET');

CREATE TABLE iceberg.staging.ocr_free_text (
    document_id      VARCHAR,
    source_filename  VARCHAR,
    ingest_timestamp VARCHAR,
    page_number      INTEGER,
    section_heading  VARCHAR,
    text_content     VARCHAR
) WITH (format = 'PARQUET');


2. Cloudera Semantic Search Index Mapping

PUT /pdf_document_chunks
{
  "settings": {
    "index.knn": true,
    "default_pipeline": "embedding-pipeline"
  },
  "mappings": {
    "properties": {
      "document_id":      { "type": "keyword" },
      "source_filename":  { "type": "keyword" },
      "ingest_timestamp": { "type": "date" },
      "page_number":      { "type": "integer" },
      "section_heading":  { "type": "text" },
      "chunk_index":      { "type": "integer" },
      "chunk_text":       { "type": "text" },
      "chunk_embedding": {
        "type": "knn_vector",
        "dimension": 768, 
        "method": {
          "name": "hnsw",
          "space_type": "cosinesimil",
          "engine": "lucene"
        }
      }
    }
  }
}


(Adjust the "dimension": 768 to match the output size of your specific embedding model).

🛠 Customization

If you need to adjust the sliding window size for text chunking before vectorization, edit the ExecuteGroovyScript processor (4B2. Chunk Text & Build Bulk NDJSON). Update the following variables at the top of the script:

int chunkSize = 1500  // Maximum characters per chunk
int overlap = 200     // Overlap window in characters
