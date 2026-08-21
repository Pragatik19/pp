# SMART Analytics ETL v2 - Data Ingestion, AI Extraction & Redshift Load

## 1. Overview

This document describes the end-to-end data pipeline for the SMART (Social Media Analytics & Reporting Tool) platform. The pipeline ingests social media and web comments, uses Azure OpenAI GPT-4o to extract structured car review data, post-processes it, and loads it into Amazon Redshift and OpenSearch for dashboarding and chatbot use.

---

## 2. High-Level Architecture Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        SMART Analytics ETL v2 Pipeline                          │
└─────────────────────────────────────────────────────────────────────────────────┘

  ┌──────────────────────────────────────────────────────────────────────────────┐
  │  STAGE 1: DATA INGESTION (Pre-Processing - EC2)                              │
  │                                                                              │
  │  Sources:                                                                    │
  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐     │
  │  │CardDekho │  │ YouTube  │  │Sprinklr  │  │ TeamBHP  │  │  Reddit  │     │
  │  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘     │
  │       └─────────────┴─────────────┴──────────────┴─────────────┘           │
  │                                   │                                          │
  │                          combine_raw_files.py                                │
  │                                   │                                          │
  │                    ┌──────────────▼──────────────┐                          │
  │                    │  S3: dataset/RAW/india/      │                          │
  │                    │  S3: dataset/RAW/international/                         │
  │                    └──────────────┬──────────────┘                          │
  └─────────────────────────────────────────────────────────────────────────────┘
                                      │
                              trigger_glue_job.py
                                      │
  ┌───────────────────────────────────▼─────────────────────────────────────────┐
  │  STAGE 2: AI EXTRACTION (AWS Batch + Azure OpenAI GPT-4o)                   │
  │                                                                              │
  │  Glue Job: aws-batch-glue-job.py                                             │
  │  Triggers AWS Batch jobs per persona (IM / PP / VE)                          │
  │                                                                              │
  │  ┌─────────────────────────────────────────────────────────────────────┐    │
  │  │  AWS Batch Container (Docker)                                        │    │
  │  │                                                                      │    │
  │  │  main.py  ──► PERSONA env var                                        │    │
  │  │               ├── IM  ──► main_im.py  (International Marketing)      │    │
  │  │               ├── PP  ──► main_pp.py  (Product Planning)             │    │
  │  │               └── VE  ──► main_ve.py  (Vehicle Engineering)          │    │
  │  │                                                                      │    │
  │  │  Per comment:                                                        │    │
  │  │  1. Language detection (has_non_english_keyboard)                    │    │
  │  │  2. Translation if needed (AWS Bedrock Claude 3.5 Sonnet)            │    │
  │  │  3. Azure OpenAI GPT-4o function calling → extract_car_details()     │    │
  │  │     Extracts: OEM, Model, Domain, Category, Sentiment,               │    │
  │  │               PartName, PerformanceIndicator, BrandKPIs              │    │
  │  │  4. convertToDataFrame() → structured DataFrame                      │    │
  │  │  5. Periodic checkpoint save to S3                                   │    │
  │  └─────────────────────────────────────────────────────────────────────┘    │
  │                                                                              │
  │  Output S3 paths:                                                            │
  │  ┌──────────────────────────────────────────────────────────────────────┐   │
  │  │  s3://smart-{env}-apsouth1-analytics-etl/dataset/PROCESSED/IM/       │   │
  │  │  s3://smart-{env}-apsouth1-analytics-etl/dataset/PROCESSED/PP/       │   │
  │  │  s3://smart-{env}-apsouth1-analytics-etl/dataset/PROCESSED/VE/       │   │
  │  │  s3://smart-{env}-apsouth1-analytics-etl/dataset/sensex_data/        │   │
  │  └──────────────────────────────────────────────────────────────────────┘   │
  └─────────────────────────────────────────────────────────────────────────────┘
                                      │
  ┌───────────────────────────────────▼─────────────────────────────────────────┐
  │  STAGE 3: POST-PROCESSING (AWS Glue)                                         │
  │  Job: smart-uat-apsouth1-analytics-etl-regular-post-processing-job.py        │
  │                                                                              │
  │  IM & PP Processing:                                                         │
  │  ┌──────────────────────────────────────────────────────────────────────┐   │
  │  │  Read PROCESSED/IM/ or PROCESSED/PP/ from S3                         │   │
  │  │  → Filter isRelevantCarReview == "Yes"                                │   │
  │  │  → Backup to S3 dataset/Backup/{date}/                                │   │
  │  │  → filter_valid_oem_rows() - fuzzy OEM matching                       │   │
  │  │  → filter_valid_model_rows() - fuzzy model matching                   │   │
  │  │  → apply_oem_model_mapping() - lookup table normalization             │   │
  │  │  → fuzzy_standardize_column() - domain/category standardization       │   │
  │  │  → get_related_keywords() - keyword enrichment from Redshift          │   │
  │  │  → generate_performance_keywords() / _pp() - stemming + fuzzy match   │   │
  │  │  → Save to S3 opensearch_data/ for embedding indexing                 │   │
  │  │  → Upload CSV to S3 tmp_im_data/                                      │   │
  │  │  → COPY to Redshift via IAM role                                      │   │
  │  └──────────────────────────────────────────────────────────────────────┘   │
  │                                                                              │
  │  Sensex KPI Processing:                                                      │
  │  ┌──────────────────────────────────────────────────────────────────────┐   │
  │  │  Read sensex_data/ from S3                                            │   │
  │  │  → filter_valid_oem_rows()                                            │   │
  │  │  → oem_ratings_final() - weighted KPI scoring (12 indicators)        │   │
  │  │  → Merge with previous 7-day ratings from Redshift                    │   │
  │  │  → Compute delta_rating                                               │   │
  │  │  → INSERT INTO smart_zone.tbl_sensex_kpi                              │   │
  │  └──────────────────────────────────────────────────────────────────────┘   │
  │                                                                              │
  │  VE Processing:                                                              │
  │  ┌──────────────────────────────────────────────────────────────────────┐   │
  │  │  Read PROCESSED/VE/ from S3                                           │   │
  │  │  → post_process() - OEM/model/part/PI extraction + word count        │   │
  │  │  → Row expansion (OEM × Model × Part × PI combinations)              │   │
  │  │  → get_pi_freq() - performance indicator frequency mapping            │   │
  │  │  → Save to S3 PROCESSED/DS/Current/processed/                        │   │
  │  └──────────────────────────────────────────────────────────────────────┘   │
  └─────────────────────────────────────────────────────────────────────────────┘
                                      │
  ┌───────────────────────────────────▼─────────────────────────────────────────┐
  │  STAGE 4: LOAD TO REDSHIFT + OPENSEARCH                                      │
  │  Job: smart-uat-apsouth1-analytics-etl-load-data-redshift.py                 │
  │                                                                              │
  │  ┌──────────────────────────────────────────────────────────────────────┐   │
  │  │  Read DS/Current/processed/ parquet from S3                           │   │
  │  │  → Column rename/mapping                                              │   │
  │  │  → Data cleaning (special chars, whitespace, OEM aliasing)            │   │
  │  │  → UUID generation for unique_id backfill                             │   │
  │  │  → COPY to Redshift staging table (parquet via IAM role)              │   │
  │  │  → INSERT INTO main table (dedup by md5(comment)+OEM+Model+Part+PI)   │   │
  │  │  → TRUNCATE staging table                                             │   │
  │  └──────────────────────────────────────────────────────────────────────┘   │
  │                                                                              │
  │  OpenSearch Indexing:                                                        │
  │  ┌──────────────────────────────────────────────────────────────────────┐   │
  │  │  VE data: clean_text() → get_embedding() (Amazon Titan v2, 1024-dim) │   │
  │  │  IM/PP data: read opensearch_data/ → embed → index                   │   │
  │  │  → client.index(index="bot-dev1-opensearch", body=doc)               │   │
  │  └──────────────────────────────────────────────────────────────────────┘   │
  └─────────────────────────────────────────────────────────────────────────────┘
                                      │
  ┌───────────────────────────────────▼─────────────────────────────────────────┐
  │  STAGE 5: CONSUMPTION                                                        │
  │                                                                              │
  │  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐                    │
  │  │  QuickSight  │   │  Chatbot     │   │  Reports     │                    │
  │  │  Dashboards  │   │  (OpenSearch │   │  (report_    │                    │
  │  │  (Redshift)  │   │   + Bedrock) │   │  generation) │                    │
  │  └──────────────┘   └──────────────┘   └──────────────┘                    │
  └─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Stage 1: Data Ingestion (Pre-Processing)

**Location:** `pre-processing/`  
**Orchestrator:** `main.py` running on EC2

### 3.1 Data Sources

| Source | Script | Data Type | S3 Destination |
|---|---|---|---|
| CardDekho | `cardekho.py` | Car reviews (India) | `dataset/RAW/india/` |
| YouTube | `youtube.py` | Video comments | `dataset/RAW/india/` or `international/` |
| Sprinklr | `sprinklr.py` | Social media posts | `dataset/RAW/india/` or `international/` |
| TeamBHP | `run_spider_teambhp.py` (Scrapy) | Forum reviews | `dataset/RAW/india/` |
| Reddit | `carscrapper/spiders/reddit.py` | Reddit posts | `dataset/RAW/international/` |

### 3.2 Pre-Processing Steps

```
main.py (EC2)
    │
    ├── cardekho.py          → scrape CardDekho reviews
    ├── youtube.py           → fetch YouTube comments via API
    ├── sprinklr.py          → pull Sprinklr social data
    ├── run_spider_teambhp.py → Scrapy spider for TeamBHP
    ├── combine_raw_files.py  → merge all raw CSVs into one file
    │                           Fields: ID, url, message, CleanedMessage,
    │                                   Date, Month, Year, Source, Platform,
    │                                   Country, Location, Language
    ├── archive_combine_raw_files.py → backup combined file to S3
    ├── load_dictionary.py   → sync im_words.json, pp_words.json,
    │                          ve_words.json, car_models.json to S3
    ├── clear_folder_contain.py → clean local temp folders
    └── trigger_glue_job.py  → trigger AWS Glue job (aws-batch-glue-job)
```

### 3.3 Raw Data Schema

| Field | Description |
|---|---|
| ID | Unique comment identifier |
| url | Source URL |
| message | Original raw comment |
| CleanedMessage | Basic cleaned comment (emoji removed, HTML stripped) |
| Date | Comment date |
| Month | Month number |
| Year | Year |
| Source | Platform type (SOCIAL, WEB, PRINT, etc.) |
| Platform | Specific platform (Sprinklr, YouTube, etc.) |
| Country | Country of commenter |
| Location | Geographic location |
| Language | Detected language |
| ParentComment | Parent comment if reply |

---

## 4. Stage 2: AI Extraction (AWS Batch + Azure OpenAI GPT-4o)

**Location:** `AWS-Batch/`  
**Trigger:** Glue job `aws-batch-glue-job.py`

### 4.1 AWS Batch Job Submission

`aws-batch-glue-job.py` submits one Batch job per file per persona:

```python
batch_client.submit_job(
    jobQueue  = "smart-{env}-apsouth1-dapf-batch-inferencing-job-queue",
    jobDefinition = "smart-{env}-apsouth1-dapf-batch-inferencing-job-def",
    containerOverrides = {
        'environment': [
            {'name': 'PERSONA', 'value': 'IM' | 'PP' | 'VE'},
            {'name': 'filename', 'value': '<filename>.csv'}
        ]
    }
)
```

Job IDs are tracked in `s3://smart-{env}-apsouth1-analytics-etl/dataset/batch_ids.txt`.

### 4.2 Persona Routing

`main.py` reads the `PERSONA` environment variable and routes to the correct extractor:

| PERSONA | Script | Source S3 Path | Target S3 Path |
|---|---|---|---|
| IM | `main_im.py` | `dataset/RAW/international/` | `dataset/PROCESSED/IM/` |
| PP | `main_pp.py` | `dataset/RAW/india/` | `dataset/PROCESSED/PP/` |
| VE | `main_ve.py` | `dataset/RAW/india/` | `dataset/PROCESSED/VE/` |

### 4.3 Language Detection & Translation

For IM persona (international data):

```
CleanedMessage
    │
    ├── has_non_english_keyboard()  → detects non-ASCII characters
    │
    ├── is_mostly_english()         → NLTK lemmatization + vocab ratio check
    │                                 threshold = 85% English words
    │
    └── translation_needed = True?
            │
            Yes → translate_text_to_english()
            │     AWS Bedrock: Claude 3.5 Sonnet (apac.anthropic.claude-3-5-sonnet-20241022-v2:0)
            │     Streamed response via invoke_model_with_response_stream()
            │
            No  → use CleanedMessage as-is
            │
            └── UserComment (final field for GPT-4o)
```

PP and VE personas skip translation (India-only data, primarily English).

### 4.4 Azure OpenAI GPT-4o Extraction

Each `UserComment` is sent to Azure OpenAI using **function calling**:

```
UserComment
    │
    └── client.chat.completions.create(
            model = "gpt-4o",
            function_call = {"name": "extract_car_details"}
        )
```

#### 4.4.1 Extracted Fields by Persona

**IM (International Marketing):**

| Field | Description |
|---|---|
| isRelevantCarReview | Yes/No - is this a genuine car review |
| OverallSentiment | positive / negative / neutral |
| CarOEM | Car manufacturer name |
| BrandReviewKPIs | List of brand KPIs with sentiment (12 possible KPIs) |
| carmodelreview[].CarModel | Car model name |
| carmodelreview[].Sentiment | Model-level sentiment |
| carmodelreview[].CarPartsDetails[].Domain | Domain from enum list |
| carmodelreview[].CarPartsDetails[].Category | Category from enum list |
| carmodelreview[].CarPartsDetails[].SpecSentiment | Part-level sentiment |

**PP (Product Planning) - same as IM minus BrandReviewKPIs**

**VE (Vehicle Engineering):**

| Field | Description |
|---|---|
| isRelevantCarReview | Yes/No |
| OverallSentiment | positive / negative / neutral |
| CarOEM | Manufacturer |
| BrandReviewKPIs | Brand KPIs with sentiment |
| carmodelreview[].CarModel | Model name |
| carmodelreview[].CarPartsDetails[].PartName | General part name |
| carmodelreview[].CarPartsDetails[].SpecPartName | Specific part from data dictionary enum |
| carmodelreview[].CarPartsDetails[].PerformanceIndicator | Performance metric (max 5 words) |
| carmodelreview[].CarPartsDetails[].ModelYear | Year of car model |
| carmodelreview[].CarPartsDetails[].FuelType | Petrol/Diesel/Electric/CNG/LPG/Hybrid |
| carmodelreview[].CarPartsDetails[].SpecSentiment | Part-level sentiment |

#### 4.4.2 Brand KPI Taxonomy (12 KPIs)

1. Vehicle Performance
2. Design & Aesthetics
3. Technology & Features
4. Mileage & Fuel efficiency
5. Service & After-Sales
6. Ride Comfort
7. Safety and Driver Assistance
8. Pricing & Value & Resale Value
9. Ownership Experience
10. Brand Perception
11. Environmental Impact
12. Product Quality

#### 4.4.3 Cost Tracking

```python
LLM_PRICING = {
    "input":  2.5 / 1_000_000,   # $2.50 per 1M input tokens
    "output": 10  / 1_000_000    # $10.00 per 1M output tokens
}
total_cost += (input_tokens * 2.5 + output_tokens * 10) / 1_000_000
```

### 4.5 Checkpoint Saves

Every 137 rows, intermediate results are saved to S3 to prevent data loss on long-running jobs:

```
Every 137 rows:
    → upload output_IM_{filename}.csv  to s3://.../PROCESSED/IM/
    → upload output_SensexKPI_IM_{filename}.csv to s3://.../sensex_data/
```

### 4.6 Output Schema (PROCESSED files)

| Field | Source |
|---|---|
| isRelevantCarReview | GPT-4o |
| OverallSentiment | GPT-4o |
| CarOEM | GPT-4o |
| CarModel | GPT-4o |
| Sentiment | GPT-4o |
| Domain / Category | GPT-4o (IM/PP) |
| PartName / SpecPartName | GPT-4o (VE) |
| PerformanceIndicator | GPT-4o (VE) |
| SpecSentiment | GPT-4o |
| FuelType / ModelYear | GPT-4o (VE) |
| comments | UserComment (translated or original) |
| ID, url, Location, Country, Date, Month, Year, Source, Platform | Metadata passthrough |
| Persona | Derived from Country (India → VE/PP, else → IM) |

---

## 5. Stage 3: Post-Processing (AWS Glue)

**Script:** `glue/post-processing/smart-uat-apsouth1-analytics-etl-regular-post-processing-job.py`

This Glue job handles three parallel processing tracks: IM/PP data, Sensex KPI data, and VE data.

### 5.1 IM & PP Post-Processing

```
S3: PROCESSED/IM/ or PROCESSED/PP/
    │
    ├── Filter isRelevantCarReview == "Yes"
    │   (Enterprise persona: skip this filter)
    │
    ├── Backup to s3://.../dataset/Backup/{date}/{PERSONA}_{date}.csv
    │
    ├── filter_valid_oem_rows(df, oem_list)
    │   └── fuzzy match CarOEM against Redshift tbl_smart_oem_model_fuel_vc
    │       scorer = fuzz.token_sort_ratio, score_cutoff = 80
    │
    ├── filter_valid_model_rows(df, model_list)
    │   └── fuzzy match CarModel, score_cutoff = 70
    │
    ├── apply_oem_model_mapping(df, lookup)
    │   └── model → OEM lookup dict from Vahan table
    │       Special case: "Maruti Suzuki" → "Suzuki" for IM persona
    │
    ├── Column rename (rename_map):
    │   CarOEM → oem, CarModel → model_name, comments → comment,
    │   SpecSentiment → spec_sentiment, Domain → domain,
    │   Category → category, Location → continent, Date → comment_datetime,
    │   ID → unique_id, message → original_comment, Platform → platform
    │
    ├── parse_datetime() - normalize to '%Y-%m-%d %H:%M:%S'
    │
    ├── get_continent(country) - pycountry_convert lookup
    │
    ├── fuzzy_standardize_column(df, 'domain', mapping_table)
    │   └── match against Redshift reference table, threshold = 85
    │
    ├── fuzzy_standardize_column(df, 'category', mapping_table)
    │
    ├── get_related_keywords(df, mapping_table)
    │   └── JOIN df.category with Redshift tbl_*_keyword.related_keywords
    │
    ├── generate_performance_keywords() [IM/Enterprise]
    │   └── PorterStemmer + Counter on comment tokens
    │       Returns {category: count} if category stem found in comment
    │
    ├── generate_performance_keywords_pp() [PP]
    │   └── Stemming + fuzzy match on related_keywords list
    │       Returns {keyword: count} for all matched keywords
    │
    ├── Save to s3://.../dataset/opensearch_data/{table}.csv
    │   (for downstream OpenSearch embedding indexing)
    │
    ├── Upload CSV (no header) to s3://.../dataset/tmp_im_data/{table}.csv
    │
    └── Redshift COPY command:
        COPY {redshift_table} (url, oem, model_name, source, comment_datetime,
             year_, month_, sentiment, comment, insert_date, persona, country,
             spec_sentiment, unique_id, original_comment, continent,
             domain, category, platform, performance_keywords)
        FROM 's3://{bucket}/{key}'
        IAM_ROLE '...role/{iam_role},...role/{glue_role}'
        FORMAT AS CSV  BLANKSASNULL  EMPTYASNULL  TIMEFORMAT 'auto'
        MAXERROR 1000
```

#### 5.1.1 Target Redshift Tables (IM/PP)

| Call | S3 Source | Redshift Table |
|---|---|---|
| Enterprise | `PROCESSED/PP/` | `smart_zone.tbl_smart_social_media_data_enterprise` |
| Product Planning | `PROCESSED/PP/` | `smart_zone.tbl_smart_social_media_data_product_planning` |
| International Marketing | `PROCESSED/IM/` | `smart_zone.tbl_smart_social_media_data_im` |

### 5.2 Sensex KPI Post-Processing

```
S3: dataset/sensex_data/
    │
    ├── Backup to s3://.../Backup/{date}/SensexKPI_{date}.csv
    │
    ├── Standardize Country names (replace "_" with " ", title case)
    │
    ├── filter_valid_oem_rows() - keep only known OEMs
    │
    ├── oem_ratings_final(df)
    │   └── Group by Country × OEM × BrandPerformanceIndicator
    │       Compute Positive_total_ratio = positive_count / total_count
    │       Apply weighted scoring (12 KPI weights, sum = 1.0):
    │         Brand Perception: 0.15, Environmental Impact: 0.05
    │         All others: 0.08 each
    │       Final rating scaled to 0-5
    │
    ├── Fetch previous 7-day ratings from Redshift:
    │   SELECT * FROM smart_zone.tbl_sensex_kpi
    │   WHERE insert_date >= CURRENT_DATE - INTERVAL '8 days'
    │
    ├── Merge current vs previous → compute delta_rating
    │
    ├── get_continent(country) enrichment
    │
    └── INSERT INTO smart_zone.tbl_sensex_kpi
        (country, oem, insert_date, delta_rating, current_rating, continent)
```

#### 5.2.1 KPI Weights

| KPI | Weight |
|---|---|
| Brand Perception | 0.15 |
| Environmental Impact | 0.05 |
| Vehicle Performance | 0.08 |
| Design & Aesthetics | 0.08 |
| Technology & Features | 0.08 |
| Mileage & Fuel efficiency | 0.08 |
| Service & After-Sales | 0.08 |
| Ride Comfort | 0.08 |
| Safety and Driver Assistance | 0.08 |
| Pricing & Value & Resale Value | 0.08 |
| Ownership Experience | 0.08 |
| Product Quality | 0.08 |

### 5.3 VE Post-Processing

```
S3: PROCESSED/VE/
    │
    ├── Filter isRelevantCarReview == "Yes"
    │
    ├── Backup to s3://.../Backup/{date}/VE_{date}.csv
    │
    ├── Column rename (comments→Comments, OverallSentiment→Sentiment, etc.)
    │
    ├── post_process(df, oem_list, model_list, lookup, part_list)
    │   For each row:
    │   ├── get_oem()    - fuzzy OEM match, score_cutoff=80
    │   ├── get_model()  - fuzzy model match, score_cutoff=70
    │   ├── model_oem_mapping() - resolve OEM from model lookup
    │   ├── get_parts()  - fuzzy part match against data dictionary
    │   │   └── extended_keywords = area + system + subsystem + part + keywords
    │   ├── get_perf_indicator() - spaCy PhraseMatcher on PI keywords
    │   ├── get_sentiment() - validate against [positive/negative/neutral]
    │   ├── get_fuel_type() - validate against [petrol/diesel/cng/electric/hybrid]
    │   ├── get_model_year() - extract year
    │   └── remove_stop_words() + word_count JSON
    │
    ├── Row Expansion Pass 1: OEM × Model × Sentiment × Fuel × ModelYear
    │   (one row per combination)
    │
    ├── Row Expansion Pass 2: Part × PerformanceIndicator
    │   (one row per part-PI pair using zip_longest)
    │
    ├── Save interim to s3://.../dataset/interim_etl_data/new_df1.csv
    │
    ├── get_pi_freq(word_count, pi)
    │   └── Match PI keywords against word_count dict
    │       text_correction() via TextBlob spell check
    │       Returns {corrected_keyword: frequency}
    │
    ├── Drop duplicates (excluding spec_sentiment)
    │
    └── Save to s3://.../PROCESSED/DS/Current/processed/processed_data_ds.csv
```

---

## 6. Stage 4: Load to Redshift & OpenSearch

**Script:** `glue/post-processing/smart-uat-apsouth1-analytics-etl-load-data-redshift.py`

### 6.1 Data Cleaning Pipeline

```
Read s3://.../PROCESSED/DS/Current/processed/processed_data_ds.csv
    │
    ├── Column rename via COLUMN_MAPPING dict
    │   sequence→id, UserComment→comment, model→Model_Name,
    │   oem→OEM, fuel→Fuel_Type, part→Part_Name, pi→Performance_Indicator,
    │   pi_freq→performance_keywords, etc.
    │
    ├── country name standardization (replace "_" → " ", title case)
    ├── continent derivation via pycountry_convert
    │
    ├── TextPreprocessing (smart_text_cleaning_helper_function):
    │   ├── restrict_character_length()
    │   ├── other_filters()
    │   ├── fill_null_values()
    │   ├── remove_special_char_in()
    │   ├── remove_single_quote_in()
    │   ├── remove_white_spaces()
    │   ├── make_running_case()
    │   ├── update_maruti_oem_to_single_value()
    │   └── filter_valid_sentiment_type()
    │
    ├── spec_sentiment validation: only [positive/negative/neutral]
    │   → empty rows with Part_Name get 'neutral'
    ├── Fuel_Type: 'Cng' → 'CNG'
    │
    ├── UUID backfill for existing Redshift rows missing unique_id:
    │   SELECT DISTINCT comment WHERE unique_id IS NULL
    │   → uuid.uuid5(NAMESPACE_DNS, comment)
    │   → COPY temp table → UPDATE main table
    │
    └── Default value updates on existing Redshift data:
        persona = 'Vehicle Engineering' if empty
        country = 'India' if empty
        spec_sentiment = sentiment if part_name exists
        original_comment = comment if empty
        continent = 'Asia' if empty
        platform = 'Sprinklr' if empty
```

### 6.2 Redshift Load (VE Data)

```
Save final_df as Parquet → s3://{bucket}/{output_file_path}

COPY smart_zone.tbl_smart_social_media_data_stg (
    platform, continent, original_comment, unique_id, spec_sentiment,
    country, persona, Performance_Indicator, SubSystem, System_, Area,
    Fuel_Type, Month_, Part_Name, Source, OEM, Sentiment, Vehicle_Category,
    performance_keywords, word_count, comment, Comment_datetime,
    Model_Name, url, uc_model_year, Year_, id
)
FROM 's3://{bucket}/{key}'
IAM_ROLE '...role/{iam_role},...role/{glue_role}'
FORMAT AS PARQUET SERIALIZETOJSON

INSERT INTO smart_zone.tbl_smart_social_media_data
SELECT s.* FROM staging s
WHERE NOT EXISTS (
    SELECT 1 FROM main m
    WHERE md5(m.comment) = md5(s.comment)
    AND m.OEM = s.OEM
    AND m.Model_Name = s.Model_Name
    AND m.Source = s.Source
    AND m.Part_Name = s.Part_Name
    AND m.Performance_Indicator = s.Performance_Indicator
)

TRUNCATE TABLE smart_zone.tbl_smart_social_media_data_stg
```

### 6.3 OpenSearch Indexing

```
Index name: "bot-dev1-opensearch"
Embedding model: amazon.titan-embed-text-v2:0 (1024 dimensions, normalized)
Search type: k-NN (HNSW, nmslib engine)

VE data:
    final_df → clean_text() → get_embedding() → client.index()

IM/PP data:
    s3://.../dataset/opensearch_data/ → clean_text() → get_embedding() → client.index()

Document fields indexed:
    id, url, oem, vehicle_category, model_name, source, part_name,
    comment_datetime, year_, month_, fuel_type, area, system_, subsystem,
    performance_indicator, sentiment, comment, uc_model_year,
    performance_keywords, persona, country, spec_sentiment, unique_id,
    cleanedData, embeddings (knn_vector[1024]),
    domain, category, platform
```

---

## 7. Redshift Schema

### 7.1 Main Tables

| Table | Persona | Key Columns |
|---|---|---|
| `smart_zone.tbl_smart_social_media_data` | VE | OEM, Model_Name, Part_Name, Performance_Indicator, Sentiment, Area, System_, SubSystem, Fuel_Type, word_count, performance_keywords |
| `smart_zone.tbl_smart_social_media_data_im` | IM | oem, model_name, domain, category, sentiment, spec_sentiment, performance_keywords |
| `smart_zone.tbl_smart_social_media_data_product_planning` | PP | oem, model_name, domain, category, sentiment, spec_sentiment, performance_keywords |
| `smart_zone.tbl_smart_social_media_data_enterprise` | Enterprise | Same as PP, no relevance filter |
| `smart_zone.tbl_sensex_kpi` | All | country, oem, current_rating, delta_rating, continent, insert_date |

### 7.2 Reference Tables

| Table | Purpose |
|---|---|
| `smart_zone.tbl_smart_oem_model_fuel_vc` | OEM/model/fuel/vehicle category master (Vahan data) |
| `smart_zone.tbl_smart_data_dictionary` | Part name → area/system/subsystem/PI mapping |
| `smart_zone.tbl_im_keyword` | IM domain/category/related_keywords reference |
| `smart_zone.tbl_product_planning_keyword` | PP domain/category/related_keywords reference |

---

## 8. S3 Bucket Structure

```
s3://smart-{env}-apsouth1-analytics-etl/
├── dataset/
│   ├── RAW/
│   │   ├── india/              ← PP and VE raw input
│   │   └── international/      ← IM raw input
│   ├── PROCESSED/
│   │   ├── IM/                 ← GPT-4o output for IM
│   │   ├── PP/                 ← GPT-4o output for PP
│   │   ├── VE/                 ← GPT-4o output for VE
│   │   ├── translated/IM/      ← Translated IM files
│   │   └── DS/Current/
│   │       ├── processed/      ← Final VE processed parquet/csv
│   │       └── not_processed/  ← Empty OEM/sentiment records
│   ├── sensex_data/            ← Sensex KPI raw output from Batch
│   ├── Backup/{date}/          ← Daily backups per persona
│   ├── tmp_im_data/            ← Temp CSV for Redshift COPY (IM/PP)
│   ├── opensearch_data/        ← IM/PP CSVs for embedding indexing
│   ├── interim_etl_data/       ← VE intermediate processing files
│   ├── batch_ids.txt           ← Submitted AWS Batch job IDs
│   └── uuid_update.csv         ← UUID backfill temp file
└── data_dictionary/
    ├── im_words.json
    ├── pp_words.json
    ├── ve_words.json
    └── car_models.json
```

---

## 9. AWS Services Used

| Service | Purpose |
|---|---|
| EC2 | Pre-processing orchestration (main.py) |
| AWS Batch | Containerized GPT-4o AI extraction per persona |
| AWS Glue | Post-processing and Redshift load orchestration |
| Amazon S3 | Raw data, processed data, backups, temp files |
| Amazon Redshift | Final analytical data store |
| Amazon OpenSearch | Vector search for chatbot (k-NN embeddings) |
| AWS Secrets Manager | Credentials for Redshift, OpenAI, Bedrock, OpenSearch |
| Amazon Bedrock | Claude 3.5 Sonnet (translation) + Titan Embed v2 (embeddings) |
| Azure OpenAI | GPT-4o for structured car review extraction |
| Amazon ECR | Docker image registry for Batch containers |

---

## 10. Security & IAM

- Redshift COPY uses chained IAM roles:
  `arn:aws:iam::825589354750:role/{iam_role},arn:aws:iam::{acc_no}:role/{glue_role}`
- All secrets (Redshift credentials, API keys, OpenSearch auth) stored in AWS Secrets Manager
- Secret IDs follow pattern: `{env}/smart_redshift_secrets`, `{env}/open_ai`, `{env}/bedrock`, `{env}/OpenSearch`
- Environment resolved at runtime via `environment` secret key

---

## 11. Glue Jobs Summary

| Job Script | Trigger | Purpose |
|---|---|---|
| `aws-batch-glue-job.py` | EC2 trigger_glue_job.py | Submit AWS Batch jobs per file/persona |
| `aws-batch-sync-job.py` | Scheduled / manual | Sync Batch job status |
| `smart-uat-apsouth1-analytics-etl-regular-post-processing-job.py` | After Batch completes | Post-process IM/PP/VE + Sensex KPI → Redshift |
| `smart-uat-apsouth1-analytics-etl-load-data-redshift.py` | After post-processing | Clean VE data + load to Redshift + index OpenSearch |
| `smart-uat-apsouth1-analytics-etl-fieldmap-area-system-subsystem.py` | Scheduled | Map area/system/subsystem fields |
| `glue/google_news/whats_new.py` | Scheduled | Google News ingestion |
| `glue/Leaderboard/live_ranks.py` | Scheduled | Live leaderboard ranking |
| `glue/Leaderboard/store_weekly_ranks.py` | Scheduled | Weekly rank archival |
