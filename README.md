# 📊 Polish IT Job Market: Data Engineering, AI & Analytics Pipeline

**[🚀 Live Dashboard - Click Here to Explore the Data]**

## 📝 Overview

As an aspiring IT/Data Engineer, I built this end-to-end pipeline to capture, clean, and visualize the dynamic Polish IT job market. Understanding that clean data is the essential fuel for any analytics, this project started as a robust data infrastructure and has now evolved into a **fully automated Machine Learning and NLP ecosystem**. 

It reverse-engineers APIs from major job boards (Just Join IT, No Fluff Jobs), normalizes chaotic JSON structures, serves real-time insights, and leverages AI to predict salaries and match candidates with jobs via a cloud-hosted dashboard.

## 💻 Tech Stack & Architecture

This project bridges traditional data engineering with modern AI and MLOps practices:

* **Extraction (Python / Requests):** Bypassing undocumented APIs, handling pagination, and harvesting raw data.
* **Raw Data Lake (MinIO / S3 / Boto3):** Storing immutable, raw JSON payloads for future auditing.
* **Transformation (Pandas):** The core ETL engine. Cleans missing values, flattens nested structures, and standardizes diverse API schemas.
* **Data Warehouse (PostgreSQL / Neon Serverless):** A relational database storing the "Gold" layer of ready-to-use data via `SQLAlchemy`.
* **Machine Learning & NLP (scikit-learn / XGBoost):** Advanced modeling for salary prediction and text vectorization.
* **MLOps & CI/CD (GitHub Actions):** Fully automated, scheduled workflows for zero-touch model retraining and data ingestion.
* **Serving (Streamlit):** An interactive, analytical frontend with spatial analysis (Folium) deployed in the cloud.

## 🧠 Engineering, Data & AI Challenges Solved

### 1. Cross-Source Deduplication & Precision Loss Recovery
Job boards often overlap. A single employer might post the exact same job on multiple platforms at different times. I engineered a robust deduplication logic that:
* Overcomes time-precision loss by extracting exact timestamps (down to the second) from nested JSONs.
* Uses string normalization (removing special characters and legal entities like "Sp. z o.o.") to match job titles and companies across different platforms.
* Retains only the most recent, up-to-date iteration of a job posting (e.g., if salary brackets were updated).

### 2. Defeating "City Spamming" (Intra-Source Deduplication)
Job boards often duplicate a single remote offer across 10+ cities. I engineered a Pandas `.groupby()` aggregation logic that merges these clones into a single row, compiling the locations into an array without losing data integrity.

### 3. Fallback Logic for "Dirty Data" (Regex Heuristics)
To combat recruiters inaccurately labeling jobs (e.g., placing a "C++ Developer" in the "Other" category), I implemented a heuristic correction layer. Using vectorized Pandas operations and Regular Expressions (Regex), the pipeline scans job titles of miscategorized listings and automatically reassigns them to the correct IT domains before loading them into the database.

### 4. Handling Deep Pagination & Cloud Timeouts
Solved server-side database crashes (e.g., Elasticsearch 10,000 record cap) by implementing smart offset conditions. Furthermore, implemented *Defensive Programming* practices (connection pooling, custom timeouts) to handle the "Cold Start" phenomenon inherent to Serverless databases (Neon) during automated GitHub Actions runs.

### 5. Machine Learning Salary Estimator (XGBoost)
The model learns non-linear relationships between seniority, tech stacks (one-hot encoded), location, and contract types (B2B vs. UoP) to estimate accurate salary brackets. Transitioned from a standard Random Forest to a highly optimized `XGBRegressor`. 

### 6. NLP Smart Job Matcher (TF-IDF & Cosine Similarity)
Engineered a recommendation engine that converts job descriptions and candidate skills into high-dimensional mathematical vectors. It uses Term Frequency-Inverse Document Frequency (TF-IDF) and Cosine Similarity to objectively score and recommend the top 5 most relevant jobs based on a user's unique CV.

### 7. Continuous Learning (MLOps Automation)
To prevent the AI from functioning on outdated data, I implemented a GitHub Actions workflow. Every week, a cloud runner automatically fetches the latest database records, retrains the XGBoost model, and commits the updated `.pkl` weights back to the repository—ensuring the app learns continuously without manual intervention.

---

## 🎯 Roadmap: Transitioning to v2.0 (From ETL to ELT)

Currently, the project utilizes a traditional **ETL (Extract, Transform, Load)** paradigm, where heavy transformations (deduplication, normalization, regex matching) are processed in-memory via Pandas before being written to the PostgreSQL database.

**In upcoming iterations (v2.0), the architecture will transition to the modern ELT (Extract, Load, Transform) paradigm:**
* **Extract & Load:** Raw data will be dumped directly into the Data Warehouse.
* **Transform:** Transformations will be executed natively within the database using SQL and tools like **dbt (data build tool)**. This will drastically improve pipeline performance, allow for better data lineage tracking, and unlock the full computational power of the modern data stack.
