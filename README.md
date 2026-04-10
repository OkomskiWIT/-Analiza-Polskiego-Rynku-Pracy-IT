# 📊 Polish IT Job Market: Data Engineering, AI & Analytics Pipeline

🚀 **[Live Dashboard - Click Here to Explore the Data](https://kamil-rynek-it.streamlit.app)**

### 📖 Overview

As an aspiring IT/Data Engineer, I built this end-to-end ETL (Extract, Transform, Load) pipeline to capture, clean, and visualize the dynamic Polish IT job market. 
Understanding that clean data is the essential fuel for any analytics, this project started as robust data infrastructure and has now evolved into a **fully automated Machine Learning and NLP ecosystem**. It reverse-engineers APIs from major job boards (Just Join IT, No Fluff Jobs), normalizes chaotic JSON structures, serves real-time insights, and leverages AI to predict salaries and match candidates with jobs via a cloud-hosted dashboard.

### 🛠️ Tech Stack & Architecture

This project bridges traditional data engineering with modern AI and MLOps practices:
* **Extraction (Python / Requests):** Bypassing undocumented APIs, handling pagination, and simulating browser headers to harvest raw data.
* **Raw Data Lake (MinIO / S3 / Boto3):** Storing immutable, raw JSON payloads for future auditing or alternative transformations.
* **Transformation (Pandas):** The core ETL engine. Cleans missing values, flattens nested data structures, and standardizes diverse API schemas into a single source of truth.
* **Data Warehouse (PostgreSQL / Neon Serverless):** A relational database storing the "Gold" layer of ready-to-use data via `SQLAlchemy`.
* **Machine Learning & NLP (scikit-learn / XGBoost):** Advanced modeling for salary prediction and text vectorization.
* **MLOps & CI/CD (GitHub Actions):** Fully automated, scheduled workflows for zero-touch model retraining.
* **Serving (Streamlit):** An interactive, analytical frontend deployed in the cloud.

### 🧠 Engineering, Data & AI Challenges Solved

* **Defeating "City Spamming" (Deduplication):** Job boards often duplicate a single remote offer across 10+ cities. I engineered a Pandas `.groupby()` aggregation logic that merges these clones into a single row, compiling the locations into an array without losing data integrity.
* **Complex Contract & Tech Stack Extraction:** Upgraded the transformation layer to independently parse complex, nested JSON arrays for employment types (e.g., merging separate B2B and UoP flags) and required tech stacks, even when salary brackets are missing.
* **Machine Learning Salary Estimator (XGBoost):** Transitioned from a standard Random Forest to a highly optimized `XGBRegressor`. The model learns non-linear relationships between seniority, tech stacks (one-hot encoded), location, and contract types (B2B vs. UoP) to estimate accurate salary brackets.
* **NLP Smart Job Matcher (TF-IDF & Cosine Similarity):** Engineered a recommendation engine that converts job descriptions and candidate skills into high-dimensional mathematical vectors. It uses Term Frequency-Inverse Document Frequency (TF-IDF) and Cosine Similarity to objectively score and recommend the top 5 most relevant jobs based on a user's unique CV.
* **MLOps & CI/CD Automation:** To prevent the AI from functioning on outdated data, I implemented a GitHub Actions workflow. Every week, a cloud runner automatically fetches the latest database records, retrains the XGBoost model, and commits the updated `.pkl` weights back to the repository—ensuring the app learns continuously without manual intervention.
* **Handling Deep Pagination Limits:** Solved server-side database crashes (e.g., Elasticsearch 10,000 record cap) by implementing smart offset conditions and automated error-handling stop triggers.