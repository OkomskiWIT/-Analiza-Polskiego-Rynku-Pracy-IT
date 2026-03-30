# 📊 Polish IT Job Market: Data Engineering & Analytics Pipeline

🚀 **[Live Dashboard - Click Here to Explore the Data][LINK_DO_STREAMLIT]**

## 📖 Overview
As an aspiring IT/Data Engineer, I built this end-to-end ETL (Extract, Transform, Load) pipeline to capture, clean, and visualize the dynamic Polish IT job market. 

Understanding that **clean data is the essential fuel for any analytics or future AI applications**, this project focuses on robust data infrastructure. It reverse-engineers APIs from major job boards (Just Join IT, No Fluff Jobs), normalizes chaotic JSON structures, and serves real-time insights through a cloud-hosted dashboard.

## 🛠️ Tech Stack & Architecture
This project bridges traditional software engineering with modern data practices:
* **Extraction (Python / Requests):** Bypassing undocumented APIs, handling pagination, and simulating browser headers to harvest raw data.
* **Raw Data Lake (MinIO / S3 / Boto3):** Storing immutable, raw JSON payloads for future auditing or alternative transformations.
* **Transformation (Pandas):** The core engine. Cleans missing values, flattens nested data structures, and standardizes diverse API schemas into a single source of truth.
* **Data Warehouse (PostgreSQL / Neon Serverless):** A relational database storing the "Gold" layer of ready-to-use data via `SQLAlchemy`.
* **Serving (Streamlit):** An interactive, analytical frontend deployed in the cloud.

## 🧠 Engineering & Data Challenges Solved
* **Defeating "City Spamming" (Deduplication):** Job boards often duplicate a single remote offer across 10+ cities. I engineered a Pandas `.groupby()` aggregation logic that merges these clones into a single row, compiling the locations into an array without losing data integrity.
* **Handling Deep Pagination Limits:** Solved server-side database crashes (e.g., Elasticsearch 10,000 record cap) by implementing smart offset conditions and automated error-handling stop triggers.
* **Dynamic Data Normalization:** Standardized deeply nested and constantly changing JSON structures (e.g., extracting base currencies from dynamic conversions, isolating real cities from complex arrays).
* **AI-Ready Structuring:** By converting messy API text into clean, categorical, and numerical features (Min/Max Salary, Remote Flags, Categories), the PostgreSQL database is now perfectly structured to feed future Machine Learning models (e.g., salary prediction algorithms).

## 💻 Local Setup
Want to run the pipeline locally?
1. Clone this repository.
2. Install dependencies: `pip install -r requirements.txt`
3. Set up your PostgreSQL URL in `.streamlit/secrets.toml` as `DB_URL = "your_database_url"`.
4. Run the visualization app: `streamlit run app.py`
