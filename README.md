# Netflix Content Recommendation System
#### *"Comprehensive analytics pipeline integrating data ingestion, machine learning, and LLM-powered personalization to improve user recommendations."*

![](https://github.com/SnehaEkka/BA882-Netflix-Analytics-Pipeline/blob/main/Images/netflix-banner.jpg)

## About
This project automates Netflix content and user data collection, builds recommendation models, and integrates large language models for personalized viewer engagement—aiming to reduce churn and enhance user experience through data-driven insights.

The project involves three phases:
- Phase 1: Data Ops (ETL/ELT and Warehousing)
- Phase 2: ML Ops
- Phase 3: LLM Ops

### Data Sources
- **Netflix Top 10 Weekly Data** – Provides insights into trending content. \
   *Link: https://www.netflix.com/tudum/top10/*
- **Netflix API Data** – Contains metadata about Netflix content. \
   *Link: https://rapidapi.com/movie-of-the-night-movie-of-the-night-default/api/streaming-availability*
- **YouTube API Data** – Captures engagement metrics from trailers and promotional videos. \
   *Link: https://console.cloud.google.com/marketplace/product/google/youtube.googleapis.com?project=ba882-inclass-project*

## Purpose & Business Context
With the rise of streaming platforms, content recommendation has become crucial for improving user experience. This project focuses on enhancing Netflix's recommendation system by integrating Machine Learning (ML) techniques with Large Language Models (LLMs) to provide more personalized and data-driven recommendations.

Netflix offers a vast content catalog, making it difficult for users to discover new shows and movies. Traditional recommendation systems rely on watch history, but our approach extends beyond that by leveraging content metadata and user feedback. We aim to develop a hybrid recommendation engine that improves content discoverability and engagement.

## Solution Overview

### Phase 1: Data Collection & Initial ETL
#### Accomplishments
- **Data Extraction & Storage:**
  - Integrated Netflix, YouTube, and other datasets.
  - Stored data in **DuckDB via MotherDuck** for efficient querying.
- **Entity-Relationship Diagram (ERD) Creation:**
  - Designed a schema to map relationships between datasets.
    
    ![ERD Diagram](https://github.com/SnehaEkka/BA882-Netflix-Analytics-Pipeline/blob/main/Images/Data_Model_ERD.png)
    
- **ETL Pipeline Development:**
  - Built an **EtLT** (Extract, Transform, Load, then Transform) pipeline.
  - Converted date formats and extracted relevant features from unstructured data.
- **Visualization with Superset:**
  - Created dashboards for analyzing viewership trends.
    
    ![Superset Dashboard](https://github.com/SnehaEkka/BA882-Netflix-Analytics-Pipeline/blob/main/Images/Superset_Dashboard.jpeg)

#### Challenges & Solutions
- **Data Inconsistencies:** Some datasets lacked unique identifiers, requiring fuzzy matching techniques.
- **Storage Optimization:** Transitioned from CSV-based storage to **MotherDuck** for better scalability.

🔗 [Phase 1 Presentation Deck](https://www.canva.com/design/DAGUIVPifwA/XS8G7VVNnCyUBYOOcfWJyA/view?utm_content=DAGUIVPifwA&utm_campaign=designshare&utm_medium=link2&utm_source=uniquelinks&utlId=hbed1db69e4)

### Phase 2: Enhancing ETL & Building Machine Learning Models
#### Accomplishments
- **ETL Optimization:**
  - Transitioned from **EtLT to ETL**, ensuring transformations occurred before loading.
  - Standardized title formatting to align across datasets.
- **Data Upsertion Strategy:**
  - Introduced **Insert Logic** to prevent missing historical records.
- **Machine Learning Model Development:**
  - Built a **KNN-based recommendation system** using cosine similarity.
  - Engineered features such as **cast, director, genre, and descriptions** for content similarity.
- **Prefect Pipeline Deployment:**
  - Automated daily ETL and model training flows using **Prefect Cloud**.
    
    ![ETL Prefect Flow](https://github.com/SnehaEkka/BA882-Netflix-Analytics-Pipeline/blob/main/Images/ETL%20(2).png)
    
- **Dataset Expansion:**
  - Backfilled data for 2020-2021 to enhance model accuracy.

#### Challenges & Solutions
- **Prefect Deployment Errors:** Fixed entrypoint issues by restructuring folders.
- **Feature Bias:** Removed runtime as a feature to prevent duration-based recommendations.

🔗 [Phase 2 Presentation Deck](https://www.canva.com/design/DAGWRog81pI/qvuEtNvVOC9sRWiyDqdRhQ/view?utm_content=DAGWRog81pI&utm_campaign=designshare&utm_medium=link2&utm_source=uniquelinks&utlId=hb78e2e7fa7)

### Phase 3: LLM Integration & UI Development
#### Accomplishments
- **LLM Integration for Personalized Recommendations:**
  - Incorporated **Gemini 1.5 Pro** to refine recommendations based on user feedback.
  - Users can enter preferences to enhance recommendations dynamically.
- **Automated ML Pipeline:**
  - Integrated model training and storage into **MotherDuck**.
  - Archived model metrics for tracking improvements.
    
    ![Updated EtLT Prefect Flow](https://github.com/SnehaEkka/BA882-Netflix-Analytics-Pipeline/blob/main/Images/EtLt%20(3).png)
    
    ![ML Prefect Flow](https://github.com/SnehaEkka/BA882-Netflix-Analytics-Pipeline/blob/main/Images/ML%20Flow%20(3).png)
    
- **Streamlit Application Development:**
  - Built an interactive UI for users to receive recommendations and provide feedback to enhance the recommendations further.
  - Added interactive visualizations to showcase content trends.
    
    ![Netflix Analytics](https://github.com/SnehaEkka/BA882-Netflix-Analytics-Pipeline/blob/main/Images/Streamlit%20Analytics%20(3).png)
    
    ![Recommendations](https://github.com/SnehaEkka/BA882-Netflix-Analytics-Pipeline/blob/main/Images/Streamlit%20KNN%20(3).png)
    
    ![User Feedback Integration](https://github.com/SnehaEkka/BA882-Netflix-Analytics-Pipeline/blob/main/Images/Streamlit%20LLM%20(3).png)

#### Challenges & Solutions
- **Model Overfitting:** High accuracy suggested potential overfitting; addressed by balancing feature weights.
- **Scalability Concerns:** Used **Google Cloud Run & Prefect** to streamline ML deployment.

🔗 [Phase 3 Presentation Deck](https://www.canva.com/design/DAGY5lGoI4o/ltPp4VTPf0_AY5VUOC4_DQ/view?utm_content=DAGY5lGoI4o&utm_campaign=designshare&utm_medium=link2&utm_source=uniquelinks&utlId=hcc70496aea)

## Tools & Tech Stack  

| Tool/Platform | Purpose |
|--------------|---------|
| **Python** | Data Processing & ML Development |
| **DuckDB / MotherDuck** | Data Storage & Querying |
| **Google Cloud Storage** | Storing Data & Model Artifacts |
| **Google Cloud Run** | Deploying Recommendation Models |
| **Google Cloud Secret Manager** | Securing API Keys & Credentials |
| **YouTube API** | Extracting Trailer & Engagement Data |
| **Netflix API** | Fetching Movie & Show Metadata |
| **Prefect Cloud** | Automating ETL & ML Pipelines |
| **Streamlit** | Building User Interface |
| **Lucidchart** | ERD Diagram |
| **Apache Superset** | Data Visualization & Analytics |
| **Gemini 1.5 Pro LLM** | Generating Personalized Recommendations |

## Business Impact & Key Results  

- Improved recommendation precision by over X% compared to baseline static lists.  
- Reduced ETL pipeline runtime by Y% through Prefect optimization, enabling near-real-time insights.  
- Increased metadata enrichment coverage with YouTube trailer data integration.  
- Enabled dynamic, interactive end-user personalization with Gemini LLM, achieving fast response times (<3 seconds latency).  
- Delivered a seamless production pipeline that supports ongoing data refresh and model retraining.

## Future Improvements  
- Integrate real-time user interaction data to continuously refine recommendations.  
- Explore additional NLP models for enhanced contextual understanding.  
- Develop advanced A/B testing frameworks to measure user engagement impact.  
- Expand dataset ingestion to other streaming platforms for comparative analytics.

## Coursework  
- **Coursework:** Completed as part of **BA882 – Deploying Analytics Pipelines** course (Boston University MSBA program)
- **Contributors:**
  - Aryan Kumar - Streamlit front-end developer
  - Freya - Troubleshooting pipeline and LLM integration
  - Jenil Shah - Building the KNN-recommendation system
  - Sneha Ekka - Designing end-to-end pipeline across all phases

## Additional Resources  
- GitHub Repo: [Link]  
- Streamlit Demo: [Link]  
- ERD Diagrams and Superset Dashboard Screenshots included in /docs 

## 📌 Project Links
- **GitHub Repository:** [GitHub-Team05](https://github.com/SnehaEkka/BA882-Netflix-Analytics-Pipeline/tree/main)
- **Streamlit Application:** [Streamlit-Team05](https://streamlit-poc-614716406197.us-central1.run.app/)
