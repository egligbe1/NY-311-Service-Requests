# NYC 311 Service Request Data Pipeline Project

## Overview

This project involves building a data pipeline to extract data from the NYC 311 Service Request API, store the raw data in an initial S3 bucket, transform and clean the data using AWS Lambda, store the cleaned data in another S3 bucket, and finally load the cleaned data into an Amazon Redshift database using Airflow running on an Amazon EC2 instance.

## Table of Contents

1. [Project Description](#project-description)
2. [Architecture](#architecture)
3. [Technologies Used](#technologies-used)
4. [Installation and Setup](#installation-and-setup)
5. [Data Pipeline Steps](#data-pipeline-steps)
6. [Usage](#usage)
7. [Contributing](#contributing)
8. [License](#license)

## Project Description

The data pipeline automates the process of extracting, transforming, and loading NYC 311 Service Request data. The main steps include:

- Extracting data from the NYC 311 Service Request API.
- Storing raw data in an S3 bucket.
- Using AWS Lambda to clean and transform the data.
- Storing cleaned data in another S3 bucket.
- Loading the cleaned data into an Amazon Redshift database using Airflow.

## Architecture

![Architecture Diagram](path/to/architecture-diagram.png)

The architecture consists of the following components:

1. **NYC 311 Service Request API**: Source of the data.
2. **Initial Raw S3 Bucket**: Storage for raw data.
3. **AWS Lambda**: Function to clean and transform data.
4. **Cleaned S3 Bucket**: Storage for cleaned data.
5. **Amazon Redshift**: Data warehouse for analytics.
6. **Airflow on EC2**: Orchestration tool for the data pipeline.

## Technologies Used

- **AWS S3**: For data storage.
- **AWS Lambda**: For data transformation.
- **Amazon Redshift**: For data warehousing.
- **Apache Airflow**: For workflow orchestration.
- **Amazon EC2**: For hosting Airflow.
- **Python**: For scripting and automation.
- **Git**: For version control.

## Installation and Setup

### Prerequisites

- AWS account
- Python 3.10+
- Git

### Steps

1. **Clone the Repository**

    ```bash
    git clone https://github.com/egligbe1/NY-311-Service-Requests.git
    cd data-pipeline-nyc-311
    ```

2. **Set Up Airflow on EC2**

-  Launch an EC2 instance with Ubuntu AMI. You can use this tutorial: https://tinyurl.com/33chtp8t. Also remember to create a key-pair private key that you use to connect your instance securely via VSCode for faster coding.
- SSH into the instance and install necessary packages and set up the EC2 environment. (https://tinyurl.com/2b3b2ekv)

    ```bash
    sudo apt update 
    sudo apt install python3-pip 
    sudo add-apt-repository ppa:deadsnakes/ppa
    sudo apt install python3.12-venv
    python3 -m venv 311-venv        //you can replace '311-venv' with you preferred environment name
    source 311-venv/bin/activate    
    pip install --upgrade awscli    
    pip install apache-airflow     
    airflow standalone              //launch airflow (copy the username and pasword given to login to airflow UI)
    pip install apache-airflow-providers-amazon 
    ```


- Start Airflow:

    Access the Airflow UI at `http://your-ec2-public-ip:8080`and login with the provided username and password given after running 'airflow standalone' command.
    You may need to allow your IP via the inbound rules in EC2.


3. **Create S3 Buckets**

    Create the raw, intermediate, and cleaned S3 buckets in the AWS S3 console.

4. **Set Up Airflow DAG**

    - Place your DAG in the `~/airflow/dags` directory.
    - The DAG handle the following tasks:
        - Extract data from the NYC 311 Service Request API and store it in the raw S3 bucket.
        - Trigger the Lambda function for transformation.
        - Load cleaned data from the cleaned S3 bucket into Amazon Redshift.

## Data Pipeline Steps

1. **Extraction**:
    - A Python callable script to fetch data from the NYC 311 Service Request API.
    - Store the fetched raw data in the initial S3 bucket using BashOperator.

2. **Transformation**:
    - Trigger AWS Lambda function upon new data in the S3 bucket.
    - Clean and transform the data using the Lambda function.
    - Store the cleaned data in another S3 bucket.

3. **Loading**:
    - Use Airflow to orchestrate the loading of cleaned data from the S3 bucket into Amazon Redshift.
    - Airflow DAG will include tasks for data extraction, transformation, and loading.

## Usage

1. **Trigger the Pipeline Manually**

    The pipeline can be triggered manually via the Airflow UI or based on a schedule(set to daily in the code).

2. **View Logs**

    Logs for each task can be viewed in the Airflow UI to monitor the pipeline's progress and debug issues.

If you have any questions or need further assistance, please feel free to contact emmanuelgligbe246@gmail.com.
