# STEDI Human Balance Analytics Udacity Project

This repository contains the code and documentation for the STEDI Human Balance Analytics project, which is part of the Udacity Data Science Nanodegree program.

## Project Overview

The STEDI Human Balance Analytics project focuses on analyzing human balance data using STEDI (Spatiotemporal Dynamic Imaging) technology. The goal is to develop insights and predictive models to understand and improve human balance performance. As a data engineer on the STEDI Step Trainer team, the goal is to extract the data produced by the STEDI Step Trainer sensors and the mobile app, and curate them into a data lakehouse solution on AWS so that Data Scientists can train the learning model.

## Project Structure

The project is structured as follows:

### Data:

STEDI has three JSON data sources to use from the Step Trainer. JSON data in the following format is extracted from [STEDI Human Balance Analytics Data](https://video.udacity-data.com/topher/2022/June/62be2ed5_stedihumanbalanceanalyticsdata/stedihumanbalanceanalyticsdata.zip) as following folders:

- customer
- step_trainer
- accelerometer

These data are populated in landing zones in the same user created S3 bucket.
The VPC and IAM role settings are appropriately established to make connection and queries to the bucket.

### Tasks: Table creations

- Using AWS Glue and AWS Athena, customer landing, and accelerator landing tables are created to make SQL queries.
- Using Visual Glue Studio jobs, customer trusted and accelerator trusted data are created for the trusted zone. These data adopt privacy filters.
- These data adopt privacy filters. The data is inquired using SQL via customer trusted and accelerator trusted tables created on Athena.
- Using Visual Glue Studio, customer trusted data is converted into customer curated data for the curated zone in the customer.
- Using customer curated data, step trainer trusted data is created through Glue Studio. This data is populated in the trusted zone of the step trainer.
- By combining accelerator trusted and step trainer trusted data, a final dataset is generated for step trainer curated zone.
- This data is used for machine learning analysis.
