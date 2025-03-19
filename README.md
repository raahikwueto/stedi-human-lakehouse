# Project : STEDI Human Balance Analytics Data lakehouse

## Introduction
Building a data lakehouse solution for STEDI team's sensor data to trains their machine learning model

## Project Details

The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:

- trains the user to do a STEDI balance exercise;
- and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
- has a companion mobile app that collects customer data and interacts with the device sensors.
STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.

Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. Privacy will be a primary consideration in deciding what data can be used.

Some of the early adopters have agreed to share their data for research purposes. Only these customers’ Step Trainer and accelerometer data should be used in the training data for the machine learning model.

## Tools
- Python and Spark
- AWS Glue
- AWS S3
- AWS Athena

## Implementation
The landing DDL folder contains SQL scripts to create the landing zone Glue tables from S3 data. Trusted and curated zone folders contain Glue ETL  jobs to transform the landing zone data through the requirements for trusted zone and curated zone.

### Schema design

#### Customer Records
Contains the following fields
- serialnumber
- sharewithpublicasofdate
- birthday
- registrationdate
- sharewithresearchasofdate
- customername
- email
- lastupdatedate
- phone
- sharewithfriendsasofdate

#### Accelerometer Records
Contains the following fields 
- user
- timeStamp
- x
- y
- z

#### Step Trainer Records 
containes the following fields 
- sensorReadingTime
- serialNumber
- distanceFromObject
