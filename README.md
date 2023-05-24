# Dataplex AutoDQ Tutorial

This repository contains step-by-step guide-based tutorial for AutoDQ

## Clone the Github repo 
```
git clone https://github.com/mansim07/dataplex-tutorials.git
```
## Setup the environment variables

```
export PROJECT_ID=$(gcloud config get-value project)
export REGION=us-central1

echo ${PROJECT_ID}
echo ${REGION}
```

## Generate and Stage Synthetic data

```
bash generate_data.sh 2
```