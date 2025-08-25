#!/bin/bash

PROJECT_URL="/mnt/d/Projects/Project-2-End-to-End-Retail-Analytics-Pipeline/"

source ${PROJECT_URL}/.venv/bin/activate
mv ${PROJECT_URL}.env ${PROJECT_URL}secrets/.env
echo "env file moved to secrets"
git add .
git commit -m "Daily Commit"
git push origin main

mv ${PROJECT_URL}secrets/.env ${PROJECT_URL}.env
echo "env file moved to main"

cd ${PROJECT_URL}; docker-compose up