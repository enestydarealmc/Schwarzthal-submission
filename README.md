# Schwarzthal Data Enginner Assignment

*Truong Nguyen*
*Innopolis University*

## Set up a virtual enviroment to run scrapy
- Create a virtual environment
- Activate your enviroment: cd to scripts folder and run "activate" file
- cd to the directory where requirements.txt is located
- run:
> pip install -r requirements.txt

## Create a scrapy project (make sure scrapy is installed)
- > scrapy startproject your_project_name
- Copy schwarzthal_spider.py into the "spiders" folder of your scrapy project

## Run the crawler
- Cd into the "spiders" folder and run the crawler
- > srapy crawl schwarzthal

## Results
- will be saved into "results.json" file
- a sample result is included into the repo
