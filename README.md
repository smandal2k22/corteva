# corteva assessment

Instructions to run the applications

Install Python3.9+

Create virtual env
  python -m venv <path-to-your-folder>

Activate the virtual env
  source <path-to-your-folder>/scripts/bin/activate
 
 
 Clone this repo
 Install all the required Python libraries
  pip install -r requirementstxt
  
Open new terminal and activate virtual env as well to run some flask commands that ll initiate, ingest and summarize the data in DB
  
Run flask app using command
  flask --app corteva_app --debug run
 
Init DB using below command
  flask -app corteva_app init-db
Ingest DATA using below command
  flask -app corteva_app init-data
Summarize the data using below command
  flask -app corteva_app summarize-data
  

Hit API From Browser
  http://127.0.0.1:5000/api/weather?page_num=5&page_size=100&station=USC00110072
  http://127.0.0.1:5000/api/weather/stats?page_num=5&page_size=100&station=USC00110072&date=2000
  
 
