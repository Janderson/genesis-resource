from __future__ import absolute_import, unicode_literals
import os
from celery import Celery, chain, signature, group, shared_task
from datetime import datetime

app = Celery(
    'celery_test', 
    broker='redis://localhost:6379/0'
)

class Config:
    enable_utc = True
    timezone = 'Europe/London'


@app.task(ignore_result=True)
def calculate(tag_task, exchange, ticker):
    print("calculate", "| ---->", exchange, ticker)

@app.task(ignore_result=False)
def update_historical_data(tag_task, exchange, ticker):
    print("update_historical_data", "| -->", exchange, ticker)


@shared_task()
def finish_job(date_started):
    print("### DONE ###")

@app.task(bind=True)
def run_process(self):
    date_started = datetime.now()
    symbols = [("A", "NYSE"), ("B", "NYSE"), ("BB", "NYSE"), ("MSFT", "NASDAQ"), ("AAPL", "NASDAQ")]
    jobs_list = []
    for ticker, exchange in symbols:
        print("! adding -->", ticker)
        tag_task = ""
        ticker_task = update_historical_data.s(tag_task, exchange, ticker)
        ticker_task.link(
            calculate.s(exchange, ticker)
        )
        jobs_list.append(ticker_task)
    
    job = group(jobs_list)
    job.link(finish_job.s(date_started))

    result = job.apply_async()




@app.task 
def hello():
    return 'hello world'



