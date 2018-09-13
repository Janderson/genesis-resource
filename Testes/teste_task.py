import os,sys
import click
import traceback


from celery import Celery, chain, signature, group
import celery


app = Celery()


@app.task(ignore_result=True)
def calculate_test(tag_task, exchange, ticker):


    """if not status_of_update:
        es_status.task_status(datetime.utcnow(), tag_task, "{}_{}".format(exchange, ticker), calculate_status="nothing")
        return False
    """
    with open("a{}.txt".format(datetime.now())) as fileopen:
        fileopen.write("")


@app.task(ignore_result=False)
def update_historical_data_test(tag_task, exchange, ticker):
    es_status = ESControlStatus()
    es_status.set_update_es(store_status_task())

    es_status.task_status(datetime.utcnow(), tag_task, "{}_{}".format(exchange, ticker), data_status="started")
    iqconfig = iqfeed_config()
    CollectionEODDataPersistence.save(
            collection_eoddata=collection_eod, 
            ticker = ticker, 
            exchange = exchange
    )

    es_status.task_status(
        datetime.utcnow(), 
        tag_task, 
        "{}_{}".format(exchange, ticker), 
        data_status="done"
    )
    return tag_task







        #open_connection(add_time, "A{}".format(keyA), symbols)

@app.task()
def finish_job_test(date_started):
    es_status = ESControlStatus()
    es_status.calculate_status(
        date_started, "done", elapsed_time=get_elapsed_time(date_started)
    )

@app.task(bind=True)
def update_all_symbols_test(param1):
    pass

if __name__=="__main__":
    app.run()