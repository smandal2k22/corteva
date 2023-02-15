import re
import sqlite3
import traceback
from . import db
from flask import current_app


def get_data(date, station, page_num, page_size, table):
    try:
        page_offset = 200
        response_data = {}
        page_num, page_size = int(page_num), int(page_size)
        # Initialize the response data
        resp = {"current_page":page_num, "max_pages":0, "max_results": 0, "data":response_data}
        if page_num and page_size:
            page_offset = (page_num - 1) * page_size
        db_conn = db.get_db()
        if not db_conn:
            return resp
        calculated_data_query = get_pg_query(table, date, station, page_offset, page_size)
        current_app.logger.info(calculated_data_query)
        if calculated_data_query:
            cursor = db_conn.cursor()
            cursor.execute(calculated_data_query)
            result = cursor.fetchall()

            if result is not None:
                # If there is data from DB
                max_pages, max_results = get_max_pages(calculated_data_query, page_num, page_size)
                response_data = format_result(result)
                resp = {"current_page":page_num, "max_pages":max_pages, "max_results": max_results, "data":response_data}
        current_app.logger.info(resp)
        return resp
    except Exception as e:
        current_app.logger.error(traceback.format_exc())

def get_pg_query(table, date, station, page_offset, page_size):
    try:
        base_query = f"SELECT * FROM {table} "
        filter = ""
        if date and station:
            filter += f" WHERE DATE LIKE '{str(date)}%' AND STATION = '{station}'"
        elif date:
            filter += f" WHERE DATE LIKE '{str(date)}%'"
        elif station:
            filter += f" WHERE STATION = '{station}'"
        # Add Filter
        data_query = base_query + filter
        # Add offset
        data_query += f" ORDER BY DATE DESC LIMIT {page_size} OFFSET {page_offset};"

        return data_query
    except Exception as e:
        current_app.logger.error(traceback.format_exc())
        return ""   

def get_max_pages(data_query, page_num, page_size):
    try:
        max_pages, max_results = 1,0
        db_conn = db.get_db()
        if not db_conn:
            return max_pages, max_results
        cursor = db_conn.cursor()
        updated_data_query = re.sub('LIMIT.*?;', ';', data_query)
        cursor.execute(updated_data_query)
        result = cursor.fetchall()
        if result is not None:
            max_results = len(result)
            max_pages = int((max_results // page_size) + 1) if ((max_results % page_size)!= 0) else (max_results // page_size)
        return max_pages, max_results
    except Exception as e:
        current_app.logger.error(traceback.format_exc())
        max_pages, max_results = 1,0
        return max_pages, max_results

def format_result(result):
    try:
        response_data = {}
        for row in result:
            station = row[0]
            date = row[1]
            avg_max_temp = row[2]
            avg_min_temp = row[3]
            avg_tot_prec = row[4]
            if not station in response_data:
                response_data[station] = []
            response_data[station].append({"station":station, "date":date, "avg_max_temp":avg_max_temp, "avg_min_temp":avg_min_temp, "avg_tot_prec":avg_tot_prec})
        return response_data
    except Exception as e:
        current_app.logger.error(traceback.format_exc())
        return {}
