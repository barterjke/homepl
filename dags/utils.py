import glob
import logging
import os
import random
import string
import traceback
import uuid
from datetime import datetime
from math import ceil

import mysql.connector


class DBHandler(logging.Handler):
    def emit(self, record):
        #todo
        conn = mysql.connector.connect(
            host="localhost",
            user="your_username",
            password="your_password",
            database="your_database_name"
        )

        with conn.cursor() as cursor:
            exc = record.__dict__['exc_info']
            trace = traceback.format_exc() if exc else None
            cursor.execute("INSERT INTO system_logs (level, stacktrace, message, tt) VALUES (%s, %s, %s, %s)" % (
                record.__dict__['levelname'],
                trace,
                record.__dict__['msg'],
                datetime.now()
            ))


def generate_random_name(size: int) -> str:
    return ''.join([random.choice(string.ascii_letters) for _ in range(size)])


def safe_indexing(obj: dict | list, *args):
    element = obj
    for arg in args:
        if hasattr(element, '__getitem__'):
            if arg < len(element):
                element = element[arg]
        elif isinstance(element, dict) and arg in element:
            element = element[arg]
        else:
            return None
    return element


def split_files(file_path, mb_size_max=512, max_partitioning=0):
    # todo: !
    all_files = glob.glob(f"{file_path}/*.csv")
    for file_name in all_files:
        file_stat = os.stat(os.path.join(file_path, file_name))
        b_size_max = mb_size_max * (10 ** 20)
        if file_stat.st_size <= b_size_max:
            continue
        n = ceil(file_stat.st_size / b_size_max)
        partitions = []
        with open(file_path, "r") as f:
            partitions.append(f.read(b_size_max))
        for partition in partitions:
            with open(os.path.join(file_path), uuid.uuid4().hex, "w") as f:
                f.write(partition)
