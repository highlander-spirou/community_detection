from mysql.connector import connect
from bs4 import BeautifulSoup
from crawler.lib.crawl import get_text_or_none, get_author, get_keyword
from crawler.lib import GraphConnector
from os import environ

MYSQL_USER=environ.get("MYSQL_USER")
MYSQL_PWD=environ.get("MYSQL_PWD")
MYSQL_DB=environ.get("MYSQL_DB")
MYSQL_HOST=3306


with connect(host="localhost",user=MYSQL_USER,password=MYSQL_PWD, database=MYSQL_DB, port=MYSQL_HOST) as conn:
    
    cursor = conn.cursor()
    cursor.execute("select * from cache_pmids")
    result = cursor.fetchall()


    for i in result:
        src = BeautifulSoup(i[2], features="xml")
        title = get_text_or_none(src.select_one("ArticleTitle"))
        authors = get_author(src)
        keywords = get_keyword(src)

        GraphConnector().insert_data({'title': title, 'authors': authors, 'keywords': keywords})