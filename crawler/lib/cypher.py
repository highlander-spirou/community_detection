from neo4j import GraphDatabase, ManagedTransaction
from contextlib import contextmanager
from os import environ


@contextmanager
def connect_neo4j():
    host = environ.get("NEO4J_HOST")
    if host is None:
        raise Exception("Missing environment variable NEO4J_HOST")
    uri = f"bolt://{host}:7687"
    access_usr = environ.get("NEO4J_USR", "neo4j")
    access_pwd = environ.get("NEO4J_PWD", "your_password")
    driver = GraphDatabase.driver(uri, auth=(access_usr, access_pwd))
    try:
        yield driver
    finally:
        driver.close()


@contextmanager
def get_neo4j_session():
    with connect_neo4j() as driver:
        session = driver.session()
        try:
            yield session
        finally:
            session.close()


class GraphConnector:
    def __init__(self) -> None:
        pass


    def insert_data(self, data):
        with get_neo4j_session() as session:
            session.execute_write(self._add_node_tx, data)

    @staticmethod
    def _add_node_tx(tx: ManagedTransaction, data):
        article_id_cursor = tx.run("MERGE (n:Article {title: $title}) "
                "RETURN id(n) AS node_id"
                , title = data['title']
        )

        author_ids_cursor = tx.run("WITH $data AS authors "
               "UNWIND $data AS authorRef "
               'MERGE (n:Author {name: COALESCE(authorRef.author, "N/A"), affiliate: COALESCE(authorRef.affiliate, "N/A")}) '
               "RETURN collect(distinct id(n)) AS authorIDs"
               , data=data["authors"])
        
        author_ids_cursor = tx.run("UNWIND $data AS authorRef "
               'MERGE (n:Author {name: authorRef.author, affiliate: COALESCE(authorRef.affiliate, "N/A")}) '
               "RETURN collect(distinct id(n)) AS authorIDs"
               , data=data["authors"])
        

    
        article_id = article_id_cursor.single()[0]
        author_ids = author_ids_cursor.single()[0]

        tx.run("MATCH (article:Article) WHERE ID(article) = $article_id "
               "WITH article "
               "MATCH (author:Author) WHERE ID(author) IN $author_ids "
               "MERGE (author)-[:Author_of]->(article) "
               , article_id=article_id, author_ids=author_ids)
        
        if data['keywords'] is not None:
            kw_ids_cursor = tx.run("UNWIND $data AS kwRef "
               'MERGE (n:Keyword {kw: kwRef}) '
               "RETURN collect(distinct id(n)) AS kwIDs"
               , data=data["keywords"])
            
            kw_ids = kw_ids_cursor.single()[0]
            tx.run("MATCH (article:Article) WHERE ID(article) = $article_id "
               "WITH article "
               "MATCH (kw:Keyword) WHERE ID(kw) IN $kw_ids "
               "MERGE (kw)-[:Keyword_of]->(article) "
               , article_id=article_id, kw_ids=kw_ids)
            
        print(f'Add article with ID: {article_id}')