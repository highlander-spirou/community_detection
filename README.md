# Community detection on NCBI's articles with keyword *Biomedical data science*

## Pre-requisted

Download the Graph Data Science (GDS) plugins from neo4j website https://neo4j.com/graph-data-science-software/, and add the `.jar` file to the volumes/neo4j/plugins folder


## Run the Docker Services

The `docker-compose.yml` contains
- Three databases:
  - Neo4J (data warehouse)
  - MySQL (data lake)
  - Redis (intermediate database for other components)

- Two crawl services:
  - Daily crawl
  - Batch crawl for initial data (from year 1995 to now)

The data science aspect (community detection) can be ran directly on the Neo4J's UI located at http://localhost:7474

## Run the Community Detection Algorithm

### 1. Run the `main` script to load data from data lake to Graph data warehouse (if not ran)

```python
export MYSQL_USER=...
export MYSQL_PWD=...
export MYSQL_DB=...
export MYSQL_HOST=...

python dev/main.py
```

### 2. Access the Neo4J UI by access http://localhost:7474 on the browser

### 3. Create an in-memory graph projection

```
CALL gds.graph.project(
  'myGraph',
  ['Article', 'Author', 'Keyword'],
  "*"
)
```

### 4. Run Louvain algorithm to perform Community Detection algorithm

```
CALL gds.louvain.write(
  'myGraph',
  {
    writeProperty: 'community',
    relationshipWeightProperty: null
  }
);
```

5. To query which community the article belongs to

```
MATCH (a:Article)
RETURN a.title, a.community
```

# Results

I intentionally stream 2955 article to data warehouse, since 50.000 data points would fry my computer 🔥🔥🔥 (I only allocated 2GB of RAM to my WSL2 machine)

```
2955 Articles
12932 Authors
162 Keywords
```
### => 2791 communities detected