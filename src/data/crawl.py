import aiohttp
import asyncio
import asyncpg
import async_timeout
from newspaper import article
from datetime import datetime
from typing import NamedTuple, Type
from google.cloud.bigquery import Client as BQClient
import concurrent.futures
import click


class Post(NamedTuple):
    id: str
    created: Type[datetime]
    subreddit: str
    domain: str
    url: str
    num_comments: int
    score: int
    ups: int
    downs: int
    title: str
    permalink: str
    gilded: int


# =======================
# Get results from bigquery
# =======================


def get_bigquery_table():
    # Create a client, and get the table that we are interested in:
    bigquery_client = BQClient(project="pocketreplacement")
    dataset_name = 'RedditData'
    dataset = bigquery_client.dataset(dataset_name)
    table = dataset.table("subset")
    table.reload()
    return table


def pre_processing(created, subreddit, domain, url, num_comments, score, ups,
                   downs, title, permalink, gilded):
    created = datetime.utcfromtimestamp(created)
    id = permalink.split("/")[4]
    return Post(id, created, subreddit, domain, url, num_comments, score, ups,
                downs, title, permalink, gilded)


def post_iterator(number=None):
    table = get_bigquery_table()
    data_iter = table.fetch_data(max_results=number)

    for item in data_iter:
        yield pre_processing(*item)


# ======================
# fetch a page
# ======================
async def fetch(session, url: str):
    with async_timeout.timeout(10):
        async with session.get(url) as response:
            return await response.text()


# =====================
# get and parse the html
# =====================
async def parse_html(session, url: str):
    html = await fetch(session, url)
    ar = newspaper.Article(url, keep_article_html=True)
    ar.download(input_html=html)
    ar.parse()
    return (html, ar.article_html)


# ===================
# write a post to the connection
# ===================
async def write_post_to_db(connection: Type[asyncpg.Connection],
                           id: str,
                           created: Type[datetime],
                           subreddit: str,
                           domain: str,
                           url: str,
                           num_comments: int,
                           score: int,
                           ups: int,
                           downs: int,
                           title: str,
                           permalink: str,
                           gilded: int,
                           html: bytes,
                           article: str):
    # insert into 'url', unless it exists.
    # We try to add the URL first because this is likely to be new and unique,
    # and the exception path should be nominally short:
    which_not_null_regex = re.compile('^null value in column "([^"]+)"')
    async with connection.transaction():
        try:
            await connection.execute('''
                INSERT INTO "urls" (name, domain, html, article) VALUES \
                    ($1, (SELECT domain FROM domains WHERE name = $2), $3, $4)
            ''', url, domain, html, article)
        except asyncpg.UniqueViolationError as unique:
            # we already have this url, so we pass
            pass

        except asyncpg.NotNullViolationError as not_null:
            m = which_not_null_regex.match(str(not_null))
            if m.group(1) == "domain":
                await connection.execute(
                    '''INSERT INTO "domains" (name) VALUES ($1)''', domain)
                await connection.execute('''
                    INSERT INTO "urls" (name, domain, html, article) VALUES \
                        ($1, (SELECT domain FROM domains WHERE name = $2), $3, $4)
                ''', url, domain, html, article)
            else:
                raise not_null

        try:
            await connection.execute('''
                INSERT INTO "posts" (id, created, subreddit, url, num_comments, score, ups, downs, title, permalink, gilded) VALUES \
                    ($1, $2, (SELECT subreddit FROM subreddits WHERE name=$3), (SELECT url FROM urls WHERE name=$4), $5, $6, $7, $8, $9, $10, $11)
                ''', id, created, subreddit, url, num_comments, score, ups,
                                     downs, title, permalink, gilded)
        except asyncpg.NotNullViolationError as not_null:
            m = which_not_null_regex.match(str(not_null))
            if m.group(1) == "subreddit":
                await connection.execute(
                    '''INSERT INTO "subreddits" (name) VALUES ($1)''',
                    subreddit)
                await connection.execute('''
                INSERT INTO "posts" (id, created, subreddit, url, num_comments, score, ups, downs, title, permalink, gilded) VALUES \
                    ($1, $2, (SELECT subreddit FROM subreddits WHERE name=$3), (SELECT url FROM urls WHERE name=$4), $5, $6, $7, $8, $9, $10, $11)
                ''', id, created, subreddit, url, num_comments, score, ups,
                                         downs, title, permalink, gilded)
            else:
                raise not_null


# ===================
# Make sure the post isn't already in the db
# ===================
async def is_post_in_db(connection: Type[asyncpg.Connection],
                        id: str,
                        subreddit: str):
    # get subreddit:
    subreddit_id = await connection.fetchrow(
        "SELECT subreddit FROM subreddits WHERE name=$1", subreddit)
    results = await connection.fetch(
        """SELECT (url) FROM posts WHERE id = $1 AND subreddit = $2""", id,
        subreddit_id["subreddit"])
    if results:
        return results
    else:
        return False


# =================
# The process worker.
# =================
async def process_worker(posts, pg_username, pg_password, pg_database, pg_host):
    # Make a connection for the database:
    pg_connection = await asyncpg.connect(
        user=pg_username,
        password=pg_password,
        database=pg_database,
        host=pg_host)

    # Make a http client session:
    async with aiohttp.ClientSession() as http_session:
        for post in posts:
            if not await is_post_in_db(pg_connection, post.id, post.subreddit):
                html, article = await parse_html(http_session, post)
                if article != "":
                    await write_post_to_db(pg_connection, *post, html, article)

@click.command()
@click.option("--number", default=1000, help="number of posts to crawl")
@click.option("--chunksize", default=100, help="number of posts to crawl per process initially")
@click.option("--processes", default=5, help="number of processes to use")
def run(number, chunksize, processes):
    # git repository directory
    from pathlib import Path
    git_dir = Path("/home/spott/code/pocket_replacement/")

    # get 'environmental' variables:
    import os, re

    #check for 'export' lines.  This is *not* robust against lots of things, but it should work for this
    export_re = re.compile(r"""^export ([^=]+)=(['\"])([^\2]*)\2;?$""")

    with open(git_dir / ".env", 'r') as env_file:
        for line in env_file:
            m = export_re.match(line)
            if m:
                os.environ[m.group(1)] = m.group(3)

    posts = post_iterator(number)

    worker = lambda p: process_worker(p,
                                        "postgres",
                                        os.environ["POSTGRES_PASSWORD"],
                                        os.environ["POSTGRES_DB"],
                                        "127.0.0.1")

    with concurrent.futures.ProcessPoolExecutor(max_workers=processes) as executor:
        executor.map(worker, posts, chunksize=chunksize)
