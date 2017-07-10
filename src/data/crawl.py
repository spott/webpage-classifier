import aiohttp
import asyncio
import asyncpg
import async_timeout
import logging
import sys
import re
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
    log = logging.getLogger('get_bigquery_table')

    log.info("get client")
    # Create a client, and get the table that we are interested in:
    bigquery_client = BQClient(project="pocketreplacement")
    log.info("get dataset")
    dataset_name = 'RedditData'
    dataset = bigquery_client.dataset(dataset_name)
    log.info("get table")
    table = dataset.table("subset")
    table.reload()
    return table


def pre_processing(created, subreddit, domain, url, num_comments, score, ups,
                   downs, title, permalink, gilded):
    log = logging.getLogger('pre_processing')
    created = datetime.utcfromtimestamp(created)
    id = permalink.split("/")[4]
    return Post(id, created, subreddit, domain, url, num_comments, score, ups,
                downs, title, permalink, gilded)


def post_iterator(number=None):
    log = logging.getLogger('post_iterator')
    table = get_bigquery_table()
    data_iter = table.fetch_data(max_results=number)

    for item in data_iter:
        yield pre_processing(*item)


# ======================
# fetch a page
# ======================
async def fetch(session, url: str, timeout: int=10):
    log = logging.getLogger('fetch')
    with async_timeout.timeout(timeout):
        async with session.get(url) as response:
            if response.status < 400:
                return await response.text()
            else:
                raise aiohttp.ClientResponseError(response.request_info, response.history)


# =====================
# get and parse the html
# =====================
async def parse_html(session, url: str):
    log = logging.getLogger('parse_html')
    log.info("url: (type: {}): {}".format(type(url), url))
    try:
        html = await fetch(session, url)
    except UnicodeDecodeError as e:
        log.info("url: {}... returned garbage utf-8 string: e".format(url[:40], e))
        return ("", "")

    except asyncio.TimeoutError as t:
        log.info("url: {}... timed out: {}".format(url[:40], t))
        return ("", "")

    except aiohttp.ClientResponseError as e:
        log.info("url: {}... probably doesn't exist: {}".format(url[:40], e))
        return ("", "")

    except aiohttp.client_exceptions.ServerDisconnectedError as e:
        log.info("url: {}... probably doesn't exist: {}".format(url[:40], e))
        return ("", "")

    except aiohttp.client_exceptions.ClientConnectorError as e:
        log.info("url: {}... {}".format(url[:40], e))
        return ("", "")


    log.info("html: (type: {}): {}...".format(type(html), html[:20]))
    try:
        ar = article.Article(url, keep_article_html=True)
        ar.download(input_html=html)
        ar.parse()
    except article.ArticleException as e:
        log.info("url: {}... failed to be understood.. {}".format(url[:40],e))
        return ("", "")
    return (bytes(html, "utf8"), ar.article_html)


# ===================
# write a post to the connection
# ===================
async def write_post_to_db(pool: Type[asyncpg.pool.Pool],
                           id_: str,
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
    log = logging.getLogger('write_post_to_db')
    log.info("writing: [id: {}, url: {}, subreddit: {}, article: {}...]".
             format(id_, url, subreddit, article[:40]))
    # insert into 'url', unless it exists.
    # We try to add the URL first because this is likely to be new and unique,
    # and the exception path should be nominally short:
    which_not_null_regex = re.compile('^null value in column "([^"]+)"')
    async with pool.acquire() as connection:
        try:
            log.info("inserting {} into urls table...".
                    format(id_))
            await connection.execute('''
                INSERT INTO "urls" (name, domain, html, article) VALUES \
                    ($1, (SELECT domain FROM domains WHERE name = $2), $3, $4)
            ''', url, domain, html, article)
            log.info("done")
        except asyncpg.UniqueViolationError as unique:
            log.info("url: {} not unique".format(url))
            # we already have this url, so we pass
            pass

        except asyncpg.NotNullViolationError as not_null:
            m = which_not_null_regex.match(str(not_null))
            if m.group(1) == "domain":
                log.info("domain: {} doesn't exist".format(domain))
                try:
                    await connection.execute(
                        '''INSERT INTO "domains" (name) VALUES ($1)''', domain)
                    log.info("domain: {} added to table, inserting {} into urls table now:".format(domain, url[:20]))
                except asyncpg.UniqueViolationError as unique:
                    log.info("domain {} got added between last time we checked and now... so we will just ignore this".format(domain))
                    pass
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
                ''', id_, created, subreddit, url, num_comments, score,
                                    ups, downs, title, permalink, gilded)
        except asyncpg.NotNullViolationError as not_null:
            m = which_not_null_regex.match(str(not_null))
            if m.group(1) == "subreddit":
                log.info("subreddit {} doesn't exist".format(subreddit))
                try:
                    await connection.execute(
                        '''INSERT INTO "subreddits" (name) VALUES ($1)''',
                        subreddit)
                except asyncpg.UniqueViolationError as unique:
                    log.info("subreddit {} got added between last time we checked and now... so we will just ignore this".format(subreddit))
                    pass
                await connection.execute('''
                INSERT INTO "posts" (id, created, subreddit, url, num_comments, score, ups, downs, title, permalink, gilded) VALUES \
                    ($1, $2, (SELECT subreddit FROM subreddits WHERE name=$3), (SELECT url FROM urls WHERE name=$4), $5, $6, $7, $8, $9, $10, $11)
                ''', id_, created, subreddit, url, num_comments, score,
                                        ups, downs, title, permalink,
                                        gilded)
            else:
                raise not_null
    log.info("finished writing id: {}".format(id_))


# ===================
# Make sure the post isn't already in the db
# ===================
async def is_post_in_db(pool: Type[asyncpg.pool.Pool],
                        id_: str,
                        subreddit: str):
    log = logging.getLogger('is_post_in_db')
    log.info("checking [id: {}, subreddit: {}]".format(id_, subreddit))
    # get subreddit:
    #async with pool.acquire() as connection:
    subreddit_id = await pool.fetchrow(
        "SELECT subreddit FROM subreddits WHERE name=$1", subreddit)
    if not subreddit_id:
        log.info("subreddit {} doesn't exist".format(subreddit))
        return False
    results = await pool.fetch(
        """SELECT (url) FROM posts WHERE id = $1 AND subreddit = $2""", id_,
        subreddit_id["subreddit"])
    if results:
        log.info("id: {} in subreddit {} exists, skipping".format(id_, subreddit))
        return results
    else:
        log.info("id: {} in subreddit {} doesn't exist".format(id_, subreddit))
        return False


# =================
# The process worker.
# =================
# async def process_worker(posts, pg_username, pg_password, pg_database, pg_host):
#     log = logging.getLogger('process_worker')

#     log.info("creating connection to the database...")
#     # Make a connection for the database:
#     pg_connection = await asyncpg.connect(
#         user=pg_username,
#         password=pg_password,
#         database=pg_database,
#         host=pg_host)
#     log.info("success!")

#     # Make a http client session:
#     async with aiohttp.ClientSession() as http_session:
#         for post in posts:
#             log.info(f"processing post: {post}")
#             await process_post(post, pg_connection, http_session)


def process_posts(posts,
                  pg_username,
                  pg_password,
                  pg_database,
                  pg_host,
                  batch_size=10):
    from itertools import islice, count, chain
    log = logging.getLogger('process_worker')

    log.info("creating connection to the database...")
    # Make a connection for the database:
    loop = asyncio.get_event_loop()
    pg_pool = loop.run_until_complete(
        asyncpg.create_pool(
            user=pg_username,
            password=pg_password,
            database=pg_database,
            host=pg_host))
    log.info("success! {}".format(pg_pool))

    def co(p):
        return process_post(p, pg_pool)

    # batch the processing... otherwise we never return to the database connections
    for i in count():
        try:
            nv = next(posts)
            log.info("finding posts {} - {}".format(i*batch_size, (i+1)*batch_size))
            async_map(co, chain([nv], islice(posts, batch_size)))
        except StopIteration:
            return


# ===============
# Single element process worker:
# ===============
async def process_post(post, pg_pool, http_session=None):
    log = logging.getLogger('process_post({})'.format(post))
    if not http_session:
        async with aiohttp.ClientSession() as http_session:
            if not await is_post_in_db(pg_pool, post.id, post.subreddit):
                html, article = await parse_html(http_session, post.url)
                if article != "":
                    await write_post_to_db(pg_pool, *post, html, article)
    else:
        if not await is_post_in_db(pg_pool, post.id, post.subreddit):
            html, article = await parse_html(http_session, post.url)
            if article != "" and article is not None:
                await write_post_to_db(pg_pool, *post, html, article)


def worker(p):
    log = logging.getLogger('worker')
    log.info("creating event loop...")
    loop = asyncio.get_event_loop()
    log.info("p is: {}".format(p))
    return loop.run_until_complete(
        process_worker(p, "postgres", os.environ["POSTGRES_PASSWORD"],
                       os.environ["POSTGRES_DB"], "127.0.0.1"))


def async_map(coroutine_func, iterable, chunk_size=1):
    from itertools import islice
    loop = asyncio.get_event_loop()
    future = asyncio.gather(*(coroutine_func(param) for param in iterable))
    return loop.run_until_complete(future)


@click.command()
@click.option("--number", default=None, help="number of posts to crawl")
@click.option(
    "--chunksize",
    default=100,
    help="number of posts to crawl per process initially")
def run(number, chunksize):
    log = logging.getLogger('run')
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

    log.info("{}".format(os.environ))

    posts = post_iterator(number)

    process_posts(posts, "postgres", os.environ["POSTGRES_PASSWORD"],
                  os.environ["POSTGRES_DB"], "127.0.0.1", chunksize)
    # with concurrent.futures.ProcessPoolExecutor(max_workers=processes) as executor:
    #     executor.map(worker, posts, chunksize=chunksize)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(threadName)10s %(name)18s: %(message)s',
        stream=sys.stderr, )
    run()
