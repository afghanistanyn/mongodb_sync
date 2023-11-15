from gevent import monkey
monkey.patch_all()
import gevent
from gevent import threading
import bson
# pymongo==3.12.3
import pymongo
import re
import sys

import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(filename)s:%(lineno)d - %(message)s')
logger = logging.getLogger(__name__)

class MongodbSyncConfig():
    def __init__(self, sync_conf):
        try:
            self.src = pymongo.MongoClient(sync_conf.get("src"), connect=True, document_class=bson.son.SON, serverSelectionTimeoutMS=5000)
            # test MongoClient connection
            self.src.server_info()
        except Exception as e:
            print(f"mongodb源库连接异常: {e}")
            sys.exit(-1)
        try:
            self.dst = pymongo.MongoClient(sync_conf.get("dst"), connect=True, document_class=bson.son.SON, serverSelectionTimeoutMS=5000)
            self.dst.server_info()
        except Exception as e:
            print(f"mongodb目标库连接异常: {e}")
            sys.exit(-1)

        db_col_filter = sync_conf.get("db_col_filter", None)
        if not db_col_filter:
            logger.error("同步配置项[db_col_filter]未配置")
            sys.exit(-1)
        #  validate db_col_filter
        try:
            if db_col_filter:
                for _filter in db_col_filter.split(","):
                    _filter = _filter.strip().lstrip()
                    [db,col] = _filter.split(".")
        except ValueError as e:
            logger.error(f"解析db_col_filter出错, 格式: db.col; db中不能含有'.'")
            sys.exit(-1)

        self.src_db_cols = []
        for _filter in db_col_filter.split(","):
            _filter = _filter.strip().lstrip()
            [db_filter, col_filter] = _filter.split(".")
            if db_filter.startswith("*"):
                db_filter = f".{db_filter}"
            if col_filter.startswith("*"):
                col_filter = f".{col_filter}"
            for db in [db for db in self.src.list_database_names() if db not in ["admin", "local", "config"]]:
                if re.match(db_filter, db):
                    for col in  [col for col in self.src[db].list_collection_names()]:
                        if re.match(col_filter, col):
                            logger.info(f"find src db.collection: {db}.{col}")
                            self.src_db_cols.append(f"{db}.{col}")


        db_transform = sync_conf.get("db_transform", None)
        col_transform = sync_conf.get("col_transform", None)

        if self.src.address == self.dst.address:
            if not db_transform and not col_transform:
                logger.error("检测到源地址与目的地址相同, 且未配置transform, 请修改配置!")
                sys.exit(-1)

        self.db_transform_map = {}
        if db_transform:
            for db_transform_item in db_transform.split(","):
                [k, v] = db_transform_item.strip().lstrip().split(":")
                self.db_transform_map.update({k:v})


        self.col_transform_map = {}
        if col_transform:
            for col_transform_item in col_transform.split(","):
                [k, v] = col_transform_item.strip().lstrip().split(":")
                self.col_transform_map.update({k:v})

        # transformed
        self.dst_db_cols = []
        for db_col in self.src_db_cols:
            # maxsplit 防止col名字中有"."
            [db, col] = db_col.split(".", maxsplit=1)
            transform_db_func = lambda x: self.db_transform_map.get(db, db)
            new_db = transform_db_func(db)
            transform_col_func = lambda x: self.col_transform_map.get(col, col)
            new_col = transform_col_func(col)
            if db != new_db or col != new_col:
                logger.info(f"transform配置: {db}.{col} ---> {new_db}.{new_col}")
            self.dst_db_cols.append(f"{new_db}.{new_col}")

        assert len(self.src_db_cols) == len(self.dst_db_cols)
        self.query_filter = sync_conf.get("query_filter", None)
        if self.query_filter:
            logger.info(f"query_filter配置: {self.query_filter}")
        self.dry_run = sync_conf.get("dry_run", True)
        self.index = sync_conf.get("index", True)


class MongodbSyncer():
    def __init__(self, mongodbSyncConfig: MongodbSyncConfig, max_threads: int = 8):
        self._query_filter = mongodbSyncConfig.query_filter
        self._src = mongodbSyncConfig.src
        self._dst  = mongodbSyncConfig.dst
        self.src_dst_db_cols_map = dict(zip(mongodbSyncConfig.src_db_cols, mongodbSyncConfig.dst_db_cols))
        self.max_threads = max_threads
        self._dry_run = mongodbSyncConfig.dry_run
        self.index = mongodbSyncConfig.index

    def _create_index(self, src_db, src_col, dst_db, dst_col):
        def format(key_direction_list):
            """ Format key and direction of index.
            """
            res = []
            for key, direction in key_direction_list:
                if isinstance(direction, float) or isinstance(direction, int):
                    direction = int(direction)
                res.append((key, direction))
            return res

        index_info = self._src[src_db][src_col].index_information()
        for name, info in index_info.items():
            keys = info['key']
            options = {}
            options['name'] = name
            if 'unique' in info:
                options['unique'] = info['unique']
            if 'sparse' in info:
                options['sparse'] = info['sparse']
            if 'expireAfterSeconds' in info:
                options['expireAfterSeconds'] = info['expireAfterSeconds']
            if 'partialFilterExpression' in info:
                options['partialFilterExpression'] = info['partialFilterExpression']
            if 'dropDups' in info:
                options['dropDups'] = info['dropDups']
            if 'weights' in info:
                options['weights'] = info['weights']
            if 'default_language' in info:
                options['default_language'] = info['default_language']
            if 'language_override' in info:
                options['language_override'] = info['language_override']
            self._dst[dst_db][dst_col].create_index(format(keys), **options)

    def _sync_small_collection(self, src_db, src_col, dst_db, dst_col):
        with self._src.start_session() as session:
            cursor = self._src[src_db][src_col].find(filter=self._query_filter, session=session, cursor_type=pymongo.cursor.CursorType.EXHAUST, no_cursor_timeout=True)
            # 覆盖已有, 可重复执行同步
            reqs = list(map(lambda doc: pymongo.ReplaceOne({"_id": doc['_id']}, doc, upsert=True), cursor))
            threads = [gevent.spawn(self._dst[dst_db][dst_col].bulk_write, reqs, ordered=False, bypass_document_validation=False)]
            gevent.joinall(threads, raise_error=True)
            logger.info(f"{self._src.address[0]}:{self._src.address[1]}/{src_db}.{src_col} --> {self._dst.address[0]}:{self._dst.address[1]}/{dst_db}.{dst_col}, document total: {len(reqs)}. 同步完成")

    def _sync_medium_collection(self, src_db, src_col, dst_db, dst_col):
        with self._src.start_session() as session:
            cursor = self._src[src_db][src_col].find(filter=self._query_filter, session=session, cursor_type=pymongo.cursor.CursorType.EXHAUST, no_cursor_timeout=True)
            # 覆盖已有, 可重复执行同步
            reqs = list(map(lambda doc: pymongo.ReplaceOne({"_id": doc['_id']}, doc, upsert=True), cursor))
            # 将reqs均分为{self.max_threads}份, 开启{self.max_threads}个线程
            per_thread_req_size = len(reqs) // self.max_threads
            remainder = len(reqs) % self.max_threads
            chunked_reqs = [reqs[i:i + per_thread_req_size] for i in range(0, len(reqs), per_thread_req_size)]
            if remainder > 0:
                # 将整除后剩余部分追击到倒数第二个chunk, 移除最后一个
                chunked_reqs[-2].extend(chunked_reqs[-1])
                chunked_reqs = chunked_reqs[:-1]
                assert len(chunked_reqs) == self.max_threads
            threads = [gevent.spawn(self._dst[dst_db][dst_col].bulk_write, chunked_reqs[i], ordered=False, bypass_document_validation=False) for i in range(self.max_threads)]
            gevent.joinall(threads, raise_error=True)
            logger.info(f"{self._src.address[0]}:{self._src.address[1]}/{src_db}.{src_col} --> {self._dst.address[0]}:{self._dst.address[1]}/{dst_db}.{dst_col}, document total: {len(reqs)}. 同步完成")


    def _sync_large_collection(self, src_db: str, src_col: str, dst_db: str, dst_col: str, query: dict, thread_name: str):
        total = self._src[src_db][src_col].count_documents(filter=query)
        if total == 0:
            logger.error(f"{thread_name} sync {src_db}.{src_col}, query: {query}, total: 0")
            return
        processed = 0
        reqs = []
        reqs_max = 1000
        groups = []
        groups_max = 10

        with self._src.start_session() as session:
            cursor = self._src[src_db][src_col].find(filter=query, cursor_type=pymongo.cursor.CursorType.EXHAUST, session=session, no_cursor_timeout=True)
            for doc in cursor:
                reqs.append(pymongo.ReplaceOne({'_id': doc['_id']}, doc, upsert=True))

                if len(reqs) == reqs_max:
                    groups.append(reqs)
                    reqs = []
                if len(groups) == groups_max:
                    threads = [gevent.spawn(self._dst[dst_db][dst_col].bulk_write, groups[i], ordered=False, bypass_document_validation=False) for i in range(groups_max)]
                    gevent.joinall(threads, raise_error=True)
                    groups = []
                    processed += reqs_max * groups_max
                    current_progress = round((processed / total) * 100, 2)
                    logger.info(f"{thread_name} sync {src_db}.{src_col}, query: {query}, total: {total}, current_progress: {current_progress}%")
        if len(groups) > 0:
            threads = [gevent.spawn(self._dst[dst_db][dst_col].bulk_write, groups[i], ordered=False, bypass_document_validation=False) for i in range(len(groups))]
            gevent.joinall(threads, raise_error=True)
            processed += len(groups) * reqs_max
            current_progress = round((processed / total) * 100, 2)
            logger.info(f"{thread_name} sync {src_db}.{src_col}, query: {query}, total: {total}, current_progress: {current_progress}%")
        if len(reqs) > 0:
            self._dst[dst_db][dst_col].bulk_write(reqs, ordered=False, bypass_document_validation=False)
            processed += len(reqs)
            current_progress = round((processed / total) * 100, 2)
            logger.info(f"{thread_name} sync {src_db}.{src_col}, query: {query}, total: {total}, current_progress: {current_progress}%")
        logger.info(f"{thread_name} sync {src_db}.{src_col}, query: {query}, done")

    def run(self):
        if len(self.src_dst_db_cols_map) == 0:
            logger.info("未找到需要同步的库和集合, 请检查配置")
            sys.exit(0)

        if self._dry_run:
            logger.warning("请注意当前处于试运行模式, 数据不会同步.")

        for src_db_col, dst_db_col in self.src_dst_db_cols_map.items():
            [src_db, src_col] = src_db_col.split(".", maxsplit=1)
            [dst_db, dst_col] = dst_db_col.split(".", maxsplit=1)

            if self._dry_run:
                with self._src.start_session() as session:
                    total = self._src[src_db][src_col].count_documents(filter=self._query_filter, session=session)
                logger.info(f"{self._src.address[0]}:{self._src.address[1]}/{src_db}.{src_col} --> {self._dst.address[0]}:{self._dst.address[1]}/{dst_db}.{dst_col}, document total: {total}.")
                continue

            if self.index:
                try:
                    self._create_index(src_db, src_col, dst_db, dst_col)
                    logger.info(f"{self._src.address[0]}:{self._src.address[1]}/{src_db}.{src_col} --> {self._dst.address[0]}:{self._dst.address[1]}/{dst_db}.{dst_col} sync index successful")
                except Exception as e:
                    logger.error(f"{self._src.address[0]}:{self._src.address[1]}/{src_db}.{src_col} --> {self._dst.address[0]}:{self._dst.address[1]}/{dst_db}.{dst_col} sync index err: {e}")
                    continue

            total = self._src[src_db][src_col].count_documents(filter=self._query_filter)
            # 大于 1k, 小于10w 则分组插入
            if  total > 1000 and total < 100000:
                try:
                    self._sync_medium_collection(src_db, src_col, dst_db, dst_col)
                except Exception as e:
                    logger.error(
                        f"{self._src.address[0]}:{self._src.address[1]}/{src_db}.{src_col} --> {self._dst.address[0]}:{self._dst.address[1]}/{dst_db}.{dst_col}, document total: {total}. 同步失败, 请修复错误后重试")
                    logger.error(e)
                    continue
            elif total >= 100000:
                split_querys = []
                db = self._src[src_db]
                collstats = db.command('collstats', src_col)
                n_points = self.max_threads - 1
                max_chunk_size = ((collstats['count'] / (self.max_threads - 1) - 1) * 2 * collstats[
                    'avgObjSize']) / 1024 / 1024
                res = db.command('splitVector', f"{src_db}.{src_col}", keyPattern={'_id': 1}, maxSplitPoints=n_points,
                                 maxChunkSize=max_chunk_size, maxChunkObjects=collstats['count'])
                split_points = [doc['_id'] for doc in res['splitKeys']]
                #build query:
                lower_bound = None
                for point in split_points:
                    if lower_bound is None:
                        split_querys.append({'_id': {'$lt': point}})
                    else:
                        split_querys.append({'_id': {'$gte': lower_bound, '$lt': point}})
                    lower_bound = point
                split_querys.append({'_id': {'$gte': lower_bound}})
                threads = []
                #  for debug split thread
                # split_querys = split_querys[7:]
                # self._sync_large_collection(src_db, src_col, dst_db, dst_col, split_querys[0], "test")
                for idx, split_query in enumerate(split_querys):
                    split_query.update(self._query_filter)
                    thread_name = f"sync_large_collection_thread_{idx}"
                    t = threading.Thread(target=self._sync_large_collection, args=(src_db, src_col, dst_db, dst_col, split_query, thread_name), name=thread_name)
                    threads.append(t)
                    t.start()
                    logger.info('start thread %s with query %s' % (t.name, split_query))
                for t in threads:
                    t.join()
            # 数量小于1k, 一次性批量插入
            else:
                try:
                    self._sync_small_collection(src_db, src_col, dst_db, dst_col)
                except Exception as e:
                    logger.error(
                        f"{self._src.address[0]}:{self._src.address[1]}/{src_db}.{src_col} --> {self._dst.address[0]}:{self._dst.address[1]}/{dst_db}.{dst_col}, document total: {total}. 同步失败, 请修复错误后重试")
                    logger.error(e)
                    continue

        logger.info("同步完成")

if __name__ == '__main__':
    sync_config = {
        # mongodb uri
        "src": "mongodb://127.0.0.1:27017/",
        "dst": "mongodb://127.0.0.1:27017/",
        # db_col_filter example: "dbA.*, dbB.logs, dbC.col_profix_*"; 支持填多个, 使用逗号分割; "*.*"表示同步所有,
        #  如果需要精确匹配请使用正则, 比如 dbA.logs$
        "db_col_filter": "test.*",
        # query_filter in dict format, 支持标准pymongo查询
        # example1:  "query_filter": {"user_name": "aaa"},
        # example2: query_filter: { "field_name": { "$regex": f".*-test", "$options": "i" } }
        "query_filter": {},
        # transform不支持正则, 必须一对一写明, 格式: "db1:dbA, db2:dbB" 或者 "col1:colA, col2:colB"
        "db_transform": "",
        "col_transform": "",
        # 试运行, 不执行同步
        "dry_run": False,
        # 是否同步索引
        "index": True,
    }

    config = MongodbSyncConfig(sync_config)
    syncer = MongodbSyncer(mongodbSyncConfig=config)
    syncer.run()