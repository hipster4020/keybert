import logging
import re
import time
from logging import handlers

import hydra
import pandas as pd
import pymysql
import swifter
from konlpy.tag import Mecab
from sqlalchemy import create_engine

from keybert import KeyBERT

m = Mecab()


# log setting
carLogFormatter = logging.Formatter("%(asctime)s,%(message)s")

carLogHandler = handlers.TimedRotatingFileHandler(
    filename="/workspace/news_keyword/log/keybert.log",
    when="midnight",
    interval=1,
    encoding="utf-8",
)
carLogHandler.setFormatter(carLogFormatter)
carLogHandler.suffix = "%Y%m%d"

scarp_logger = logging.getLogger()
scarp_logger.setLevel(logging.INFO)
scarp_logger.addHandler(carLogHandler)


# data processing
def processing(content):
    result = (
        str(content).replace("뉴스코리아", "")
        .replace("및", "")
        .replace("Copyright", "")
        .replace("copyright", "")
        .replace("COPYRIGHT", "")
        .replace("저작권자", "")
        .replace("ZDNET A RED VENTURES COMPANY", "")
        .replace("appeared first on 벤처스퀘어", "")
        .replace("appeared first on 벤처 스퀘어", "")
        .replace("appeared first on 모비인사이드 MOBIINSIDE", "")
        .replace("appeared first on 모비 인사이드 MOBIINSIDE", "")
        .replace("The post", "")
    )
    result = re.sub("http[s]?://(?:[a-zA-Z]|[0-9]|[$\-@\.&+:/?=]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+", "", result)
    result = re.sub(r"[a-zA-Z가-힣]+뉴스", "", result)
    result = re.sub(r"[a-zA-Z가-힣]+ 뉴스", "", result)
    result = re.sub(r"[a-zA-Z가-힣]+newskr", "", result)
    result = re.sub(r"[a-zA-Z가-힣]+Copyrights", "", result)
    result = re.sub(r"[a-zA-Z가-힣]+ Copyrights", "", result)
    result = re.sub(r"\s+Copyrights", "", result)
    result = re.sub(r"[a-zA-Z가-힣]+com", "", result)
    result = re.sub(r"[가-힣]+ 기자", "", result)
    result = re.sub(r"[가-힣]+기자", "", result)
    result = re.sub(r"[가-힣]+ 신문", "", result)
    result = re.sub(r"[가-힣]+신문", "", result)
    result = re.sub(r"데일리+[가-힣]", "", result)
    result = re.sub(r"[가-힣]+투데이", "", result)
    result = re.sub(r"[가-힣]+미디어", "", result)
    result = re.sub(r"[가-힣]+ 데일리", "", result)
    result = re.sub(r"[가-힣]+데일리", "", result)
    result = re.sub(r"[가-힣]+ 콘텐츠 무단", "", result)
    result = re.sub(r"전재\s+변형", "전재", result)
    result = re.sub(r"[가-힣]+ 전재", "", result)
    result = re.sub(r"[가-힣]+전재", "", result)
    result = re.sub(r"[가-힣]+배포금지", "", result)
    result = re.sub(r"[가-힣]+배포 금지", "", result)
    result = re.sub(r"\s+배포금지", "", result)
    result = re.sub(r"\s+배포 금지", "", result)
    result = re.sub(r"[a-zA-Z가-힣]+.kr", "", result)
    result = re.sub(r"/^[a-z0-9_+.-]+@([a-z0-9-]+\.)+[a-z0-9]{2,4}$/", "", result)
    result = re.sub(r"[\r|\n]", "", result)
    result = re.sub(r"\[[^)]*\]", "", result)
    result = re.sub(r"\([^)]*\)", "", result)
    result = re.sub(r"[^ ㄱ-ㅣ가-힣A-Za-z0-9.]", "", result)
    result = re.sub(r"이 글은 외부 필자인 +[a-zA-Z가-힣]", "", result)
    result = re.sub(r"[a-zA-Z가-힣]+기고입니다.", "", result)
    result = result.replace(".", "")
    result = result[:result.find('관련기사')]
    result = result[:result.find('관련 기사')]
    result = result.strip()

    return result


# batch
def batch(iterable, batch_size):
  l = len(iterable)
  for ndx in range(0, l, batch_size):
    yield iterable[ndx:min(ndx + batch_size, l)]


# data load
def data_load(**kwargs):
    try:
        logging.info("dataload start")
        pymysql.install_as_MySQLdb()

        engine_conn = "mysql://%s:%s@%s/%s" % (
            kwargs.get("user"),
            kwargs.get("passwd"),
            kwargs.get("host"),
            kwargs.get("db"),
        )
        engine = create_engine(engine_conn)

        df = pd.read_sql(
            kwargs.get("bquery"),
            engine,
        )
        df['title_content'] = df.title + " " + df.content
        df = df[['id', 'title_content']]
        df = df.drop_duplicates()

        logging.info(f"df's length : {len(df)}")
        logging.info("dataload end")

        return df

    except Exception as e:
        logging.info(e)


# database update query
def update(param, **kwargs):
    conn = pymysql.connect(
        user=kwargs.get("user"),
        passwd=kwargs.get("passwd"),
        db=kwargs.get("db"),
        host=kwargs.get("host"),
        port=kwargs.get("port"),
        charset="utf8",
        use_unicode=True,
    )
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.executemany(kwargs.get("uquery"), param)
    conn.commit()
    logging.info("update end")


@hydra.main(config_name='config.yml')
def main(cfg):
    try:
        # start time
        start = time.time()
        
        # Data Load
        df = data_load(**cfg.DATABASE)
        
        # data processing
        logging.info("processing start")
        df['content'] = df.title_content.swifter.apply(processing)
        
        # 50 미만 제거
        logging.info("remove len 50 less")
        df = df[df.content.swifter.apply(lambda x: len(x) >= 50)]

        # 명사 추출
        logging.info("extract nouns")
        df['token'] = df.content.swifter.apply(lambda x: " ".join([w for w, t in m.pos(str(x)) if t.startswith('N') or t=='SL']))

        
        # keybert
        kw_model = KeyBERT(cfg.MODEL.model)
        
        # batch 단위 처리
        result = []
        for batch_df in batch(df, cfg.DIR.batch_size):
            keywords = kw_model.extract_keywords(batch_df.token.tolist(),
                                                keyphrase_ngram_range=eval(cfg.MODEL.range),        # 단어 추출을 위해 unigram - unigram
                stop_words='english',                               # 영어 불용어 처리
                use_maxsum=True,                                    # Max Sum Similarity(상위 top n개 추출 중 가장 덜 유사한 키워들 조합 계산)
                nr_candidates=cfg.MODEL.nr_candidates,              # 후보 갯수
                top_n=cfg.MODEL.top_n)                              # 키워드 추출 갯수
            
            for keyword in keywords:
                if keyword[0] == "None Found":
                    keyword = [('NONE KEYWORD', '')]
                keyword.sort(key=lambda x: x[1], reverse=True)
                keyword = [w.upper() for w, t in keyword]
                result.append(', '.join(keyword))

        logging.info(len(result))
        df['keywords'] = result

        logging.info(f"processed df's length : {len(df)}")
        logging.info(df.head())
    

        # dataframe to update query
        param = []
        for k, i in zip(df["keywords"], df["id"]):
            param.append((k, i))

        update(param, **cfg.DATABASE)


        # end time
        logging.info("time :" + str(time.time() - start))
        
    except Exception as e:
        logging.info(e)
        return 200
    
if __name__ == "__main__":
    main()

