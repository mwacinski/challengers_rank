from time import time
from ETL.config import Config as Cfg
from ETL.extractor.lol_content_extractor import LOLContentExtractor
from ETL.transformer.lol_content_transformer import LOLContentTransformer


class Job:
    def __init__(self, extractor, transformer):
        self.extractor = extractor
        self.transformer = transformer

    def run(self):
        """Running processes"""
        try:
            self.extractor.extract_data()
            self.transformer.implemeting_scd2()
            return "SUCCESS"
        except Exception as err:
            print(err)
            return "FAILED"


def timeis(func):
    """Decorator that reports the execution time."""

    def wrap(*args, **kwargs):
        start = time()
        result = func(*args, **kwargs)
        end = time()

        print(f"Time execution: {end - start}")
        return result

    return wrap


@timeis
def run_etl_job():
    extractor = LOLContentExtractor(Cfg.URL, Cfg.API_KEY)
    transformer = LOLContentTransformer(Cfg.FILE_PATH)

    etl = Job(
        extractor=extractor,
        transformer=transformer,
    )

    status = etl.run()
    print(f"Job status: {status}")


if __name__ == "__main__":
    run_etl_job()
