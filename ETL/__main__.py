import os
from ETL.config import Config as Cfg
from ETL.loader.lol_content_loader import LOLContentLoader
from ETL.extractor.lol_content_extractor import LOLContentExtractor
from ETL.transformer.lol_content_transformer import LOLContentTransformer


class Job:
    def __init__(self, extractor, transformer, loader):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader
        self.path = Cfg.DATA_DIR

    def run(self):
        """Running processes"""
        try:
            self.extractor.extract_data()
            if not os.path.exists(f"{Cfg.DATA_DIR}\\dim_data.csv"):
                self.transformer.create_dim_model()
            self.transformer.implementing_scd2()
            self.loader.load_content()
            return "DONE"
        except Exception as err:
            print(err)
            return "FAILED"


def run_etl_job():
    extractor = LOLContentExtractor(Cfg.URL, Cfg.API_KEY, Cfg.DATA_DIR)
    transformer = LOLContentTransformer(Cfg.DATA_DIR)
    loader = LOLContentLoader(f"{Cfg.LOADER_UTILS_DIR}\\create_table.sql", f"{Cfg.DATA_DIR}\\final_data.csv")

    etl = Job(extractor,
              transformer,
              loader)

    status = etl.run()
    print(f"Job status: {status}")


if __name__ == "__main__":
    run_etl_job()
