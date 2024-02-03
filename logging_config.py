import logging

### logging configuration file ###
def logger(fn):
  logging.basicConfig(filename=fn,
                      filemode='w',
                      format='%(asctime)s, %(levelname)s %(message)s',
                      datefmt='%d-%m-%Y %H:%M:%S',
                      level=logging.INFO)

  logger = logging.getLogger(__name__)
  return