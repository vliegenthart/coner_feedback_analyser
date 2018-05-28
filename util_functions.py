import json
import time
from config import ROOTPATH, viewer_pids, data_date

def read_json_file(file_path):
  data_string = open(file_path).read()
  data_json = json.loads(data_string)

  return data_json