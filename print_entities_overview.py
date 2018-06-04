# @author Daniel Vliegenthart
# Print overview of entities' relevance score and other meta-data

import json
import time
import os
import os.path
from config import ROOTPATH, viewer_pids, data_date, facets, thres_min_ratings, thres_max_rating_time
from colorama import Fore, Back, Style
import csv
import datetime
import math

rel_color_ENUM = { 'neutral': Fore.BLUE, 'relevant': Fore.GREEN, 'irrelevant': Fore.RED }

def main():
  print("\n\n---------------------------------------------------")
  print(f'-     {"{0:%Y-%m-%d %H:%M:%S}".format(datetime.datetime.now())} - ENTITIES OVERVIEW     -')
  print("---------------------------------------------------")

  # ############################################ #
  #      PRINT ENTITIES OVERVIEW FOR EACH FACET  #
  # ############################################ #

  for facet in facets:
    print(f'\n\nEntities overview for "{facet}" facet:')

    entities = read_overview_csv(facet)
    header = [f'<{column.upper()}>' for column in entities.pop(0)]
    entity_trunc = 50

    for entity in entities:
      entity_string = entity[0].ljust(math.floor(float((len(entity[0]))/entity_trunc)+1)*entity_trunc)
      entity[0] = "\n ".join([entity_string[i:i+entity_trunc] for i in range(0, len(entity_string), entity_trunc)])

    print("", "{: <50} {: <20} {: <20} {: <20} {: <20} {: <20}".format(*header))

    for row in entities:
      color = rel_color_ENUM[row[1]]
      print(color, "{: <50} {: <20} {: <20} {: <20} {: <20} {: <20}".format(*row), Style.RESET_ALL)

# Read papers and number entities overview file
def read_overview_csv(facet):
  file_path = f'results/entities_overview_{facet}_{data_date}.csv'
  csv_raw = open(file_path, 'r').readlines()
  csv_raw = [line.rstrip('\n').split(',') for line in csv_raw]
  
  return csv_raw

if __name__=='__main__':
  main()
