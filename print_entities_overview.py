# @author Daniel Vliegenthart
# Print overview of entities' relevance score and other meta-data

import json
import time
import os
import os.path
from config import ROOTPATH, viewer_pids, data_date, facets, thres_min_ratings, thres_max_rating_time, seedsize
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
    print_file(facet, seedsize, "\n\n---------------------------------------------------")
    print_file(facet, seedsize, f'-     {"{0:%Y-%m-%d %H:%M:%S}".format(datetime.datetime.now())} - ENTITIES OVERVIEW     -')
    print_file(facet, seedsize, "---------------------------------------------------")

    print(f'\n\nEntities overview for "{facet}" facet:')
    print_file(facet, seedsize, f'\n\nEntities overview for "{facet}" facet:')

    entities = read_overview_csv(facet, seedsize)
    header = [f'<{column.upper()}>' for column in entities.pop(0)]
    entity_trunc = 50

    for entity in entities:
      entity_string = entity[0].ljust(math.floor(float((len(entity[0]))/entity_trunc)+1)*entity_trunc)
      entity[0] = "\n ".join([entity_string[i:i+entity_trunc] for i in range(0, len(entity_string), entity_trunc)])

    print("", "{: <50} {: <20} {: <20} {: <20} {: <20} {: <20}".format(*header))
    print_file(facet, seedsize, " {: <50} {: <20} {: <20} {: <20} {: <20} {: <20}".format(*header))

    for row in entities:
      color = rel_color_ENUM[row[1]]
      print(color, "{: <50} {: <20} {: <20} {: <20} {: <20} {: <20}".format(*row), Style.RESET_ALL)
      print_file(facet, seedsize, " {: <50} {: <20} {: <20} {: <20} {: <20} {: <20}".format(*row))

    generate_entity_lists(facet, seedsize, entities)

# Read papers and number entities overview file
def read_overview_csv(facet, seedsize):
  file_path = f'results/entities_overview_{facet}_{seedsize}_{data_date}.csv'
  csv_raw = open(file_path, 'r').readlines()
  csv_raw = [line.rstrip('\n').split(',') for line in csv_raw]
  
  return csv_raw


def print_file(facet, seedsize, line):
  file_path = f'results/entities_overview_{facet}_{seedsize}_{data_date}.txt'
  os.makedirs(os.path.dirname(file_path), exist_ok=True)

  with open(file_path, 'a') as out:
    out.write(line + '\n')

def generate_entity_lists(facet, seedsize, entities, iteration=0):
  rel_labels = ['irrelevant', 'relevant']
  for label in rel_labels:
    file_path = f'results/{facet}_{seedsize}_coner_{label}_entities_{iteration}.txt'
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, 'w') as out:
      for entity in entities:
        if entity[1] == label: out.write(entity[0] + '\n')

  file_path = f'results/{facet}_{seedsize}_coner_all_entities_{iteration}.txt'
  os.makedirs(os.path.dirname(file_path), exist_ok=True)

  with open(file_path, 'w') as out:
    for entity in entities:
        out.write(entity[0] + '\n')

  print("Wrote entity lists for all facets in 'results/' directory")

if __name__=='__main__':
  main()
