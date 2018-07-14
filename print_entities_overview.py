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
    print_small_entities(facet, seedsize, entities)
    average_rel_score(facet, seedsize, entities)
    
    continue

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

  # Check how many entities doubly facet classified
  dataset_entities = list(set(read_all_entities('dataset')))
  method_entities = list(set(read_all_entities('method')))

  doubly_facet =  list(set(dataset_entities).intersection(method_entities))

  print("Doubly facet classified:", len(doubly_facet))

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

def print_small_entities(facet, seedsize, entities):
  result_list = []
  gen_entities = read_all_entities(facet)


  rel_scores = {'generated': [], 'selected': []}

  file_path = f'results/entities_overview_small_{facet}_{seedsize}_{data_date}.txt'
  os.makedirs(os.path.dirname(file_path), exist_ok=True)
  entities.pop(0)
  entities.sort(key=lambda entity: (entity[5], entity[2]))

  with open(file_path, 'w') as out:
    for entity in entities:
      type = entity[5]
      rel_score = entity[2]
      
      if type == 'generated' and entity[0] not in gen_entities: continue

      temp = f'{type} {rel_score} {entity[0]}'
      out.write(temp + '\n')
      result_list.append(temp)

  return result_list

def average_rel_score(facet, seedsize, all_ent):
  all_ent.pop(0)
  all_ent.sort(key=lambda entity: (entity[5], entity[2]))
  all_ent = { ent[0]: ent for ent in all_ent}
  
  result_list = []

  coner_ent = read_coner_entities(facet, seedsize)

  rel_scores_high = {'generated': [], 'selected': []}
  rel_scores_low = {'generated': [], 'selected': []}

  rel_sc_high = []
  rel_sc_low = []
  
  print(len(coner_ent))

  for entity in coner_ent:

    if not entity in all_ent.keys(): continue
    ent_obj = all_ent[entity]
    type1 = ent_obj[5]
    rel_score = float(ent_obj[2])

    if rel_score > 0.5:
      rel_scores_high[type1].append(rel_score)
      rel_sc_high.append(rel_score)
    elif rel_score < 0.5:
      rel_scores_low[type1].append(rel_score)
      rel_sc_low.append(rel_score)

  
  print(facet, avg_list(rel_sc_high), avg_list(rel_sc_low))

    # rel_scores[type1].append(rel_score)
    
    # if type == 'generated' and entity[0] not in gen_entities: continue

    # temp = f'{type} {rel_score} {entity[0]}'
    # out.write(temp + '\n')
    # result_list.append(temp)

  return result_list

def avg_list(l):
  return round(sum(l) / float(len(l)),2)

def read_all_entities(facet):
  file_path = f'{ROOTPATH}/data/{facet}_all_entities.txt'
  set_raw = open(file_path, 'r').readlines()
  set_raw = [line.rstrip('\n').lower() for line in set_raw]
  
  return set_raw

def read_coner_entities(facet, seedsize):
  file_path = f'{ROOTPATH}/results/smartpub/{facet}_{seedsize}_extracted_entities_coner_2018_05_28.txt'
  set_raw = open(file_path, 'r').readlines()
  set_raw = [line.rstrip('\n').lower() for line in set_raw]
  
  return set_raw

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
