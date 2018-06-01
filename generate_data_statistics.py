# @author Daniel Vliegenthart
# Generate data statistics for firebase json export

import json
import time
import os
from config import ROOTPATH, viewer_pids, data_date, facets
from util_functions import read_json_file
from colorama import Fore, Back, Style
from itertools import groupby

# TODO
# - Average entity rating time per paper, user and total
# - Total nr. ratings per user
# - #ratings difference for selected and generated (and why!)

# ######################## #
#      SETUP VARIABLES     #
# ######################## #

# Highlight attribute: <key: highlight_id> = <value: <highlight attributes>>
highlights = { "dataset": {}, "method": {} }

# Rating attribute: <key: entity_text> = <value: [<#relevant>, <#irrelevant>, <#ratings>]>
ratings = { "dataset": {}, "method": {} }
categories = ['generated','selected']

relevance_ENUM = { 'relevant': 0, 'irrelevant': 1 }

def main():

  # ######################################### #
  #      GENERATE FIREBASE DATA STATISTICS    #
  # ######################################### #

  data_json = read_json_file(f'data/firebase-{data_date}-coner-viewer-export.json')
  
  # highlights_json = data_json['highlights']
  # ratings_json = data_json['ratings']
  # rewards_json = data_json['rewards']
  # users_json = data_json['users']

  # highlights = []
  
  # rewards = []
  # users = []

  # Perform some preprocessing and formatting for data attributes
  for key in data_json.keys():
    for pid in data_json[key].keys():
      for firebase_id, data_obj in data_json[key][pid].items():
        if not type(data_obj) is dict: continue
        if 'timestamp' in data_obj.keys(): data_obj['timestamp_formatted'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(data_obj['timestamp']))

        data_obj['firebase_id'] = firebase_id
  
  # Process and print ratings results
  ratings_raw_arr, ratings_arr = process_ratings(data_json['ratings'])

  for facet in facets:
    for rating in [obj for obj in ratings_arr if obj['facet'] == facet]:

      entity = rating['entityText'].strip(" \t,-.[]()").lower()


      if not entity in ratings[facet].keys(): ratings[facet][entity] = [0,0,0, rating['highlightType'], rating['highlightId'], rating['pid']]

      ratings[facet][entity][2] += 1
      ratings[facet][entity][relevance_ENUM[rating['relevance']]] += 1


    print(f'\n\n##### {facet}: RELEVANT #####')
    for key, rating in sorted(ratings[facet].items()):
      rel = rating[0]
      total = rating[2]

      print_str = f'{key}: {rel}/{total} - {rating[3]}'
      rel_score = float(rel)/total

      if total > 1:
        if rel_score > 0.5: print(Fore.GREEN, print_str, Style.RESET_ALL, end='\t')
        if rel_score < 0.5: print(Fore.RED, print_str, Style.RESET_ALL, end='\t')
      else:
        print(Fore.BLUE, print_str, Style.RESET_ALL, end='\t')

  # Process highlights results
  for pid in data_json['highlights'].keys():

    for highlight_id, highlight in data_json['highlights'][pid].items():
      facet = highlight['metadata']['facet']
      highlight['facet'] = facet
      highlight['id'] = highlight_id
      if not highlight_id in highlights[facet].keys(): highlights[facet][highlight_id] = highlight


  # !!! CHANGE TO ENTITIES OVERVIEW, NOT NR HIGHLIGHTS BUT NR ENTITIES SELECTED!! (USE RATINGS ARRAY!!)

  # Generate data statistics for 'ratings'
  print("\n\n###############################")
  print("##### ENTITIES STATISTICS #####")
  print("###############################")

  total_highlights = []
  for facet in facets:
    total_highlights += highlights[facet].values()
  nr_total_highlights = len(total_highlights)

  # Generic overview highlights
  print(f'Total highlights: {nr_total_highlights}')  

  for cat in categories:
    nr_cat_highlights = len([highlight for highlight in total_highlights if highlight["metadata"]["type"] == cat])
    print(f'"{cat}" highlights: {nr_cat_highlights}/{nr_total_highlights} ({round(float(nr_cat_highlights)/nr_total_highlights*100,1)}%)')  


  # Overview of number highlights for faets and categories
  print(f'\n\nNumber highlights (<HIGHLIGHT TYPE> - <FACET>): <NUMBER HIGHLIGHTS>\n----------------------------------')

  print_columns= []
  for facet in facets:
    facet_highlights = highlights[facet].values()
    nr_facet_highlights = len(facet_highlights)
    print_columns.append([f'<HIGHLIGHT_TYPE>: all', f'<FACET>: {facet}', f'<NUMBER HIGHLIGHTS>: {nr_facet_highlights}/{nr_total_highlights} ({round(float(nr_facet_highlights)/nr_total_highlights*100,1)}% of total highlights)' ])

    for cat in categories:
      nr_cat_highlights = len([highlight for highlight in facet_highlights if highlight["metadata"]["type"] == cat])
      print_columns.append([f'<HIGHLIGHT_TYPE>: {cat}', f'<FACET>: {facet}', f'<NUMBER HIGHLIGHTS>: {nr_cat_highlights}/{nr_facet_highlights} ({round(float(nr_cat_highlights)/nr_facet_highlights*100,1)}% of "{facet}" highlights)']) 

  for row in print_columns:
    print("{: <30} {: <20} {: <30}".format(*row))

  # Overview of highlight for facets and categories for each paper
  print(f'\n\n<PAPER ID>: Number highlights (<HIGHLIGHT TYPE> - <FACET>): <NUMBER HIGHLIGHTS>\n----------------------------------')

  print_columns= []
  for pid in data_json['highlights'].keys():
    pid_highlights = data_json['highlights'][pid].values()
    nr_pid_highlights = len(pid_highlights)
    
    for facet in facets:
      facet_highlights = [highlight for highlight in pid_highlights if highlight['facet'] == facet]
      nr_facet_highlights = len(facet_highlights)
      print_columns.append([f'<PAPER ID>: {pid}', f'<HIGHLIGHT_TYPE>: all', f'<FACET>: {facet}', f'<NUMBER HIGHLIGHTS>: {nr_facet_highlights}/{nr_pid_highlights} ({round(float(nr_facet_highlights)/nr_pid_highlights*100,1)}% of paper highlights)' ])

      for cat in categories:
        nr_cat_highlights = len([highlight for highlight in facet_highlights if highlight["metadata"]["type"] == cat])
        print_columns.append([f'<PAPER ID>: {pid}', f'<HIGHLIGHT_TYPE>: {cat}', f'<FACET>: {facet}', f'<NUMBER HIGHLIGHTS>: {nr_cat_highlights}/{nr_facet_highlights} ({round(float(nr_cat_highlights)/nr_facet_highlights*100,1)}% of paper "{facet}" highlights)']) 


  # Change sorting of overview highlights data to be written
  print_columns.sort(key= lambda row: (row[1], row[2]))
  for row in print_columns:
    print("{: <40} {: <30} {: <20} {: <30}".format(*row))


  # Generate data statistics for 'ratings'
  print("\n\n#############################")
  print("##### RATING STATISTICS #####")
  print("#############################")

  # Generic overview ratings
  print(f'Total entities rated: {len(ratings[facets[0]].keys())}')
  print(f'Total ratings: {len(ratings_arr)}')
  for facet in facets:
    print(f'Ratings entity as "{facet}": {len([rating for rating in ratings_arr if rating["facet"] == facet])}')

  print(f'Number of times people changed rating (changed mind or missclick): {len(ratings_raw_arr) - len(ratings_arr)}')


  # Overview of ratings for facets and categories
  print(f'\n\nAverage number ratings per entity (<HIGHLIGHT_TYPE> - <FACET>): <NUMBER_RATINGS>\n--------------------------------------------------------------------------------')

  # Calculate average number of ratings for each entity (per facet and highlight type)
  print_columns = []
  for facet in facets:
    print_columns.append([f'<HIGHLIGHT_TYPE>: all', f'<FACET>: {facet}', f'<NUMBER_RATINGS>: {round(float(sum([rating[2] for key, rating in ratings[facet].items()]))/len(ratings[facet].items()),2)}'])    


    for cat in categories:
      cat_ratings = [rating for key, rating in ratings[facet].items() if rating[3] == cat]
      print_columns.append([f'<HIGHLIGHT_TYPE>: {cat}', f'<FACET>: {facet}', f'<NUMBER_RATINGS>: {round(float(sum([rating[2] for rating in cat_ratings]))/len(cat_ratings),2)}'])    

  for row in print_columns:
    print("{: <30} {: <20} {: <30}".format(*row))


  # Overview of ratings for relevance and FPR
  print(f'\n\n<HIGHLIGHT_TYPE>: Entities (identified as <FACET>) rated as relevant for <FACET> based on voters majority vote: <RELEVANT>: <RELEVANT>/<TOTAL> (<PERCENTAGE>)')
  print(f'<HIGHLIGHT_TYPE>: False positives entities (identified as <FACET>) based on voters  majority vote: <FPR>: <FP>/<TOTAL> (<PERCENTAGE>)\n------------------------------------------------------------------------------------------------------------------------------------')

  print_columns= []
  for facet in facets:
    facet_highlight_ratings = [rating for key, rating in ratings[facet].items() if rating[4] in highlights[facet].keys()]
    nr_rel = sum([1 for rating in facet_highlight_ratings if rating[0] > rating[1]])
    nr_entities = len(facet_highlight_ratings)
    print_columns.append([f'<HIGHLIGHT_TYPE>: all', f'<FACET>: {facet}', f'<RELEVANT>: {nr_rel}/{nr_entities} ({round(float(nr_rel)/nr_entities*100,1)}%)', f'<FPR>: {nr_entities - nr_rel}/{nr_entities} ({round(float(nr_entities - nr_rel)/nr_entities*100,1)}%)'])    

    for cat in categories:
      cat_ratings = [rating for key, rating in ratings[facet].items() if rating[3] == cat]

      facet_highlight_ratings = [rating for rating in cat_ratings if rating[4] in highlights[facet].keys()]
      nr_rel = sum([1 for rating in facet_highlight_ratings if rating[0] > rating[1]])
      nr_entities = len(facet_highlight_ratings)

      print_columns.append([f'<HIGHLIGHT_TYPE>: {cat}', f'<FACET>: {facet}', f'<RELEVANT>: {nr_rel}/{nr_entities} ({round(float(nr_rel)/nr_entities*100,1)}%)', f'<FPR>: {nr_entities - nr_rel}/{nr_entities} ({round(float(nr_entities - nr_rel)/nr_entities*100,1)}%)'])    

  for row in print_columns:
    print("{: <30} {: <20} {: <30} {: <30}".format(*row))


  # Nr entities generated vs selected
  # Interannotator agreement generated vs selected
  # Nr. False positives for generated, Nr. False positives for selected
  # Total percentage relevant vs irrelevant

def process_ratings(ratings_json):
  ret_ratings_raw = []
  ret_ratings = []

  for pid in ratings_json.keys():
    pid_ratings = [rating for firebase_id, rating in ratings_json[pid].items()]
    pid_ratings.sort(key=lambda rating: (rating['facet'], rating['highlightId'], rating['uid']))
    ret_ratings_raw += pid_ratings


    groups = groupby(pid_ratings, lambda rating: (rating['facet'], rating['highlightId'], rating['uid']))

    for key1, group in groups:
      ret_ratings.append(max(list(group), key=lambda rating: rating['version']))
      
  return ret_ratings_raw, ret_ratings


# Write the array of highlights to json file
def write_rewards_json(rewards):
  file_content = json.dumps(rewards, indent=2)
  file_path = f'/data/rewards/firebase-{data_date}-rewards.json'
  os.makedirs(os.path.dirname(ROOTPATH + file_path), exist_ok=True)

  with open(ROOTPATH + file_path, 'w+') as file:
    file.write(file_content)

  len_rewards = len(rewards)
  
  print(f'Wrote {len_rewards} rewards (JSON) to {file_path}')

if __name__=='__main__':
  main()
