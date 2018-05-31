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

# Rating attribute: <entity_text> = [<#relevant>, <#irrelevant>, <#ratings>]
ratings = { "dataset": {}, "method": {} }

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

  # Generate data statistics for 'ratings'
  ratings_raw_arr, ratings_arr = process_ratings(data_json['ratings'])
  print(len(ratings_raw_arr), len(ratings_arr))

  for facet in facets:
    for rating in [obj for obj in ratings_arr if obj['facet'] == facet]:

      entity = rating['entityText'].strip(" \t,-.[]()").lower()

      if not entity in ratings[facet].keys(): ratings[facet][entity] = [0,0,0, rating['highlightType']]

      ratings[facet][entity][2] += 1
      ratings[facet][entity][relevance_ENUM[rating['relevance']]] += 1


    print(f'\n\n##### {facet}: RELEVANT #####\n\n')
    for key, rating in sorted(ratings[facet].items()):
      rel = rating[0]
      total = rating[2]

      print_str = f'{key}: {rel}/{total} - {rating[3]}'
      rel_score = float(rel)/total

      if total > 1:
        if rel_score > 0.5: print(Fore.GREEN, print_str, Style.RESET_ALL)
        if rel_score < 0.5: print(Fore.RED, print_str, Style.RESET_ALL)
      else:
        print(Fore.BLUE, print_str, Style.RESET_ALL)

  print("\n\n##### RATING STATISTICS #####")
  print(f'Total entities rated: {len(ratings[facets[0]].keys())}')
  print(f'Total ratings: {len(ratings_arr)}')
  print(f'Number of times people changed rating: {len(ratings_raw_arr) - len(ratings_arr)}')

  for facet in facets:
    print(f'Average number ratings per entity ({facet}): {round(float(sum([rating[2] for key, rating in ratings[facet].items()]))/len(ratings[facet]),2)}')

  # Take rating versioning into account!!!
  # Average Nr ratings generated vs selected
  # Nr highlights generated vs selected
  # Interannotator agreement generated vs selected
  # How often did people change their mind or missclick (check version numbers and how many)

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
