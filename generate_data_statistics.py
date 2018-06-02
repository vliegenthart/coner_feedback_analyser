# @author Daniel Vliegenthart
# Generate data statistics for firebase json export

import json
import time
import os
from config import ROOTPATH, viewer_pids, data_date, facets, thres_min_ratings, thres_max_rating_time
from util_functions import read_json_file
from colorama import Fore, Back, Style
from itertools import groupby
from lib.sliding_window import sliding_window

# TODO
# - Average entity rating time per paper, user and total
# - Total nr. ratings per user
# - #ratings difference for selected and generated (and why!)

# ######################## #
#      SETUP VARIABLES     #
# ######################## #

# Highlight attribute: <key: highlight_id> = <value: <highlight attributes>>
highlights = { "dataset": {}, "method": {} }

entities = { "dataset": {}, "method": {} }


# Rating attribute: <key: entity_text> = <value: [<#relevant>, <#irrelevant>, <#ratings>, <highlight_type>, <highlight_id>, <paper_id>]>
ratings = { "dataset": {}, "method": {} }
categories = ['generated','selected']

relevance_ENUM = { 'relevant': 0, 'irrelevant': 1 }

def main():

  # ############################################################## #
  #      PROCESS DATA FOR RATINGS, HIGHLIGHTS, ENTITIES AND USERS  #
  # ############################################################## #

  data_json = read_json_file(f'data/firebase-{data_date}-coner-viewer-export.json')

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

      # if total > 1:
      #   if rel_score > 0.5: print(Fore.GREEN, print_str, Style.RESET_ALL, end='\t')
      #   if rel_score < 0.5: print(Fore.RED, print_str, Style.RESET_ALL, end='\t')
      # else:
      #   print(Fore.BLUE, print_str, Style.RESET_ALL, end='\t')

  # Process highlights results
  for pid in data_json['highlights'].keys():

    for highlight_id, highlight in data_json['highlights'][pid].items():
      facet = highlight['metadata']['facet']
      highlight['facet'] = facet
      highlight['id'] = highlight_id
      if not highlight_id in highlights[facet].keys(): highlights[facet][highlight_id] = highlight


  total_highlights = []
  for facet in facets:
    total_highlights += highlights[facet].values()

  total_highlights.sort(key=lambda highlight: highlight['metadata']['type'])

  # Process entities results
  for facet in facets:
    for highlight in [obj for obj in total_highlights if obj['facet'] == facet]:

      entity = highlight['content']['text'].strip(" \t,-.[]()").lower()

      if not entity in entities[facet].keys(): entities[facet][entity] = { 'type': highlight['metadata']['type'], 'highlight_id': highlight['id'], 'paper_id': highlight['pid'] }

  total_entities = []
  seen_entities = set() 

  for facet in facets:
    [seen_entities.add(key) or total_entities.append(obj) for key, obj in entities[facet].items() if key not in seen_entities]

  total_entities.sort(key=lambda entity: entity['type'])

  # Process users results
  users = {}
  for key, user in data_json['users'].items():
    user['id'] = key
    users[user['id']] = user

  users_ratings = { uid : list() for uid in users.keys() }

  for rating in ratings_arr:
    users_ratings[rating['uid']].append(rating)

  for uid, user_ratings in users_ratings.items():
    user_ratings.sort(key=lambda rating: rating['timestamp'])

  users_ratings_time = { uid : list() for uid in users.keys() if not len(users_ratings[uid]) < thres_min_ratings }
  users_ratings_time_raw = { uid : list() for uid in users.keys() if not len(users_ratings[uid]) < thres_min_ratings }

  for uid, user_ratings in users_ratings.items():
    both_facets = 0
    if len(user_ratings) < thres_min_ratings: continue

    highlights_rated = set([user_ratings[0]['highlightId']])
    rating_chunks = sliding_window(user_ratings,2)
            
    # Calculate time between each rating pair's timestamp (ratings sorted on earliest to latest timestamp) 
    # Only add entity rating time if rating time < max rating threshold (to see if different sessions, or other unaccounted for breaks) and pid the same
    for rating_pair in rating_chunks:
      rat1 = rating_pair[0]
      rat2 = rating_pair[1]
      time_diff = rat2['timestamp'] - rat1['timestamp']

      if not rat1['pid'] == rat2['pid'] or time_diff > thres_max_rating_time: continue

      if rat2['highlightId'] not in highlights_rated: 
        highlights_rated.add(rat2['highlightId']
        both_facets +=1

      users_ratings_time[uid].append(time_diff)

  # print("\n\nPER RATING CLICK")
  # for uid, times in users_ratings_time_raw.items():
  #   print(users[uid]['email'], round(float(sum(times))/len(times)), max(times), min(times))

  print("\n\nPER ENTITY")
  for uid, times in users_ratings_time.items():
    print(users[uid]['email'], round(float(sum(times))/(len(times)*(len(times)/2)))), max(times), min(times))

  return

  # ####################################################################### #
  #      WRITE DATA STATISTICS FOR RATINGS, HIGHLIGHTS, ENTITIES AND USERS  #
  # ####################################################################### #

  # Generate data statistics for 'highlights'
  print("\n\n################################")
  print("##### HIGHLIGHTS STATISTICS #####")
  print("################################")

  nr_total_highlights = len(total_highlights)


  # Generic overview highlights
  print(f'Total highlights: {nr_total_highlights}')  

  for cat in categories:
    nr_cat_highlights = len([highlight for highlight in total_highlights if highlight["metadata"]["type"] == cat])
    print(f'"{cat}" highlights: {nr_cat_highlights}/{nr_total_highlights} ({round(float(nr_cat_highlights)/nr_total_highlights*100,1)}%)')  

  # Generate data statistics for 'entities'
  print("\n\n###############################")
  print("##### ENTITIES STATISTICS #####")
  print("###############################")

  nr_total_entities = len(total_entities)

  # Generic overview highlights
  print(f'Total entities: {nr_total_entities}')  

  for cat in categories:
    nr_cat_entities = len([entity for entity in total_entities if entity['type'] == cat])
    print(f'"{cat}" entities: {nr_cat_entities}/{nr_total_entities} ({round(float(nr_cat_entities)/nr_total_entities*100,1)}%)')  

  # Overview of number entities for faets and categories
  print(f'\n\nNumber entities (<ENTITY TYPE> - <FACET>): <NUMBER entities>\n----------------------------------')

  print_columns= []
  for facet in facets:
    facet_entities = entities[facet].values()
    nr_facet_entities = len(facet_entities)
    print_columns.append([f'<ENTITY TYPE>: all', f'<FACET>: {facet}', f'<NUMBER entities>: {nr_facet_entities}/{nr_total_entities} ({round(float(nr_facet_entities)/nr_total_entities*100,1)}% of total entities)' ])

    for cat in categories:
      nr_cat_entities = len([entity for entity in facet_entities if entity['type'] == cat])
      print_columns.append([f'<ENTITY TYPE>: {cat}', f'<FACET>: {facet}', f'<NUMBER entities>: {nr_cat_entities}/{nr_facet_entities} ({round(float(nr_cat_entities)/nr_facet_entities*100,1)}% of "{facet}" entities)']) 

  for row in print_columns:
    print("{: <30} {: <20} {: <30}".format(*row))

  # Overview of entity for facets and categories for each paper
  print(f'\n\n<PAPER ID>: Number entities (<ENTITY TYPE> - <FACET>): <NUMBER entities>\n----------------------------------')

  print_columns= []
  for pid in data_json['highlights'].keys():

    pid_entities = [entity for entity in total_entities if entity['paper_id'] == pid]
    nr_pid_entities = len(pid_entities)
    
    for facet in facets:
      facet_entities = [entity for entity in pid_entities if entity['highlight_id'] in highlights[facet].keys()]
      nr_facet_entities = len(facet_entities)
      print_columns.append([f'<PAPER ID>: {pid}', f'<ENTITY TYPE>: all', f'<FACET>: {facet}', f'<NUMBER entities>: {nr_facet_entities}/{nr_pid_entities} ({round(float(nr_facet_entities)/nr_pid_entities*100,1)}% of paper entities)' ])

      for cat in categories:
        nr_cat_entities = len([entity for entity in facet_entities if entity['type'] == cat])
        print_columns.append([f'<PAPER ID>: {pid}', f'<ENTITY TYPE>: {cat}', f'<FACET>: {facet}', f'<NUMBER entities>: {nr_cat_entities}/{nr_facet_entities} ({round(float(nr_cat_entities)/nr_facet_entities*100,1)}% of paper "{facet}" entities)']) 


  # Change sorting of overview entities data to be written
  print_columns.sort(key=lambda row: (row[1], row[2]))
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

  # Calculate average number of ratings for each entity (per facet and ENTITY TYPE)
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


  # Generate data statistics for 'users'
  print("\n\n#############################")
  print("##### USER STATISTICS #####")
  print("#############################")

  print_columns = []

  for row in print_columns:
    print("{: <30} {: <20} {: <30}".format(*row))

  for uid, user_ratings in users_ratings.items():
    print(users[uid]['email'], len(user_ratings))


  # TODO
  # Interannotator agreement generated vs selected
  # Average entity rating time per user and total
  # [X] Nr entities generated vs selected
  # [X] Nr. False positives for generated, Nr. False positives for selected
  # [X] Total percentage relevant vs irrelevant

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

    ret_ratings_raw.sort(key=lambda rating: rating['highlightType'])
    ret_ratings.sort(key=lambda rating: rating['highlightType'])

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
