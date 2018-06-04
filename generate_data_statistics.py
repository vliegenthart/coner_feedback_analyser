# @author Daniel Vliegenthart
# Generate data statistics for firebase json export

import json
import time
import os
import os.path
from config import ROOTPATH, viewer_pids, data_date, facets, thres_min_ratings, thres_max_rating_time
from util_functions import read_json_file
from colorama import Fore, Back, Style
from itertools import groupby
from lib.sliding_window import sliding_window
import datetime
import csv
import re

# TODO
# - [X} Average entity rating time per paper, user and total
# - [X] Total nr. ratings per user
# - [X} #ratings difference for selected and generated (and why!)
# - Interannotator agreement generated vs selected
# - [x] Average entity rating time per user and total/per paper
# - [X] Nr entities generated vs selected
# - [X] Nr. False positives for generated, Nr. False positives for selected
# - [X] Total percentage relevant vs irrelevant
# - WRITE: General statistics file with sorting settings printed
# - Clean up code & add comments & split up each processing & writing category for highlights, entities, ratings and users
#   in separate methods and/or files

# WRITE
# All entities with ratings 1 file per facet
# 1 file relevant for X with all entities and label method or dataset with in title the setting (majority vote at least 2 evaluators)
# 1 file irrelevant for X with all entities
# per facet file: all assets rated for that facet as relevant or irrelevant with in title the voting setting

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
  print_file("\n\n-------------------------------------------------")
  print_file(f'-     {"{0:%Y-%m-%d %H:%M:%S}".format(datetime.datetime.now())} - DATA STATISTICS     -')
  print_file("-------------------------------------------------")

  # ############################################################## #
  #      PROCESS DATA FOR RATINGS, HIGHLIGHTS, ENTITIES AND USERS  #
  # ############################################################## #

  data_json = read_json_file(f'data/firebase-{data_date}-coner-viewer-export.json')
  paper_ids = data_json['highlights'].keys()

  # Perform some preprocessing and formatting for data attributes
  for key in data_json.keys():
    for pid in data_json[key].keys():
      for firebase_id, data_obj in data_json[key][pid].items():
        if not type(data_obj) is dict: continue
        if 'timestamp' in data_obj.keys(): data_obj['timestamp_formatted'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(data_obj['timestamp']))

        data_obj['firebase_id'] = firebase_id
  

  # Process highlights results
  for pid in paper_ids:

    for highlight_id, highlight in data_json['highlights'][pid].items():
      facet = highlight['metadata']['facet']
      highlight['facet'] = facet
      highlight['id'] = highlight_id
      if not highlight_id in highlights[facet].keys(): highlights[facet][highlight_id] = highlight


  total_highlights = []
  total_highlights_obj = {}
  for facet in facets:
    total_highlights += highlights[facet].values()
    total_highlights_obj.update(highlights[facet])

  total_highlights.sort(key=lambda highlight: highlight['metadata']['type'])

  # Process ratings results
  ratings_raw_arr, ratings_arr = process_ratings(data_json['ratings'])

  for facet in facets:
    for rating in [obj for obj in ratings_arr if obj['facet'] == facet]:

      entity = re.sub(" +", " ", total_highlights_obj[rating['highlightId']]['content']['text'].strip(" \t,-.[]()").lower())

      if not entity in ratings[facet].keys(): ratings[facet][entity] = [0,0,0, rating['highlightType'], rating['highlightId'], rating['pid']]

      ratings[facet][entity][2] += 1
      ratings[facet][entity][relevance_ENUM[rating['relevance']]] += 1

    write_entities_overview_csv(facet, ratings[facet].items())
    # Print entities with ratings in colors
    # for key, rating in sorted(ratings[facet].items()):
    #   rel = rating[0]
    #   total = rating[2]

    #   print_str = f'{key}: {rel}/{total} - {rating[3]}'
    #   rel_score = float(rel)/total

    #   if total > 1:
    #     if rel_score > 0.5: print(Fore.GREEN, print_str, Style.RESET_ALL, end='\t')
    #     if rel_score < 0.5: print(Fore.RED, print_str, Style.RESET_ALL, end='\t')
    #   else:
    #     print(Fore.BLUE, print_str, Style.RESET_ALL, end='\t')

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


  for uid, user_ratings in list(users_ratings.items()):
    if len(user_ratings) < thres_min_ratings:
      users_ratings.pop(uid, None)
      users.pop(uid, None)
      continue

    user_ratings.sort(key=lambda rating: rating['timestamp'])

  users_ratings_time = { uid : list() for uid in users.keys() }
  users_both_facets = { uid : 0 for uid in users.keys() }

  papers_ratings_time = { pid : list() for pid in paper_ids }
  papers_both_facets = { pid : 0 for pid in paper_ids }


  for uid, user_ratings in users_ratings.items():
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
        highlights_rated.add(rat2['highlightId'])
        users_both_facets[uid]+=1
        papers_both_facets[rat2['pid']]+=1

      users_ratings_time[uid].append(time_diff)
      papers_ratings_time[rat2['pid']].append(time_diff)

  # Calculate by taking the time off all ratings, and normalizing for the amount of entities that have been rated twice (for both facets),
  # because we want time to rate each entity (thus each highlight, because highlights for samen entity text greyed out after both facets rated) instead of time each rating click
  # Average for all 10 users: 18.6 seconds per entity
  # !!! CHECKKKK

  # [<Average per rating>, <Average per entity>, <max>, <min>]
  users_ratings_overview = { uid : [0, 0, 0, 0] for uid in users.keys() }
  papers_ratings_overview = { pid : [0, 0, 0, 0] for pid in paper_ids }

  for uid, times in users_ratings_time.items():
    per_rating = round(float(sum(times))/len(times), 1)
    double_highlight_ratio = float(len(times))/float(users_both_facets[uid])
    per_entity = round(float(sum(times))/float((len(times)/float(double_highlight_ratio))), 1)

    users_ratings_overview[uid] = [per_rating, per_entity, max(times), min(times)]

  for pid, times in papers_ratings_time.items():
    per_rating = round(float(sum(times))/len(times), 1)
    double_highlight_ratio = float(len(times))/float(papers_both_facets[pid])
    per_entity = round(float(sum(times))/float((len(times)/float(double_highlight_ratio))), 1)

    papers_ratings_overview[pid] = [per_rating, per_entity, max(times), min(times)]


  # ####################################################################### #
  #      WRITE DATA STATISTICS FOR RATINGS, HIGHLIGHTS, ENTITIES AND USERS  #
  # ####################################################################### #

  # Generate data statistics for 'highlights'
  print_file("\n\n################################")
  print_file("##### HIGHLIGHTS STATISTICS #####")
  print_file("################################")

  nr_total_highlights = len(total_highlights)

  # Generic overview highlights
  print_file(f'Total highlights: {nr_total_highlights}')  

  for cat in categories:
    nr_cat_highlights = len([highlight for highlight in total_highlights if highlight["metadata"]["type"] == cat])
    print_file(f'"{cat}" highlights: {nr_cat_highlights}/{nr_total_highlights} ({round(float(nr_cat_highlights)/nr_total_highlights*100,1)}%)')  

  # Generate data statistics for 'entities'
  print_file("\n\n###############################")
  print_file("##### ENTITIES STATISTICS #####")
  print_file("###############################")

  nr_total_entities = len(total_entities)

  # Generic overview highlights
  print_file(f'Total entities: {nr_total_entities}')  

  for cat in categories:
    nr_cat_entities = len([entity for entity in total_entities if entity['type'] == cat])
    print_file(f'"{cat}" entities: {nr_cat_entities}/{nr_total_entities} ({round(float(nr_cat_entities)/nr_total_entities*100,1)}%)')  

  # Overview of number entities for faets and categories
  print_file(f'\n\nNumber entities (<ENTITY TYPE> - <FACET>): <NUMBER ENTITIES>\n------------------------------------------------------------')

  header = [f'<ENTITY TYPE>', f'<FACET>', f'<NUMBER ENTITIES>']
  print_file("{: <20} {: <20} {: <30}".format(*header))

  table_data= []
  for facet in facets:
    facet_entities = entities[facet].values()
    nr_facet_entities = len(facet_entities)
    table_data.append([f'all', f'{facet}', f'{nr_facet_entities}/{nr_total_entities} ({round(float(nr_facet_entities)/nr_total_entities*100,1)}% of total entities)' ])

    for cat in categories:
      nr_cat_entities = len([entity for entity in facet_entities if entity['type'] == cat])
      table_data.append([f'{cat}', f'{facet}', f'{nr_cat_entities}/{nr_facet_entities} ({round(float(nr_cat_entities)/nr_facet_entities*100,1)}% of "{facet}" entities)']) 

  for row in table_data:
    print_file("{: <20} {: <20} {: <30}".format(*row))

  # Overview of entity for facets and categories for each paper
  print_file(f'\n\n<PAPER ID>: Number entities (<ENTITY TYPE> - <FACET>): <NUMBER ENTITIES>\n------------------------------------------------------------------------')

  header = [f'<PAPER ID>', f'<ENTITY TYPE>', f'<FACET>', '<NUMBER ENTITIES>']
  print_file("{: <30} {: <20} {: <20} {: <30}".format(*header))

  table_data= []
  for pid in paper_ids:

    pid_entities = [entity for entity in total_entities if entity['paper_id'] == pid]
    nr_pid_entities = len(pid_entities)
    
    for facet in facets:
      facet_entities = [entity for entity in pid_entities if entity['highlight_id'] in highlights[facet].keys()]
      nr_facet_entities = len(facet_entities)
      table_data.append([f'{pid}', f'all', f'{facet}', f'{nr_facet_entities}/{nr_pid_entities} ({round(float(nr_facet_entities)/nr_pid_entities*100,1)}% of paper entities)' ])

      for cat in categories:
        nr_cat_entities = len([entity for entity in facet_entities if entity['type'] == cat])
        table_data.append([f'{pid}', f'{cat}', f'{facet}', f'{nr_cat_entities}/{nr_facet_entities} ({round(float(nr_cat_entities)/nr_facet_entities*100,1)}% of paper "{facet}" entities)']) 


  # Change sorting of overview entities data to be written
  table_data.sort(key=lambda row: (row[1], row[2]))
  for row in table_data:
    print_file("{: <30} {: <20} {: <20} {: <30}".format(*row))


  # Generate data statistics for 'ratings'
  print_file("\n\n#############################")
  print_file("##### RATING STATISTICS #####")
  print_file("#############################")

  # Generic overview ratings
  print_file(f'Total entities rated: {len(ratings[facets[0]].keys())}')
  print_file(f'Total ratings: {len(ratings_arr)}')
  for facet in facets:
    print_file(f'Ratings entity as "{facet}": {len([rating for rating in ratings_arr if rating["facet"] == facet])}')

  print_file(f'Number of times people changed rating (changed mind or missclick): {len(ratings_raw_arr) - len(ratings_arr)}')


  # Overview of ratings for facets and categories
  print_file(f'\n\nAverage number ratings per entity (<HIGHLIGHT TYPE> - <FACET>): <NUMBER RATINGS>\n--------------------------------------------------------------------------------')

  header = [f'<HIGHLIGHT TYPE>', f'<FACET>', f'<NUMBER RATINGS>']
  print_file("{: <20} {: <20} {: <30}".format(*header))

  # Calculate average number of ratings for each entity (per facet and ENTITY TYPE)
  table_data = []
  for facet in facets:
    table_data.append([f'all', f'{facet}', f'{round(float(sum([rating[2] for key, rating in ratings[facet].items()]))/len(ratings[facet].items()),2)}'])    


    for cat in categories:
      cat_ratings = [rating for key, rating in ratings[facet].items() if rating[3] == cat]
      table_data.append([f'{cat}', f'{facet}', f'{round(float(sum([rating[2] for rating in cat_ratings]))/len(cat_ratings),2)}'])    

  for row in table_data:
    print_file("{: <20} {: <20} {: <30}".format(*row))


  # Overview of ratings for relevance and FPR
  print_file(f'\n\n<HIGHLIGHT TYPE>: Entities (identified as <FACET>) rated as relevant for <FACET> based on voters majority vote: <RELEVANT>: <RELEVANT>/<TOTAL> (<PERCENTAGE>)')
  print_file(f'<HIGHLIGHT TYPE>: False positives entities (identified as <FACET>) based on voters  majority vote: <FPR>: <FP>/<TOTAL> (<PERCENTAGE>)\n-------------------------------------------------------------------------------------------------------------------------------------')

  header = [f'<HIGHLIGHT TYPE>', f'<FACET>', f'<RELEVANT>', f'<FPR>']
  print_file("{: <20} {: <20} {: <30} {: <30}".format(*header))

  table_data = []
  for facet in facets:
    facet_highlight_ratings = [rating for key, rating in ratings[facet].items() if rating[4] in highlights[facet].keys()]
    nr_rel = sum([1 for rating in facet_highlight_ratings if rating[0] > rating[1]])
    nr_entities = len(facet_highlight_ratings)
    table_data.append([f'all', f'{facet}', f'{nr_rel}/{nr_entities} ({round(float(nr_rel)/nr_entities*100,1)}%)', f'{nr_entities - nr_rel}/{nr_entities} ({round(float(nr_entities - nr_rel)/nr_entities*100,1)}%)'])    

    for cat in categories:
      cat_ratings = [rating for key, rating in ratings[facet].items() if rating[3] == cat]

      facet_highlight_ratings = [rating for rating in cat_ratings if rating[4] in highlights[facet].keys()]
      nr_rel = sum([1 for rating in facet_highlight_ratings if rating[0] > rating[1]])
      nr_entities = len(facet_highlight_ratings)

      table_data.append([f'{cat}', f'{facet}', f'{nr_rel}/{nr_entities} ({round(float(nr_rel)/nr_entities*100,1)}%)', f'{nr_entities - nr_rel}/{nr_entities} ({round(float(nr_entities - nr_rel)/nr_entities*100,1)}%)'])    

  for row in table_data:
    print_file("{: <20} {: <20} {: <30} {: <30}".format(*row))


  # Generate data statistics for 'users'
  print_file("\n\n#############################")
  print_file("##### USER STATISTICS #####")
  print_file("#############################")

  print_file(f'Total users: {len(users)}')
  print_file(f'Users\' average rating time: {float(sum([int(user[0]) for key, user in users_ratings_overview.items()]))/len(users.keys())}')
  print_file(f'Users\' average entity rating time: {float(sum([int(user[1]) for key, user in users_ratings_overview.items()]))/len(users.keys())}')

  print_file(f'\n\n<EMAIL> has given <NR RATINGS> ratings for <NR ENTITIES> entities in <NR PAPERS> papers with times (seconds): <AVG RATING TIME>, <AVG ENTITY TIME>, <MAX RATING TIME>, <MIN RATING TIME>\n----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------')
  
  header = ['<EMAIL>', '<NR RATINGS>', '<NR ENTITIES>', '<NR PAPERS>', '<AVG RATING TIME>', '<AVG ENTITY TIME>', '<MAX RATING TIME>', '<MIN RATING TIME>']
  print_file("{: <45} {: <20} {: <20} {: <20} {: <20} {: <20} {: <20} {: <20}".format(*header))

  table_data = []

  for uid, user_ratings in users_ratings.items():
    nr_entities = len(set([total_highlights_obj[rating['highlightId']]['content']['text'] for rating in user_ratings]))
    nr_papers = len(set([rating['pid'] for rating in user_ratings]))
    table_data.append([users[uid]["email"], len(user_ratings), nr_entities, nr_papers] + users_ratings_overview[uid])

  table_data.sort(key=lambda row: int(row[2]), reverse=True)

  for row in table_data:
    print_file("{: <45} {: <20} {: <20} {: <20} {: <20} {: <20} {: <20} {: <20}".format(*row))


  print_file(f'\n\n<PAPERID>: <AVG RATING TIME> time (seconds) and <AVG ENTITY TIME> time (seconds)\n--------------------------------------------------------------------------------')

  header = ['<PAPER_ID>', '<AVG RATING TIME>', '<AVG ENTITY TIME>']
  print_file("{: <30} {: <20} {: <20}".format(*header))

  table_data = []
  for pid, paper_ratings in papers_ratings_overview.items(): table_data.append([pid] + paper_ratings[0:2])
  
  table_data.sort(key=lambda row: int(row[1]), reverse=True)

  for row in table_data:
    print_file("{: <30} {: <20} {: <20}".format(*row))

  print(f'Wrote data statistics to "results/data_statistics_{data_date.replace("-", "_")}.txt"')

def write_entities_overview_csv(facet, entities):
  column_names = ['entity', 'relevance', 'relevance_score', 'ratings_relevant', 'ratings_total', 'type']
  file_path = f'results/entities_overview_{facet}_{data_date.replace("-", "_")}.csv'
  os.makedirs(os.path.dirname(file_path), exist_ok=True)

  with open(file_path, 'w+') as outputFile:

    csv_out=csv.writer(outputFile)
    csv_out.writerow(column_names)
    
    for entity_text, entity_info in sorted(entities):
      rel = entity_info[0]
      total = entity_info[2]
      rel_score = round(float(rel)/total, 2)

      temp = [entity_text, get_relevance(rel_score, total), rel_score, rel, total, entity_info[3]]
      csv_out.writerow(temp)

    print(f'Wrote {facet} entity ratings overview to "results/entities_overview_{facet}_{data_date.replace("-", "_")}.csv"')

# Relevance is based on majority vote is 2 or more ratings for facet
def get_relevance(score, total):
  if total < 2: return 'neutral'

  if score > 0.5: return 'relevant'
  if score < 0.5: return 'irrelevant'

  return 'neutral'

def print_file(line):

  with open(f'results/data_statistics_{data_date.replace("-", "_")}.txt', 'a') as out:
    out.write(line + '\n')

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
