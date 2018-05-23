

# @author Daniel Vliegenthart
# Generate overview JSON of rewards
import json
import time
import os
from config import ROOTPATH, viewer_pids, data_date

# ################### #
#      SETUP ARGS     #
# ################### #

def main():

  # ########################################### #
  #      GENERATE OVERVIEW FILES FOR HAS PDF    #
  # ########################################### #

  data_string = open(f'data/firebase-{data_date}-coner-viewer-export.json').read()
  data_json = json.loads(data_string)

  rewards_json = data_json['rewards']
  rewards = []
  
  for pid in rewards_json.keys():
    for reward_id, transaction in rewards_json[pid].items():
      transaction['firebase_id'] = reward_id
      transaction['timestamp_formatted'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(transaction['timestamp']))
      rewards.append(transaction)

  write_rewards_json(rewards)

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
