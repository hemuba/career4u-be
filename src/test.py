from datetime import datetime
import logging
import logging.config
import json
import os


logging_config = "logging.ini]"
if os.path.exists(logging_config):
    logging.config.fileConfig("logging.ini")
else:
    print(f"ERROR: cannot use {logging_config} file as logger configuration: it does not exists.")


logger = logging.getLogger(__name__)


competencies_ale = {}
with open("resources/alessandro_competencies.json", "r", encoding="utf-8") as alessando_competencies_file:
    data = json.load(alessando_competencies_file)
    for k, v in data.items():
        competencies_ale[k] = v

levels = {1: "beginner", 2: "intermediate", 3: "advanced"}

competencies_to_action = {}
with open("resources/competencies_to_action.json", "r", encoding="utf-8") as json_competencies_file:
    data = json.load(json_competencies_file)
    for k, v in data.items():
        competencies_to_action[k] = v

def ask_yn(question: str) -> bool | None:
    ans = input(f"{question} (1 to yes, 0 to no)? ")
    if ans.strip() == "1":
        return True
    elif ans.strip() == "0":
        return False
    else:
        return None

liked = {}      
for k, v in competencies_ale.items():
    liked_vals = []
    print(f"Checking competence group: {k}")
    for comp, val in v:
        question = f"Do you like competence {comp} "
        res = ask_yn(question)
        if res is True:
            liked_vals.append((comp, val))
        elif res is None:
            print(f"Warning: answer should be 1 (liked) or 0 (not-liked) for competence: {comp} is not valid, skipped ")
            logger.warning(f"answer should be 1 (liked) or 0 (not-liked) for competence: {comp} is not valid, skipped")
            continue
    liked[k] = liked_vals
            

market_valuable = {}

for k, v in liked.items():
    market_valuable_vals = []
    print(f"Checking liked competencies in group: {k}")
    for comp, val in v:
        question = f"Do you think people can spend money on competence {comp}?"
        res = ask_yn(question)
        if res is True:
            market_valuable_vals.append((comp, val))
        elif res is None:
            print(f"Warning: answer should be 1 (marketable) or 0 (not-marketable) for competence: {comp} is not valid, skipped ")
            logger.warning(f"answer should be 1 (marketable) or 0 (not-marketable) for competence: {comp} is not valid, skipped")
            continue
            
    market_valuable[k] = market_valuable_vals
    

with open("resources/competenze.txt", "w", encoding="utf-8") as wf:
    wf.write(f"Alessandro competences inventory: liked and marketable {datetime.now().date()}\n\n")
    for k, v in market_valuable.items():
        for comp, val in v:
            wf.write(f"\t{k}:  competence: {comp} - level: {levels[val]} - action: {competencies_to_action.get(comp,  "No action added to the compentence.")}\n")
        wf.write("\n")


        

