# -*- coding: utf-8 -*-
# @File  : data_process.py
# @Author: LVFANGFANG
# @Date  : 2021/3/4 8:46
# @Desc  :

import json
import os
import zipfile
from collections import defaultdict

from tqdm import tqdm

processed_data_dir = './data/crosswoz/processed'


def read_zipped_json(filepath):
    with zipfile.ZipFile(filepath) as myzip:
        for filename in myzip.namelist():
            with myzip.open(filename) as myfile:
                return json.load(myfile)


def process():
    os.makedirs(processed_data_dir, exist_ok=True)
    for key in ('train', 'val', 'test'):
        data = read_zipped_json(os.path.join(os.path.dirname(processed_data_dir), f'{key}.json.zip'))
        dialogues = []
        for dialogue_id in tqdm(data, desc=f'processing {key} dataset'):
            item = data[dialogue_id]
            turns = []
            belief_state = []
            system_transcript = "对话开始"
            for turn_id, raw_turn in enumerate(item['messages']):
                if raw_turn["role"] == "sys":
                    system_transcript = raw_turn["content"]
                    dialog_act = raw_turn["dialog_act"]
                    for da in dialog_act:
                        intent, domain, slot, value = da
                        if intent not in ('Inform', 'Request'):
                            continue
                        if intent == 'Request':
                            label = [domain, "Request", slot]
                        else:
                            label = [domain, slot, value]
                        if value == '否':
                            if {
                                "slots": label[:2] + ['是'],
                                "act": intent
                            } in belief_state:
                                belief_state.remove({
                                    "slots": label[:2] + ['是'],
                                    "act": intent
                                })
                else:
                    transcript = raw_turn["content"]
                    dialog_act = raw_turn["dialog_act"]
                    turn_label = []
                    state = belief_state.copy()
                    for da in dialog_act:
                        intent, domain, slot, value = da
                        if intent not in ('Inform', 'Request'):
                            continue
                        if intent == 'Request':
                            label = [domain, "Request", slot]
                        else:
                            label = [domain, slot, value]
                            belief_state.append({
                                "slots": label,
                                "act": intent
                            })
                        turn_label.append(label)
                        state.append({
                            "slots": label,
                            "act": intent
                        })
                    turn = {
                        "system_transcript": system_transcript,
                        "belief_state": state,
                        "turn_id": turn_id,
                        "transcript": transcript,
                        "turn_label": turn_label
                    }
                    turns.append(turn)
            dialogue = {
                "turns": turns,
                "dialogue_id": dialogue_id
            }
            dialogues.append(dialogue)
        result = {"dialogues": dialogues}
        with open(os.path.join(processed_data_dir, f'{key}.json'), 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)


def generate_ontology():
    values = defaultdict(set)
    for key in ('train', 'val', 'test'):
        with open(os.path.join(processed_data_dir, f'{key}.json')) as f:
            data = json.load(f)
        for dialogue in data['dialogues']:
            for turn in dialogue['turns']:
                belief_state = turn['belief_state']
                turn_label = turn['turn_label']
                for state in belief_state:
                    domian, slot, value = state['slots']
                    values[domian + '-' + slot].add(value)
                for label in turn_label:
                    domian, slot, value = label
                    values[domian + '-' + slot].add(value)
    slots = list(values.keys())
    values = {key: list(values[key]) for key in values}
    result = {
        "slots": slots,
        "values": values
    }
    with open(os.path.join(processed_data_dir, 'ontology.json'), 'w') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)


if __name__ == '__main__':
    process()
    generate_ontology()
