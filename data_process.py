# -*- coding: utf-8 -*-
# @File  : data_process.py
# @Author: LVFANGFANG
# @Date  : 2021/3/4 8:46
# @Desc  :

import json
import os
import zipfile
from tqdm import tqdm, trange


def read_zipped_json(filepath):
    with zipfile.ZipFile(filepath) as myzip:
        for filename in myzip.namelist():
            with myzip.open(filename) as myfile:
                return json.load(myfile)


def process():
    processed_data_dir = './data/crosswoz/processed'

    os.makedirs(processed_data_dir, exist_ok=True)
    for key in ('train', 'val', 'test'):
    # for key in ('test',):
        data = read_zipped_json(os.path.join(os.path.dirname(processed_data_dir), f'{key}.json.zip'))
        dialogues = []
        for dialogue_id in tqdm(data,desc=f'processing {key} dataset'):
            item = data[dialogue_id]
            turns = []
            belief_state = []
            system_transcript = "对话开始"  # TODO change to Chinese
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
                    # dialog_act -> turn_label
                    # user_state -> belief_state
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


if __name__ == '__main__':
    process()
