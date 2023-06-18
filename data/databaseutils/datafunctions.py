import json


def get_field_list_from_json(item_name):
    with open(f"UFCStatsScraper/itemfieldlists/{item_name.lower().replace('item','')}_fields.json", 'r') as f:
        fieldlist = []
        fielddict = json.load(f)
        for cat in fielddict.keys():
            for key in fielddict[cat].keys():
                fieldlist.append(key)
        return fieldlist