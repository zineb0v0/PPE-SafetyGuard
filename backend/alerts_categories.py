CLASS_NAMES = {
    0: 'boots',
    1: 'gloves',
    2: 'goggles',
    3: 'helmet',
    4: 'no_boots',
    5: 'no_gloves',
    6: 'no_goggles',
    7: 'no_helmet',
    8: 'no_vest',
    9: 'person',
    10: 'vest',
}

# Category mapping: adjust these however you want
RED_CLASSES = [4, 5, 6, 7, 8]  # Absence of PPE = dangerous
YELLOW_CLASSES = [0, 1, 2, 3, 10]  # Correct PPE = normal
BLUE_CLASSES = [9]  # Just person – could be neutral

def get_danger_info(cls_id):
    if cls_id in RED_CLASSES:
        return {"color": (0, 0, 255), "category": "DANGER"}
    elif cls_id in YELLOW_CLASSES:
        return {"color": (0, 255, 255), "category": "SAFE"}
    elif cls_id in BLUE_CLASSES:
        return {"color": (255, 0, 0), "category": "PERSON"}
    else:
        return {"color": (255, 255, 255), "category": "UNKNOWN"}
