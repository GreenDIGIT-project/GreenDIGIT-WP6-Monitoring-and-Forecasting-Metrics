# topology_loader.py

import yaml


CONFIG_PATH = "/home/greendigit/app/greendigit-l3-iot-uth-deployment/config/nodes.yml"


with open(CONFIG_PATH, "r") as f:
    cfg = yaml.safe_load(f)


LINK_NODE_MAP = cfg["links"]


# -------------------------------------------------
# LINK DIRECTIONS
# -------------------------------------------------

# IMPORTANT:
# order in YAML:
# [AP, STA]
#
# AP -> STA = downlink
# STA -> AP = uplink
# -------------------------------------------------

LINK_DIRECTIONS = {}

for link_id, pair in LINK_NODE_MAP.items():

    ap = pair[0]
    sta = pair[1]

    LINK_DIRECTIONS[link_id] = {
        "downlink": (ap, sta),
        "uplink": (sta, ap)
    }


# -------------------------------------------------
# LINK → NODE RESOLUTION (NEW CRITICAL LAYER)
# -------------------------------------------------

def resolve_nodes(link_id: str, direction: str = "downlink"):

    if link_id not in LINK_DIRECTIONS:
        raise ValueError(f"Unknown link_id: {link_id}")

    if direction not in ("downlink", "uplink"):
        raise ValueError(f"Invalid direction: {direction}")

    return LINK_DIRECTIONS[link_id][direction]