from fastapi import FastAPI, HTTPException
from typing import List
import pandas as pd
import os

app = FastAPI(title="Recommendation Service")

OFFLINE_PATH = "recommendationsPERSONAL.parquet"
ONLINE_PATH = "events.parquet"

if not os.path.exists(OFFLINE_PATH) or not os.path.exists(ONLINE_PATH):
    raise RuntimeError("Не найдены файлы recommendationsPERSONAL.parquet или events.parquet")

offline_df = pd.read_parquet(OFFLINE_PATH)
online_df = pd.read_parquet(ONLINE_PATH)

def mix_recommendations(user_id: int) -> List[int]:
    offline_items = (
        offline_df[offline_df["user_id"] == user_id]
        .sort_values("final_score", ascending=False)
        .head(20)["item_id"]
        .tolist()
    )

    online_items = (
        online_df[online_df["user_id"] == user_id]
        .sort_values("event_timestamp", ascending=False)
        .head(10)["item_id"]
        .tolist()
    )

    if len(offline_items) == 0 and len(online_items) == 0:
        return ["default_1", "default_2", "default_3"]

    if len(offline_items) > 0 and len(online_items) == 0:
        return offline_items

    if len(offline_items) == 0 and len(online_items) > 0:
        return online_items

    n_off = max(1, int(len(offline_items) * 0.7))
    n_on = max(1, int(len(online_items) * 0.3))

    mixed = offline_items[:n_off] + online_items[:n_on]
    seen = set()
    unique = [x for x in mixed if not (x in seen or seen.add(x))]
    return unique


@app.get("/recommendations")
def get_recommendations(user_id: int):
    try:
        recs = mix_recommendations(user_id)
        return {"user_id": user_id, "recommendations": recs}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
