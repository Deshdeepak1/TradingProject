import json
import os
import uuid
from dataclasses import asdict, dataclass

import aiofiles
import aiofiles.os
from dask import dataframe as dd
from django.conf import settings
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render

from . import forms

# Create your views here.


@dataclass
class Candle:
    id: int
    banknifty: str
    date: int
    time: str
    open: float
    high: float
    low: float
    close: float
    volume: int


async def read_csv(csv_file_name):
    csv_file_path = os.path.join(settings.MEDIA_ROOT, csv_file_name)

    dtype = {
        "BANKNIFTY": str,
        "DATE": int,
        "TIME": str,
        "OPEN": float,
        "HIGH": float,
        "LOW": float,
        "CLOSE": float,
        "VOLUME": int,
    }

    df = dd.read_csv(csv_file_path, dtype=dtype)
    return df


async def get_in_timeframe(df, timeframe):
    timeframe_df = df.head(timeframe)
    candles: list[Candle] = []
    for i, record in timeframe_df.iterrows():
        banknifty = record["BANKNIFTY"]
        date = record["DATE"]
        time = record["TIME"]
        open = record["OPEN"]
        high = record["HIGH"]
        low = record["LOW"]
        close = record["CLOSE"]
        volume = record["VOLUME"]
        candle = Candle(i, banknifty, date, time, open, high, low, close, volume)
        candles.append(candle)

    banknifty = candles[0].banknifty
    date = candles[0].date
    time = candles[0].time
    open = candles[0].open
    high = max(candles, key=lambda candle: candle.high).high
    low = min(candles, key=lambda candle: candle.low).low
    close = candles[-1].close
    volume = candles[-1].volume

    candle = Candle(
        timeframe + 1, banknifty, date, time, open, high, low, close, volume
    )
    candle_dict = asdict(candle)
    return candle_dict


async def save_csv_file(csv_file):
    csv_file_name = str(uuid.uuid4()) + ".csv"
    await aiofiles.os.makedirs(settings.MEDIA_ROOT, exist_ok=True)
    csv_file_path = os.path.join(settings.MEDIA_ROOT, csv_file_name)
    async with aiofiles.open(csv_file_path, "wb") as file:
        for chunk in csv_file.chunks():
            await file.write(chunk)

    return csv_file_name


async def save_json(csv_file_name: str, candle_dict: dict):
    json_file_name = csv_file_name.replace(".csv", ".json")
    json_file_path = os.path.join(settings.MEDIA_ROOT, json_file_name)
    async with aiofiles.open(json_file_path, "w") as file:
        await file.write(json.dumps(candle_dict))
    return json_file_name


async def hello(request: HttpRequest):
    form = forms.CsvForm()
    json_file_name = ""
    if request.method == "POST":
        form = forms.CsvForm(request.POST, request.FILES)
        if form.is_valid():
            csv_file = form.cleaned_data["csv_file"]
            timeframe = form.cleaned_data["timeframe"]
            csv_file_name = await save_csv_file(csv_file)
            df = await read_csv(csv_file_name)
            candle_dict = await get_in_timeframe(df, timeframe)
            json_file_name = await save_json(csv_file_name, candle_dict)
    return render(
        request, "index.html", context={"form": form, "json_file": json_file_name}
    )
