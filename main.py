import multiprocessing
from multiprocessing.managers import ValueProxy
import os
import threading
import time
from typing import List
import aiohttp
import asyncio
import pandas as pd
from datetime import datetime
from io import StringIO
from bs4 import BeautifulSoup
from utils import printProgressBar, save_to_sheet
from stocks import INDICES, fetch_stocks

target_cols = [
    "Ticker",
    "Industry",
    "Sector",
    "PE Ratio (Current Year Earnings Estimate)",
    "Implied Volatility (Puts) (90-Day)",
    "52-Week High Price",
    "52-Week Low Price",
    "Next Expected Quarterly Earnings Report Date",
    "Annual Dividend (Based on Last Quarter)",
]


async def fetch_volatility(
    symbol: str,
    session: aiohttp.ClientSession,
    shared_count: ValueProxy[int],
    count_lock: threading.Lock,
):
    """Fetches the volatility data for the given symbol"""

    url = f"https://www.alphaquery.com/stock/{symbol}/all-data-variables"

    async with session.get(url) as response:
        if response.status != 200:
            return None

        html = await response.text()
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table")

        if not table:
            return None

        # Wrap the HTML string in a StringIO object
        html_string_io = StringIO(str(table))
        df = pd.read_html(html_string_io)[0]

        # Iterate through each row in the DataFrame
        row_object = dict()
        for _, row in df.iterrows():
            try:
                row_object[row[0]] = float(row[1])
            except ValueError:
                row_object[row[0]] = row[1]
        row_object = dict(
            filter(
                lambda x: x[0] in target_cols,
                row_object.items(),
            )
        )
        row_object["time_stamp"] = datetime.now()

        # Create a new DataFrame from the row_object
        df = pd.DataFrame([row_object])

        # increment the shared count by accessing the lock
        count_lock.acquire()
        shared_count.value += 1
        printProgressBar(
            shared_count.value,
            631,
            prefix="Progress:",
            suffix="Complete",
            length=50,
        )
        count_lock.release()

        return df


async def fetch_lastclose(symbol: str, session: aiohttp.ClientSession):
    """Fetches the last close data for the given symbol"""

    start_timestamp = int(datetime(2023, 1, 1).timestamp())
    end_timestamp = int(datetime.now().timestamp())

    url = f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}?period1={start_timestamp}&period2={end_timestamp}&interval=1d&events=history&includeAdjustedClose=true"
    async with session.get(url) as response:
        # Ensure that the request was successful
        if response.status != 200:
            return None

        # Convert the response to a DataFrame
        df = pd.read_csv(StringIO(await response.text()))

        # Add the symbol column
        df["Ticker"] = symbol
        df["time_stamp"] = datetime.now()
        df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d").dt.date

        # Select only the Date and Close columns
        df = df[["Ticker", "Date", "Close", "time_stamp"]]

        # Rename the Close column to Last Close
        df = df.rename(columns={"Close": "Last Close"})

        return df


def process_chunk(args):
    chunk, shared_count, count_lock = args

    async def fetch_chunk():
        async with aiohttp.ClientSession() as session:
            volatlity_chunk: List[pd.DataFrame] = await asyncio.gather(
                *[
                    fetch_volatility(symbol, session, shared_count, count_lock)
                    for symbol in chunk
                ]
            )

            last_close_chunk: List[pd.DataFrame] = await asyncio.gather(
                *[fetch_lastclose(symbol, session) for symbol in chunk]
            )

        return volatlity_chunk, last_close_chunk

    return asyncio.run(fetch_chunk())


async def main(file_path: str):
    start_time = time.time()

    # Fetch stock symbols from indices
    async with aiohttp.ClientSession() as session:
        index_dfs = await asyncio.gather(
            *[fetch_stocks(index, session) for index in INDICES]
        )

    stocks_df = save_to_sheet(index_dfs, "stocks_data", file_path)
    stock_symbols = stocks_df["Symbol"].unique().tolist()

    # Create a multiprocessing pool to process the each chunk of stock symbols in parallel
    with multiprocessing.Manager() as manager:
        shared_count = manager.Value("i", 0)
        count_lock = manager.Lock()

        chunk_size = 10
        chunks = [
            (stock_symbols[i : i + chunk_size], shared_count, count_lock)
            for i in range(0, len(stock_symbols), chunk_size)
        ]
        with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
            results = pool.map(
                process_chunk,
                chunks,
            )

    print(f"Total time taken: {time.time() - start_time} seconds")

    volatility_data: List[pd.DataFrame] = []
    last_close_data: List[pd.DataFrame] = []
    for volatlity_chunk, lastclose_chunk in results:
        volatility_data.extend(volatlity_chunk)
        last_close_data.extend(lastclose_chunk)

    save_to_sheet(volatility_data, "volatility_data", file_path)
    save_to_sheet(last_close_data, "lastclose_data", file_path)


if __name__ == "__main__":
    # Read the Excel file and extract the stock symbols from the Stock List tab
    # Set current working directory to the directory of this file
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    while True:
        try:
            file_path = input("Enter the path to the Excel file: ")
            if file_path.split(".")[-1] != "xlsx":
                file_path += ".xlsx"
        except FileNotFoundError:
            print("The path does not exist")
        except KeyError:
            print("The Excel file does not contain a tickers tab")
        except Exception as e:
            print(e)
        else:
            print("Excel file loaded successfully\n")
            break

    asyncio.run(main(file_path))
