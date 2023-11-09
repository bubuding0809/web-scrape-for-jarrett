import asyncio
import aiohttp
from bs4 import BeautifulSoup
from io import StringIO
import pandas as pd

INDICES = ["sp500", "dowjones", "nasdaq100"]

INDICES_MAP = {"sp500": "S&P 500", "dowjones": "Dow Jones", "nasdaq100": "Nasdaq 100"}


async def fetch_stocks(index: str, session):
    """
    This function will scrape the stock tickers from the url with the index
    Eg: from "https://www.slickcharts.com/dowjones"
    """

    url = f"https://www.slickcharts.com/{index}"
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
        df = df[["Company", "Symbol"]]
        df["Index"] = INDICES_MAP[index]

        return df
