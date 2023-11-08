from datetime import datetime
import os
import aiohttp
import asyncio
import pandas as pd
from io import StringIO
from bs4 import BeautifulSoup

from progress import printProgressBar

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

# Global variable to keep track of the number of stocks scraped
count = 0


async def fetch_volatility(symbol: str, session: aiohttp.ClientSession):
    """Fetches the volatility data for the given symbol"""

    global count
    url = f"https://www.alphaquery.com/stock/{symbol}/all-data-variables"

    async with session.get(url) as response:
        if response.status != 200:
            print(f"Error fetching data for {symbol}")
            return None

        html = await response.text()
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table")

        if not table:
            print(f"Could not find table for {symbol}")
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

        count += 1
        printProgressBar(
            count,
            len(stock_symbols),
            prefix="Volatility progress:",
            suffix=f"Complete, scraped {symbol}",
            length=50,
        )

        return df


async def fetch_lastclose(symbol: str, session: aiohttp.ClientSession):
    """Fetches the last close data for the given symbol"""

    global count

    start_timestamp = int(datetime(2023, 1, 1).timestamp())
    end_timestamp = int(datetime.now().timestamp())

    url = f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}?period1={start_timestamp}&period2={end_timestamp}&interval=1d&events=history&includeAdjustedClose=true"
    async with session.get(url) as response:
        # Ensure that the request was successful
        if response.status != 200:
            print(f"Error fetching data for {symbol}")
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

        count += 1
        printProgressBar(
            count,
            len(stock_symbols),
            prefix="Last close progress:",
            suffix=f"Complete, scraped {symbol}",
            length=50,
        )

        return df


def save_to_sheet(results: list[pd.DataFrame], sheet_name: str):
    """Saves the results to the desired sheet in the Excel file"""

    combined = pd.concat([df for df in results if df is not None], ignore_index=True)

    # Write the data back to the Excel file
    while True:
        try:
            # Open the Excel file
            with pd.ExcelWriter(
                file_path,
                engine="openpyxl",
                mode="a",
            ) as writer:
                # Remove the sheet if it already exists
                if sheet_name in writer.book.sheetnames:
                    writer.book.remove(writer.book[sheet_name])

                # Write the DataFrame to the Excel file
                combined.to_excel(writer, sheet_name=sheet_name, index=False)

                resize_columns(writer, sheet_name)

                # Coerce the date/time columns to the correct format for the lastclose-data sheet
                if sheet_name == "lastclose-data":
                    ws = writer.sheets[sheet_name]

                    # format each cell in the Date column to the correct format
                    for cell in ws["B:B"]:
                        cell.number_format = "dd/mm/yyyy;@"

        except Exception as e:
            print(f"Error writing data to Excel file: {e}")
            if input("Press enter to try again or type 'exit' to quit: ") == "exit":
                break
        else:
            print(f"***Data written to {sheet_name} sheet successfully***\n")
            break


def resize_columns(writer: pd.ExcelWriter, sheet_name: str):
    """Set all columns of the sheet to autofit"""

    columns = writer.sheets[sheet_name].columns
    for column in columns:
        max_length = 0
        column_name = column[0].column_letter

        # Find the length of the longest string in the column
        for cell in column:
            max_length = max(max_length, len(str(cell.value)))

        # Set the column width to the length of the longest string + 2
        writer.sheets[sheet_name].column_dimensions[column_name].width = max_length + 2


async def main(stock_symbols: list[str]):
    # Scrape volatility data for each symbol
    async with aiohttp.ClientSession() as session:
        # Scraping volatility data
        volatility_data = await asyncio.gather(
            *[fetch_volatility(symbol, session) for symbol in stock_symbols]
        )
        print(f"Volatilty data scraping complete for {len(volatility_data)} symbols")
        save_to_sheet(volatility_data, "volatility-data")

        # Reset the count
        global count
        count = 0

        # Scraping last close data
        lastclose_data = await asyncio.gather(
            *[fetch_lastclose(symbol, session) for symbol in stock_symbols]
        )
        print(f"Last close data scraping complete for {len(lastclose_data)} symbols")
        save_to_sheet(lastclose_data, "lastclose-data")


if __name__ == "__main__":
    # Read the Excel file and extract the stock symbols from the Stock List tab
    # Set current working directory to the directory of this file
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    while True:
        try:
            file_path = input("Enter the path to the Excel file: ")
            if file_path.split(".")[-1] != "xlsx":
                file_path += ".xlsx"

            df = pd.read_excel(file_path, sheet_name="stocks")
            stock_symbols = df["Ticker"].tolist()
        except FileNotFoundError:
            print("The path does not exist")
        except KeyError:
            print("The Excel file does not contain a tickers tab")
        except Exception as e:
            print(e)
        else:
            print("Excel file loaded successfully\n")
            break

    asyncio.run(main(stock_symbols))
