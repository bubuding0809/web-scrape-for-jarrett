import os
import aiohttp
import asyncio
import pandas as pd
from io import StringIO
from bs4 import BeautifulSoup


async def fetch_data(symbol, session):
    url = f"https://www.alphaquery.com/stock/{symbol}/volatility-option-statistics/90-day/historical-volatility"

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
        row_object = {
            "Symbol": symbol,
        }
        for _, row in df.iterrows():
            try:
                row_object[row[0]] = float(row[1])
            except ValueError:
                row_object[row[0]] = row[1]
        row_object = dict(
            filter(
                lambda x: x[0] not in ["Volatility Metrics", "Option Statistics"],
                row_object.items(),
            )
        )

        # Create a new DataFrame from the row_object
        df = pd.DataFrame([row_object])

        print(f"Scraped data for {symbol}")
        return df


async def main(stock_symbols: list[str]):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_data(symbol, session) for symbol in stock_symbols]
        results = await asyncio.gather(*tasks)

    print(f"Scraping complete for {len(results)} symbols")

    # Combine the data for all symbols into a single DataFrame
    combined_data = pd.concat(
        [df for df in results if df is not None], ignore_index=True
    )

    # Write the data to an Excel file called AlphaQuery.xlsx
    while True:
        try:
            save_path = input(
                "Enter the path to save the Excel file, (eg: results.xlsx): "
            )
            with pd.ExcelWriter(save_path) as writer:
                combined_data.to_excel(writer, index=False)
        except PermissionError:
            print("Please close the Excel file before saving")
        except Exception as e:
            print(e)
        else:
            print("Excel file saved successfully")
            break


if __name__ == "__main__":
    # Read the Excel file and extract the stock symbols from the Stock List tab
    # Set current working directory to the directory of this file
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    while True:
        try:
            file_path = input("Enter the path to the Excel file: ")
            df = pd.read_excel(file_path, sheet_name="Stock List")
            stock_symbols = df["Symbol"].tolist()
        except FileNotFoundError:
            print("The path does not exist")
        except KeyError:
            print("The Excel file does not contain a Stock List tab")
        except Exception as e:
            print(e)
        else:
            print("Excel file loaded successfully")
            break

    asyncio.run(main(stock_symbols))
