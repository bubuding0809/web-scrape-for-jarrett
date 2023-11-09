import pandas as pd


def printProgressBar(
    iteration,
    total,
    prefix="",
    suffix="",
    decimals=1,
    length=100,
    fill="█",
    printEnd="\r",
    flush=True,
):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + "-" * (length - filledLength)
    print(f"\r{prefix} |{bar}| {percent}% {suffix}", end=printEnd)
    # Print New Line on Complete
    if iteration == total:
        print()


def save_to_sheet(results: list[pd.DataFrame], sheet_name: str, file_path: str):
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

    return combined


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
