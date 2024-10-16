import os
import pdfplumber
import pandas as pd
from datetime import datetime

def extract_data_from_pdf(pdf_path):
    predefined_locations = [
        "Anuradhapura", "Badulla", "Bandarawela", "Batticaloa", "Colombo",
        "Galle", "Hambantota", "Jaffna", "Moneragala", "Katugasthota", "Katunayake",
        "Kurunagala", "Maha Illuppallama", "Mannar", "Polonnaruwa",
        "Nuwara Eliya", "Pothuvil", "Puttalam", "Rathmalana",
        "Ratnapura", "Trincomalee", "Vavuniya", "Mattla", "Mullaitivu"
    ]

    with pdfplumber.open(pdf_path) as pdf:
        page = pdf.pages[0]
        tables = page.extract_tables()

        if tables:
            first_table = tables[0]
            df = pd.DataFrame(first_table[1:], columns=first_table[0])

            # Extract rows 4 to 28 (zero-indexed, hence 3:28)
            df_extracted = df.iloc[3:27]

            # Prepare a list to hold results
            results = []

            # Extract data for each predefined location
            for index, location in enumerate(predefined_locations):
                if index < len(df_extracted):
                    max_temp = df_extracted.iloc[index, 1] if not pd.isna(df_extracted.iloc[index, 1]) else None
                    min_temp = df_extracted.iloc[index, 2] if not pd.isna(df_extracted.iloc[index, 2]) else None
                    rainfall = df_extracted.iloc[index, 3] if not pd.isna(df_extracted.iloc[index, 3]) else None

                    # Append data to results in long format
                    results.append({'Date': datetime.now().strftime('%m/%d/%Y'), 'Location': location, 'Variable': 'Tmax', 'Value': max_temp})
                    results.append({'Date': datetime.now().strftime('%m/%d/%Y'), 'Location': location, 'Variable': 'Tmin', 'Value': min_temp})
                    results.append({'Date': datetime.now().strftime('%m/%d/%Y'), 'Location': location, 'Variable': 'Rainfall', 'Value': rainfall})

            # Create DataFrame from results
            df_results = pd.DataFrame(results)

            # Convert the 'Value' column to numeric, coerce errors to NaN
            df_results['Value'] = pd.to_numeric(df_results['Value'], errors='coerce')

            # Pivot the DataFrame to have dates and variables as rows, and locations as columns
            pivot_df = df_results.pivot_table(index=['Date', 'Variable'], columns='Location', values='Value')

            # Reset the index to flatten the DataFrame
            pivot_df.reset_index(inplace=True)

            return pivot_df
    return None

def main(pdf_path):
    # Extract the data from the downloaded PDF
    df_daily_weather = extract_data_from_pdf(pdf_path)

    if df_daily_weather is not None:
        # Save the cleaned DataFrame to a CSV file in 'extracted_data' folder
        os.makedirs('extracted_data', exist_ok=True)
        csv_file_path = os.path.join('extracted_data', 'extracted_climate_metdata.csv')

        # Append to CSV if it already exists, otherwise create a new one
        if os.path.exists(csv_file_path):
            df_daily_weather.to_csv(csv_file_path, mode='a', header=False, index=False)
        else:
            df_daily_weather.to_csv(csv_file_path, index=False)

        print(f"Data extracted and appended to '{csv_file_path}'.")
    else:
        print("No table found in the PDF or no matching locations found.")

if __name__ == "__main__":
    # Get the current date in YYYY-MM-DD format for the filename
    date_string = datetime.now().strftime('%Y-%m-%d')
    pdf_filename = f'metdata/daily_climate_update_{date_string}.pdf'  # Format the filename with the current date
    main(pdf_filename)
