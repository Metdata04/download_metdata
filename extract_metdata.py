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

            # Prepare a DataFrame to hold results
            results = {
                'Date': datetime.now().strftime('%m/%d/%Y'),
                'Tmax': [],
                'Tmin': [],
                'Rainfall': []
            }

            # Loop through each predefined location by index
            for index in range(len(predefined_locations)):
                # Use the row index directly to extract data
                if index < len(df_extracted):
                    max_temp = df_extracted.iloc[index, 1] if not pd.isna(df_extracted.iloc[index, 1]) else None
                    min_temp = df_extracted.iloc[index, 2] if not pd.isna(df_extracted.iloc[index, 2]) else None
                    rainfall = df_extracted.iloc[index, 3] if not pd.isna(df_extracted.iloc[index, 3]) else None
                else:
                    max_temp = None
                    min_temp = None
                    rainfall = None

                # Collect the data for Tmax, Tmin, Rainfall
                results['Tmax'].append(max_temp)
                results['Tmin'].append(min_temp)
                results['Rainfall'].append(rainfall)

            # Create a DataFrame with three rows for each location
            final_df = pd.DataFrame({
                'Date': [results['Date']] * len(predefined_locations),
                'Variable': ['Tmax', 'Tmin', 'Rainfall'] * len(predefined_locations),
                **{predefined_locations[i]: [results['Tmax'][i], results['Tmin'][i], results['Rainfall'][i]] for i in range(len(predefined_locations))}
            })

            return final_df
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
    pdf_filename = f'metdata/daily_climate_update_{date_string}.pdf'
    main(pdf_filename)
