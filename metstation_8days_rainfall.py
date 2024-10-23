import os
import pdfplumber
import pandas as pd
from datetime import datetime

# Predefined locations for rainfall data extraction
predefined_locations = [
    "Anuradhapura", "Badulla", "Bandarawela", "Batticaloa", "Colombo", 
    "Galle", "Hambantota", "Jaffna", "Moneragala", "Katugasthota", 
    "Katunayake", "Kurunagala", "Maha Illuppallama", "Mannar", 
    "Polonnaruwa", "Nuwara Eliya", "Pothuvil", "Puttalam", 
    "Rathmalana", "Ratnapura", "Trincomalee", "Vavuniya", "Mattala", 
    "Mullaitivu"
]

def extract_rainfall_data_from_pdf(pdf_path=None, pdf_missing=False): 
    # If the PDF is missing, create a DataFrame with 'NA' values for all locations
    if pdf_missing:
        data = {
            'Date': [datetime.now().strftime('%m/%d/%Y')],
            'Variable': ['Rainfall'],
        }
        for location in predefined_locations:
            data[location] = [0.0]  # Set '0.0' for rainfall data
        return pd.DataFrame(data)

    # Open the PDF and extract tables
    with pdfplumber.open(pdf_path) as pdf:
        page = pdf.pages[0]  
        tables = page.extract_tables()

        if tables:
            first_table = tables[0]
            df = pd.DataFrame(first_table[1:], columns=first_table[0])

            # Extract rows 4 to 28 (zero-indexed, hence 3:27)
            df_extracted = df.iloc[3:27]

            # Prepare a DataFrame to hold results
            results = {
                'Date': datetime.now().strftime('%m/%d/%Y'),
                'Rainfall': []
            }

            # Loop through each predefined location and extract rainfall data
            for index in range(len(predefined_locations)):
                rainfall = df_extracted.iloc[index, 3] if index < len(df_extracted) else 'NA'
                
                # Handle 'tr'/'TR' values as trace rainfall, convert to float otherwise
                rainfall = rainfall.strip() if isinstance(rainfall, str) else rainfall
                if isinstance(rainfall, str) and rainfall.lower() == 'tr':
                    rainfall_value = 0.0  # Trace amount represented as 0.0
                else:
                    try:
                        rainfall_value = float(rainfall) if pd.notna(rainfall) and rainfall != 'NA' else 0.0
                    except ValueError:
                        rainfall_value = 0.0  # Handle any other invalid values

                results['Rainfall'].append(rainfall_value)

            # Convert rainfall data to numeric, errors='coerce' will replace non-convertible values with NaN
            rainfall_values = pd.to_numeric(results['Rainfall'], errors='coerce')

            # Calculate total, average, max, and min rainfall
            total_rainfall = rainfall_values.sum()  # Total rainfall
            average_rainfall = rainfall_values.mean()  # Average rainfall
            max_rainfall = rainfall_values.max()  # Maximum rainfall
            min_rainfall = rainfall_values.min()  # Minimum rainfall

            # Create a DataFrame for results
            final_df = pd.DataFrame({
                'Date': [results['Date']],
                'Variable': ['Rainfall'],
                **{predefined_locations[i]: [results['Rainfall'][i]] for i in range(len(predefined_locations))},
                'Total Rainfall': [total_rainfall],
                'Average Rainfall': [average_rainfall],
                'Max Rainfall': [max_rainfall],
                'Min Rainfall': [min_rainfall]
            })

            return final_df

    return None

def main(pdf_path):
    # Extract the data from the downloaded PDF
    df_daily_weather = extract_rainfall_data_from_pdf(pdf_path)

    if df_daily_weather is not None:
        # Save the cleaned DataFrame to a CSV file in 'extracted_data' folder
        os.makedirs('extracted_data', exist_ok=True)
        csv_file_path = os.path.join('extracted_data', 'metstation_8days_rainfall.csv')

        # Append to CSV if it already exists, otherwise create a new one
        if os.path.exists(csv_file_path):
            df_daily_weather.to_csv(csv_file_path, mode='a', header=False, index=False)
        else:
            df_daily_weather.to_csv(csv_file_path, mode='w', header=True, index=False)

        print(f"Data extracted and saved to '{csv_file_path}'.")
    else:
        print("No table found in the PDF or no matching locations found.")

if __name__ == "__main__":
    # Get the current date in YYYY-MM-DD format for the filename
    date_string = datetime.now().strftime('%Y-%m-%d')
    pdf_filename = f'metdata/daily_climate_update_{date_string}.pdf'  # Format the filename with the current date
    main(pdf_filename)
