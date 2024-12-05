import os
import pdfplumber
import pandas as pd
from datetime import datetime, timedelta

# Predefined locations for Tmin data extraction
predefined_locations = [
    "Anuradhapura", "Badulla", "Bandarawela", "Batticaloa", "Colombo", 
    "Galle", "Hambantota", "Jaffna", "Moneragala", "Katugasthota", 
    "Katunayake", "Kurunagala", "Maha Illuppallama", "Mannar", 
    "Polonnaruwa", "Nuwara Eliya", "Pothuvil", "Puttalam", 
    "Rathmalana", "Ratnapura", "Trincomalee", "Vavuniya", "Mattala", 
    "Mullaitivu"
]

# Define the zones and the stations that belong to each zone
zones = {
    "Northern Plains": ["Vavuniya", "Anuradhapura", "Mullaitivu", "Puttalam", "Jaffna", "Maha Illuppallama", "Mannar"],
    "Eastern Plains": ["Batticaloa", "Pothuvil", "Polonnaruwa", "Moneragala", "Trincomalee"],
    "Eastern Hills": ["Badulla", "Bandarawela"],
    "Western Plains": ["Colombo", "Galle", "Katunayake", "Kurunagala", "Rathmalana"],
    "Western Hills": ["Katugasthota", "Nuwara Eliya", "Ratnapura"],
    "Southern Plains": ["Hambantota", "Mattala"]
}

def extract_tmin_from_pdf(pdf_path=None, pdf_missing=False):
    # If the PDF is missing, create a DataFrame with '0.0' values for all locations
    if pdf_missing:
        data = {
            'Date': [datetime.now().strftime('%m/%d/%Y')],
            'Variable': ['Tmin'],
        }
        for location in predefined_locations:
            data[location] = [0.0]  # Set '0.0' for Tmin data
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
                'Tmin': []
            }

            # Loop through each predefined location and extract Tmin data
            for index in range(len(predefined_locations)):
                tmin = df_extracted.iloc[index, 2] if index < len(df_extracted) else 0.0
                results['Tmin'].append(float(tmin) if pd.notna(tmin) and tmin not in ['NA', 'tr', 'TR'] else 0.0)

            # Convert Tmin data to numeric, errors='coerce' will replace non-convertible values with NaN
            tmin_values = pd.to_numeric(results['Tmin'], errors='coerce')

            # Calculate total, average, max, and min Tmin
            total_tmin = round(tmin_values.sum(),2) 
            average_tmin = tmin_values.mean()  
            max_tmin = tmin_values.max()  
            min_tmin = tmin_values.min()  

            # Calculate zone-wise averages for the current day
            zone_averages = {zone: round(tmin_values[[predefined_locations.index(station) for station in stations]].mean(),2) for zone, stations in zones.items()}

            # Create a DataFrame for daily results
            final_df = pd.DataFrame({
                'Date': [results['Date']],
                'Variable': ['Tmin'],
                **{predefined_locations[i]: [results['Tmin'][i]] for i in range(len(predefined_locations))},
                'Total Tmin': [total_tmin],
                'Average Tmin': [average_tmin],
                'Max Tmin': [max_tmin],
                'Min Tmin': [min_tmin],
                **{f'Average {zone}': [zone_averages[zone]] for zone in zones}
            })

            return final_df

    return None

def calculate_8_day_average(df): 
    # Only calculate 8-day average if the current length is a multiple of 8
    if len(df) % 8 == 0 and len(df) >= 8:
        # Calculate 8-day averages for each zone
        zone_averages_8_days = {}
        for zone, stations in zones.items():
            station_columns = [station for station in stations if station in df.columns]
            zone_averages_8_days[f'8-Day Average {zone}'] = round(df[station_columns].tail(8).mean().mean(), 2)

        # Append the 8-day averages row to the DataFrame
        zone_averages_row = pd.DataFrame(zone_averages_8_days, index=[0])
        zone_averages_row['Date'] = df['Date'].max()  # Use the last date for the 8-day average
        zone_averages_row['Variable'] = '8-Day Average Tmin'

        # Append the zone averages to the existing dataframe
        df = pd.concat([df, zone_averages_row], ignore_index=True)

    return df

def main(pdf_path):
    # Extract Tmin data from the PDF
    df_daily_tmin = extract_tmin_from_pdf(pdf_path, pdf_missing=False)  # Adjust pdf_missing as needed

    if df_daily_tmin is not None:
        # Save the cleaned DataFrame to a CSV file in 'extracted_data' folder
        os.makedirs('extracted_data', exist_ok=True)
        csv_file_path = os.path.join('extracted_data', 'metstation_tmin_data.csv')

        # Append to CSV if it already exists, otherwise create a new one
        if os.path.exists(csv_file_path):
            # Load the existing data
            df_existing = pd.read_csv(csv_file_path)
            # Append the new daily data
            df_combined = pd.concat([df_existing, df_daily_tmin], ignore_index=True)
        else:
            df_combined = df_daily_tmin

        # Calculate 8-day average if enough data exists
        df_combined = calculate_8_day_average(df_combined)

        # Save the updated data back to the same CSV
        df_combined.to_csv(csv_file_path, mode='w', header=True, index=False)

        print(f"Data (including 8-day averages) saved to '{csv_file_path}'.")
    else:
        print("No table found in the PDF or no matching locations found.")

if __name__ == "__main__":
    # Get the current date in YYYY-MM-DD format for the filename
    date_string = datetime.now().strftime('%Y-%m-%d')
    pdf_filename = f'metdata/daily_climate_update_{date_string}.pdf'
    main(pdf_filename)
