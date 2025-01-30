import os
import pdfplumber
import pandas as pd
from datetime import datetime, timedelta

# Predefined locations for Rainfall data extraction
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

def extract_rainfall_data_from_pdf(pdf_path=None, pdf_missing=False):
    # Get yesterday's date
    yesterday_date = (datetime.now() - timedelta(days=1)).strftime('%m/%d/%Y')
    
    # If the PDF is missing, create a DataFrame with '0.0' values for all locations
    if pdf_missing:
        data = {
            'Date': [yesterday_date],
            'Variable': ['Rainfall'],
        }
        for location in predefined_locations:
            data[location] = [0.0]  # Set '0.0' for Rainfall data
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
                'Date': yesterday_date,
                'Rainfall': []
            }

            # Loop through each predefined location and extract Rainfall data
            for index in range(len(predefined_locations)):
                rainfall = df_extracted.iloc[index, 3] if index < len(df_extracted) else 0.0
                results['Rainfall'].append(float(rainfall) if pd.notna(rainfall) and rainfall not in ['NA', 'tr', 'TR'] else 0.0)

            # Convert Rainfall data to numeric, errors='coerce' will replace non-convertible values with NaN
            rainfall_values = pd.to_numeric(results['Rainfall'], errors='coerce')

            # Calculate total, average, max, and min Rainfall
            total_rainfall = rainfall_values.sum()  
            average_rainfall = round(rainfall_values.mean(), 2)  
            max_rainfall = rainfall_values.max()  
            min_rainfall = rainfall_values.min()  

            # Calculate zone-wise averages for the current day (we will later calculate for 7 days)
            zone_averages = {zone: round(rainfall_values[[predefined_locations.index(station) for station in stations]].mean(), 2) for zone, stations in zones.items()}

            # Create a DataFrame for daily results
            final_df = pd.DataFrame({
                'Date': [results['Date']],
                'Variable': ['Rainfall'],
                **{predefined_locations[i]: [results['Rainfall'][i]] for i in range(len(predefined_locations))},
                'Total Rainfall': [total_rainfall],
                'Average Rainfall': [average_rainfall],
                'Max Rainfall': [max_rainfall],
                'Min Rainfall': [min_rainfall],
                **{f'Average {zone}': [zone_averages[zone]] for zone in zones}
            })

            return final_df

    return None

def calculate_zone_average(df):
    # Get the current date
    today = datetime.now()

    # Find the previous Thursday
    last_thursday = today-timedelta(days=7) # previous Thursday
    # if days_since_thursday < 0:
    #     days_since_thursday += 7  # Adjust if today is before Thursday in the week
    # last_thursday = today - timedelta(days=days_since_thursday)

    # Find the previous Wednesday (6 days before last Thursday)
    last_wednesday = today - timedelta(days=1)

    # Filter the DataFrame to only include data from the previous Thursday to this Wednesday
    filtered_df = df[(df['Date'] >= last_thursday.strftime('%Y-%m-%d')) & (df['Date'] <= last_wednesday.strftime('%Y-%m-%d'))]
    
    # If we don't have data for the full week, skip calculating averages
    # if len(filtered_df) < 6:
    #     print(f"Not enough data for the week from {last_thursday.strftime('%Y-%m-%d')} to {last_wednesday.strftime('%Y-%m-%d')}.")
    #     return df  # Return the unmodified DataFrame
    
    # Calculate zone averages for the Thursday to Wednesday period
    if len(filtered_df) % 7 == 0 and len(filtered_df) >= 7:
        zone_averages = {}
        for zone, stations in zones.items():
            station_columns = [station for station in stations if station in filtered_df.columns]
            if station_columns:
                df[f'Zone Average {zone}'] = round(filtered_df[station_columns].mean(axis=1), 2)
    
    
    # Create a new row with the zone averages and append it to the DataFrame
    zone_averages_row = pd.DataFrame(zone_averages, index=[0])
    zone_averages_row['Date'] = last_wednesday.strftime('%Y-%m-%d')  # Use Wednesday's date for the average
    zone_averages_row['Variable'] = '8-Day Average'

    df = pd.concat([df, zone_averages_row], ignore_index=True)

    return df


def main(pdf_path):
    # Check if the PDF exists
    pdf_missing = not os.path.exists(pdf_path)

    # Extract the data from the downloaded PDF
    df_daily_rainfall = extract_rainfall_data_from_pdf(pdf_path, pdf_missing)

    if df_daily_rainfall is not None:
        # Save the cleaned DataFrame to a CSV file in 'extracted_data' folder
        os.makedirs('extracted_data', exist_ok=True)
        csv_file_path = os.path.join('extracted_data', 'metstation_rainfall.csv')

        # Append to CSV if it already exists, otherwise create a new one
        if os.path.exists(csv_file_path):
            # Load the existing data
            df_existing = pd.read_csv(csv_file_path)
            # Append the new daily data
            df_combined = pd.concat([df_existing, df_daily_rainfall], ignore_index=True)
        else:
            df_combined = df_daily_rainfall

        # Calculate weekly averages (Thursday to Wednesday)
        df_combined = calculate_zone_average(df_combined)

        # Save the updated data back to the same CSV
        df_combined.to_csv(csv_file_path, mode='w', header=True, index=False)
        print(f"Data (including weekly averages) saved to '{csv_file_path}'.")
    else:
        print("No table found in the PDF or no matching locations found.")

if __name__ == "__main__":
    # Get the current date in YYYY-MM-DD format for the filename
    date_string = datetime.now().strftime('%Y-%m-%d')
    pdf_filename = f'metdata/daily_climate_update_{date_string}.pdf'
    main(pdf_filename)
