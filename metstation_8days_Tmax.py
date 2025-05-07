import os
import pdfplumber
import pandas as pd
from datetime import datetime, timedelta

# Predefined locations for Tmax data extraction
predefined_locations = [
    "Anuradhapura", "Badulla", "Bandarawela", "Batticaloa", "Colombo",
    "Galle", "Hambantota", "Jaffna", "Moneragala", "Katugasthota",
    "Katunayake", "Kurunagala", "Maha Illuppallama", "Mannar",
    "Polonnaruwa", "Nuwara Eliya", "Pothuvil", "Puttalam",
    "Rathmalana", "Ratnapura", "Trincomalee", "Vavuniya", "Mattala",
    "Mullaitivu"
]

# Zones and stations
zones = {
    "Northern Plains": ["Vavuniya", "Anuradhapura", "Mullaitivu", "Puttalam", "Jaffna", "Maha Illuppallama", "Mannar"],
    "Eastern Plains": ["Batticaloa", "Pothuvil", "Polonnaruwa", "Moneragala", "Trincomalee"],
    "Eastern Hills": ["Badulla", "Bandarawela"],
    "Western Plains": ["Colombo", "Galle", "Katunayake", "Kurunagala", "Rathmalana"],
    "Western Hills": ["Katugasthota", "Nuwara Eliya", "Ratnapura"],
    "Southern Plains": ["Hambantota", "Mattala"]
}

def extract_tmax_data_from_pdf(pdf_path=None, pdf_missing=False):
    yesterday_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        return df

    prev_thurs = today - timedelta(days=(today.weekday() - 3) % 7)
    prev_prev_thurs = prev_thurs - timedelta(days=7)
    prev_wed = prev_thurs - timedelta(days=1)

    df_filtered = df[
        (df['Date'] >= prev_prev_thurs.strftime('%Y-%m-%d')) &
        (df['Date'] <= prev_wed.strftime('%Y-%m-%d'))
    ]

    if df_filtered.empty:
        return df

    zone_averages_weekly = {}
    for zone, stations in zones.items():
        station_columns = [s for s in stations if s in df_filtered.columns]
        zone_averages_weekly[f'8-Day Average {zone}'] = round(df_filtered[station_columns].mean().mean(), 2)

    zone_row = pd.DataFrame(zone_averages_weekly, index=[0])
    zone_row['Date'] = prev_thurs.strftime('%Y-%m-%d')
    zone_row['Variable'] = '8-day Average'

    return pd.concat([df, zone_row], ignore_index=True)

def main(pdf_path):
    pdf_missing = not os.path.exists(pdf_path)
    df_today = extract_tmax_data_from_pdf(pdf_path, pdf_missing)

    if df_today is not None:
        os.makedirs('extracted_data', exist_ok=True)
        csv_file_path = os.path.join('extracted_data', 'metstation_tmax_data.csv')

        if os.path.exists(csv_file_path):
            df_existing = pd.read_csv(csv_file_path)
            if df_today['Date'].iloc[0] in df_existing['Date'].values:
                print("Data for today already exists. Skipping append.")
                df_combined = df_existing
            else:
                df_combined = pd.concat([df_existing, df_today], ignore_index=True)
        else:
            df_combined = df_today

        df_combined = calculate_weekly_average(df_combined)
        df_combined.to_csv(csv_file_path, index=False)
        print(f"Data (including 8-day averages) saved to '{csv_file_path}'.")
    else:
        print("No table found or no matching locations found in the PDF.")

if __name__ == "__main__":
    date_string = datetime.now().strftime('%Y-%m-%d')
    pdf_filename = f'metdata/daily_climate_update_{date_string}.pdf'
    main(pdf_filename)
