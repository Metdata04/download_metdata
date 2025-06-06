import pandas as pd 
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import os  # Import os module to create directories

# Load the rainfall data from the CSV file
csv_file_path = 'extracted_data/metstation_rainfall.csv'  
df = pd.read_csv(csv_file_path)

# Convert the 'Date' column to datetime format
df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y')

# Get the current date
current_date = datetime.now()

# Calculate the date 30 days ago
thirty_days_ago = current_date - timedelta(days=30)

# Filter data for the past 30 days
df_last_30_days = df[(df['Date'] >= thirty_days_ago) & (df['Date'] <= current_date)]

# Plot daily average rainfall as a bar chart
plt.figure(figsize=(12, 6))
plt.bar(df_last_30_days['Date'], df_last_30_days['Average Rainfall'], color='blue', width=0.8, label='Average Rainfall')

# Customize the plot
plt.title('Daily Average Rainfall All Over the Srilanka Past 30 Days', fontsize=14, weight='bold')
plt.xlabel('Date', fontsize=12)
plt.ylabel('Average Rainfall (mm)', fontsize=12)
plt.grid(axis='y', linestyle='--', alpha=0.7)

# Set x-axis ticks to show every date with proper rotation
plt.xticks(df_last_30_days['Date'], df_last_30_days['Date'].dt.strftime('%m/%d/%Y'), rotation=45, ha='right')

# Generate a unique filename based on the current date and time
current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
directory = 'Graphs/Avg_RF'  # Directory where the graphs will be saved
filename = f'{directory}/daily_rainfall_average_past_30_days_{current_time}.png'

# Create the directory if it doesn't exist
if not os.path.exists(directory):
    os.makedirs(directory)

# Save the plot with the unique filename
plt.tight_layout()
plt.savefig(filename)
plt.show()
