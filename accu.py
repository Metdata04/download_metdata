import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# Load the rainfall data from the CSV file
csv_file_path = 'extracted_data/metstation_rainfall.csv'  # Adjust the file path as necessary
df = pd.read_csv(csv_file_path)

# Convert the 'Date' column to datetime format
df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y')

# Get the current date
current_date = datetime.now()

# Calculate the date 30 days ago
thirty_days_ago = current_date - timedelta(days=30)

# Filter data for the past 30 days
df_last_30_days = df[(df['Date'] >= thirty_days_ago) & (df['Date'] <= current_date)]

# Calculate cumulative rainfall
df_last_30_days['Cumulative Rainfall'] = df_last_30_days['Average Rainfall'].cumsum()

# Define normal rainfall line (this is an example, adjust it as necessary)
df_last_30_days['Normal Rainfall'] = df_last_30_days['Cumulative Rainfall'].mean()

# Plot cumulative rainfall with shading
plt.figure(figsize=(12, 6))
plt.plot(df_last_30_days['Date'], df_last_30_days['Cumulative Rainfall'], label='Cumulative Rainfall', color='blue')

# Fill area above and below the normal line
plt.fill_between(df_last_30_days['Date'], df_last_30_days['Cumulative Rainfall'], df_last_30_days['Normal Rainfall'], 
                 where=(df_last_30_days['Cumulative Rainfall'] >= df_last_30_days['Normal Rainfall']), 
                 facecolor='green', alpha=0.5, label='Above Normal')
plt.fill_between(df_last_30_days['Date'], df_last_30_days['Cumulative Rainfall'], df_last_30_days['Normal Rainfall'], 
                 where=(df_last_30_days['Cumulative Rainfall'] < df_last_30_days['Normal Rainfall']), 
                 facecolor='brown', alpha=0.5, label='Below Normal')

# Customize the plot
plt.title('Observed Accumulated Rainfall (mm)', fontsize=14, weight='bold')
plt.xlabel('Date', fontsize=12)
plt.ylabel('Cumulative Rainfall (mm)', fontsize=12)
plt.grid(axis='y', linestyle='--', alpha=0.7)

# Set x-axis ticks to show every date with proper rotation
plt.xticks(df_last_30_days['Date'], df_last_30_days['Date'].dt.strftime('%d%b'), rotation=45, ha='right')

# Add legend
plt.legend(loc='upper left')

# Generate a unique filename based on the current date and time
current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
filename = f'Graphs/Avg_RF/cumulative_rainfall_graph_{current_time}.png'

# Save the plot with the unique filename
plt.tight_layout()
plt.savefig(filename)
plt.show()
