import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2
import statsmodels.api as sm
from dotenv import load_dotenv, dotenv_values

load_dotenv()
database_url = dotenv_values('.env')['AIVEN_CONN']
# Connect to the database
with st.spinner("Connecting to Database..."):
    try:
        conn = psycopg2.connect(database_url)
    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        st.stop()

# Create a cursor object
cur = conn.cursor()

# Execute a query
try:
    cur.execute("SELECT * FROM university_rankings")  # Replace `your_table_name` with the actual table name

    # Fetch all rows from the query result
    rows = cur.fetchall()

    # Get the column names (optional, for DataFrame column labels)
    colnames = [desc[0] for desc in cur.description]

    # Convert rows to a pandas DataFrame
    df = pd.DataFrame(rows, columns=colnames)
    st.write("Query successful!")
except Exception as e:
    st.error(f"Error executing query: {e}")
finally:
    # Close the cursor and connection
    cur.close()
    conn.close()


# Create a dictionary to map countries to continents
country_to_continent = {
    'United States': 'North America',
    'China': 'Asia',
    'United Kingdom': 'Europe',
    'Italy': 'Europe',
    'Australia': 'Oceania',
    'Canada': 'North America',
    'Germany': 'Europe',
    'Spain': 'Europe',
    'Netherlands': 'Europe',
    'Japan': 'Asia',
    'South Korea': 'Asia',
    'France': 'Europe',
    'Finland': 'Europe',
    'Sweden': 'Europe',
    'Denmark': 'Europe',
    'Saudi Arabia': 'Asia',
    'Iran': 'Asia',
    'Norway': 'Europe',
    'South Africa': 'Africa',
    'Portugal': 'Europe',
    'Austria': 'Europe',
    'Switzerland': 'Europe',
    'Hong Kong': 'Asia',
    'Belgium': 'Europe',
    'Egypt': 'Africa',
    'Taiwan': 'Asia',
    'Brazil': 'South America',
    'Thailand': 'Asia',
    'Poland': 'Europe',
    'Malaysia': 'Asia',
    'Ireland': 'Europe',
    'United Arab Emirates': 'Asia',
    'New Zealand': 'Oceania',
    'Israel': 'Asia',
    'Singapore': 'Asia',
    'Croatia': 'Europe',
    'Vietnam': 'Asia',
    'Chile': 'South America',
    'Greece': 'Europe',
    'Pakistan': 'Asia',
    'Slovenia': 'Europe',
    'Mexico': 'North America',
    'Russian Federation': 'Europe',
    'Lebanon': 'Asia',
    'Macao': 'Asia',
    'Bicocca': 'Europe',  # This seems like a university name, may need further clarification
    'Columbia': 'South America',
    'Iceland': 'Europe',
    'Hungary': 'Europe',
    'Estonia': 'Europe',
    'Serbia': 'Europe'
}

def add_continent_column(df, location_column):
    df['Continent'] = df[location_column].map(country_to_continent)
    return df

# Add 'Continent' column
df = add_continent_column(df, 'location')

st.write("# World University Rankings Data Insight")


# Display dataset preview
st.write("## Dataset")
st.write(df)

# Check if required columns exist in the dataset
def check_columns(columns):
    missing_columns = [col for col in columns if col not in df.columns]
    if missing_columns:
        st.error(f"Missing columns in the dataset: {', '.join(missing_columns)}")
        return False
    return True

# Plot 1: Teaching Score vs Rank
st.write("## Teaching Score vs Rank")
if check_columns(['teaching', 'rank', 'Continent']):
    fig = px.scatter(df, x='teaching', y='rank', color='Continent', title="teaching Score vs rank",
                        labels={'teaching': 'teaching Score', 'rank': 'rank (Lower is Better)'},
                        hover_data=df.columns)
    fig.update_layout(yaxis=dict(autorange="reversed"))  # Flip y-axis for rank
    st.plotly_chart(fig)

# Plot 2: Research Quality vs Average Citations
st.write("## Research Quality vs Average Citations")
if check_columns(['research_quality', 'average_citations']):
    fig = px.scatter(df, x='research_quality', y='average_citations', title="research_quality vs average_citations",
                        labels={'research_quality': 'research_quality', 'average_citations': 'average_citations'},
                        hover_data=df.columns)
    st.plotly_chart(fig)

# Plot 4: 11 Years Articles vs 11 Years Citations (with Regression)
st.write("## 11 Years Articles vs 11 Years Citations with Regression Line")
if check_columns(['eleven_years_articles', 'eleven_years_citations']):
    fig = px.scatter(df, 
                     x='eleven_years_articles', 
                     y='eleven_years_citations', 
                     title="11 Years Articles vs 11 Years Citations (with Regression Line)",
                     labels={'eleven_years_articles': '11 Years Articles', 
                             'eleven_years_citations': '11 Years Citations'},
                     trendline="ols",
                     trendline_color_override='red',      # Ordinary Least Squares regression
                     hover_data=df.columns)
    st.plotly_chart(fig)

st.write("## Research Environment vs Research Quality")
if check_columns(['research_environment', 'research_quality', 'Continent']):
    fig = px.scatter(df, x='research_environment', y='research_quality', color='Continent', title="Research Environment vs Research Quality",
                        labels={'research_environment': 'Research Environment', 'research_quality': 'Research Quality'},
                        hover_data=df.columns)
    fig.update_layout(yaxis=dict(autorange="reversed"))  # Flip y-axis for rank
    st.plotly_chart(fig)

st.write("## Research Quality vs Industry")
if check_columns(['research_quality', 'industry', 'Continent']):
    fig = px.scatter(df, x='research_quality', y='industry', color='Continent', title="Research Quality vs Industry",
                        labels={'research_quality': 'Research Quality', 'industry': 'Industry'},
                        hover_data=df.columns)
    fig.update_layout(yaxis=dict(autorange="reversed"))  # Flip y-axis for rank
    st.plotly_chart(fig)

st.write('## Industry vs International Outlook')
if check_columns(['industry', 'international_outlook', 'Continent']):
    fig = px.scatter(df, x='industry', y='international_outlook', color='Continent', title="Industry vs International Outlook",
                        labels={'industry': 'Industry', 'international_outlook': 'International Outlook'},
                        hover_data=df.columns)
    fig.update_layout(yaxis=dict(autorange="reversed"))  # Flip y-axis for rank
    st.plotly_chart(fig)

# Plot 5: H-Index vs Rank
st.write("## H-Index vs Research Quality")
if check_columns(['h_index', 'research_quality', 'Continent']):
    fig = px.scatter(df, x='h_index', y='research_quality', color='Continent', title="h_index vs research_quality",
                        labels={'h_index': 'h_index', 'research_quality': 'research_quality (Lower is Better)'},
                        hover_data=df.columns)
    fig.update_layout(yaxis=dict(autorange="reversed"))  # Flip y-axis for rank
    st.plotly_chart(fig)

st.write('## Research Quality vs International Outlook')
if check_columns(['research_quality', 'international_outlook', 'Continent']):
    fig = px.scatter(df, x = 'research_quality', y='international_outlook', color='Continent', title='Research Quality vs International Outlook',
                     labels={'research_quality' : 'Research Quality', 'international_outlook' : 'International Outlook'},
                     hover_data=df.columns)
    fig.update_layout(yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig)

# Assuming `df` is your DataFrame loaded earlier in the Streamlit app

# Plotly Bar Plot - International Outlook by Location
st.write("## International Outlook by Location")

# Create bar plot in Plotly
if check_columns(['location', 'international_outlook']):
    # Aggregate data by location and calculate the mean of international_outlook
    df_agg = df.groupby('location').agg({'international_outlook': 'mean'}).reset_index()

    # Create bar plot in Plotly
    fig = px.bar(df_agg,
                 x='location', 
                 y='international_outlook', 
                 title='International Outlook by Location',
                 labels={'location': 'Location', 'international_outlook': 'International Outlook'},
                 color='location',  # Color bars by location
                 color_continuous_scale='viridis',  # Set the color palette
                 text='international_outlook')  # Show the value on top of bars

    # Customize layout
    fig.update_layout(
        xaxis_tickangle=90,  # Rotate x-axis labels by 90 degrees
        title_font=dict(size=15),  # Title font size
        xaxis_title=None,  # Remove the x-axis title
        yaxis_title=None,  # Remove the y-axis title
    )

    # Show the plot in Streamlit
    st.plotly_chart(fig, use_container_width = True)

st.write('## University Impact on Industry Based on Location')

if check_columns(['location', 'industry']):
    industry_agg = df.groupby('location').agg({'industry': 'mean'}).reset_index()
    fig = px.bar(industry_agg,
                 x='location', 
                 y='industry', 
                 title='University Impact on Industry Based on Location',
                 labels={'location': 'Location', 'industry': 'Industry'},
                 color='location',  # Color bars by location
                 color_continuous_scale='viridis',  # Set the color palette
                 text='industry')  # Show the value on top of bars

    # Customize layout
    fig.update_layout(
        xaxis_tickangle=90,  # Rotate x-axis labels by 90 degrees
        title_font=dict(size=15),  # Title font size
        xaxis_title=None,  # Remove the x-axis title
        yaxis_title=None,  # Remove the y-axis title
    )

    # Show the plot in Streamlit
    st.plotly_chart(fig, use_container_width = True)
