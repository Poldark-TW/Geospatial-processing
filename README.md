# SE4GEO
This project repository is developed for the course "Geospatial processing".

## Scrum Platform

 We use JIRA. [Click here to Check.](https://mail-team-padca5iq.atlassian.net/jira/software/projects/SE4G/list)


# SE4GEO Enhanced: Interactive GIS System for Disaster Risk Management

An improved interactive GIS application built with Python, PostgreSQL/PostGIS, Folium, Plotly, and Jupyter Widgets. This tool supports **dynamic disaster risk visualization**, **interactive filtering**, and **zoom-sensitive analytics**, focused on **flood and landslide data**.

GitHub: [https://github.com/Junjie-Mu/SE4GEO](https://github.com/Junjie-Mu/SE4GEO)

---

## Key Improvements

### 1. Dynamic Province Dropdown
Provinces are now fetched in real-time from the PostgreSQL database, eliminating hardcoded dropdowns and supporting database scalability.

### 2. Interactive Map with Marker Clustering
Using `folium.plugins.MarkerCluster`, markers are clustered to enhance map readability and performance.

### 3. Zoom-Sensitive Data Visualization
Integrated with `Javascript` and `Plotly`, the app dynamically renders bar charts based on the current zoom level of the map.

### 4. Improved SQL Integration
Efficient data fetching and insertion into `osm_province`, including:
- Geometry handling via Shapely & WKB
- Spatial integrity using `GEOMETRY(MultiPolygon, 4326)`
- Centralized `commit()` calls to enhance performance

### 5. Robust Error Handling and Logging
More reliable geocoding, graceful fallbacks for OSM ID/name mismatches, and real-time console messages for tracing.

---

## Installation

### 1. Clone the Repository
```bash
git clone https://github.com/Junjie-Mu/SE4GEO.git
cd SE4GEO

### 2. Setup Environment Variables
- Create a .env file in backend/database/.env
- And include:
DB_NAME=your_database_name
USER=your_postgres_user
PASSWORD=your_password
HOST=localhost

### 3. Install Required Packages
Packages needed:
- psycopg2
- osmnx
- folium
- ipywidgets
- plotly
- geopandas
- shapely
- python-dotenv

### 4. Set Up PostgreSQL + PostGIS
create necessary tables using the provided SQL and Python setup scripts (osm_province, osm_regioni, etc.).

---

## How to Use

###1. Launch the Notebook
- Load the Map Interface
- Execute the cell containing the improved interface.
- Display a form with dropdowns (scale, disaster type, province)
- Render a dynamic map using Folium
- Enable marker clustering
- Update charts based on zoom events

### 2. Populate osm_province Table
- Use the province_boundary_loader.py or notebook cell with the following logic:
Extract province names or OSM IDs from the province table
Fetch geometry from OpenStreetMap
Insert into PostGIS with proper spatial reference

### 3. 
- Fork the repository
- Create a new branch:
git checkout -b feature/my-improvement

- Commit your changes:
git commit -am 'Add XYZ feature'

- Push to your fork:
git push origin feature/my-improvement

-Submit a pull request!

---

## How to run the programmes
Find the file **'webserver.py'** and run it.

After the run is complete, use the browser to enter http://localhost:5000



