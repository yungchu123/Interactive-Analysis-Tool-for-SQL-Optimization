# Visualizing PostgreSQL Query Execution: An Interactive Analysis Tool for SQL Optimization

This project focused on creating a database application aimed at revealing PostgreSQL's internal processing of SQL queries, especially through Query Execution Plans (QEPs). The design approach emphasized user understanding of data manipulation at each stage of query execution, addressing the limitation that PostgreSQL’s QEP lacks visibility into intermediate data states.

## Features
1. **Visualization of QEP**: A tree structure representing QEP nodes allows users to interactively explore details by hovering or clicking, making complex query processes comprehensible.
2. **Data Block Mapping**: Users can view the physical storage of tuples by selecting specific block numbers.
3. **Algorithm Implementation**: A QEP class and associated methods were developed to transform operator nodes into SQL queries for each query stage, handling scans, joins, aggregates, limits, and sorts.
4. **Data Storage**: Intermediate query results are stored in CSV files, enabling user interaction through a graphical interface.

## Explanation for each script
### explore.py
- This script implements the main functionality for interacting with PostgreSQL to analyze SQL queries through Query Execution Plans (QEP).
- It connects to a PostgreSQL database, retrieves QEP data in JSON format, and recursively processes each QEP node to extract conditions, joins, filters, and other details.
- The script builds intermediate SQL queries based on the QEP and visualizes accessed data blocks in a grid format to display physical storage details.
- It contains methods for generating disk memory grids, mapping table conditions, and preparing visual representations of the database operations involved in query execution.

### interface.py
- This script provides a GUI interface using tkinter for user interactions with the query analyzer.
- It has a ConnectionPage for users to input database connection details, which are validated and saved.
- A QueryPage allows users to input SQL queries, execute them, and view a history of previous queries.
- The QueryResultPage displays QEP visualizations, query explanations, and accessed blocks visualization, offering a user-friendly way to interact with and understand query execution.

### project.py
- This script serves as the main entry point for the application, initializing and running the GUI by creating an instance of MainApplication from interface.py.

## How to Run
To install all required packages
```
pip install -r requirements.txt
```
Run the project.py
```
python project.py
```
