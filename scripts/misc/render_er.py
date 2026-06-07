import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Load the environment variables from the project root .env file
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

import graphviz

# Add the main scripts directory to PYTHONPATH so utils can be imported
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "main")))

from utils import get_trino_connection



def build_erd():
    dot = graphviz.Digraph(comment='Trino ER Diagram', format='png')
    
    # Set high resolution (300 DPI) and spacing parameters for a balanced distribution
    dot.attr(rankdir='TB', dpi='300', nodesep='0.6', ranksep='0.8')
    
    # Premium node and edge styling
    dot.attr('node', shape='record', fontname='Helvetica', fontsize='10', style='filled', fillcolor='#fdfdfd', color='#444444')
    dot.attr('edge', arrowhead='crow', arrowtail='none', dir='both', color='#666666', penwidth='1.2')

    # Get Trino connection
    conn = get_trino_connection()
    cur = conn.cursor()

    # Define the tables you want to inspect
    schemas_and_tables = {
        "bronze": ["raw_patients", "raw_wards", "raw_event_metadata", "raw_visits", "raw_events"],
        "silver": ["stg_patients", "stg_wards", "stg_event_metadata", "stg_visits", "stg_events"],
        "gold": ["dim_patient", "dim_ward", "dim_event_type", "fact_visit", "fact_clinical_event"]
    }

    # Custom colors for schemas
    schema_colors = {
        "bronze": {"color": "#a86036", "fillcolor": "#fff2cc"},
        "silver": {"color": "#708090", "fillcolor": "#e6ecf0"},
        "gold": {"color": "#b8860b", "fillcolor": "#fefbec"}
    }

    # Group tables into visual clusters (subgraphs) for schemas
    for schema, tables in schemas_and_tables.items():
        with dot.subgraph(name=f"cluster_{schema}") as c:
            colors = schema_colors.get(schema, {"color": "#dcdcdc", "fillcolor": "#f7f7f7"})
            c.attr(
                label=f"{schema.upper()} SCHEMA", 
                style='filled', 
                color=colors["color"], 
                fillcolor=colors["fillcolor"], 
                fontname='Helvetica-Bold', 
                fontsize='12'
            )
            for table in tables:
                # Query column metadata from Trino
                cur.execute(f"DESCRIBE delta.{schema}.{table}")
                cols = cur.fetchall()
                
                # Format columns for Graphviz record shape
                col_labels = []
                for col in cols:
                    col_name, col_type = col[0], col[1]
                    col_labels.append(f"<{col_name}> {col_name} : {col_type}")
                
                record_label = f"{{ {table} | " + " | ".join(col_labels) + " }"
                c.node(f"{schema}_{table}", record_label)

    # Define logical relations manually (since metadata doesn't contain FKs)
    relations = [
        # Bronze Schema Internal Relationships
        ("bronze_raw_patients:patient_id", "bronze_raw_visits:patient_id"),
        ("bronze_raw_wards:ward_id", "bronze_raw_visits:ward_id"),
        ("bronze_raw_visits:visit_id", "bronze_raw_events:visit_id"),
        ("bronze_raw_event_metadata:event_type_id", "bronze_raw_events:event_type_id"),

        # Silver Schema Internal Relationships
        ("silver_stg_patients:patient_id", "silver_stg_visits:patient_id"),
        ("silver_stg_wards:ward_id", "silver_stg_visits:ward_id"),
        ("silver_stg_visits:visit_id", "silver_stg_events:visit_id"),
        ("silver_stg_event_metadata:event_type_id", "silver_stg_events:event_type_id"),

        # Gold Schema Internal Relationships
        ("gold_dim_patient:patient_key", "gold_fact_visit:patient_key"),
        ("gold_dim_ward:ward_key", "gold_fact_visit:ward_key"),
        ("gold_dim_patient:patient_key", "gold_fact_clinical_event:patient_key"),
        ("gold_dim_ward:ward_key", "gold_fact_clinical_event:ward_key"),
        ("gold_dim_event_type:event_type_key", "gold_fact_clinical_event:event_type_key"),
        ("gold_fact_visit:visit_id", "gold_fact_clinical_event:visit_id"),
    ]

    for src, dst in relations:
        dot.edge(src, dst)

    output_path = "artifacts/trino_erd"
    dot.render(output_path, cleanup=True)
    print(f"ER diagram rendered successfully to: {output_path}.png")

if __name__ == "__main__":
    build_erd()
