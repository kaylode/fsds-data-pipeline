#!/usr/bin/env python3
"""
EHR Lakehouse & Feature Store — Pipeline Overview Diagram

Generates a high-level architecture diagram to:
    artifacts/pipeline_diagram.png

Run:
    uv run scripts/misc/render_pipeline.py
"""

from pathlib import Path

from diagrams import Diagram, Cluster, Edge
from diagrams.custom import Custom
from diagrams.onprem.queue import Kafka
from diagrams.onprem.analytics import Spark, Flink, Trino
from diagrams.onprem.database import PostgreSQL, Duckdb
from diagrams.onprem.inmemory import Redis
from diagrams.onprem.workflow import Airflow
from diagrams.aws.storage import S3 as MinIO          # S3 icon for MinIO
from diagrams.onprem.compute import Server

# ── Output & Logo paths ───────────────────────────────────────────────────────
script_dir   = Path(__file__).resolve().parent
artifacts    = script_dir.parents[1] / "artifacts"
artifacts.mkdir(exist_ok=True)
output = str(artifacts / "pipeline_diagram")

datahub_logo = str(script_dir / "logos" / "datahub.png")
feast_logo   = str(script_dir / "logos" / "feast.png")
podman_logo  = str(script_dir / "logos" / "podman.png")
uv_logo      = str(script_dir / "logos" / "uv.png")

# ── Graph config ──────────────────────────────────────────────────────────────
graph_attr = {
    "fontsize": "16",
    "bgcolor":  "white",
    "pad":      "0.8",
    "splines":  "ortho",
    "nodesep":  "0.6",
    "ranksep":  "1.2",
    "fontname": "Helvetica-Bold",
}
node_attr = {
    "fontname": "Helvetica",
    "fontsize": "11",
}
edge_attr = {
    "fontname": "Helvetica",
    "fontsize": "10",
}

with Diagram(
    "",
    filename=output,
    outformat="png",
    show=False,
    direction="LR",
    graph_attr=graph_attr,
    node_attr=node_attr,
    edge_attr=edge_attr,
):
    # ── Edge styles ───────────────────────────────────────────────────────────
    batch_edge  = Edge(color="#1565C0", style="bold",   label="batch")
    stream_edge = Edge(color="#BF360C", style="bold",   label="stream")
    gov_edge    = Edge(color="#6A1B9A", style="dashed", label="govern")
    mat_edge    = Edge(color="#2E7D32", style="bold",   label="materialize")
    meta_edge   = Edge(color="#78909C", style="dashed")

    # ── Data Sources ──────────────────────────────────────────────────────────
    with Cluster("Data Sources"):
        stream_gen  = Server("Step 1b: Stream Producer\n(Live Stream)")
        offline_gen = Server("Step 1a: EHR Generator\n(Offline Batch)")

    # ── Streaming Path ────────────────────────────────────────────────────────
    with Cluster("Streaming Data Path"):
        kafka_in  = Kafka("Kafka Topic\n(patient-events)")
        flink_job = Flink("Flink Stream Processor\n(24h sliding window)")
        kafka_out = Kafka("Kafka Topic\n(patient-features-24h)")

    # ── Medallion Lakehouse (MinIO · Delta Lake) ───────────────────────────────
    with Cluster("Medallion Lakehouse  ·  MinIO (Delta Lake)"):
        with Cluster("🥉 Bronze Layer"):
            bronze = MinIO("raw_patients, raw_visits\nraw_events, raw_streams")
        
        spark_silver = Spark("Spark Processing\n(Bronze → Silver)")
        
        with Cluster("🥈 Silver Layer"):
            silver = MinIO("stg_patients, stg_visits\nstg_events")
            
        spark_gold = Spark("Spark Processing\n(Silver → Gold)")
        
        with Cluster("🥇 Gold Layer"):
            gold = MinIO("dim_*, fact_*\nobt_clinical_events\nfeat_*, visit_labels")

    # ── Query Engine & Metadata Backend ───────────────────────────────────────
    with Cluster("Query Engine & Metadata Infrastructure"):
        trino = Trino("Trino Query Engine\n(delta catalog)")
        hive_metastore = Server("Hive Metastore\n(Schema Registry)")
        postgres = PostgreSQL("PostgreSQL\n(Metastore & Airflow DB)")

    # ── Platform & Runtimes ───────────────────────────────────────────────────
    with Cluster("Platform & Runtimes"):
        podman = Custom("Podman\n(Container Engine)", podman_logo)
        uv_pkg = Custom("uv\n(Package Manager)", uv_logo)
        
        # Horizontal layout constraint
        podman >> Edge(style="invis") >> uv_pkg

    # ── Governance & Orchestration ────────────────────────────────────────────
    with Cluster("Governance & Orchestration"):
        airflow  = Airflow("Airflow Orchestration\n(DAG Scheduler)")
        datahub  = Custom("DataHub Platform\n(Lineage & Catalog)", datahub_logo)
        
        # Horizontal layout constraint
        airflow >> Edge(style="invis") >> datahub
        uv_pkg >> Edge(style="invis") >> airflow


    # ── Feature Store (Feast) ─────────────────────────────────────────────────
    with Cluster("Feast Feature Store"):
        feast_pipeline = Custom("Feature Pipeline\n(Feast Apply/Materialize)", feast_logo)
        # feast_offline  = Duckdb("Offline Store\n(Trino Delta Catalog)")
        feast_online   = Redis("Online Store\n(Redis)")

    # ── Batch Pipeline Flows ──────────────────────────────────────────────────
    offline_gen >> batch_edge >> bronze
    bronze      >> batch_edge >> spark_silver >> batch_edge >> silver
    silver      >> batch_edge >> spark_gold   >> batch_edge >> gold
    gold        >> mat_edge >> feast_pipeline

    # ── Trino Queries all Lakehouse Layers ────────────────────────────────────
    trino >> Edge(color="#607D8B", style="dotted", dir="both") >> bronze
    trino >> Edge(color="#607D8B", style="dotted", dir="both") >> silver
    trino >> Edge(color="#607D8B", style="dotted", dir="both") >> gold

    # ── Streaming Pipeline Flows ──────────────────────────────────────────────
    stream_gen  >> stream_edge >> kafka_in
    kafka_in    >> stream_edge >> flink_job
    flink_job   >> stream_edge >> kafka_out       # Sink B: windowed features
    kafka_out   >> stream_edge >> feast_pipeline
    
    # ── Feast materialization and serving flows ───────────────────────────────
    # trino          >> Edge(color="#2E7D32", style="bold", constraint="false") >> feast_pipeline
    feast_pipeline >> mat_edge >> feast_online
    # feast_offline  >> Edge(color="#2E7D32", style="dotted") >> feast_pipeline

    # ── Governance, Lineage & Metadata Connections ───────────────────────────
    airflow  >> gov_edge >> datahub
    # airflow  >> meta_edge >> postgres
    trino    >> meta_edge >> hive_metastore
    hive_metastore >> meta_edge >> postgres

    # silver >> Edge(style="invis") >> trino
    # airflow >> Edge(style="invis") >> kafka_out

print(f"✅  Saved to: {output}.png")

