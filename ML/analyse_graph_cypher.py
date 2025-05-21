#!/usr/bin/env python3
import os
import logging
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from neo4j import GraphDatabase, basic_auth
from neo4j.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("graph_analytics")

# ─── ENVIRONMENT ──────────────────────────────────────────────────────────
NEO_URI  = os.environ['NEO_URI']
NEO_USER = os.environ['NEO_USER']
NEO_PW   = os.environ['NEO_PW']

# ─── NEO4J DRIVER ─────────────────────────────────────────────────────────
drv = GraphDatabase.driver(NEO_URI, auth=basic_auth(NEO_USER, NEO_PW))
def run(tx, cypher, params=None):
    tx.run(cypher, params or {})

GRAPH_NAME = "BDM_CourseEnrollGraph"

def project_and_embed():
    with drv.session() as session:
        # 1) drop any existing in-memory graph
        for stmt in (
            f"CALL gds.graph.drop('{GRAPH_NAME}', false)",
            f"CALL gds.graph.drop('{GRAPH_NAME}')"
        ):
            try:
                session.execute_write(run, stmt)
                break
            except ClientError:
                continue

        # 2) project a mixed-type graph
        session.execute_write(run, f"""
          CALL gds.graph.project(
            '{GRAPH_NAME}',
            ['Student','Course','Assessment','MentalHealthSurvey','HealthData'],
            {{
              ENROLLED_IN: {{orientation:'UNDIRECTED'}},
              TOOK:        {{orientation:'UNDIRECTED'}},
              BELONGS_TO:  {{orientation:'UNDIRECTED'}},
              SURVEYED:    {{orientation:'UNDIRECTED'}},
              ADMITTED:    {{orientation:'UNDIRECTED'}}
            }}
          )
        """)
        log.info("Projected in-memory graph")

        # 3) Node2Vec embeddings → node property 'embedding'
        session.execute_write(run, f"""
          CALL gds.node2vec.write(
            '{GRAPH_NAME}',
            {{
              embeddingDimension: 64,
              walkLength: 10,
              iterations: 5,
              writeProperty: 'embedding'
            }}
          )
        """)
        log.info("Computed Node2Vec embeddings")

        # 4) PageRank → 'pagerank'
        session.execute_write(run, f"""
          CALL gds.pageRank.write(
            '{GRAPH_NAME}',
            {{ writeProperty: 'pagerank' }}
          )
        """)
        log.info("Computed PageRank")

        # 5) Louvain → 'community'  (drop unsupported writePropertyType)
        session.execute_write(run, f"""
          CALL gds.louvain.write(
            '{GRAPH_NAME}',
            {{ writeProperty: 'community' }}
          )
        """)
        log.info("Computed Louvain communities")

        # 6) cleanup
        session.execute_write(run, f"CALL gds.graph.drop('{GRAPH_NAME}')")
        log.info("Dropped in-memory graph")
def cluster_and_label():
    # 7) pull Student embeddings back into pandas
    with drv.session() as session:
        # consume the result inside the session
        records = list(session.run("""
            MATCH (s:Student)
            RETURN s.id AS id, s.embedding AS embed
        """))

    
    df = pd.DataFrame([
        {"id": rec["id"], **{f"e{i}": v for i, v in enumerate(rec["embed"])}}
        for rec in records
    ])
    emb_cols = [c for c in df.columns if c.startswith("e")]

    # 8) choose optimal k via silhouette score
    best_k, best_score = 0, -1
    for k in range(2, 7):
        km = KMeans(n_clusters=k, random_state=42, n_init="auto")
        labels = km.fit_predict(df[emb_cols])
        score = silhouette_score(df[emb_cols], labels)
        log.info(f"k={k} → silhouette={score:.3f}")
        if score > best_score:
            best_k, best_score = k, score
    log.info(f"Selected k={best_k} (silhouette={best_score:.3f})")

    # 9) final clustering & write back as `risk_cohort`
    km_final = KMeans(n_clusters=best_k, random_state=42, n_init="auto")
    df['risk_cohort'] = km_final.fit_predict(df[emb_cols]).astype(int)

    # write the cluster labels back to Neo4j
    with drv.session() as session:
        for _, row in df.iterrows():
            session.execute_write(run,
                """
                MATCH (s:Student {id: $id})
                SET s.risk_cohort = $risk_cohort
                """,
                {"id": row["id"], "risk_cohort": int(row["risk_cohort"])}
            )
    log.info("✔ Wrote `risk_cohort` back to Neo4j")


if __name__ == "__main__":
    project_and_embed()
    cluster_and_label()
    log.info(" Graph analytics & downstream ML complete.")
