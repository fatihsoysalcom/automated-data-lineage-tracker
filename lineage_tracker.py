import json
from datetime import datetime

class LineageTracker:
    """
    A lightweight lineage tracker that captures metadata, dataset dependencies,
    and column-level mappings during ETL pipeline execution.
    """
    def __init__(self):
        self.nodes = {}
        self.edges = []

    def register_dataset(self, dataset_id, columns, description):
        """Registers a dataset (node) in the lineage graph."""
        self.nodes[dataset_id] = {
            "columns": columns,
            "description": description,
            "registered_at": datetime.utcnow().isoformat()
        }

    def add_dependency(self, source, target, transformation_name, column_mapping=None):
        """Registers a dependency (edge) between two datasets with column-level mapping."""
        self.edges.append({
            "source": source,
            "target": target,
            "transformation": transformation_name,
            "column_mapping": column_mapping or {}
        })

    def print_ascii_lineage(self):
        """Renders a visual representation of the data flow in the console."""
        print("\n" + "="*60)
        print(" AUTOMATED DATA LINEAGE MAP ".center(60, "="))
        print("="*60)
        
        # Find root sources (nodes with no incoming edges)
        targets = {edge["target"] for edge in self.edges}
        sources = {edge["source"] for edge in self.edges if edge["source"] not in targets}

        for src in sources:
            print(f"\n[Source Dataset] {src} ({', '.join(self.nodes[src]['columns'])})")
            self._print_downstream(src, depth=1)
        print("="*60 + "\n")

    def _print_downstream(self, current_node, depth):
        indent = "    " * depth
        for edge in self.edges:
            if edge["source"] == current_node:
                target = edge["target"]
                print(f"{indent}│")
                print(f"{indent}├──► [Transformation: {edge['transformation']}]")
                if edge['column_mapping']:
                    for tgt_col, src_cols in edge['column_mapping'].items():
                        print(f"{indent}│     └─ {tgt_col} ◀─ [{', '.join(src_cols)}]")
                print(f"{indent}│")
                print(f"{indent}└──► [Target Dataset] {target} ({', '.join(self.nodes[target]['columns'])})")
                self._print_downstream(target, depth + 1)

    def export_manifest(self):
        """Exports the entire lineage metadata as a JSON string."""
        return json.dumps({"datasets": self.nodes, "flow": self.edges}, indent=2)


# --- ETL Pipeline Demonstration with Integrated Lineage Tracking ---

def run_pipeline():
    # Initialize the automated lineage tracker
    tracker = LineageTracker()

    # Mock Raw Data Sources
    raw_crm_users = [
        {"id": 1, "name": "ahmet yilmaz", "email": "AHMET@mail.com ", "country": "tr"},
        {"id": 2, "name": "ayse demir", "email": "ayse@mail.com", "country": "tr"}
    ]
    raw_api_transactions = [
        {"tx_id": 101, "user_id": 1, "amount": 150.0, "date": "2023-10-01"},
        {"tx_id": 102, "user_id": 1, "amount": 300.0, "date": "2023-10-02"},
        {"tx_id": 103, "user_id": 2, "amount": 50.0, "date": "2023-10-03"}
    ]

    # 1. Register Raw Sources
    tracker.register_dataset("raw_crm_users", ["id", "name", "email", "country"], "Raw CRM User Data")
    tracker.register_dataset("raw_api_transactions", ["tx_id", "user_id", "amount", "date"], "Raw API Transactions")

    # 2. Step 1: Clean Users
    # Lineage registration happens dynamically during pipeline definition
    tracker.register_dataset("clean_users", ["user_id", "name", "email", "country"], "Standardized User Profiles")
    tracker.add_dependency(
        source="raw_crm_users",
        target="clean_users",
        transformation_name="Standardize Emails & Names",
        column_mapping={
            "user_id": ["id"],
            "name": ["name"],
            "email": ["email"],
            "country": ["country"]
        }
    )
    
    # Actual processing logic
    clean_users = []
    for u in raw_crm_users:
        clean_users.append({
            "user_id": u["id"],
            "name": u["name"].strip().title(),
            "email": u["email"].strip().lower(),
            "country": u["country"].upper()
        })

    # 3. Step 2: Aggregate Transactions
    tracker.register_dataset("user_spending", ["user_id", "total_spent"], "Aggregated Spending per User")
    tracker.add_dependency(
        source="raw_api_transactions",
        target="user_spending",
        transformation_name="Sum amount grouped by user_id",
        column_mapping={
            "user_id": ["user_id"],
            "total_spent": ["amount"]
        }
    )

    # Actual processing logic
    spending_map = {}
    for tx in raw_api_transactions:
        uid = tx["user_id"]
        spending_map[uid] = spending_map.get(uid, 0.0) + tx["amount"]
    user_spending = [{"user_id": k, "total_spent": v} for k, v in spending_map.items()]

    # 4. Step 3: Join and Segment (Multiple Inputs to Single Output)
    tracker.register_dataset("marketing_segmented_users", ["user_id", "name", "email", "segment"], "Final Campaign Targets")
    
    # Record dependency from clean_users
    tracker.add_dependency(
        source="clean_users",
        target="marketing_segmented_users",
        transformation_name="Join User Details",
        column_mapping={
            "user_id": ["user_id"],
            "name": ["name"],
            "email": ["email"]
        }
    )
    # Record dependency from user_spending
    tracker.add_dependency(
        source="user_spending",
        target="marketing_segmented_users",
        transformation_name="Apply VIP Segmentation",
        column_mapping={
            "segment": ["total_spent"]
        }
    )

    # Actual processing logic
    spending_lookup = {s["user_id"]: s["total_spent"] for s in user_spending}
    final_segmented = []
    for u in clean_users:
        spent = spending_lookup.get(u["user_id"], 0.0)
        segment = "VIP" if spent > 200 else "Standard"
        final_segmented.append({
            "user_id": u["user_id"],
            "name": u["name"],
            "email": u["email"],
            "segment": segment
        })

    # --- Output Results and Lineage Metadata ---
    print("Pipeline Execution Completed Successfully.")
    print(f"Processed {len(final_segmented)} records into 'marketing_segmented_users'.")

    # Print ASCII Lineage Map
    tracker.print_ascii_lineage()

    # Print JSON Lineage Manifest (Can be sent to metadata catalogs like OpenLineage/Apache Atlas)
    print("--- JSON LINEAGE MANIFEST ---")
    print(tracker.export_manifest())

if __name__ == "__main__":
    run_pipeline()