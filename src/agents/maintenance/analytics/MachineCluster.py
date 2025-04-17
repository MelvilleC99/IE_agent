import json
import pandas as pd
import traceback
from datetime import datetime
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

def compute_machine_age(machine_data):
    """Calculate machine age in days from purchase date"""
    if isinstance(machine_data, dict) and 'purchaseDate' in machine_data:
        try:
            if isinstance(machine_data['purchaseDate'], str):
                purchase_date = datetime.fromisoformat(machine_data['purchaseDate'].replace('Z', '+00:00'))
            else:
                purchase_date = machine_data['purchaseDate']
            return (datetime.now() - purchase_date).days
        except Exception as e:
            print(f"Error computing machine age: {e}")
            return None
    return None

def run_analysis(records=None):
    """Perform machine clustering analysis based on failure count, age, and downtime"""
    if records is None or len(records) == 0:
        print("Error: No maintenance records provided to Machine Cluster analysis")
        return {
            "error": "No data provided",
            "message": "Machine Cluster analysis requires maintenance records to be provided"
        }

    try:
        print(f"Machine Cluster analysis: Processing {len(records)} records")
        df = pd.DataFrame(records)

        if 'machineNumber' not in df.columns or 'machineData' not in df.columns:
            return {
                "error": "Missing required fields",
                "message": "Fields 'machineNumber' and 'machineData' are required"
            }

        df['machine_age'] = df['machineData'].apply(compute_machine_age)

        print("Aggregating data by machine...")
        agg = df.groupby('machineNumber').agg(
            failure_count=('id', 'count'),
            total_downtime_ms=('totalDowntime', 'sum'),
            machine_age=('machine_age', 'first'),
            manufacturer=('machineData', lambda x: x.iloc[0].get('make', 'Unknown') if isinstance(x.iloc[0], dict) else 'Unknown'),
            machine_type=('machineData', lambda x: x.iloc[0].get('type', 'Unknown') if isinstance(x.iloc[0], dict) else 'Unknown')
        ).reset_index()

        agg = agg[agg['machine_age'].notnull()]

        if len(agg) < 2:
            print("Not enough machines with age data for clustering")
            return {
                "error": "Insufficient data",
                "message": "Need at least 2 machines with age data for clustering analysis"
            }

        agg['machine_age_years'] = agg['machine_age'] / 365.0
        agg['total_downtime_minutes'] = agg['total_downtime_ms'] / 60000.0

        # ✅ Use 3 features for clustering now
        features = agg[['failure_count', 'machine_age_years', 'total_downtime_minutes']].copy()
        scaler = StandardScaler()
        features_scaled = scaler.fit_transform(features)

        print("Performing KMeans clustering...")
        kmeans = KMeans(n_clusters=min(2, len(agg)), random_state=42)
        agg['cluster'] = kmeans.fit_predict(features_scaled)

        cluster_summary = agg.groupby('cluster').agg(
            avg_failure_count=('failure_count', 'mean'),
            avg_total_downtime=('total_downtime_minutes', 'mean'),
            avg_machine_age_years=('machine_age_years', 'mean'),
            n_machines=('machineNumber', 'count')
        ).reset_index()

        # 📌 Re-rank clusters by performance score (lower is better)
        if len(cluster_summary) >= 2:
            cluster_summary['performance_score'] = (
                cluster_summary['avg_failure_count'] +
                cluster_summary['avg_total_downtime'] +
                cluster_summary['avg_machine_age_years']
            )

            if (cluster_summary.loc[cluster_summary['cluster'] == 1, 'performance_score'].values[0] <
                cluster_summary.loc[cluster_summary['cluster'] == 0, 'performance_score'].values[0]):
                print("Swapping cluster labels to ensure cluster 0 is the better performer")
                agg['cluster'] = 1 - agg['cluster']

                cluster_summary = agg.groupby('cluster').agg(
                    avg_failure_count=('failure_count', 'mean'),
                    avg_total_downtime=('total_downtime_minutes', 'mean'),
                    avg_machine_age_years=('machine_age_years', 'mean'),
                    n_machines=('machineNumber', 'count')
                ).reset_index()

                cluster_summary['performance_score'] = (
                    cluster_summary['avg_failure_count'] +
                    cluster_summary['avg_total_downtime'] +
                    cluster_summary['avg_machine_age_years']
                )

        # Calculate % differences from best cluster (cluster 0)
        baseline_failure = cluster_summary.loc[cluster_summary['cluster'] == 0, 'avg_failure_count'].values[0]
        baseline_downtime = cluster_summary.loc[cluster_summary['cluster'] == 0, 'avg_total_downtime'].values[0]

        cluster_summary['pct_diff_failure'] = ((cluster_summary['avg_failure_count'] - baseline_failure) / max(baseline_failure, 0.001)) * 100
        cluster_summary['pct_diff_downtime'] = ((cluster_summary['avg_total_downtime'] - baseline_downtime) / max(baseline_downtime, 0.001)) * 100

        if 'performance_score' in cluster_summary.columns:
            cluster_summary = cluster_summary.drop(columns=['performance_score'])

        print("Machine Cluster analysis completed successfully")

        return {
            "aggregated_data": agg[['machineNumber', 'failure_count', 'machine_age_years', 'total_downtime_minutes', 'manufacturer', 'machine_type', 'cluster']].to_dict(orient='records'),
            "cluster_summary": cluster_summary.to_dict(orient='records')
        }

    except Exception as e:
        error_traceback = traceback.format_exc()
        print(f"Error in Machine Cluster analysis: {str(e)}")
        print(error_traceback)
        return {
            "error": str(e),
            "traceback": error_traceback
        }

if __name__ == '__main__':
    print("This module should be imported and used via the Flask API")