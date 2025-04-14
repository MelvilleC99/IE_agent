import json
import pandas as pd
import numpy as np

# --- Keep your helper functions like safe_pct ---
def safe_pct(current, best):
    if best > 0:
        return ((current - best) / best) * 100
    else:
        return None

# --- Keep your group_summary function ---
def group_summary(df, group_field_1, group_field_2=None):
    if group_field_2:
        grouped = df.groupby([group_field_1, group_field_2]).agg(
            avgRepairTime_min=("repairTime_min", "mean"),
            avgResponseTime_min=("responseTime_min", "mean"),
            count=("repairTime_min", "count")
        ).reset_index()
    else:
        grouped = df.groupby([group_field_1]).agg(
            avgRepairTime_min=("repairTime_min", "mean"), 
            avgResponseTime_min=("responseTime_min", "mean"),
            count=("repairTime_min", "count")
        ).reset_index()

    if grouped.empty:
        return {}

    best_row = grouped.loc[grouped["avgRepairTime_min"].idxmin()]
    best_val = best_row["avgRepairTime_min"]
    grouped["pct_worse_than_best"] = grouped["avgRepairTime_min"].apply(lambda x: safe_pct(x, best_val))

    result = {
        "stats": grouped.to_dict(orient="records"),
        "best": {k:v for k,v in best_row.items()}
    }
    return result

def convert_to_native_types(obj):
    if isinstance(obj, (np.int_, np.intc, np.intp, np.int8, np.int16, np.int32,
                       np.int64, np.uint8, np.uint16, np.uint32, np.uint64)):
        return int(obj)
    elif isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
        return float(obj)
    elif isinstance(obj, dict):
        return {k: convert_to_native_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_native_types(item) for item in obj]
    return obj

def run_mechanic_analysis(data_source_path: str = "/Users/melville/Documents/Industrial_Engineering_Agent/maintenance_data.json") -> dict:
    """
    Reads maintenance data, performs analysis, and returns the summary dictionary.
    """
    print(f"ANALYZER: Reading data from {data_source_path}")
    try:
        with open(data_source_path, "r") as f:
            data = json.load(f)
        df = pd.DataFrame(data)
    except Exception as e:
        print(f"ANALYZER: Error reading data file: {e}")
        return {} # Return empty dict on error

    # --- Your existing pandas conversion and calculation logic ---
    conversion_factor = 60000
    df["repairTime_min"] = pd.to_numeric(df["totalRepairTime"], errors="coerce").fillna(0) / conversion_factor
    df["responseTime_min"] = pd.to_numeric(df["totalResponseTime"], errors="coerce").fillna(0) / conversion_factor

    # --- Your existing aggregation logic ---
    overall_stats = df.groupby("mechanicName").agg(
        avgRepairTime_min=("repairTime_min", "mean"),
        avgResponseTime_min=("responseTime_min", "mean")
    ).reset_index()

    if overall_stats.empty:
        print("ANALYZER: No data found for overall stats.")
        return {}

    best_row = overall_stats.loc[overall_stats["avgRepairTime_min"].idxmin()]
    best_val = best_row["avgRepairTime_min"]
    overall_stats["pct_worse_than_best"] = overall_stats["avgRepairTime_min"].apply(lambda x: safe_pct(x, best_val))

    overall_summary = {
        "mechanic_stats": overall_stats.to_dict(orient="records"),
        "best": {
            "mechanicName": best_row["mechanicName"],
            "avgRepairTime_min": best_row["avgRepairTime_min"],
            "avgResponseTime_min": best_row["avgResponseTime_min"]
        }
    }

    machine_summary = group_summary(df, "machineType")
    reason_summary = group_summary(df, "reason")
    machine_reason_summary = group_summary(df, "machineType", "reason")
    # --- End of your existing logic ---

    final_summary = {
        "overall": overall_summary,
        "byMachineType": machine_summary,
        "byFailureReason": reason_summary,
        "byMachineAndReason": machine_reason_summary
    }

    print("ANALYZER: Analysis complete.")
    # --- Return the dictionary instead of writing to file here ---
    # output_path = "/Users/melville/Documents/Industrial_Engineering_Agent/summary_clean.json"
    # with open(output_path, "w") as f:
    #     json.dump(final_summary, f, indent=2)
    # print("Summary data written to", output_path)
    return final_summary

# You can add a main block for testing this script directly
if __name__ == '__main__':
    summary = run_mechanic_analysis()
    if summary:
        print("\n--- Generated Summary (sample) ---")
        print(json.dumps(summary.get('overall', {}), indent=2))
        # Save to test_summary_output.json
        summary = convert_to_native_types(summary)
        with open("test_summary_output.json", "w") as f:
            json.dump(summary, f, indent=2)
        print("Summary saved to test_summary_output.json")