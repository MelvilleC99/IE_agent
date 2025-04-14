import json
import pandas as pd

def safe_pct(current, best):
    """Return percentage worse than best safely."""
    if best > 0:
        return ((current - best) / best) * 100
    else:
        return None

# Set the conversion factor:
# Use 60000 if your maintenance times are in milliseconds,
# or 60 if they are in seconds.
conversion_factor = 60000  # change to 60 if your data is in seconds

# Load data from your local file path
with open("/Users/melville/Documents/Industrial_Engineering_Agent/maintenance_data.json", "r") as f:
    data = json.load(f)
df = pd.DataFrame(data)

# Convert times to minutes using the conversion_factor
df["repairTime_min"] = pd.to_numeric(df["totalRepairTime"], errors="coerce").fillna(0) / conversion_factor
df["responseTime_min"] = pd.to_numeric(df["totalResponseTime"], errors="coerce").fillna(0) / conversion_factor

# 1. Overall mechanic performance (averages, not totals)
overall_stats = df.groupby("mechanicName").agg(
    avgRepairTime_min=("repairTime_min", "mean"),
    avgResponseTime_min=("responseTime_min", "mean")
).reset_index()

# Determine the best (lowest average repair time) for overall comparison
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

# 2. Function to produce grouped (nested) summaries
def group_summary(df, group_field_1, group_field_2=None):
    """
    Create a nested summary.
    If group_field_2 is provided, then output is nested as:
      { str(group_field_1): { str(group_field_2): entry } }.
    Otherwise, output is a flat dictionary keyed by str(group_field_1).
    """
    result = {}
    grouped = df.groupby([group_field_1] if group_field_2 is None else [group_field_1, group_field_2])
    for keys, group in grouped:
        if group_field_2 is None:
            group_key = str(keys)
            sub_key = None
        else:
            group_key = str(keys[0])
            sub_key = str(keys[1])
        
        # Compute average repair and response times per mechanic for the group
        mech_stats = group.groupby("mechanicName").agg(
            avgRepairTime_min=("repairTime_min", "mean"),
            avgResponseTime_min=("responseTime_min", "mean")
        ).reset_index()
        
        if not mech_stats.empty:
            best = mech_stats.loc[mech_stats["avgRepairTime_min"].idxmin()]
            best_val = best["avgRepairTime_min"]
            mech_stats["pct_worse_than_best"] = mech_stats["avgRepairTime_min"].apply(lambda x: safe_pct(x, best_val))
            
            entry = {
                "mechanic_stats": mech_stats.to_dict(orient="records"),
                "best": {
                    "mechanicName": best["mechanicName"],
                    "avgRepairTime_min": best["avgRepairTime_min"],
                    "avgResponseTime_min": best["avgResponseTime_min"]
                }
            }
            
            if group_field_2:
                if group_key not in result:
                    result[group_key] = {}
                result[group_key][sub_key] = entry
            else:
                result[group_key] = entry
    return result

# 3. Nested summaries by various groupings
machine_summary = group_summary(df, "machineType")
reason_summary = group_summary(df, "reason")
machine_reason_summary = group_summary(df, "machineType", "reason")

# 4. Combine all summaries into a final JSON structure
final_summary = {
    "overall": overall_summary,
    "byMachineType": machine_summary,
    "byFailureReason": reason_summary,
    "byMachineAndReason": machine_reason_summary
}

# Output final summary to JSON file
output_path = "/Users/melville/Documents/Industrial_Engineering_Agent/summary_clean.json"
with open(output_path, "w") as f:
    json.dump(final_summary, f, indent=2)

print("Summary data written to", output_path)
