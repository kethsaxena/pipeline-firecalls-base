from pathlib import Path
import pandas as pd
import plotly.express as px
import re
import io

# -----------------------------
# Step 1: Search for README.md in parent directory
# -----------------------------
parent_dir = Path(__file__).resolve().parent.parent  # go one level up
readme_file = None

for f in parent_dir.iterdir():
    if f.name.lower() == "readme.md":
        readme_file = f
        break

if readme_file is None:
    raise FileNotFoundError("README.md not found in parent directory.")

print(f"Found README.md at: {readme_file}")

# -----------------------------
# Step 2: Read README.md and extract TODO table
# -----------------------------
with open(readme_file, "r", encoding="utf-8") as f:
    lines = f.readlines()

# Find table header (case-insensitive)
start_idx = None
for i, line in enumerate(lines):
    if re.match(r"\| *Task *\| *Status *\| *Start *\| *End *\|", line, re.IGNORECASE):
        start_idx = i
        break

if start_idx is None:
    raise ValueError("TODO table header not found in README.md.")

# Find end of table (empty line or end of file)
end_idx = None
for i in range(start_idx + 1, len(lines)):
    if lines[i].strip() == "":
        end_idx = i
        break
if end_idx is None:
    end_idx = len(lines)

table_lines = lines[start_idx:end_idx]
table_text = "".join(table_lines)

# -----------------------------
# Step 3: Convert Markdown table to DataFrame
# -----------------------------
df = pd.read_csv(io.StringIO(table_text), sep="|", skipinitialspace=True)

# Clean column names and string values
df.columns = [c.strip() for c in df.columns]
df = df.astype(str).apply(lambda col: col.str.strip())

# Remove separator lines (like '------') and empty rows
df = df[~df['Task'].str.contains('^-+$', regex=True)]
df = df[df['Task'].str.strip() != '']

# Convert Start and End columns to datetime
df['Start'] = pd.to_datetime(df['Start'], errors='coerce')
df['End'] = pd.to_datetime(df['End'], errors='coerce')

# Drop rows with invalid dates
df = df.dropna(subset=['Start', 'End'])

# -----------------------------
# Step 4: Generate Gantt chart
# -----------------------------
fig = px.timeline(
    df,
    x_start="Start",
    x_end="End",
    y="Task",
    color="Status",
    title="Project Gantt Chart",
)

fig.update_yaxes(autorange="reversed")  # Top-down order

# -----------------------------
# Step 5: Show and save
# -----------------------------
fig.show()
fig.write_html("gantt_chart.html")
fig.write_image("gantt_chart.png")  # Requires kaleido

print("Gantt chart generated and saved as gantt_chart.html and gantt_chart.png")
