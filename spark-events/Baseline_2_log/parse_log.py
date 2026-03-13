import json

file_path = "events_baseline.json"

# Count different event types
event_counts = {}
line_count = 0

with open(file_path, "r") as f:
    for line in f:
        line_count += 1
        try:
            event = json.loads(line)
            event_type = event.get("Event", "Unknown")
            event_counts[event_type] = event_counts.get(event_type, 0) + 1
        except:
            print(f"Error on line {line_count}")
            continue

print(f"Total lines: {line_count}")
print("\nEvent counts:")
for event_type, count in sorted(event_counts.items()):
    print(f"  {event_type}: {count}")