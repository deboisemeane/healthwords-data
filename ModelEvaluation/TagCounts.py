"""Script to plot the counts for each tag in the collection of conversations wtih potential issues."""

from collections import Counter

# Raw text data
data = '''User Value
Articles, User Value
Medication, User Value
Feel/Flow, User Value
Feel/Flow, Conversation Structure
Self Awareness
Medication
Conversation Structure

Healthcare Provider
Feel/Flow
Conversation Structure
Medical Opinion
Language Understanding
Feel/Flow, User Value
Conversation Structure, Feel/Flow
Language Understanding
Medical Opinion, Urgency
Self Awareness
User Value, Language Understanding
Language Understanding
Medication, Language Understanding
Articles
Urgency, Medical Opinion
User Value
Feel/Flow
Feel/Flow, Language Understanding'''

# Split into lines and then split tags by comma
tags = [tag.strip() for line in data.split('\n') for tag in line.split(',') if tag]

# Count tag occurrences
tag_counts = Counter(tags)

# Print counts
for tag, count in tag_counts.items():
    print(f"{tag}: {count}")

# Optional: Quick plot
import matplotlib.pyplot as plt

plt.figure(figsize=(10, 6))
plt.bar(tag_counts.keys(), tag_counts.values(), color="skyblue")
plt.xticks(rotation=45, ha="right")
plt.title("Tag Counts")
plt.ylabel("Count")
plt.tight_layout()
plt.show()
