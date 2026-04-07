import re

with open('etl.py', 'r', encoding='utf-8') as f:
    content = f.read()

lines = content.splitlines(keepends=True)
for i, l in enumerate(lines[108:123], start=109):
    print(f'{i}: {repr(l)}')
