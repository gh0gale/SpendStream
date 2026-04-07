import os

file_path = 'etl.py'
with open(file_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

new_lines = []
for line in lines:
    # Handle the existing Telecom / Utilities section and add the new rules
    if '(r"(?i)\\bjio\\b"' in line or '(r"(?i)\x08jio\x08"' in line:
        new_lines.append('    (r"(?i)\\bjio\\b",             "Jio"),\n')
    elif '(r"(?i)airtel"' in line:
        new_lines.append('    (r"(?i)airtel",              "Airtel"),\n')
    elif '(r"(?i)bsnl"' in line:
        new_lines.append('    (r"(?i)bsnl",                "BSNL"),\n')
    elif '(r"(?i)vi\\b|vodafone|idea"' in line or '(r"(?i)vi\x08|vodafone|idea"' in line:
        new_lines.append('    (r"(?i)\\bvilpremum\\b",       "Vi"),           # Vi premium recharge UPI handle\n')
        new_lines.append('    (r"(?i)vi\\b|vodafone|idea",  "Vi"),\n')
    elif '(r"(?i)gas\\s*(supply|bill)|mahanagar\\s*gas|indraprastha"' in line:
        new_lines.append(line)
        new_lines.append('\n')
        new_lines.append('    # Transport - Indian Railways UTS (local unreserved train tickets)\n')
        new_lines.append('    (r"(?i)indian\\s*railways?\\s*(uts)?",  "Indian Railways UTS"),\n')
        new_lines.append('    (r"(?i)\\biruts\\b",                    "Indian Railways UTS"),\n')
    else:
        new_lines.append(line)

with open(file_path, 'w', encoding='utf-8') as f:
    f.writelines(new_lines)

print("SUCCESS: etl.py updated with robust replacement")
