import os

with open("templates/module.html", "r", encoding="utf-8") as f:
    lines = f.readlines()

os.makedirs("templates/modules", exist_ok=True)

new_lines = []
current_mod = None
mod_lines = []
in_mod = False

for line in lines:
    if "{% if module.key == " in line:
        mod_key = line.split("==")[1].split("%}")[0].strip().strip("'")
        current_mod = mod_key
        in_mod = True
        mod_lines = []
        new_lines.append(f"{{% if module.key == '{mod_key}' %}}\n")
        new_lines.append(f"    {{% include 'modules/{mod_key}.html' %}}\n")
        continue

    if in_mod and "{% endif %}" in line and line.strip() == "{% endif %}":
        # check if this endif belongs to an inner if by counting
        # actually, just counting if/endif would be safer
        pass

# Since doing it line by line with nesting is hard, let me just write a basic stack-based parser
stack = []
extracted = {}
new_lines = []
current_mod = None
mod_lines = []
in_main_mod = False

for line in lines:
    if "{% if module.key == " in line and len(stack) == 0:
        current_mod = line.split("==")[1].split("%}")[0].strip().strip("'")
        in_main_mod = True
        mod_lines = []
        new_lines.append(f"{{% if module.key == '{current_mod}' %}}\n")
        new_lines.append(f"    {{% include 'modules/{current_mod}.html' %}}\n")
        # Don't add line to new_lines except the include
    elif in_main_mod:
        if "{% if " in line or "{% for " in line:
            stack.append("open")
        if "{% endif %}" in line or "{% endfor %}" in line:
            if len(stack) > 0:
                stack.pop()
            else:
                # End of module block
                extracted[current_mod] = mod_lines
                new_lines.append(line)
                in_main_mod = False
                current_mod = None
                continue
        mod_lines.append(line)
    else:
        new_lines.append(line)

for mod, content in extracted.items():
    with open(f"templates/modules/{mod}.html", "w", encoding="utf-8") as f:
        f.writelines(content)

with open("templates/module.html", "w", encoding="utf-8") as f:
    f.writelines(new_lines)

print("Split completed.")
