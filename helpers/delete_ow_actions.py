import subprocess

actions_output = subprocess.run(["wsk", "-i", "action", "list", "--limit", "150"], capture_output=True, text=True)
actions = actions_output.stdout.split("\n")

for act in actions:
    try:
        curr = act.split(" ")[0]
        print("Deleting: ", curr)
        subprocess.run(["wsk", "-i", "action", "delete", curr])
    except:
        pass
