import os
for root, dirs, files in os.walk("."):
    for file in files:
        if file.endswith(".pyc") or file == "__pycache__":
            try:
                os.remove(os.path.join(root, file))
            except:
                pass
