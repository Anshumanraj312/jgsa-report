# consolidate_scripts.py
import os
import glob

OUTPUT_FILENAME = "consolidated_scripts.txt"
EXCLUDE_DIRS = ['.venv', '__pycache__', '.git', '.vscode'] # Add other directories to exclude
EXCLUDE_FILES = ['consolidate_scripts.py', OUTPUT_FILENAME] # Exclude self and output

def consolidate():
    """Finds all .py files, excluding specified ones, and concatenates them."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    all_py_files = []

    print(f"Scanning for .py files in: {script_dir}")

    # Walk through directory structure
    for root, dirs, files in os.walk(script_dir, topdown=True):
        # Modify dirs in-place to prevent descending into excluded ones
        dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]

        for file in files:
            if file.endswith(".py") and file not in EXCLUDE_FILES:
                full_path = os.path.join(root, file)
                all_py_files.append(full_path)

    print(f"Found {len(all_py_files)} Python files to consolidate.")

    if not all_py_files:
        print("No Python files found to consolidate.")
        return

    all_py_files.sort() # Optional: sort files alphabetically

    try:
        with open(os.path.join(script_dir, OUTPUT_FILENAME), 'w', encoding='utf-8') as outfile:
            for filepath in all_py_files:
                relative_path = os.path.relpath(filepath, script_dir)
                print(f"  Adding: {relative_path}")
                # Add header comment
                outfile.write(f"\n{'='*10} File: {relative_path} {'='*10}\n\n")
                try:
                    with open(filepath, 'r', encoding='utf-8') as infile:
                        outfile.write(infile.read())
                    outfile.write("\n\n") # Add spacing between files
                except Exception as e:
                    print(f"    Error reading file {relative_path}: {e}")
                    outfile.write(f"\n--- ERROR READING FILE: {relative_path} ---\n\n")

        print(f"\nSuccessfully consolidated scripts into: {OUTPUT_FILENAME}")

    except IOError as e:
        print(f"\nError writing output file {OUTPUT_FILENAME}: {e}")
    except Exception as e:
         print(f"\nAn unexpected error occurred: {e}")


if __name__ == "__main__":
    consolidate()