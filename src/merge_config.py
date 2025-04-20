import configparser
import sys

def merge_configs(existing_file, new_file, output_file):
    existing = configparser.ConfigParser()
    new_config = configparser.ConfigParser()

    # Read configs
    existing.read(existing_file)
    new_config.read(new_file)

    # Merge missing sections and options
    for section in new_config.sections():
        if section not in existing:
            existing[section] = {}
        for option, value in new_config[section].items():
            if option not in existing[section]:
                existing[section][option] = value

    # Write merged config
    with open(output_file, 'w') as f:
        existing.write(f)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: merge_config.py <existing_config> <new_config> <output_config>")
        sys.exit(1)
    merge_configs(sys.argv[1], sys.argv[2], sys.argv[3])
