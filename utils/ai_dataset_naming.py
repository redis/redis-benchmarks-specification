#!/usr/bin/env python3
"""
AI-powered dataset naming script that uses OpenAI to generate descriptive dataset names.
Analyzes test description, preload commands, and keyspace checks to create meaningful dataset names.
"""

import os
import yaml
import glob
import json
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import openai

    OPENAI_AVAILABLE = True
    # Set your OpenAI API key here or via environment variable
    # openai.api_key = os.getenv("OPENAI_API_KEY")
except ImportError:
    OPENAI_AVAILABLE = False
    print("Warning: OpenAI package not installed. Install with: pip install openai")


def extract_full_context(content, dbconfig):
    """Extract all relevant context for dataset analysis."""
    context = {
        "test_name": content.get("name", ""),
        "description": content.get("description", ""),
        "keyspacelen": dbconfig.get("check", {}).get("keyspacelen", 0),
        "preload_details": {},
        "tested_commands": content.get("tested-commands", []),
        "tested_groups": content.get("tested-groups", []),
    }

    # Extract preload information
    if "preload_tool" in dbconfig:
        preload = dbconfig["preload_tool"]
        context["preload_details"] = {
            "type": "preload_tool",
            "tool": preload.get("tool", ""),
            "arguments": preload.get("arguments", ""),
            "run_image": preload.get("run_image", ""),
        }
    elif "init_commands" in dbconfig:
        context["preload_details"] = {
            "type": "init_commands",
            "commands": dbconfig["init_commands"],
        }

    return context


def generate_manual_description(context, dataset_name):
    """Generate a manual description for the dataset."""
    keyspacelen = context["keyspacelen"]

    # Parse dataset name components
    parts = dataset_name.split("-")

    description_parts = []

    # Key count description
    if "Mkeys" in dataset_name:
        description_parts.append(f"{keyspacelen//1000000} million keys")
    elif "Kkeys" in dataset_name:
        description_parts.append(f"{keyspacelen//1000} thousand keys")
    elif "key" in dataset_name:
        if keyspacelen == 1:
            description_parts.append("1 key")
        else:
            description_parts.append(f"{keyspacelen} keys")

    # Data type description
    if "hash" in parts:
        description_parts.append("containing Redis hash data structures")
        if any("field" in p for p in parts):
            field_part = next(p for p in parts if "field" in p)
            field_count = field_part.split("-")[0]
            description_parts.append(f"with {field_count} field(s) each")
    elif "zset" in parts:
        description_parts.append("containing Redis sorted set data structures")
        if "elements" in dataset_name:
            elem_part = next(
                p
                for p in parts
                if "elements" in p or p.endswith("M") or p.endswith("K")
            )
            description_parts.append(f"with {elem_part} elements each")
    elif "string" in parts:
        description_parts.append("containing Redis string values")
    elif "list" in parts:
        description_parts.append("containing Redis list data structures")
    elif "set" in parts:
        description_parts.append("containing Redis set data structures")
    elif "bitmap" in parts:
        description_parts.append("containing Redis bitmap data structures")

    # Size description
    size_parts = [p for p in parts if "B-size" in p or "KiB-size" in p]
    if size_parts:
        size_info = size_parts[0].replace("-size", "")
        description_parts.append(f"where each value is {size_info}")

    return ". ".join(description_parts).capitalize() + "."


def generate_dataset_name_with_ai(context):
    """Use OpenAI to generate a descriptive dataset name based on full context."""
    if not OPENAI_AVAILABLE:
        manual_name = analyze_context_manually(context)
        manual_desc = generate_manual_description(context, manual_name)
        return manual_name, manual_desc, "Generated from manual analysis"

    try:
        # Prepare detailed context for AI
        preload_info = ""
        if context["preload_details"].get("type") == "preload_tool":
            preload_info = f"""
Preload Tool: {context['preload_details'].get('tool', '')}
Arguments: {context['preload_details'].get('arguments', '')}
"""
        elif context["preload_details"].get("type") == "init_commands":
            preload_info = f"""
Init Commands: {context['preload_details'].get('commands', '')}
"""

        prompt = f"""
You are analyzing Redis benchmark tests to create precise dataset names. The dataset name should describe the exact data structure and characteristics being loaded, not the operations performed.

Test Information:
- Test Name: {context['test_name']}
- Description: {context['description']}
- Keyspace Length: {context['keyspacelen']} keys
- Tested Commands: {context['tested_commands']}
- Tested Groups: {context['tested_groups']}

{preload_info}

Requirements for dataset name:
1. Start with key count: "1Mkeys", "100Kkeys", "1key", etc.
2. Include data type: "string", "hash", "zset", "list", "set", "bitmap"
3. Include structure details:
   - For hashes: number of fields ("1-field", "5-fields", "50-fields")
   - For sorted sets: element count and score type ("1M-elements-integer", "100-elements-float")
   - For strings: just the size ("32B", "1KiB")
   - For lists: element count if relevant
4. Include size information: "32B-size", "100B-size", "1KiB-size"
5. Be specific but concise (4-6 components max)

Examples:
- "1Mkeys-hash-1-field-32B-size" (1M hash keys, 1 field each, 32 bytes per field)
- "1Mkeys-hash-5-fields-1000B-size" (1M hash keys, 5 fields each, 1000 bytes per field)
- "1key-zset-1M-elements-integer" (1 sorted set key, 1M elements, integer scores)
- "1Mkeys-string-100B-size" (1M string keys, 100 bytes each)

For dataset_description, provide a clear, concise description that:
1. States the exact number and type of keys
2. Describes the data structure details (fields, elements, etc.)
3. Mentions data sizes and any special characteristics
4. Uses consistent language and format
5. Is 1-2 sentences maximum

Example descriptions:
- "This dataset contains 1 million hash keys, each with 5 fields and each field has a data size of 1000 bytes."
- "This dataset contains 1 sorted set key with 1 million elements, each with an integer score."
- "This dataset contains 1 million string keys, each with a data size of 100 bytes."

Analyze the preload commands and description to determine:
- Exact data type being created
- Number of fields/elements
- Data size per field/element
- Any special characteristics

Respond with JSON:
{{
    "dataset_name": "precise dataset name",
    "dataset_description": "clear description of what the dataset contains",
    "reasoning": "detailed explanation of data structure analysis",
    "data_type": "string/hash/zset/list/set/bitmap",
    "key_count": "number of keys",
    "structure_details": "fields, elements, etc.",
    "size_info": "data size details"
}}
"""

        response = openai.chat.completions.create(
            model="gpt-4",  # Use GPT-4 for better analysis
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=500,
        )

        result = json.loads(response.choices[0].message.content)
        return (
            result.get("dataset_name", ""),
            result.get("dataset_description", ""),
            result.get("reasoning", ""),
        )

    except Exception as e:
        manual_name = analyze_context_manually(context)
        manual_desc = generate_manual_description(context, manual_name)
        return manual_name, manual_desc, f"AI error, used manual analysis: {e}"


def analyze_context_manually(context):
    """Manual analysis fallback when AI is not available."""
    parts = []

    # Key count
    keyspacelen = context["keyspacelen"]
    if keyspacelen >= 1000000:
        parts.append(f"{keyspacelen//1000000}Mkeys")
    elif keyspacelen >= 1000:
        parts.append(f"{keyspacelen//1000}Kkeys")
    elif keyspacelen > 1:
        parts.append(f"{keyspacelen}keys")
    elif keyspacelen == 1:
        parts.append("1key")

    # Analyze preload for data type and structure
    if context["preload_details"].get("type") == "preload_tool":
        args = context["preload_details"].get("arguments", "").lower()

        # Detect data type and structure
        if "hset" in args:
            parts.append("hash")
            # Count fields in HSET command
            field_count = args.count("field")
            if field_count > 0:
                parts.append(f"{field_count}-fields")
            else:
                # Look for pattern like "field __data__" or "field1 __data__ field2 __data__"
                import re

                field_matches = re.findall(r"field\d*", args)
                if field_matches:
                    parts.append(f"{len(field_matches)}-fields")
                else:
                    parts.append("1-fields")  # Default assumption
        elif "zadd" in args:
            parts.append("zset")
        elif "lpush" in args or "rpush" in args:
            parts.append("list")
        elif "sadd" in args:
            parts.append("set")
        elif "setbit" in args:
            parts.append("bitmap")
        else:
            parts.append("string")

        # Extract data size
        import re

        size_match = re.search(r'--data-size["\s]+(\d+)', args)
        if size_match:
            size = int(size_match.group(1))
            if size >= 1024:
                parts.append(f"{size//1024}KiB-size")
            else:
                parts.append(f"{size}B-size")

    # Use tested groups as hint if no preload info
    elif context["tested_groups"]:
        data_type = context["tested_groups"][0]
        parts.append(data_type)
        if data_type == "hash":
            parts.append("1-fields")  # Default assumption
        parts.append("32B-size")  # Default assumption

    return "-".join(parts) if len(parts) > 1 else "unknown-dataset"


def should_have_dataset_name(dbconfig):
    """Check if a file should have dataset_name based on keyspacelen."""
    keyspacelen = dbconfig.get("check", {}).get("keyspacelen", None)
    return keyspacelen is not None and keyspacelen > 0


def process_file(file_path):
    """Process a single YAML file."""
    try:
        with open(file_path, "r") as f:
            content = yaml.safe_load(f)

        if not content or "dbconfig" not in content:
            return None, "No dbconfig section"

        dbconfig = content["dbconfig"]

        # Check if file should have dataset_name
        should_have_dataset = should_have_dataset_name(dbconfig)
        has_dataset_name = "dataset_name" in dbconfig

        if not should_have_dataset:
            if has_dataset_name:
                del dbconfig["dataset_name"]
                return content, f"Removed dataset_name (keyspacelen=0)"
            else:
                return None, f"Correct (no dataset_name for keyspacelen=0)"

        # Extract full context for analysis
        context = extract_full_context(content, dbconfig)

        # Generate AI-suggested dataset name and description
        ai_dataset_name, ai_dataset_description, reasoning = (
            generate_dataset_name_with_ai(context)
        )

        if not ai_dataset_name or ai_dataset_name == "unknown-dataset":
            return None, f"Could not generate dataset name: {reasoning}"

        current_dataset_name = dbconfig.get("dataset_name", None)
        current_dataset_description = dbconfig.get("dataset_description", None)

        # Check if both name and description are already optimal
        if (
            current_dataset_name == ai_dataset_name
            and current_dataset_description == ai_dataset_description
        ):
            return None, f"Already optimal: {ai_dataset_name}"

        # Update dataset_name and dataset_description
        dbconfig["dataset_name"] = ai_dataset_name
        dbconfig["dataset_description"] = ai_dataset_description

        changes = []
        if current_dataset_name != ai_dataset_name:
            changes.append(f"name: {current_dataset_name} -> {ai_dataset_name}")
        if current_dataset_description != ai_dataset_description:
            changes.append(
                f"description: {'added' if not current_dataset_description else 'updated'}"
            )

        return content, f"Updated {', '.join(changes)} | {reasoning[:100]}..."

    except Exception as e:
        return None, f"Error: {e}"


def extract_preload_signature(dbconfig):
    """Extract a normalized signature of the preload configuration for comparison."""
    if "preload_tool" in dbconfig:
        preload = dbconfig["preload_tool"]
        # Create signature from tool and arguments (normalized)
        tool = preload.get("tool", "")
        args = preload.get("arguments", "").strip()
        # Normalize whitespace and quotes for comparison
        args = " ".join(args.split())
        return f"{tool}:{args}"
    elif "init_commands" in dbconfig:
        # For init_commands, use the commands themselves
        commands = dbconfig["init_commands"]
        if isinstance(commands, list):
            return "init:" + "|".join(str(cmd).strip() for cmd in commands)
        else:
            return f"init:{str(commands).strip()}"
    else:
        return "no_preload"


def parse_memtier_command(arguments):
    """Parse memtier_benchmark arguments to extract key characteristics."""
    import re

    parsed = {
        "data_size": None,
        "command_type": None,
        "fields": [],
        "key_pattern": None,
        "ratio": None,
        "pipeline": None,
        "connections": None,
        "threads": None,
    }

    # Extract data size
    data_size_match = re.search(r'--data-size["\s]+(\d+)', arguments)
    if data_size_match:
        parsed["data_size"] = int(data_size_match.group(1))

    # Extract command and analyze structure
    command_match = re.search(r'--command["\s]+"([^"]+)"', arguments)
    if command_match:
        command = command_match.group(1)
        parsed["command_type"] = command.split()[0].upper()  # GET, SET, HSET, etc.

        # Count fields in HSET commands
        if "HSET" in command:
            field_matches = re.findall(r"field\d*", command.lower())
            parsed["fields"] = field_matches

        # Detect ZADD patterns
        if "ZADD" in command:
            if "__key__" in command:
                parsed["score_type"] = "integer"
            else:
                parsed["score_type"] = "float"

    # Extract other parameters
    ratio_match = re.search(r'--ratio["\s]+"([^"]+)"', arguments)
    if ratio_match:
        parsed["ratio"] = ratio_match.group(1)

    pipeline_match = re.search(r'--pipeline["\s]+(\d+)', arguments)
    if pipeline_match:
        parsed["pipeline"] = int(pipeline_match.group(1))

    return parsed


def analyze_inconsistency_with_ai(dataset_name, inconsistencies, file_details):
    """Use OpenAI to analyze dataset inconsistencies with deep memtier command understanding."""
    if not OPENAI_AVAILABLE:
        return "manual_review", "OpenAI not available - manual review required"

    try:
        # Parse memtier commands for detailed analysis
        command_analysis = ""
        for detail in file_details:
            if "memtier_benchmark:" in detail["preload_sig"]:
                args = detail["preload_sig"].split("memtier_benchmark:", 1)[1]
                parsed = parse_memtier_command(args)
                command_analysis += f"\nFile: {detail['file']}\n"
                command_analysis += f"  Command Type: {parsed['command_type']}\n"
                command_analysis += f"  Data Size: {parsed['data_size']}B\n"
                command_analysis += (
                    f"  Fields: {len(parsed['fields'])} ({parsed['fields']})\n"
                )
                command_analysis += f"  Score Type: {parsed.get('score_type', 'N/A')}\n"
                command_analysis += f"  Full Command: {args}\n"

        inconsistency_details = ""
        for inc in inconsistencies:
            inconsistency_details += f"\nFile: {inc['file']}\n"
            inconsistency_details += f"Type: {inc['type']}\n"
            inconsistency_details += f"Value: {inc['value']}\n"
            if inc["type"] == "preload":
                inconsistency_details += f"Expected: {inc['expected']}\n"

        prompt = f"""
You are a Redis benchmark expert analyzing memtier_benchmark command inconsistencies. You need to determine if different memtier commands are functionally equivalent or if they create different datasets.

Dataset Name: {dataset_name}

Detailed Command Analysis:
{command_analysis}

Inconsistencies Found:
{inconsistency_details}

Your expertise in memtier_benchmark:
1. Understand that --data-size affects value size
2. HSET commands with different field counts create different data structures
3. ZADD commands with __key__ use integer scores, others use float scores
4. --ratio affects read/write mix but not dataset structure
5. --pipeline affects performance but not dataset content
6. --key-pattern affects key distribution but not individual key structure

Analysis Tasks:
1. Parse each memtier command to understand what data structure it creates
2. Determine if commands are functionally equivalent (create same dataset)
3. Check if dataset name accurately reflects the actual data structure
4. Identify which command (if any) matches the dataset name
5. Recommend corrections to align commands with dataset name

Key Questions:
- Do all commands create the same data structure?
- Does the dataset name accurately describe what's being created?
- Are differences in --pipeline, --ratio, etc. irrelevant to dataset structure?
- Should commands be standardized or should dataset be split?

Respond with JSON:
{{
    "commands_equivalent": true/false,
    "dataset_name_accurate": true/false,
    "canonical_command": "which command correctly matches dataset name",
    "issues_found": ["list of specific issues"],
    "recommendation": "detailed technical explanation",
    "action": "standardize_commands|split_datasets|fix_dataset_name|manual_review",
    "corrected_command": "suggested memtier command if standardization needed",
    "suggested_dataset_names": ["alternative names if splitting"]
}}
"""

        response = openai.chat.completions.create(
            model="gpt-4",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=1200,
        )

        result = json.loads(response.choices[0].message.content)
        return (
            result.get("action", "manual_review"),
            result.get("recommendation", ""),
            result,
        )

    except Exception as e:
        return "manual_review", f"AI analysis error: {e}", {}


def get_human_approval(dataset_name, action, recommendation, affected_files):
    """Get human approval for dataset changes."""
    print(f"\n{'='*80}")
    print(f"HUMAN APPROVAL REQUIRED")
    print(f"{'='*80}")
    print(f"Dataset: {dataset_name}")
    print(f"Recommended Action: {action}")
    print(f"AI Recommendation: {recommendation}")
    print(f"Affected Files: {', '.join(affected_files)}")
    print(f"{'='*80}")

    while True:
        response = (
            input("Do you approve this change? (y/n/s for skip): ").lower().strip()
        )
        if response in ["y", "yes"]:
            return True
        elif response in ["n", "no"]:
            return False
        elif response in ["s", "skip"]:
            return None
        else:
            print("Please enter 'y' for yes, 'n' for no, or 's' to skip")


def validate_dataset_consistency(memtier_files):
    """Validate that files with same dataset_name use identical preload commands and descriptions."""
    dataset_info_map = (
        {}
    )  # dataset_name -> {preload_sig, description, files, file_details}
    inconsistencies = []

    print("\n" + "=" * 60)
    print("DATASET CONSISTENCY VALIDATION")
    print("=" * 60)

    # Collect all dataset names and their info
    for file_path in memtier_files:
        try:
            with open(file_path, "r") as f:
                content = yaml.safe_load(f)

            if not content or "dbconfig" not in content:
                continue

            dbconfig = content["dbconfig"]
            dataset_name = dbconfig.get("dataset_name")

            if not dataset_name:
                continue

            preload_sig = extract_preload_signature(dbconfig)
            dataset_desc = dbconfig.get("dataset_description", "")
            file_name = Path(file_path).name

            if dataset_name not in dataset_info_map:
                dataset_info_map[dataset_name] = {
                    "preload_sig": preload_sig,
                    "description": dataset_desc,
                    "files": [],
                    "file_details": [],
                }

            dataset_info_map[dataset_name]["files"].append(file_name)
            dataset_info_map[dataset_name]["file_details"].append(
                {
                    "file": file_name,
                    "path": file_path,
                    "preload_sig": preload_sig,
                    "description": dataset_desc,
                }
            )

            # Check for preload inconsistency
            if dataset_info_map[dataset_name]["preload_sig"] != preload_sig:
                inconsistencies.append(
                    {
                        "dataset": dataset_name,
                        "type": "preload",
                        "file": file_name,
                        "value": preload_sig,
                        "expected": dataset_info_map[dataset_name]["preload_sig"],
                    }
                )

            # Check for description inconsistency
            expected_desc = dataset_info_map[dataset_name]["description"]
            if expected_desc and dataset_desc and expected_desc != dataset_desc:
                inconsistencies.append(
                    {
                        "dataset": dataset_name,
                        "type": "description",
                        "file": file_name,
                        "value": dataset_desc,
                        "expected": expected_desc,
                    }
                )
            elif not expected_desc and dataset_desc:
                # Update with first non-empty description found
                dataset_info_map[dataset_name]["description"] = dataset_desc

        except Exception as e:
            print(f"Error reading {file_path}: {e}")

    # Process inconsistencies with AI analysis
    if inconsistencies:
        print(f"\n⚠️  Found {len(inconsistencies)} inconsistencies")

        # Group inconsistencies by dataset
        dataset_inconsistencies = {}
        for inc in inconsistencies:
            dataset = inc["dataset"]
            if dataset not in dataset_inconsistencies:
                dataset_inconsistencies[dataset] = []
            dataset_inconsistencies[dataset].append(inc)

        # Analyze each dataset's inconsistencies
        for dataset_name, dataset_incs in dataset_inconsistencies.items():
            print(f"\nAnalyzing inconsistencies for dataset: {dataset_name}")

            # Get detailed file information for this dataset
            dataset_file_details = dataset_info_map[dataset_name]["file_details"]

            action, recommendation, ai_result = analyze_inconsistency_with_ai(
                dataset_name, dataset_incs, dataset_file_details
            )
            affected_files = list(set(inc["file"] for inc in dataset_incs))

            print(f"AI Analysis:")
            print(
                f"  Commands Equivalent: {ai_result.get('commands_equivalent', 'Unknown')}"
            )
            print(
                f"  Dataset Name Accurate: {ai_result.get('dataset_name_accurate', 'Unknown')}"
            )
            print(f"  Issues Found: {', '.join(ai_result.get('issues_found', []))}")
            print(f"  Recommendation: {action}")
            print(f"  Details: {recommendation}")

            if ai_result.get("corrected_command"):
                print(f"  Suggested Command: {ai_result['corrected_command']}")

            if action in ["standardize_commands", "split_datasets", "fix_dataset_name"]:
                approval = get_human_approval(
                    dataset_name, action, recommendation, affected_files
                )

                if approval is True:
                    print(f"✅ Approved: Will {action} for {dataset_name}")
                    # TODO: Implement the actual fixes based on action and ai_result
                elif approval is False:
                    print(f"❌ Rejected: Skipping changes for {dataset_name}")
                else:
                    print(f"⏭️  Skipped: No changes for {dataset_name}")
            else:
                print(f"ℹ️  No action required for {dataset_name}")

    # Report datasets with multiple files
    shared_datasets = {
        name: info for name, info in dataset_info_map.items() if len(info["files"]) > 1
    }

    if shared_datasets:
        print(f"\nDatasets shared by multiple files:")
        for dataset_name, info in shared_datasets.items():
            print(f"  {dataset_name}: {len(info['files'])} files")
            for file_name in info["files"]:
                print(f"    - {file_name}")
            if not info["description"]:
                print(f"    ⚠️  Missing dataset_description!")
    else:
        print("\n✅ No datasets are shared by multiple files")

    return len(shared_datasets)


def synchronize_dataset_descriptions(memtier_files):
    """Ensure files with same dataset_name have identical dataset_description."""
    print("\n" + "=" * 60)
    print("SYNCHRONIZING DATASET DESCRIPTIONS")
    print("=" * 60)

    # First pass: collect canonical descriptions for each dataset
    dataset_canonical_desc = {}
    dataset_files = {}

    for file_path in memtier_files:
        try:
            with open(file_path, "r") as f:
                content = yaml.safe_load(f)

            if not content or "dbconfig" not in content:
                continue

            dbconfig = content["dbconfig"]
            dataset_name = dbconfig.get("dataset_name")
            dataset_desc = dbconfig.get("dataset_description", "")

            if not dataset_name:
                continue

            if dataset_name not in dataset_files:
                dataset_files[dataset_name] = []
            dataset_files[dataset_name].append(file_path)

            # Use the first non-empty description as canonical
            if dataset_desc and dataset_name not in dataset_canonical_desc:
                dataset_canonical_desc[dataset_name] = dataset_desc

        except Exception as e:
            print(f"Error reading {file_path}: {e}")

    # Second pass: update files with missing or inconsistent descriptions
    updated_count = 0
    for dataset_name, files in dataset_files.items():
        if len(files) <= 1:
            continue  # Skip single-file datasets

        canonical_desc = dataset_canonical_desc.get(dataset_name, "")
        if not canonical_desc:
            print(f"⚠️  No description found for shared dataset: {dataset_name}")
            continue

        for file_path in files:
            try:
                with open(file_path, "r") as f:
                    content = yaml.safe_load(f)

                dbconfig = content["dbconfig"]
                current_desc = dbconfig.get("dataset_description", "")

                if current_desc != canonical_desc:
                    dbconfig["dataset_description"] = canonical_desc

                    with open(file_path, "w") as f:
                        yaml.dump(content, f, default_flow_style=False, sort_keys=False)

                    updated_count += 1
                    print(f"  ✓ Updated {Path(file_path).name}")

            except Exception as e:
                print(f"  ✗ Error updating {file_path}: {e}")

    print(f"\nSynchronized descriptions for {updated_count} files")
    return updated_count


def process_file_thread_safe(file_path):
    """Thread-safe wrapper for process_file that returns results with file path."""
    try:
        updated_content, message = process_file(file_path)

        if updated_content:
            # Write the file
            with open(file_path, "w") as f:
                yaml.dump(updated_content, f, default_flow_style=False, sort_keys=False)
            return file_path, "success", message, updated_content is not None
        else:
            return file_path, "no_change", message, False

    except Exception as e:
        return file_path, "error", f"Processing error: {e}", False


def main():
    """Main function to process all memtier files with threading."""
    print("AI-powered dataset naming with detailed analysis (Multi-threaded)...")

    if OPENAI_AVAILABLE:
        print("✓ OpenAI available - will use GPT-4 for analysis")
    else:
        print("⚠ OpenAI not available - using manual analysis")

    # Find all memtier files
    pattern = "redis_benchmarks_specification/test-suites/memtier*.yml"
    memtier_files = glob.glob(pattern)

    # Filter out defaults.yml if it exists
    memtier_files = [f for f in memtier_files if not f.endswith("defaults.yml")]

    print(f"\nFound {len(memtier_files)} memtier benchmark files to process")

    # First, validate existing dataset consistency
    shared_count = validate_dataset_consistency(memtier_files)

    results = {
        "updated": 0,
        "already_optimal": 0,
        "removed_dataset": 0,
        "no_dataset_needed": 0,
        "errors": 0,
    }

    # Process files with threading
    max_workers = min(8, len(memtier_files))  # Use up to 8 threads
    print(f"\nProcessing with {max_workers} threads...")

    completed = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_file = {
            executor.submit(process_file_thread_safe, file_path): file_path
            for file_path in sorted(memtier_files)
        }

        # Process completed tasks
        for future in as_completed(future_to_file):
            file_path, status, message, was_updated = future.result()
            completed += 1

            # Progress indicator
            progress = f"[{completed}/{len(memtier_files)}]"
            file_name = Path(file_path).name

            if status == "success":
                if "Updated:" in message:
                    results["updated"] += 1
                    print(f"{progress} ✓ {file_name}: {message[:80]}...")
                elif "Removed dataset_name" in message:
                    results["removed_dataset"] += 1
                    print(f"{progress} ✓ {file_name}: {message}")
                else:
                    results["updated"] += 1
                    print(f"{progress} ✓ {file_name}: Updated")

            elif status == "no_change":
                if "Already optimal:" in message:
                    results["already_optimal"] += 1
                elif "Correct (no dataset_name" in message:
                    results["no_dataset_needed"] += 1
                elif "error" in message.lower():
                    results["errors"] += 1
                    print(f"{progress} ✗ {file_name}: {message}")
                else:
                    print(f"{progress} - {file_name}: {message[:60]}...")

            elif status == "error":
                results["errors"] += 1
                print(f"{progress} ✗ {file_name}: {message}")

    # Synchronize dataset descriptions for shared datasets
    sync_count = synchronize_dataset_descriptions(memtier_files)

    # Validate consistency again after updates
    if results["updated"] > 0 or sync_count > 0:
        print(f"\n" + "=" * 60)
        print("POST-UPDATE CONSISTENCY VALIDATION")
        print("=" * 60)
        shared_count_after = validate_dataset_consistency(memtier_files)

        if shared_count_after > shared_count:
            print(
                f"⚠️  Warning: {shared_count_after - shared_count} new dataset conflicts introduced!"
            )
        else:
            print("✅ All dataset consistency checks passed!")

    # Print summary
    print(f"\n{'='*60}")
    print("PROCESSING SUMMARY")
    print(f"{'='*60}")
    print(f"Total files processed: {len(memtier_files)}")
    print(f"Dataset names updated: {results['updated']}")
    print(f"Dataset descriptions synchronized: {sync_count}")
    print(f"Already optimal: {results['already_optimal']}")
    print(f"Dataset names removed (load tests): {results['removed_dataset']}")
    print(f"No dataset needed (keyspacelen=0): {results['no_dataset_needed']}")
    print(f"Errors: {results['errors']}")

    if results["updated"] > 0:
        print(f"\n✅ Successfully updated {results['updated']} dataset names!")
    if sync_count > 0:
        print(f"✅ Synchronized descriptions for {sync_count} files!")
    if results["errors"] > 0:
        print(f"\n⚠️  {results['errors']} files had errors - please check manually")


if __name__ == "__main__":
    main()
